// handler.go —— 协议处理器：发起匹配、操作透传、结果上报。
//
// 服务端只做三件事（见包注释）：通知发起、消息透传、接收结果。
// 战斗演算在客户端，这里不做任何伤害/胜负校验。
package battle

import (
	"fmt"

	"game_study/internal/common/errcode"
	"game_study/internal/game/module"

	gamev1 "game_study/pkg/pb/game/v1"
	"google.golang.org/protobuf/proto"
)

// onMatch 发起匹配：入队 → 凑够人建房 → 双方下发 BattleStartNotify。
func (m *Module) onMatch(ctx *module.Context, req *gamev1.MatchReq, resp *gamev1.MatchResp) error {
	// mode 空串兜底为 arena：避免客户端不传模式时把队列打散到 "" 键下永远凑不到人
	mode := req.Mode
	if mode == "" {
		mode = "arena"
	}
	// playerID 一律取 Actor 内的会话身份，不信协议体携带的 id（防伪造他人匹配）
	playerID := ctx.State.PlayerID()

	*resp = gamev1.MatchResp{Mode: mode}

	// 跨服节点全挂时直接拒绝：不给玩家「进了队列却永远匹配不到」的希望
	if !m.cross.Available() {
		return ctx.FailCode(resp, errcode.ErrBattleModeClosed)
	}

	m.mu.Lock()
	if m.queued[playerID] {
		m.mu.Unlock()
		return ctx.FailCode(resp, errcode.ErrBattleAlreadyIn)
	}
	if s, inBattle := m.sessions[playerID]; inBattle {
		if !s.Settled {
			m.mu.Unlock()
			return ctx.FailCode(resp, errcode.ErrBattleAlreadyIn)
		}
		// 已结算但会话还未回收（结算到清理之间有短暂窗口）：
		// 立即清掉残留，允许重新匹配 —— 否则玩家打完一场要等 2s 才能再排。
		delete(m.sessions, playerID)
	}

	// 中心服可用 → 走中心服匹配（P8 主路径）；否则降级为本服匹配（P13 直连）
	if m.center != nil && m.center.Available() {
		// 先解锁再调中心服：Match 内部走 RPC，可能慢，绝不能持锁等待
		m.mu.Unlock()
		// demo 期无独立 Elo 存储，用等级折算近似分（等级 ×100）
		elo := ctx.State.Profile().Level * 100
		if err := m.center.Match(playerID, mode, elo, m.buildSnapshot(playerID)); err != nil {
			m.logger.Warn("center match failed", "player_id", playerID, "err", err)
			return ctx.FailCode(resp, errcode.ErrBattleModeClosed)
		}
		resp.Code = int32(errcode.OK)
		resp.Queued = 1 // 中心服匹配池不回传精确人数，客户端按「已入队」处理
		return nil
	}

	m.queue[mode] = append(m.queue[mode], playerID)
	m.queued[playerID] = true
	// queued 在锁内取快照：解锁后的回包计算不能再摸共享 map（数据竞争）
	queued := len(m.queue[mode])

	// 凑够人数 → 出队建房
	var pair []int64
	if queued >= matchPlayers {
		pair = append(pair, m.queue[mode][:matchPlayers]...)
		m.queue[mode] = m.queue[mode][matchPlayers:]
		for _, p := range pair {
			delete(m.queued, p)
		}
		// queue 与 queued 两个结构必须同步变更，否则「防重复入队」防护失效
	}
	m.mu.Unlock()

	resp.Code = int32(errcode.OK)
	// 凑满时两人出队后剩 0；未凑满则把当前排队人数透给客户端做「匹配中 n 人」
	resp.Queued = int32(queued - len(pair))
	// 回包由 module.Typed 统一完成（这里再回一次会多发一条响应，
	// 多出来的那条会被下一次请求当成自己的响应读走）。

	// 建房放在回包之后：先让客户端渲染「匹配中」，再同步建房（本地路径量小，
	// 阻塞当前 Actor 可接受；建房内部还要 RPC CrossServer，可能秒级返回）
	if len(pair) == matchPlayers {
		m.startMatch(mode, pair)
	}
	return nil
}

// onOpReport 客户端操作上报 → 原样透传到房间（不解析 body）。
func (m *Module) onOpReport(ctx *module.Context, _ uint32, body []byte) error {
	var req gamev1.BattleOpReport
	if err := proto.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("decode BattleOpReport: %w", err)
	}
	playerID := ctx.State.PlayerID()

	m.mu.Lock()
	s := m.sessions[playerID]
	m.mu.Unlock()
	// Settled 之后到达的操作同样丢弃：房间即将销毁，透传已无意义
	if s == nil || s.RoomID != req.RoomId || s.Settled {
		return nil // 非当前战斗的消息，静默丢弃（客户端重发/延迟）
	}

	// 透传：body 原样，不解析
	if err := m.cross.SendOp(req.RoomId, playerID, req.MsgId, req.Body); err != nil {
		m.logger.Debug("relay op failed", "room_id", req.RoomId, "err", err)
	}
	return nil
}

// onResultReport 客户端上报结论：令牌校验 + 幂等 → 转发跨服 → 回执。
func (m *Module) onResultReport(ctx *module.Context, req *gamev1.BattleResultReport, resp *gamev1.BattleSettleNotify) error {
	playerID := ctx.State.PlayerID()

	*resp = gamev1.BattleSettleNotify{RoomId: req.RoomId}

	m.mu.Lock()
	s := m.sessions[playerID]
	m.mu.Unlock()

	// 房间不匹配视同令牌无效：会话里的 room 是建房时绑定的，
	// 客户端改报别的房间，等价于拿着别房的信物来冒领
	if s == nil || s.RoomID != req.RoomId {
		return ctx.FailCode(resp, errcode.ErrBattleTokenInvalid) // 房间不匹配视同令牌无效
	}
	// 令牌逐个比对：防玩家 A 用自己房间的令牌给 B 的房间冒领结算
	if req.BattleToken != s.Token {
		return ctx.FailCode(resp, errcode.ErrBattleTokenInvalid)
	}
	// 已结算 → 6003：客户端重发/双端重复上报的幂等回执
	if s.Settled {
		return ctx.FailCode(resp, errcode.ErrResultReported)
	}

	// 幂等标记（先在内存置位，避免并发重复上报）
	// 先占坑再转发：即使下面转发失败也不接受第二次上报——
	// 简化取舍：宁可丢一次转发（CrossServer 侧还有兜底结算），也不能重复计分
	m.mu.Lock()
	s.Settled = true
	m.mu.Unlock()

	// 转发给 CrossServer（CrossServer 收齐双方结果后统一结算回推）
	if err := m.cross.SendResult(req.RoomId, playerID, s.Token, req.Result, req.Rewards); err != nil {
		// 转发失败只告警、不回错误码：客户端侧结果已显示，
		// 权威奖励由 CrossServer 收齐双方后回推 5011，最终一致
		m.logger.Warn("send result to cross failed", "room_id", req.RoomId, "err", err)
	}

	// 即时回执（权威奖励由结算推送 5011 再下发一次）
	resp.Code = int32(errcode.OK)
	resp.Rewards = req.Rewards
	return nil
}
