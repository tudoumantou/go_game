// handler.go —— 客户端协议：报名/取消、界面查询。
//
// 与 callback.go 的运行上下文完全不同：这里的处理器在玩家 Actor 内执行
// （同一玩家串行），但读写的是进程级的 Registry，所以照样要经过它的锁。
package crossplay

import (
	"context"
	"strconv"

	"game_study/internal/common/errcode"
	"game_study/internal/game/module"
	gamev1 "game_study/pkg/pb/game/v1"
	internalv2 "game_study/pkg/pb/internalv2"
	"game_study/pkg/writebehind"
)

// onEnroll 报名 / 取消报名。
//
// 关键顺序：**先问中心服，成功才写本地**。反过来的话，中心服拒绝（满员、
// 非报名期、内部错误）时本地已经留了一行，玩家会一直显示「已报名」却
// 永远等不到分组。
//
// 幂等由中心服保证（重复报名返回成功、取消不存在的报名返回成功），
// 本地只需要照结果写/删，不做额外判断——两处判断一定会有一天不一致。
func (m *Module) onEnroll(c *module.Ctx[module.NoState], req *gamev1.CrossPlayEnrollReq) error {
	resp := &gamev1.CrossPlayEnrollResp{}
	now := m.nowUnix()

	// 阶段判定在本地做一次是「提前拒绝」，不是权威：真正的裁决在中心服。
	// 好处是活动期玩家点报名能立刻得到反馈，不用等一次 RPC 超时。
	if !req.GetCancel() && !m.cfg.inEnroll(now) {
		resp.Code = int32(errcode.ErrCrossNotEnroll)
		return c.Reply(resp)
	}
	if m.center == nil || !m.center.Ready() {
		// 中心服不可用用通用系统错误：这不是「玩法层面的拒绝」，
		// 玩家重试可能就成功了，不值得为他造一个专门的码。
		resp.Code = int32(errcode.ErrSystem)
		return c.Reply(resp)
	}

	playerID := c.State.PlayerID()
	creq := &internalv2.CrossEnrollReq{
		PlayerId: playerID,
		ServerId: m.serverID,
		Cancel:   req.GetCancel(),
	}
	if !req.GetCancel() {
		creq.Power = powerOf(c.State)
		creq.Info = infoOf(c.State.Profile(), creq.Power)
	}

	// 用 context.Background() 而不是请求 ctx：处理器返回后请求 ctx 即取消，
	// 会把这次 RPC 连同中心服的写一起掐断（中心服那边可能已经写成功了）。
	res, err := m.center.CrossEnroll(context.Background(), creq)
	if err != nil {
		m.logger.Warn("cross enroll rpc failed", "player", playerID, "err", err)
		resp.Code = int32(errcode.ErrSystem)
		return c.Reply(resp)
	}
	if res.GetCode() != 0 {
		resp.Code = res.GetCode()
		return c.Reply(resp)
	}

	if req.GetCancel() {
		m.reg.Cancel(playerID)
		if m.repo != nil && m.repo.Ready() {
			if err := m.repo.DeleteEnroll(context.Background(), playerID); err != nil {
				// 本地删失败不回错误码：中心服已经删了，玩家就是「没报名」。
				// 本地残留一行会在下次报名时被 UPSERT 覆盖，或切期被清掉。
				m.logger.Warn("delete cross_enroll failed", "player", playerID, "err", err)
			}
		}
		return c.Reply(resp)
	}

	e := &Entry{
		PlayerID:  playerID,
		ServerID:  m.serverID,
		Power:     creq.Power,
		Score:     InitialScore,
		UpdatedAt: now,
		Info:      toKVs(creq.Info),
	}
	m.reg.Enroll(e)

	if m.repo != nil && m.repo.Ready() {
		row := enrollRow(e, now)
		if err := m.repo.UpsertEnroll(context.Background(), row); err != nil {
			// 本地写失败同样不回错误码：中心服已经记下了，玩家确实报上了。
			// 本地这一行只影响「重启后界面还能显示已报名」，丢了由中心服的
			// 分组快照补回来。
			m.logger.Warn("upsert cross_enroll failed", "player", playerID, "err", err)
		}
	}
	return c.Reply(resp)
}

// onInfo 玩法界面：阶段 + 自己的报名/分组状态。
//
// 全部读本服内存，不经过中心服——1000 人同时点开界面就是 1000 次查询，
// 中心服不该承担玩家读流量（它连匹配和中转都在同一个进程里跑）。
func (m *Module) onInfo(c *module.Ctx[module.NoState], _ *gamev1.CrossPlayInfoReq) error {
	resp := &gamev1.CrossPlayInfoResp{}
	now := m.nowUnix()
	phase, _, end := m.cfg.PhaseAt(now)
	b := m.cfg.boundsOf(now)

	resp.Phase = int32(phase)
	resp.PhaseEndAt = end
	resp.ActiveAt = b.activeStart

	playerID := c.State.PlayerID()
	resp.Enrolled = m.reg.IsEnrolled(playerID)

	// 已分组才有积分与名次：分组前显示 0 而不是 1000，避免玩家以为
	// 自己已经在榜上了。
	if e, ok := m.reg.Player(playerID); ok && e.GroupID > 0 {
		resp.GroupId = e.GroupID
		resp.Score = e.Score
		resp.Rank, resp.GroupSize = m.reg.RankOf(playerID)
	}
	return c.Reply(resp)
}

// powerOf 由战斗属性折算战力，分组排序的唯一依据。
//
// 项目里没有现成的「玩家战力」字段：player 表只有 level/exp/coin，
// player_attr 是 hp/mp/atk/def/spd；worldmap 里那套战力带权重配置和
// ±10% 扰动，语义是「随机决定战斗胜负」，拿来当排序依据会不可复现
// （同一批人两次分组结果不同，对账无从做起）。
//
// 所以这是一个**可替换的纯函数**而不是写死在业务里：将来真有战力字段了，
// 只改这里，表结构、协议、分组算法都不用动。权重是先占位的默认值，
// 上线前要用运营口径定一次。
//
// 用 GetAttr 而不是 State.Attr()：前者是「基础值 + 各模块加成」的最终值
// （装备、伙伴都算进去），玩家换装后战力立刻反映出来——这才是玩家
// 在界面上看到的那个数。
func powerOf(st module.PlayerState) int64 {
	p := st.GetAttr("atk")*2 + st.GetAttr("def") + st.GetAttr("spd") + st.GetAttr("hp")/10
	if p <= 0 {
		// 新角色属性全 0 时给个非零值：0 战力会排在所有人最后，
		// 而且分组算法里「战力相同按 player_id 升序」的分支会把它和
		// 别的 0 战力归成一片。
		return 1
	}
	return p
}

// infoOf 组装展示用的基本信息。
//
// 服务端不解释这些 key 的含义，展示层认不出的直接忽略——这样加一项展示
// 只需要改上报端，不动协议、不动表。
func infoOf(p writebehind.Profile, power int64) []*internalv2.CrossInfoItem {
	return []*internalv2.CrossInfoItem{
		{K: "name", V: p.Name},
		{K: "level", V: strconv.FormatInt(int64(p.Level), 10)},
		{K: "power", V: strconv.FormatInt(power, 10)},
	}
}

// toKVs proto 的 KV 列表 → 领域结构。
func toKVs(items []*internalv2.CrossInfoItem) []KV {
	if len(items) == 0 {
		return nil
	}
	out := make([]KV, 0, len(items))
	for _, it := range items {
		if it == nil || it.GetK() == "" {
			continue
		}
		out = append(out, KV{K: it.GetK(), V: it.GetV()})
	}
	return out
}
