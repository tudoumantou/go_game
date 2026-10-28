// internal/game/server_adapters.go —— 层间适配器
//
// 每个类型都在做「把某个包的接口翻译成另一个包期望的形状」：
// chatMessenger 把 Server 的能力翻译成 chat 模块的 Messenger；
// centerHandler 把中心服回调翻译成 Server 的动作；rankQuerier 把中心服
// 客户端翻译成 rank 模块的 Querier。
//
// 放在一起的理由：改动它们时你关心的永远是「两侧接口是否对齐」，
// 而不是「Server 的生命周期怎么走」——两件事分文件，读的时候不互相干扰。
package game

import (
	"context"
	"fmt"

	gamecenter "game_study/internal/game/center"
	"game_study/internal/game/module/battle"
	rankmodule "game_study/internal/game/module/rank"
	"game_study/internal/game/player"
	seatpkg "game_study/internal/game/seat"
	"game_study/pkg/actor"
	"game_study/pkg/codec"
	"game_study/pkg/log"
	pkgmq "game_study/pkg/mq"
	pkgseat "game_study/pkg/seat"

	gamev1 "game_study/pkg/pb/game/v1"
	internalv2 "game_study/pkg/pb/internalv2"
	"google.golang.org/protobuf/proto"
)

type chatMessenger struct {
	srv *Server
}

// SelfNode 实现 chat.Messenger。
func (m chatMessenger) SelfNode() string { return m.srv.cfg.Server.Name }

// OnlineNode 实现 chat.Messenger。
//
// 无 Redis 时退化为「本地 PlayerManager 有没有这个 Actor」——本地判断的
// 精度低于全局 presence，但保证无依赖环境仍可运行。
func (m chatMessenger) OnlineNode(ctx context.Context, playerID int64) (string, bool) {
	if m.srv.presence == nil {
		// 无 Redis：退化为本地在线判断
		_, ok := m.srv.players.Get(playerID)
		return m.srv.cfg.Server.Name, ok
	}
	return m.srv.presence.IsOnline(ctx, playerID)
}

// Push 实现 chat.Messenger。
//
// 优先走 Actor（保证与玩家自身消息的处理顺序一致）；Actor 尚未创建时
// （玩家只登录、还没发过业务包）直接走会话下行 —— 推送是纯下行，不需要业务状态。
//
// Resolve 而不是 Get（P0）：玩家可能在线在**别的游戏节点**——本地表
// 未命中时查在线归属表，拿到远程 PID 后 Send 自动经中心服中转投递。
// 跨节点推送上线前，这里对异地在线玩家只能落到 sendToPlayer（本节点
// 会话下行），消息实际丢失。
//
// 顺序敏感的根源：聊天回包与玩家自己的业务回包若走两条通道，客户端可能
// 看到「先收到回复、后收到请求响应」的乱序。
func (m chatMessenger) Push(ctx context.Context, playerID int64, msgID uint32, body []byte) bool {
	if pid, ok := m.srv.players.Resolve(ctx, playerID); ok {
		if err := m.srv.actorSys.Send(pid, &player.PushMsg{MsgID: msgID, Body: body}); err == nil {
			return true
		}
	}
	return m.srv.sendToPlayer(playerID, msgID, body)
}

// ProduceCross 实现 chat.Messenger。
//
// 按 ToID 分区：同一接收方的消息落到同一 Kafka 分区，跨服消费者侧即可
// 按 msg_id 幂等且保持相对顺序。
func (m chatMessenger) ProduceCross(ctx context.Context, msg *pkgmq.ChatCrossMsg) error {
	if m.srv.producer == nil {
		return fmt.Errorf("kafka producer not available")
	}
	env, err := pkgmq.NewEnvelope(fmt.Sprintf("%d", msg.MsgID), pkgmq.TopicChatCross, fmt.Sprintf("%d", msg.ToID), msg)
	if err != nil {
		return err
	}
	return m.srv.producer.ProduceEnvelope(ctx, env)
}

// PendOffline 实现 chat.Messenger。
//
// 接收方不在线时把消息暂存进 Redis List（有上限 + TTL，防止无限堆积）；
// 登录后由 offline.go 的 drainOffline 补投。暂存失败不影响发送方体验——
// 私聊尽力而为，丢消息比卡住发送方更可接受。
func (m chatMessenger) PendOffline(ctx context.Context, playerID int64, msg *pkgmq.ChatCrossMsg) error {
	if m.srv.presence == nil {
		return nil
	}
	env, err := pkgmq.NewEnvelope(fmt.Sprintf("%d", msg.MsgID), pkgmq.TopicChatCross, fmt.Sprintf("%d", playerID), msg)
	if err != nil {
		return err
	}
	raw, err := env.Marshal()
	if err != nil {
		return err
	}
	return m.srv.presence.PendPush(ctx, "chat", playerID, raw, 200, 0)
}

// sendToClient 下行发送：把模块响应经 GameService.Channel 写回网关（18.2 同流保证有序）。
type gamePusher struct {
	srv *Server
}

// Push 实现 battle.Pusher。
func (p gamePusher) Push(playerID int64, msgID uint32, body []byte) bool {
	p.srv.mu.RLock()
	sessionID, ok := p.srv.players2sess[playerID]
	p.srv.mu.RUnlock()
	if !ok || sessionID == 0 {
		return false
	}
	if err := p.srv.sendToClient(sessionID, msgID, body); err != nil {
		p.srv.logger.Debug("battle push failed", "player_id", playerID, "msg_id", msgID, "err", err)
		return false
	}
	return true
}

// crossHandler 实现 cross.Handler：把跨服下行帧分发给 pool（建房响应）与 battle 模块。
type crossHandler struct {
	srv *Server
}

// OnCreateResp 实现 cross.Handler。
func (h crossHandler) OnCreateResp(resp *internalv2.RoomCreateResp) {
	if h.srv.crossPool != nil {
		h.srv.crossPool.OnCreateResp(resp)
	}
}

// OnRoomReady 实现 cross.Handler。
func (h crossHandler) OnRoomReady(roomID uint64) {
	if h.srv.battleMod != nil {
		h.srv.battleMod.OnRoomReady(roomID)
	}
}

// OnBattleSync 实现 cross.Handler。
func (h crossHandler) OnBattleSync(msg *internalv2.BattleMsg) {
	if h.srv.battleMod != nil {
		h.srv.battleMod.OnBattleSync(msg)
	}
}

// OnBattleResult 实现 cross.Handler。
//
// 两个动作都做：battle 模块给玩家发结算，pool 清掉房间路由——
// 只做前者会让 rooms 表无限增长（每个房间永久占一个 map 项）。
func (h crossHandler) OnBattleResult(res *internalv2.BattleResult) {
	if h.srv.battleMod != nil {
		h.srv.battleMod.OnBattleResult(res)
	}
	if h.srv.crossPool != nil {
		h.srv.crossPool.OnRoomClosed(res.GetRoomId())
	}
}

// CrossReady 已就绪的跨服节点数（测试/运维探针用）。
func (s *Server) CrossReady() int {
	if s.crossPool == nil {
		return 0
	}
	return s.crossPool.ReadyCount()
}

// BattleModule 暴露战斗模块（测试用）。
func (s *Server) BattleModule() *battle.Module { return s.battleMod }

// centerMatchAdapter 把中心服客户端适配为 battle.CenterMatcher。
//
// 适配器存在的意义：battle 模块只依赖「能不能匹配」的抽象，不关心背后是
// 中心服还是本服降级逻辑——未配置中心服时 Available() 为 false，battle
// 自动走本服匹配。
type centerMatchAdapter struct {
	cli *gamecenter.Client
}

// Available 实现 battle.CenterMatcher。
func (a centerMatchAdapter) Available() bool {
	return a.cli != nil && a.cli.Ready()
}

// Match 实现 battle.CenterMatcher。
func (a centerMatchAdapter) Match(playerID int64, mode string, elo int32, snapshot []byte) error {
	if a.cli == nil {
		return fmt.Errorf("center client not configured")
	}
	return a.cli.SendMatch(playerID, mode, elo, snapshot)
}

// centerHandler 实现 center.Handler：把中心服下行指令转给对应模块。
type centerHandler struct {
	srv *Server
}

// OnMatchResult 实现 center.Handler。
func (h centerHandler) OnMatchResult(res *internalv2.MatchResult) {
	if h.srv.battleMod != nil {
		h.srv.battleMod.OnMatchResult(res)
	}
}

// OnRelayPush 实现 center.Handler：跨节点 Actor 投递的接收侧（P0）。
//
//	payload 是 actor.RemoteEnvelope（JSON）：解出目标 ActorID 与消息后
//
// 投入本地 System。语义是「尽力而为 + 对账兜底」——玩家已下线时
// DeliverRemote 返回 ErrActorGone，记日志即可：客户端重连会拉全量
// 权威状态，丢一条下行推送不构成正确性缺口。
func (h centerHandler) OnRelayPush(push *internalv2.RelayPush) {
	actorID, msg, err := actor.DecodeRemoteEnvelope(push.GetPayload())
	if err != nil {
		h.srv.logger.Warn("relay push decode failed",
			"from", push.GetFromNode(), "relay_id", push.GetRelayId(), "err", err)
		return
	}
	if err := h.srv.actorSys.DeliverRemote(actorID, msg); err != nil {
		h.srv.logger.Debug("relay push deliver failed",
			"from", push.GetFromNode(), "actor", actorID, "err", err)
		return
	}
	h.srv.logger.Debug("relay push delivered",
		"from", push.GetFromNode(), "actor", actorID, "relay_id", push.GetRelayId())
}

// OnGroupUpdate 实现 center.Handler：组内跨服地址变化 → 重建/新增跨服流。
func (h centerHandler) OnGroupUpdate(g *internalv2.GroupUpdate) {
	if h.srv.crossPool == nil || len(g.GetCrossAddrs()) == 0 {
		return
	}
	h.srv.crossPool.Start(context.Background(), g.GetCrossAddrs())
	h.srv.logger.Info("group update applied", "group", g.GetGroupId(),
		"games", len(g.GetGameNodes()), "cross", len(g.GetCrossAddrs()))
}

// OnKick 实现 center.Handler：中心服滚动重启，本节点会自行重连。
func (h centerHandler) OnKick(k *internalv2.KickNode) {
	h.srv.logger.Warn("center asked to reconnect", "reason", k.GetReason())
}

// OnConfigPush 实现 center.Handler（配置热更新 P7+ 接入）。
func (h centerHandler) OnConfigPush(c *internalv2.ConfigPush) {
	h.srv.logger.Info("config push received", "key", c.GetConfigKey(), "checksum", c.GetChecksum())
}

// CenterReady 中心服 Report 流是否就绪（测试/探针用）。
func (s *Server) CenterReady() bool {
	return s.centerCli != nil && s.centerCli.Ready()
}

// rankReporter 把中心服客户端适配为 battle.RankReporter。
type rankReporter struct {
	cli *gamecenter.Client
}

// ReportArenaScore 实现 battle.RankReporter。
//
// bizID 透传给中心服去重：同一场结算因重投/补发被上报多次时，中心服按
// bizID 只认第一次，排名不会重复加分。
func (r rankReporter) ReportArenaScore(playerID int64, score int64, bizID string) error {
	if r.cli == nil {
		return fmt.Errorf("center client not configured")
	}
	return r.cli.SendRank("arena_score", playerID, score, "", bizID)
}

// rankQuerier 把中心服客户端适配为 rank 模块的查询接口。
type rankQuerier struct{ cli *gamecenter.Client }

// Query 实现 rank.Querier；中心服回非 0 code 视为查询失败（不要当成空榜）。
func (q rankQuerier) Query(ctx context.Context, rankType string, playerID int64, topN int32) (*rankmodule.Result, error) {
	if q.cli == nil {
		return nil, fmt.Errorf("center client not configured")
	}
	res, err := q.cli.QueryRank(ctx, rankType, playerID, topN)
	if err != nil {
		return nil, err
	}
	if res.GetCode() != 0 {
		return nil, fmt.Errorf("center rank code %d", res.GetCode())
	}
	out := &rankmodule.Result{
		Season:    res.GetSeason(),
		OnBoard:   res.GetOnBoard(),
		MyRank:    res.GetMyRank(),
		MyScore:   res.GetMyScore(),
		UpdatedAt: res.GetUpdatedAt(),
	}
	for _, e := range res.GetTop() {
		out.Entries = append(out.Entries, rankmodule.Entry{PlayerID: e.GetPlayerId(), Score: e.GetScore(), Rank: e.GetRank()})
	}
	for _, e := range res.GetAround() {
		out.Around = append(out.Around, rankmodule.Entry{PlayerID: e.GetPlayerId(), Score: e.GetScore(), Rank: e.GetRank()})
	}
	return out, nil
}

// buildIDTables 后台构建 ID 表（12.5.1）。
//
// 必须先建热表（player/player_equip/mail）再建其余表：热表决定登录与穿戴是否
// 能走 Redis 快路径，冷表（如 friend）缺失只影响对应功能的性能。

// seatRewarder 座位奖励投递（实现 seatpkg.Rewarder）。
//
// 座位管理器只负责产生奖励事件，是否在线由这里判断：在线直投玩家 Actor，
// 离线（或投递失败）写 seat_pending_reward，等下次登录由 seat 模块补发。
// 放在适配器层的理由与其它 adapter 一致——它翻译的是「座位域的事件」到
// 「玩家域的投递方式」，两边都不该知道对方。
type seatRewarder struct {
	players *player.Manager
	sys     *actor.System
	store   *seatpkg.Store
	logger  *log.Logger
}

func (r seatRewarder) Reward(ctx context.Context, rel pkgseat.Release) error {
	if r.players != nil {
		if pid, ok := r.players.Get(rel.PlayerID); ok {
			err := r.sys.Send(pid, &player.GrantItemsMsg{
				BizID:  fmt.Sprintf("%d", rel.OccupyID),
				Source: "seat",
				Items:  []player.GrantItem{{ItemID: pkgseat.RewardItem, Count: rel.Amount}},
			})
			if err == nil {
				if raw, e := proto.Marshal(&gamev1.SeatRewardNotify{
					Tier: int32(rel.Tier), Minutes: rel.Minutes, Reward: rel.Amount,
				}); e == nil {
					_ = r.sys.Send(pid, &player.PushMsg{MsgID: codec.MsgSeatRewardNotify, Body: raw})
				}
				return nil
			}
			r.logger.Warn("seat reward send failed, fallback to outbox",
				"player_id", rel.PlayerID, "err", err)
		}
	}
	if r.store == nil {
		return fmt.Errorf("seat: 玩家 %d 不在线且无持久化，奖励未投递", rel.PlayerID)
	}
	return r.store.AddPending(ctx, rel.PlayerID, rel.OccupyID, rel.Tier, rel.Minutes)
}
