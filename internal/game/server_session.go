// internal/game/server_session.go —— 会话与玩家绑定
//
// 这一段是「网络会话 ↔ 玩家 Actor」的桥：网关回调（OnBind/OnUnbind/
// OnClientPacket）在这里被翻译成 Actor 消息，Actor 的下行也在这里回到会话。
//
// 并发约束：sessions/accounts/players2sess 三张表共用一把锁（mu）。
// players2sess 是 player→session 的唯一权威映射，顶号判定与下行路由都读它，
// 必须与另两张在同一把锁内原子更新——分开加锁会出现「玩家已绑定但找不到会话」。
package game

import (
	"context"
	"errors"
	"fmt"
	"time"

	"game_study/internal/game/player"
	"game_study/internal/game/service"
	"game_study/pkg/actor"
	"game_study/pkg/codec"

	gamev1 "game_study/pkg/pb/game/v1"
	gatev1 "game_study/pkg/pb/gate/v1"
	internalv2 "game_study/pkg/pb/internalv2"

	"google.golang.org/protobuf/proto"
)

func (s *Server) sendToClient(sessionID uint64, msgID uint32, body []byte) error {
	if s.gameRPC == nil {
		return fmt.Errorf("game rpc not ready")
	}
	frame := &internalv2.StreamFrame{
		Data: &internalv2.StreamFrame_ClientPacket{
			ClientPacket: &internalv2.ClientPacket{
				SessionId: sessionID,
				MsgId:     msgID,
				Body:      body,
			},
		},
	}
	return s.gameRPC.SendToSession(sessionID, frame)
}

// saveAckTimeout 等待 Actor 回执真实保存结果的上限。
//
// 超时后 flusher 会重新标脏、下轮重试；迟到回执由 Done 的容量 1 + 非阻塞写丢弃，
// 不会拖住 Actor。
const saveAckTimeout = 5 * time.Second

// persistPlayer 由 flusher 回调，把单个玩家落库。
//
// 返回 nil 的语义是「SQL 已成功」而不是「消息已投递」：以前只 Send 就返回 nil，
// flusher 会把玩家从脏队列移除，保存失败时数据静默丢失。
func (s *Server) persistPlayer(ctx context.Context, playerID int64) error {
	// 缓存权威模式下直连落库已禁用（persist.Save 同样拒绝执行）：脏数据在
	// Redis 队列里由 wbWorker 消费，flusher 只服务非缓存玩家。
	if s.repo.CacheEnabled() {
		return nil
	}
	// 玩家不在线（从未上线或 PID 已不在索引）→ 没有 Actor 可保存：
	// 此时库里的数据就是最新状态，视为成功，不能当成失败重试。
	pid, ok := s.players.Get(playerID)
	if !ok {
		return nil
	}
	done := make(chan error, 1)
	if err := s.actorSys.Send(pid, &player.SaveMsg{Done: done}); err != nil {
		return fmt.Errorf("player %d save: %w", playerID, err)
	}
	waitCtx, cancel := context.WithTimeout(ctx, saveAckTimeout)
	defer cancel()
	select {
	case err := <-done:
		if err != nil {
			return fmt.Errorf("player %d save: %w", playerID, err)
		}
		return nil
	case <-waitCtx.Done():
		return fmt.Errorf("player %d save ack: %w", playerID, waitCtx.Err())
	}
}

// 会话级协议号（未选角时由组装层直接处理，不进 Actor）。
const (
	msgIDEnterGame  = codec.MsgEnterGameReq
	msgIDCreateRole = codec.MsgCreateRoleReq
	// msgIDKickNotify 与网关 common.MsgIDKickNotify 保持一致：10。
	msgIDKickNotify uint32 = 10
)

// OnBind 实现 rpc.StreamHandler：按 account_id 查角色，有角色才拉起 Actor。
//
// playerID>0 时（网关已做过选角）直接绑定；否则按账号查唯一角色，
// 查不到属于正常情况——等客户端发 CreateRoleReq / EnterGameReq。
func (s *Server) OnBind(ctx context.Context, sessionID uint64, accountID, playerID int64) error {
	s.mu.Lock()
	s.accounts[sessionID] = accountID
	s.sessions[sessionID] = 0
	s.mu.Unlock()

	if playerID > 0 {
		return s.bindPlayer(ctx, sessionID, playerID)
	}
	if accountID <= 0 || s.roleSvc == nil {
		return nil
	}
	role, err := s.roleSvc.FindByAccount(ctx, accountID)
	if err != nil {
		// 无角色属于正常情况：等客户端发 CreateRoleReq / EnterGameReq
		s.logger.Info("session bind without role", "session_id", sessionID, "account_id", accountID)
		return nil
	}
	return s.bindPlayer(ctx, sessionID, role.Id)
}

// onlinePresence 是顶号抢占用到的在线归属能力（接口隔离）。
//
// 只声明这两个方法：抢占用 TrySetOnline（带冲突检测），接管用 SetOnline
// （强制覆盖）。*cache.Presence 隐式实现，测试可注入 fake。
type onlinePresence interface {
	TrySetOnline(ctx context.Context, playerID int64, nodeID string, ttl time.Duration) (ok bool, owner string, err error)
	SetOnline(ctx context.Context, playerID int64, nodeID string, ttl time.Duration) error
}

// claimOnline 占用玩家在线归属；被别的节点持有时先通知对方踢人再强制接管。
//
// 为什么是「接管」而不是「拒绝」：顶号的产品语义是「新登录挤掉旧会话」，
// 与连接落在哪个节点无关。早先的实现跨节点直接拒绝，玩家换设备时如果
// 网关把连接路由到另一个游戏服，就会看到「已在其它节点登录」的报错——
// 同节点顶号、跨节点报错，行为不一致。
//
// 时序：notify 是异步通知（远程投递单向、不等待），随后立刻覆盖 presence。
// 旧节点的 Actor 可能还活着一小会儿，但它的快照提交会被围栏令牌（epoch）
// 挡掉——这正是 R2.5 设计的目标场景，不需要在顶号路径上额外加锁等待。
//
// 返回值 tookOver 表示「是否从别的节点手里接管」，只用于日志与断言。
func claimOnline(ctx context.Context, p onlinePresence, self string, playerID int64,
	ttl time.Duration, notify func(nodeID string, playerID int64)) (tookOver bool, err error) {
	ok, owner, err := p.TrySetOnline(ctx, playerID, self, ttl)
	switch {
	case err != nil:
		// 占用失败不阻断绑定：Redis 抖动时宁可双开一会儿，也别让玩家登不进来。
		// 双开的窗口由围栏令牌兜底（后写者胜），最终一致。
		return false, nil
	case ok || owner == self:
		return false, nil
	}

	if notify != nil {
		notify(owner, playerID)
	}
	if err := p.SetOnline(ctx, playerID, self, ttl); err != nil {
		return true, fmt.Errorf("force online after remote kick: %w", err)
	}
	return true, nil
}

// claimOnline 是 claimOnline 自由函数的 Server 绑定版（补日志与踢人实现）。
func (s *Server) claimOnline(ctx context.Context, playerID int64) error {
	tookOver, err := claimOnline(ctx, s.presence, s.cfg.Server.Name, playerID, onlineTTL, s.kickRemotePlayer)
	if err != nil {
		return err
	}
	if tookOver {
		s.logger.Info("took over player from other node", "player_id", playerID)
	}
	return nil
}

// kickRemotePlayer 通知持有该玩家的节点踢掉本地会话。
//
// 发不出去不算失败：presence 已被覆盖，旧节点的提交会被围栏令牌挡住，
// 它自己的心跳/UNBIND 也会让它下线，只是晚一点。
func (s *Server) kickRemotePlayer(nodeID string, playerID int64) {
	if nodeID == "" || s.actorSys == nil {
		return
	}
	pid := &actor.PID{ID: player.ActorID(playerID), Node: nodeID}
	if err := s.actorSys.Send(pid, player.KickMsg{FromNode: s.cfg.Server.Name}); err != nil {
		s.logger.Warn("notify remote kick failed",
			"player_id", playerID, "node", nodeID, "err", err)
	}
}

// kickLocalPlayer 踢掉玩家在本节点的网关会话（Actor 收到 KickMsg 时回调）。
//
// 只发 KickNotify 不断 TCP：与同节点顶号走同一条路径，客户端收到通知后
// 自行断开，随后的 UNBIND 走标准下线（模块 OnLogout + 落库）。
func (s *Server) kickLocalPlayer(playerID int64, fromNode string) {
	s.mu.RLock()
	sid := s.players2sess[playerID]
	s.mu.RUnlock()
	if sid == 0 {
		return
	}
	s.kickSession(sid, "login_elsewhere")
	s.logger.Info("local session kicked by remote node",
		"player_id", playerID, "session_id", sid, "from", fromNode)
}

// bindPlayer 拉起玩家 Actor 并绑定会话。
//
// 顶号规则（R3.1）：同一角色只允许一个权威会话。新会话绑定成功后主动踢掉旧会话，
// 旧连接收到 KickNotify 后断开，其迟到的 UNBIND 会被 Actor 的会话身份校验忽略。
func (s *Server) bindPlayer(ctx context.Context, sessionID uint64, playerID int64) error {
	// 跨服权威归属：占用在线状态。被别的节点持有时通知对方踢人后强制接管。
	// 同节点旧会话（重连）视为成功，由下面的顶号逻辑处理。
	if s.presence != nil {
		if err := s.claimOnline(ctx, playerID); err != nil {
			return err
		}
	}

	// 三张表在同一把锁内完成「读旧会话 → 写新映射」：old 是被顶号的旧会话
	// （0 表示首次绑定）。players2sess 是顶号判定的唯一权威，必须先于
	// kick 完成切换，否则旧连接的迟到 UNBIND 会命中错误的会话身份。
	s.mu.Lock()
	old := s.players2sess[playerID]
	s.sessions[sessionID] = playerID
	s.players2sess[playerID] = sessionID
	accountID := s.accounts[sessionID]
	s.mu.Unlock()

	if old != 0 && old != sessionID {
		s.kickSession(old, "login_elsewhere")
	}
	if err := s.bindActor(ctx, sessionID, playerID); err != nil {
		return err
	}
	s.logger.Info("session bound", "session_id", sessionID, "player_id", playerID, "account_id", accountID, "replaced", old)
	return nil
}

// kickAllSessions 关闭前通知所有在线会话重连。
//
// 有状态实例下线时，绑定在本实例的会话无法迁移（Actor 状态在内存），
// 服务端能做的是**尽早明确告知**：客户端收到 KickNotify 后立即重连到
// 其它实例，而不是让后续请求一个个超时。网关转发 KickNotify 后会断开连接。
//
// 返回通知数量（推送失败也计入，只是日志不同）。
func (s *Server) kickAllSessions(reason string) int {
	s.mu.RLock()
	ids := make([]uint64, 0, len(s.sessions))
	for sid := range s.sessions {
		ids = append(ids, sid)
	}
	s.mu.RUnlock()
	for _, sid := range ids {
		s.kickSession(sid, reason)
	}
	return len(ids)
}

// kickSession 通知旧会话下线；客户端收到 KickNotify 后断开，触发 OnUnbind。
//
// 推不出去不算失败：旧会话可能已经断开，此时 OnUnbind 迟早会到，
// Actor 的会话身份校验保证它不会影响新会话。
func (s *Server) kickSession(sessionID uint64, reason string) {
	body, err := proto.Marshal(&gatev1.KickNotify{Reason: reason})
	if err != nil {
		return
	}
	if err := s.sendToClient(sessionID, msgIDKickNotify, body); err != nil {
		s.logger.Info("kick stale session failed; waiting for stale unbind",
			"session_id", sessionID, "reason", reason, "err", err)
		return
	}
	s.logger.Info("stale session kicked", "session_id", sessionID, "reason", reason)
}

// ensureActor 确保玩家 Actor 已创建并与当前会话绑定。
//
// 只在「Actor 不存在」或「会话变了（重连）」时才发 BindMsg —— 不能每个业务包都发：
// onBind 会跑一遍全部模块的 OnLogin（活动进度要写库），重复 Bind 等于每条消息
// 多两次落库，直接把延迟打到超时。
//
// 反过来，重连时必须补 Bind：Actor 离线后不销毁（Tier-2 保留 90s），
// 里面的 sessionID 还是旧的/0，不刷新的话回包发不出去，客户端只看到超时。
//
// presence 不在这里设置 —— 在线状态属于「会话」，不属于「业务状态」。
func (s *Server) ensureActor(ctx context.Context, sessionID uint64, playerID int64) error {
	if _, ok := s.players.Get(playerID); ok {
		s.mu.RLock()
		bound := s.players2sess[playerID]
		s.mu.RUnlock()
		if bound == sessionID {
			return nil // 已绑定到本会话
		}
	}
	return s.bindActor(ctx, sessionID, playerID)
}

// bindActor 无条件把会话绑定到 Actor（Actor 不存在则创建）。
func (s *Server) bindActor(ctx context.Context, sessionID uint64, playerID int64) error {
	pid, ok := s.players.Get(playerID)
	if !ok {
		spawned, err := s.players.GetOrSpawn(ctx, playerID)
		if err != nil {
			return err
		}
		pid = spawned
	}
	// sender 传 nil：装配层不接收 BindResult（ctx.Respond 对 nil 发送者静默
	// 丢弃），绑定失败经 player 包的下行错误通道通知客户端（cache_actor.go
	// 的 cacheFailure）。Bind 与后续业务包走同一信箱 FIFO，Actor 必然
	// 先处理 Bind、再处理业务包。
	if err := s.actorSys.SendWithSender(pid,
		&player.BindMsg{SessionID: sessionID, PlayerID: playerID}, nil); err != nil {
		return err
	}
	s.mu.Lock()
	s.players2sess[playerID] = sessionID
	s.mu.Unlock()
	return nil
}

// markOnline 标记玩家在线（会话绑定/创角时调用，与 Actor 是否加载无关）。
func (s *Server) markOnline(ctx context.Context, playerID int64) {
	if s.presence == nil {
		return
	}
	if err := s.presence.SetOnline(ctx, playerID, s.cfg.Server.Name, onlineTTL); err != nil {
		s.logger.Warn("set online failed", "player_id", playerID, "err", err)
	}
}

// OnUnbind 实现 rpc.StreamHandler。
//
// 三段式：摘本地映射 → 判定是否被顶号 → 权威会话才解 Actor + 清在线归属。
// 「被顶号的旧会话」是最容易写错的分支：它的一切副作用都必须跳过，
// 否则会把仍在线的权威会话打成离线（见下方 stale 注释）。
func (s *Server) OnUnbind(ctx context.Context, sessionID uint64) error {
	s.mu.Lock()
	playerID := s.sessions[sessionID]
	delete(s.sessions, sessionID)
	delete(s.accounts, sessionID)
	stale := false
	if playerID != 0 {
		if s.players2sess[playerID] == sessionID {
			delete(s.players2sess, playerID)
		} else {
			stale = true // 该会话已被新会话顶号
		}
	}
	s.mu.Unlock()
	if playerID == 0 {
		return nil
	}
	if stale {
		// 旧连接被顶号后才断开：不能解绑 Actor，也不能清在线归属——
		// 否则会把仍在线的权威会话打成离线，其它节点还能趁机抢占。
		s.logger.Info("stale session unbind ignored", "player_id", playerID, "session_id", sessionID)
		return nil
	}
	if pid, ok := s.players.Get(playerID); ok {
		if err := s.actorSys.Send(pid, &player.UnbindMsg{SessionID: sessionID}); err != nil {
			// 解绑消息被拒（信箱满/Actor 已终止）：脏数据仍在 flusher 队列里会周期性落库，
			// 但这里要明确记录，不能静默吞掉。
			s.logger.Warn("unbind message rejected",
				"player_id", playerID, "session_id", sessionID, "err", err)
		}
	}
	if s.presence != nil {
		if node, online := s.presence.IsOnline(ctx, playerID); online && node == s.cfg.Server.Name {
			_ = s.presence.SetOffline(ctx, playerID)
		}
	}
	return nil
}

// OnClientPacket 实现 rpc.StreamHandler。
func (s *Server) OnClientPacket(ctx context.Context, sessionID uint64, msgID uint32, seq uint32, body []byte) error {
	s.mu.RLock()
	playerID := s.sessions[sessionID]
	accountID := s.accounts[sessionID]
	s.mu.RUnlock()

	// 会话级协议始终在此处理。
	//
	// 注意：不能只在 playerID==0 时才放行 —— 已有角色的账号在 OnBind 阶段就被
	// 自动绑定（省一次往返），此时客户端仍会发 EnterGame(1010)。若走 Actor 路由，
	// 会因「无模块处理 1010」而静默丢包，客户端只能等超时（重连场景必现）。
	switch msgID {
	case msgIDEnterGame:
		return s.handleEnterGame(ctx, sessionID, accountID, body)
	case msgIDCreateRole:
		return s.handleCreateRole(ctx, sessionID, accountID, body)
	}

	if playerID == 0 {
		return fmt.Errorf("session %d has no role, drop msg %d", sessionID, msgID)
	}

	// 懒加载：创角/进游戏只记 sessions，Actor 在首个业务包时创建。
	//
	// 好处有两个：① 创角后立刻加载会读到「刚建的空角色」，若随后有数据写入
	// （补偿道具、GM 改库）内存与库就不一致；② 只登录不操作的账号不占 Actor 资源。
	if err := s.ensureActor(ctx, sessionID, playerID); err != nil {
		return fmt.Errorf("bind player %d: %w", playerID, err)
	}
	pid, ok := s.players.Get(playerID)
	if !ok {
		return fmt.Errorf("player %d actor missing after bind", playerID)
	}
	// 同一 Actor 信箱 FIFO：Bind 先于业务包被处理，预加载完成后再处理业务
	return s.actorSys.Send(pid, &player.PacketMsg{MsgID: msgID, Seq: seq, Body: body})
}

// handleEnterGame 进游戏：返回已有角色，无角色回 2003 引导创角。
func (s *Server) handleEnterGame(ctx context.Context, sessionID uint64, accountID int64, body []byte) error {
	var req gamev1.EnterGameReq
	if err := proto.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("decode EnterGameReq: %w", err)
	}
	resp := &gamev1.EnterGameResp{Code: 0, ServerTime: time.Now().Format(time.RFC3339)}

	if s.roleSvc == nil {
		resp.Code = 2003
		return s.reply(sessionID, 1011, resp)
	}
	role, err := s.roleSvc.FindByAccount(ctx, accountID)
	if err != nil {
		resp.Code = 2003 // 无角色
		return s.reply(sessionID, 1011, resp)
	}
	resp.RoleId = role.Id
	if err := s.bindPlayer(ctx, sessionID, role.Id); err != nil {
		return err
	}
	if err := s.reply(sessionID, 1011, resp); err != nil {
		return err
	}
	// 离线暂存补投（12.6.2）：必须放在 1011 之后 ——
	// 客户端收到进游戏回执才开始处理推送，过早补投会被当成「未知消息」丢掉。
	go s.drainOffline(role.Id, sessionID)
	return nil
}

// handleCreateRole 创角：校验 → 落库 → 拉起 Actor → 回包。
func (s *Server) handleCreateRole(ctx context.Context, sessionID uint64, accountID int64, body []byte) error {
	var req gamev1.CreateRoleReq
	if err := proto.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("decode CreateRoleReq: %w", err)
	}

	resp := &gamev1.CreateRoleResp{}
	if s.roleSvc == nil {
		resp.Code = 5000
		return s.reply(sessionID, 1021, resp)
	}
	roleID, err := s.roleSvc.Create(ctx, accountID, req.Name, req.Career)
	if err != nil {
		resp.Code = mapCreateRoleErr(err)
		s.logger.Info("create role failed", "account_id", accountID, "err", err)
		return s.reply(sessionID, 1021, resp)
	}
	resp.Code = 0
	resp.RoleId = roleID
	// 只记录会话 → 角色映射，不立即创建 Actor（见 OnClientPacket 的懒加载说明）。
	// 否则随后的补偿道具/改库写入会被「创角时加载的空快照」覆盖。
	s.mu.Lock()
	s.sessions[sessionID] = roleID
	s.players2sess[roleID] = sessionID
	s.mu.Unlock()
	// 创角即有会话 → 在线；Actor 留到首个业务包再创建
	s.markOnline(ctx, roleID)
	return s.reply(sessionID, 1021, resp)
}

// reply 序列化并下行。
func (s *Server) reply(sessionID uint64, msgID uint32, msg proto.Message) error {
	body, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	return s.sendToClient(sessionID, msgID, body)
}

// mapCreateRoleErr 错误 → 业务码（4.10）。
func mapCreateRoleErr(err error) int32 {
	switch {
	case errors.Is(err, service.ErrNameUsed):
		return 2004
	case errors.Is(err, service.ErrNameIllegal):
		return 2005
	case errors.Is(err, service.ErrCareerBad):
		return 2001
	default:
		return 5000
	}
}

// gamePusher 实现 battle.Pusher：把推送转成对玩家会话的下行。
//
// 与 chatMessenger.Push 的差别：战斗推送不经过 Actor——结算/匹配是纯下行
// 事件，不需要（也不应该）排在玩家业务消息队列里。
