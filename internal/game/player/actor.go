// internal/game/player/actor.go —— 玩家 Actor

// Package player 玩家 Actor 及其状态、缓存路径与持久化的实现。
//
// 关键约定：
//   - 一个玩家一个 Actor，Actor 内单线程处理消息；State 的锁只服务于
//     「Actor 外的只读访问」（探针/测试），写路径一律发生在消息处理里；
//   - 缓存权威模式下遵守 R2.1 契约「一次消息 = 一份快照 = 一次 CAS」：
//     业务 handler 只改内存，提交/回滚/pending 收尾由 cache_actor.go 负责；
//   - player 包不依赖 scene/gateway：场景连接经 SceneLink 注入、下行经
//     SendFunc 注入，接线由组装层（internal/game/server.go）完成。
package player

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"game_study/internal/game/module"
	"game_study/pkg/actor"
	"game_study/pkg/log"
	"game_study/pkg/metrics"
	"game_study/pkg/writebehind"

	gamev1 "game_study/pkg/pb/game/v1"
	"google.golang.org/protobuf/proto"
)

// 下行推送协议号（4.9：2011 等级变化）。
const msgIDLevelInfoNotify uint32 = 2011

// Message 类型常量。
const (
	MsgBind   = "player.bind"
	MsgUnbind = "player.unbind"
	MsgPacket = "player.packet"
	MsgSave   = "player.save"
	MsgKick   = "player.kick"
)

// BindMsg 绑定消息。
type BindMsg struct {
	// SessionID 权威会话 ID：后续 UNBIND 的身份校验与下行路由都以它为准。
	SessionID uint64
	// PlayerID 与 Actor 身份一致（Manager 按它索引 PID），用于日志核对。
	PlayerID int64
}

// UnbindMsg 解绑消息。
type UnbindMsg struct {
	// SessionID 发起解绑的会话；与 Actor 当前 sessionID 不一致即判定为
	// 「被顶号旧连接的迟到解绑」，直接忽略（见 onUnbind）。
	SessionID uint64
}

// PacketMsg 客户端消息。
type PacketMsg struct {
	// MsgID 业务协议号，Registry 按它分发到模块。
	MsgID uint32
	// Seq 客户端序号，目前仅透传（回包由模块自行决定是否带回）。
	Seq uint32
	// Body protobuf 包体；解码失败由模块自行处理，玩家层不猜格式。
	Body []byte
}

// GrantItem 奖励道具。
type GrantItem struct {
	ItemID int32
	Count  int64
}

// PushMsg 向该玩家客户端推送一条消息（跨模块统一出口）。
type PushMsg struct {
	MsgID uint32
	Body  []byte
}

// SceneResult 场景 Actor 对玩家请求（进入/移动/离开/同步）的处理结果。
//
// 走正常快照路径：SetScene 为 true 时更新 Profile.SceneID 并标脏，
// 回包在缓存提交成功后下发——否则会出现「客户端以为进了场景、服务端没记住」。
type SceneResult struct {
	SceneID  int32
	MsgID    uint32
	Body     []byte
	SetScene bool // true：把 Profile.SceneID 更新为该场景
}

// ScenePush 场景广播（实体出现/移动/离开）。
//
// **纯下行**：不触碰玩家状态、不触发快照 CAS。AOI 推送频率远高于业务消息，
// 若走 CAS 路径，每次广播都会写一次 Redis。
type ScenePush struct {
	SceneID int32
	MsgID   uint32
	Body    []byte
}

// SceneLink 玩家与场景 Actor 的连接（由组装层注入，避免 player 依赖 scene）。
//
// 只放两个「玩家 Actor 主动发起」的动作；场景 → 玩家的方向走 SceneResult/ScenePush。
type SceneLink interface {
	// Heartbeat 刷新场景内的在场时间，防止挂机被兜底清理误踢。
	Heartbeat(playerID int64, sceneID int32)
	// Resync 请求场景推送一次全量视野（重连后由服务端主动发起）。
	Resync(playerID int64, sceneID int32, pid *actor.PID)
}

// sceneHeartbeatInterval 心跳间隔：场景侧 90 秒无消息才判超时，30 秒一次足够。
const sceneHeartbeatInterval = 30 * time.Second

// sceneHeartbeatTick 心跳定时消息（自调度）。
type sceneHeartbeatTick struct{}

// GrantItemsMsg 发放奖励（充值发货/邮件领取等）。
type GrantItemsMsg struct {
	BizID  string // 与奖励一起持久化的业务唯一键，用于重复投递。
	Source string // 来源标记（recharge/mail/...），用于日志与幂等排查
	Items  []GrantItem
	Done   chan error // 可选：同步等待结果
}

// SendFunc 下行发送（sessionID + 协议号 + 包体），由组装层注入。
type SendFunc func(sessionID uint64, msgID uint32, body []byte) error

// Actor 玩家 Actor。
type Actor struct {
	// playerID 只读标识；logger 已携带该字段，排错时无需再翻调用栈。
	playerID int64
	state    *State
	registry *module.Registry
	logger   *log.Logger
	repo     *Repository
	// preloader 非缓存模式的并行预加载器；prefetch 是它本次拉取的结果，
	// 模块 OnLogin 经 module.Context.Prefetch 消费，避免二次查库。
	preloader *Preloader
	prefetch  *Prefetch
	// onDirty 任一数据变更 → 通知 flusher 排落库；缓存权威模式下为空
	// （持久化由 Redis 脏队列 + writebehind worker 负责，不走 flusher）。
	onDirty  func(playerID int64)
	sendFunc SendFunc

	// sceneLink 由组装层注入（WithSceneLink）；player 包不直接依赖 scene 包。
	sceneLink      SceneLink
	heartbeatOn    bool          // 心跳自调度只启动一次，置位后 onBind 重复进入不会重启
	heartbeatEvery time.Duration // 0 用默认；测试注入短间隔
	// sessionID 当前权威会话（0 = 不在线）；UNBIND 身份校验与下行都读它。
	sessionID uint64
	online    bool
	// loaded 预加载已完成；false 时丢弃业务包（onPacket）——
	// 用空状态处理业务会写坏数据。
	loaded      bool
	needsReload bool // 发奖提交结果未知时，禁止旧内存再次落库。
	// cacheVersion 本地已知的 Redis 快照版本，Commit 时作为 CAS 期望值；
	// 提交成功则前进为新版本，失败保持旧值（下轮按同一版本重试）。
	cacheVersion int64
	// cacheReload 下一条消息前必须重新 LoadCached：上一次提交已回滚或
	// 结果未知，内存相对 Redis 已过期，先恢复权威状态再进业务。
	cacheReload bool
	// bufferReplies 提交窗口内置位：业务产生的下行先入 queuedReplies，
	// CAS 成功后才真正发出——客户端不会先看到「成功回包」再被告知失败。
	bufferReplies bool
	queuedReplies []cachedReply
	// cachePending 结果未知的旧快照（来自不可回滚消息）：保留版本与状态，
	// 后续消息先在 finishPending 里核对 Redis 实际版本，再放行新业务。
	cachePending *writebehind.Record
}

// NewActor 创建玩家 Actor。
func NewActor(playerID int64, registry *module.Registry, logger *log.Logger, repo *Repository, onDirty func(int64), sendFunc SendFunc) *Actor {
	state := NewState(playerID)
	// 任何变更立刻标记落库（12.4.1 ①）：不这样的话在线玩家数据只在离线时才写，
	// 进程崩溃会丢掉全部在线会话的进度。
	if onDirty != nil {
		state.SetDirtyHook(func() { onDirty(playerID) })
	}
	return &Actor{
		playerID: playerID,
		state:    state,
		registry: registry,
		logger:   logger.With("player_id", playerID),
		repo:     repo,
		onDirty:  onDirty,
		sendFunc: sendFunc,
	}
}

// WithSceneLink 注入场景连接（心跳与重连同步）。
func (a *Actor) WithSceneLink(l SceneLink) *Actor {
	a.sceneLink = l
	return a
}

// WithPreloader 注入预加载器（12.6：一次往返拿完全部登录数据）。
func (a *Actor) WithPreloader(p *Preloader) *Actor {
	a.preloader = p
	return a
}

// State 返回玩家状态。
func (a *Actor) State() *State { return a.state }

// Receive 实现 actor.Actor。
//
// 分发逻辑：缓存权威模式下，纯下行消息（Push/ScenePush/心跳）不参与快照
// 提交，直接进 receive；其余消息进 receiveCached 的「克隆→业务→CAS」流程。
func (a *Actor) Receive(ctx *actor.Context) {
	if a.repo.CacheEnabled() {
		// 纯下行消息不参与「克隆快照 → 提交」流程：它们只读状态、不改状态。
		switch ctx.Message().(type) {
		case *PushMsg, *ScenePush, *sceneHeartbeatTick:
		default:
			a.receiveCached(ctx)
			return
		}
	}
	a.receive(ctx)
}

// receive 非缓存路径的消息分发（直连 MySQL 落库形态）。
//
// needsReload 闸门放在最前面：上次 COMMIT 结果未知（ErrCommitUnknown）时，
// 除 Save/Unbind 外的任何消息都先触发行锁重载，避免把过期背包写回库。
func (a *Actor) receive(ctx *actor.Context) {
	if a.needsReload {
		switch ctx.Message().(type) {
		case *SaveMsg, *UnbindMsg:
			// 不能将可能过期的背包写回。解绑仍需清除会话。
			if _, ok := ctx.Message().(*UnbindMsg); ok {
				a.online = false
				a.sessionID = 0
				return
			}
			// 保存请求必须回执失败：否则调用方只能等到超时，无法区分
			// 「在途」和「被拒绝」。
			if msg, ok := ctx.Message().(*SaveMsg); ok {
				replySave(msg.Done, fmt.Errorf("player %d needs reload; refusing to overwrite", a.playerID))
			}
			return
		default:
			reloadCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			err := a.repo.ReloadItems(reloadCtx, a.state)
			cancel()
			if err != nil {
				a.logger.Error("reload after uncertain commit failed", "err", err)
				switch msg := ctx.Message().(type) {
				case *BindMsg:
					_ = ctx.Respond(&BindResult{Code: 1, Err: err.Error()})
				case *GrantItemsMsg:
					if msg.Done != nil {
						msg.Done <- err
					}
				}
				return
			}
			a.needsReload = false
		}
	}
	switch msg := ctx.Message().(type) {
	case *BindMsg:
		a.onBind(ctx, msg)
	case *UnbindMsg:
		a.onUnbind(ctx, msg)
	case *PacketMsg:
		a.onPacket(ctx, msg)
	case *SaveMsg:
		a.onSave(ctx, msg)
	case *GrantItemsMsg:
		a.onGrantItems(ctx, msg)
	case *PushMsg:
		_ = a.send(msg.MsgID, msg.Body)
	case *SceneResult:
		a.onSceneResult(msg)
	case *ScenePush:
		a.onScenePush(msg)
	case *sceneHeartbeatTick:
		a.onSceneHeartbeat(ctx)
	default:
		a.logger.Debug("unknown message", "type", fmt.Sprintf("%T", msg))
	}
}

// onSceneResult 处理场景 Actor 的回执：更新场景归属后回包。
func (a *Actor) onSceneResult(msg *SceneResult) {
	if msg.SetScene && a.state.Profile().SceneID != msg.SceneID {
		a.state.SetSceneID(msg.SceneID)
		a.state.MarkDirty("profile")
	}
	if len(msg.Body) > 0 {
		_ = a.send(msg.MsgID, msg.Body)
	}
}

// onScenePush 转发场景广播。
//
// 按当前场景过滤是必要的：切场景时旧场景的离场消息与新场景的进场消息
// 属于不同 Actor，没有全局顺序，旧场景可能仍在推送——不过滤就会出现实体串场。
func (a *Actor) onScenePush(msg *ScenePush) {
	if msg.SceneID != a.state.Profile().SceneID {
		return
	}
	if len(msg.Body) > 0 {
		_ = a.send(msg.MsgID, msg.Body)
	}
}

// SaveMsg 触发保存。
//
// Done 可选：调用方应提供容量 1 的通道；Actor 在**真实保存结束**后写入结果
// （成功 nil，失败 err），调用方据此区分「投递成功」与「已落库」。
// 回执使用非阻塞发送，调用方超时离开后不会阻塞 Actor。
type SaveMsg struct {
	Done chan error
}

// onBind 登录/重连入口：预加载（仅首次）→ 属性重算 → 模块 OnLogin → 场景同步。
//
// 失败即回 BindResult{Code:1}：半成品 Actor 上线比拒绝登录更危险。
func (a *Actor) onBind(ctx *actor.Context, msg *BindMsg) {
	a.sessionID = msg.SessionID
	a.online = true

	// 首次上线：并行预加载全部数据（12.6）；重连时 Actor 未销毁，跳过
	if !a.loaded {
		if err := a.preload(ctx); err != nil {
			a.logger.Error("preload player failed", "err", err)
			_ = ctx.Respond(&BindResult{Code: 1, Err: err.Error()})
			return
		}
	}

	// 登录/重连：按等级重算基础属性（基础 + 成长），清空旧加成，再由各模块叠加。
	// ClearBonus 放在这里而不是某个模块里：模块注册顺序会变，
	// 「谁先登录谁清」会让后注册的模块加成被前一个清掉。
	a.state.RecalcAttrs()
	a.state.ClearBonus()

	mctx := a.moduleCtx(ctx)
	if err := a.registry.LoginAll(mctx); err != nil {
		a.logger.Error("module login failed", "err", err)
		_ = ctx.Respond(&BindResult{Code: 1, Err: err.Error()})
		return
	}
	a.startSceneLink(ctx)
	a.logger.Info("player online", "session_id", msg.SessionID)
	_ = ctx.Respond(&BindResult{Code: 0})
}

// startSceneLink 重连后主动同步一次场景，并启动心跳。
//
// 断线期间视野里可能有人进出、也可能因为心跳超时被场景清掉——
// 客户端不知道，所以由服务端推一次全量，而不是等客户端发现断档再拉。
// 登录时若还没进场景（SceneID=0），这里什么也不做，等客户端发进入请求。
func (a *Actor) startSceneLink(ctx *actor.Context) {
	if a.sceneLink == nil {
		return
	}
	if sceneID := a.state.Profile().SceneID; sceneID > 0 {
		a.sceneLink.Resync(a.playerID, sceneID, ctx.PID())
	}
	if !a.heartbeatOn {
		a.heartbeatOn = true
		ctx.ScheduleOnce(a.heartbeatInterval(), ctx.PID(), &sceneHeartbeatTick{})
	}
}

// onSceneHeartbeat 定时向场景刷新在场时间。
//
// 不做心跳的后果：挂机不动的玩家会被场景的「90 秒无消息」兜底当成幽灵清掉，
// 但人其实还在线——下一次移动就收到 4005（不在场景中），表现为「原地卡住」。
func (a *Actor) onSceneHeartbeat(ctx *actor.Context) {
	if a.online && a.sceneLink != nil {
		if sceneID := a.state.Profile().SceneID; sceneID > 0 {
			a.sceneLink.Heartbeat(a.playerID, sceneID)
		}
	}
	ctx.ScheduleOnce(a.heartbeatInterval(), ctx.PID(), &sceneHeartbeatTick{})
}

// heartbeatInterval 心跳间隔（测试可注入短间隔）。
func (a *Actor) heartbeatInterval() time.Duration {
	if a.heartbeatEvery > 0 {
		return a.heartbeatEvery
	}
	return sceneHeartbeatInterval
}

// setState 替换玩家状态并重新挂上 dirty 回调。
//
// **必须走这里而不是直接 `a.state = state`**：预加载返回的是全新的 State 实例，
// 构造时挂的 dirtyHook 在它身上不存在。直接赋值会让「加载完成后的所有修改都不再
// 通知 flusher」——数据永远不落库，且不会有任何报错（线上表现为玩家进度丢失）。
func (a *Actor) setState(state *State) {
	a.state = state
	if a.onDirty != nil {
		state.SetDirtyHook(func() { a.onDirty(a.playerID) })
	}
}

// preload 登录数据加载（12.6）。
//
// 优先用 Preloader（5 查询并行 + 超时保护）；未注入时退回逐表串行加载。
func (a *Actor) preload(ctx *actor.Context) error {
	if a.repo.CacheEnabled() {
		loadCtx, cancel := context.WithTimeout(context.Background(), DefaultPreloadTimeout)
		defer cancel()
		state, version, err := a.repo.LoadCached(loadCtx, a.playerID)
		if err != nil {
			return err
		}
		a.setState(state)
		a.cacheVersion = version
		a.loaded = true
		a.prefetch = nil
		return nil
	}
	if a.preloader != nil {
		state, pf, err := a.preloader.Load(context.Background(), a.playerID)
		if err != nil {
			return err
		}
		a.setState(state)
		a.prefetch = pf
		a.loaded = true
		return nil
	}
	if a.repo != nil {
		state, err := a.repo.Load(context.Background(), a.playerID)
		if err != nil {
			return err
		}
		a.setState(state)
		a.loaded = true
		return nil
	}
	// 无 DB 环境（单测）：用空状态
	a.loaded = true
	return nil
}

// onUnbind 下线入口：会话身份校验 → 模块 OnLogout → 标脏落库。
//
// 身份校验失败的旧会话 UNBIND 直接忽略：玩家可能已在另一设备顶号，
// 此时解绑会把仍在线的权威会话打成离线。
func (a *Actor) onUnbind(ctx *actor.Context, msg *UnbindMsg) {
	if !a.online {
		return
	}
	// 会话身份校验：旧连接的延迟 UNBIND 不能把已经顶号的新会话解绑。
	// 没有这层校验时，玩家在另一台设备登录后，旧连接断线会把新连接踢成离线。
	if msg.SessionID != a.sessionID {
		a.logger.Info("ignore stale unbind",
			"player_id", a.playerID, "current_session", a.sessionID, "stale_session", msg.SessionID)
		return
	}
	mctx := a.moduleCtx(ctx)
	if err := a.registry.LogoutAll(mctx); err != nil {
		a.logger.Warn("module logout failed", "err", err)
	}
	a.online = false
	a.sessionID = 0
	// 标记 dirty，由 flusher 统一落库
	if a.onDirty != nil {
		a.onDirty(a.playerID)
	}
	a.logger.Info("player offline", "session_id", msg.SessionID)
}

// onPacket 业务包入口：loaded 之前一律丢弃（空状态处理业务会写坏数据），
// 之后交 Registry 按 MsgID 分发到对应模块。
func (a *Actor) onPacket(ctx *actor.Context, msg *PacketMsg) {
	if !a.loaded {
		// 预加载失败或尚未完成：拒绝业务包，避免用空状态处理（会写坏数据）
		a.logger.Warn("packet before loaded, drop", "msg_id", msg.MsgID)
		return
	}
	mctx := a.moduleCtx(ctx)
	start := time.Now()
	err := a.registry.Dispatch(mctx, msg.MsgID, msg.Body)
	if metrics.Enabled() {
		metrics.MsgObserve("in", time.Since(start), len(msg.Body))
		if err != nil {
			metrics.MsgInc("in", "error")
		} else {
			metrics.MsgInc("in", "ok")
		}
	}
	if err != nil {
		a.logger.Warn("packet handle failed", "msg_id", msg.MsgID, "err", err)
	}
}

// onGrantItems 发放奖励（Actor 内单线程执行，无需加锁）。
func (a *Actor) onGrantItems(ctx *actor.Context, msg *GrantItemsMsg) {
	if msg.BizID != "" {
		// A 类幂等（R2.1 §2.2）：键 = source:bizID，与快照同一次 CAS、
		// 同落库到 applied_grants，进程重启后依然有效——充值回调/Kafka
		// 重投不会导致二次发奖。B 类（主动领取）禁止塞这里，会持续膨胀。
		key := msg.Source + ":" + msg.BizID
		if a.state.appliedGrants[key] {
			if msg.Done != nil && !a.bufferReplies {
				msg.Done <- nil
			}
			return
		}
		if a.state.appliedGrants == nil {
			a.state.appliedGrants = make(map[string]bool)
		}
		a.state.appliedGrants[key] = true
	}

	for _, it := range msg.Items {
		if it.Count <= 0 {
			continue
		}
		a.state.AddItem(it.ItemID, it.Count)
	}
	a.state.MarkDirty("bag")
	if a.onDirty != nil {
		a.onDirty(a.playerID)
	}
	a.logger.Info("grant items", "source", msg.Source, "count", len(msg.Items))
	if msg.Done != nil && !a.bufferReplies {
		msg.Done <- nil
	}
}

// onSave 执行保存并回执真实结果；没有 Done 时保持 fire-and-forget。
func (a *Actor) onSave(ctx *actor.Context, msg *SaveMsg) {
	replySave(msg.Done, a.savePlayer(ctx))
}

// savePlayer 真正执行落库，返回第一个错误（模块钩子或 SQL 失败）。
func (a *Actor) savePlayer(ctx *actor.Context) error {
	if a.repo.CacheEnabled() {
		return nil // 缓存权威：落库由 writebehind worker 负责
	}
	var firstErr error
	if a.registry != nil {
		mctx := a.moduleCtx(ctx)
		for _, m := range a.registry.All() {
			if err := m.OnSave(mctx.Actor.Context()); err != nil {
				a.logger.Warn("module save failed", "module", m.Name(), "err", err)
				if firstErr == nil {
					firstErr = err
				}
			}
		}
	}
	if a.repo != nil {
		if err := a.repo.Save(context.Background(), a.state); err != nil {
			a.logger.Error("save player failed", "err", err)
			return err // SQL 失败优先于模块钩子错误
		}
	}
	return firstErr
}

// replySave 向调用方回报保存结果；Done 容量应为 1，非阻塞写避免拖住 Actor。
func replySave(done chan error, err error) {
	if done == nil {
		return
	}
	select {
	case done <- err:
	default:
	}
}

// moduleCtx 组装模块执行上下文：每条消息一份，模块只经由它访问玩家状态。
//
// CommitItems 仅在非缓存模式挂载：缓存模式下玩家的唯一写入口是快照 CAS
// （R2.1 契约），直连 SQL 会造成「库已扣、Redis 未扣」的双写分歧。
func (a *Actor) moduleCtx(actx *actor.Context) *module.Context {
	mctx := &module.Context{
		Actor: actx,
		State: a.state,
		Send:  a.send,
	}
	if a.prefetch != nil {
		mctx.Prefetch = a.prefetch
	}
	mctx.AddExp = a.addExp
	mctx.CacheEnabled = a.repo.CacheEnabled()
	if a.repo != nil && !a.repo.CacheEnabled() {
		mctx.CommitItems = func(ctx context.Context, grants []module.ItemGrant, business func(*sql.Tx) error) error {
			// 先保存已有玩法变更，提交结果未知后重载也不会丢掉此前的背包进度。
			if err := a.repo.Save(ctx, a.state); err != nil {
				return err
			}
			err := a.repo.CommitItems(ctx, a.state, grants, business)
			if errors.Is(err, ErrCommitUnknown) {
				a.needsReload = true
			}
			return err
		}
	}
	mctx.Emit = func(evt module.Event) {
		a.registry.DispatchEvent(mctx, evt)
	}
	return mctx
}

// addExp 增加经验并推送等级变化（任何模块发经验都走这里，保证通知一致）。
func (a *Actor) addExp(n int64) (bool, int32) {
	up, level := a.state.AddExp(n)
	if n > 0 {
		a.state.MarkDirty("profile")
		if a.onDirty != nil {
			a.onDirty(a.playerID)
		}
	}
	if a.sendFunc != nil && a.sessionID != 0 {
		p := a.state.Profile()
		notify := &gamev1.LevelInfoNotify{
			Level:  p.Level,
			Exp:    p.Exp,
			ExpMax: a.state.ExpMax(),
		}
		if raw, err := proto.Marshal(notify); err == nil {
			_ = a.send(msgIDLevelInfoNotify, raw)
		}
	}
	if up {
		a.logger.Info("level up", "player_id", a.playerID, "level", level)
	}
	return up, level
}

// send 包装为模块可用的下行发送（携带当前 sessionID）。
func (a *Actor) send(msgID uint32, body []byte) error {
	if a.bufferReplies {
		a.queuedReplies = append(a.queuedReplies, cachedReply{id: msgID, body: append([]byte(nil), body...)})
		return nil
	}
	if a.sendFunc == nil || a.sessionID == 0 {
		return nil
	}
	return a.sendFunc(a.sessionID, msgID, body)
}

// BindResult 绑定结果。
type BindResult struct {
	Code int32
	Err  string
}
