// internal/game/player/actor.go —— 玩家 Actor
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
	SessionID uint64
	PlayerID  int64
}

// UnbindMsg 解绑消息。
type UnbindMsg struct {
	SessionID uint64
}

// PacketMsg 客户端消息。
type PacketMsg struct {
	MsgID uint32
	Seq   uint32
	Body  []byte
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
	playerID  int64
	state     *State
	registry  *module.Registry
	logger    *log.Logger
	repo      *Repository
	preloader *Preloader
	prefetch  *Prefetch
	onDirty   func(playerID int64)
	sendFunc  SendFunc

	sessionID     uint64
	online        bool
	loaded        bool
	needsReload   bool // 发奖提交结果未知时，禁止旧内存再次落库。
	cacheVersion  int64
	cacheReload   bool
	bufferReplies bool
	queuedReplies []cachedReply
	cachePending  *writebehind.Record
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

// WithPreloader 注入预加载器（12.6：一次往返拿完全部登录数据）。
func (a *Actor) WithPreloader(p *Preloader) *Actor {
	a.preloader = p
	return a
}

// State 返回玩家状态。
func (a *Actor) State() *State { return a.state }

// Receive 实现 actor.Actor。
func (a *Actor) Receive(ctx *actor.Context) {
	if a.repo.CacheEnabled() {
		if _, purePush := ctx.Message().(*PushMsg); !purePush {
			a.receiveCached(ctx)
			return
		}
	}
	a.receive(ctx)
}

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
	default:
		a.logger.Debug("unknown message", "type", fmt.Sprintf("%T", msg))
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

	mctx := a.moduleCtx(ctx)
	if err := a.registry.LoginAll(mctx); err != nil {
		a.logger.Error("module login failed", "err", err)
		_ = ctx.Respond(&BindResult{Code: 1, Err: err.Error()})
		return
	}
	a.logger.Info("player online", "session_id", msg.SessionID)
	_ = ctx.Respond(&BindResult{Code: 0})
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

func (a *Actor) onUnbind(ctx *actor.Context, msg *UnbindMsg) {
	if !a.online {
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
