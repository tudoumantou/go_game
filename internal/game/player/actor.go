// internal/game/player/actor.go —— 玩家 Actor：生命周期、消息分发与场景接线
//
// 架构职责：
//   - 每个在线玩家恰好一个 Actor，由 pkg/actor 的 mailbox（有界 channel）+
//     分片 goroutine 串行化该玩家的全部消息——业务侧因此完全不用加锁，
//     State 内的 RWMutex 只服务 Actor 外的只读访问（探针/测试）；
//   - 本文件管「进程编排」：Bind/Unbind/保存/发奖/场景心跳/经验；
//     缓存权威模式的「克隆 → 业务 → diff → CAS → 回滚/保留」流程在
//     cache_actor.go，Receive 按 repo.CacheEnabled() 二选一；
//   - 不依赖 scene/gateway 包：场景连接经 SceneLink 注入、下行经 SendFunc
//     注入，反向接线由组装层 internal/game/server.go 完成。
//
// 设计取舍：为什么发奖/移动等「可能失败的写」必须先克隆再业务，见
// cache_actor.go 文件头；为什么登录失败要拒绝上线而不是放行半成品
// Actor，见 onBind 注释。
package player

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"game_study/internal/game/module"
	"game_study/pkg/actor"
	"game_study/pkg/log"
	"game_study/pkg/metrics"

	gamev1 "game_study/pkg/pb/game/v1"
	"google.golang.org/protobuf/proto"
)

// 下行推送协议号（4.9：2011 等级变化）。数值与客户端协议表绑定，
// 改动前先确认 Demo 客户端已同步。
const msgIDLevelInfoNotify uint32 = 2011

// SetKickHook 注入跨节点顶号回调（组装层在 Spawn 前调用）。
//
// 传 nil 表示「不支持跨节点顶号」：收到 KickMsg 时只记日志不处理，
// 用于单机/测试环境——那里不会有别的节点来抢同一个玩家。
func (a *Actor) SetKickHook(fn func(playerID int64, fromNode string)) { a.onKick = fn }

// Message 类型常量。
//
// 字符串即消息的类型标识：actor.System 不做反射分发，这些常量只用于
// 日志与测试断言，实际路由靠 Receive 里的类型 switch。
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

// KickMsg 跨节点顶号通知：让持有该玩家的旧节点踢掉本地会话。
//
// 为什么用 Actor 消息而不是节点级 RPC：踢人是「这个玩家的会话」的事，
// 玩家 Actor 是它的唯一权威（数据所有权）。节点级广播要先查 sid 表，
// 还会绕过 Actor 的串行化——那条路径上的竞态（正在处理业务包时被踢）
// 得用锁重新解决一遍。
//
// 不带 Done channel：远程投递是单向的，发起方（新节点）不等待确认。
// 旧节点即使没收到也不影响正确性——presence 已被强制覆盖，旧 Actor
// 的后续提交会被围栏令牌（epoch）挡掉，最终由 UNBIND 正常下线。
type KickMsg struct {
	// FromNode 发起顶号的节点（日志/排查用）
	FromNode string
}

// RemoteTypeURL 实现 actor.RemoteMessage。
func (KickMsg) RemoteTypeURL() string { return remoteTypeURLKickMsg }

// MarshalRemote 实现 actor.RemoteMessage。
func (m KickMsg) MarshalRemote() ([]byte, error) { return json.Marshal(m) }

// PacketMsg 客户端消息。
// PacketMsg 客户端上行数据包（经网关透传，含业务 protobuf）。
//
// 跨节点可投递（P0）：Kafka 消费（邮件/充值发货）等路径可能把数据包
// 投给在线在别的节点的玩家。
type PacketMsg struct {
	// MsgID 业务协议号，Registry 按它分发到模块。
	MsgID uint32
	// Seq 客户端序号，目前仅透传（回包由模块自行决定是否带回）。
	Seq uint32
	// Body protobuf 包体；解码失败由模块自行处理，玩家层不猜格式。
	Body []byte
}

// RemoteTypeURL 实现 actor.RemoteMessage（编解码注册表的键）。
func (*PacketMsg) RemoteTypeURL() string { return remoteTypeURLPacketMsg }

// MarshalRemote 实现 actor.RemoteMessage（JSON：纯数据，无 channel）。
func (m *PacketMsg) MarshalRemote() ([]byte, error) { return json.Marshal(m) }

// GrantItem 奖励道具。
type GrantItem struct {
	ItemID int32
	Count  int64
}

// PushMsg 向该玩家客户端推送一条消息（跨模块统一出口）。
// PushMsg 下行推送消息（聊天/GM 广播等纯下行路径）。
//
// 跨节点可投递（P0）：实现 actor.RemoteMessage——聊天推送给在线在
// 别的节点的玩家时，消息经中心服中转投递到目标节点的本地 Actor。
type PushMsg struct {
	MsgID uint32
	Body  []byte
}

// RemoteTypeURL 实现 actor.RemoteMessage（编解码注册表的键）。
func (*PushMsg) RemoteTypeURL() string { return remoteTypeURLPushMsg }

// MarshalRemote 实现 actor.RemoteMessage（JSON：纯数据，无 channel）。
func (m *PushMsg) MarshalRemote() ([]byte, error) { return json.Marshal(m) }

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
	// Sync 为真时走同步事务落库，MySQL 提交后才回执（A 类数据：充值、
	// 交易）。为假时保持原行为——改内存 + 标脏，由异步通道批量落库。
	// 判据：这笔奖励丢了是否要赔钱/回滚订单；是就置真。
	// 前置条件：Sync 必须同时给 BizID，否则重投时无幂等键可查，会二次发放。
	Sync bool
	Done chan error // 可选：同步等待结果
}

// SendFunc 下行发送（sessionID + 协议号 + 包体），由组装层注入。
type SendFunc func(sessionID uint64, msgID uint32, body []byte) error

// Actor 玩家 Actor。
//
// 约定：全部字段只在 Actor 自己的分片 goroutine 内读写（mailbox 串行化
// 保证），不存在字段级并发；唯一的例外是 State，它的锁只给外部只读探针。
type Actor struct {
	// playerID 只读标识；logger 已携带该字段，排错时无需再翻调用栈。
	playerID int64
	state    *State
	registry *module.Registry
	logger   *log.Logger
	repo     *Repository
	// preloader 非缓存模式的并行预加载器（结果只在组装 State 时用一次）。
	preloader *Preloader
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

	// onKick 跨节点顶号回调（由组装层注入：关闭该玩家在本节点的网关会话）。
	// 不在这里直接清 sessionID：等网关断连后的 UNBIND 走标准下线流程，
	// 否则会跳过模块 OnLogout 的落库时机。
	onKick func(playerID int64, fromNode string)
	online bool
	// loaded 预加载已完成；false 时丢弃业务包（onPacket）——
	// 用空状态处理业务会写坏数据。
	loaded      bool
	needsReload bool // 发奖提交结果未知时，禁止旧内存再次落库。
	// token 会话所有权令牌（R2.5）。preload 时从 Redis 取得（AcquireLease），
	// 之后每次 Commit 都带上它；被新会话接管后令牌失效，提交会被拒。
	// 与 cacheVersion 的分工：token 决定「我还能不能写」，version 只用于
	// writebehind 的落库确认。
	token int64

	// cacheVersion 本地已知的 Redis 快照版本（仅用于落库确认与日志）；
	// 提交成功则前进为新版本，失败保持旧值（下轮按同一版本重试）。
	cacheVersion int64
	// cacheReload 下一条消息前必须重新 LoadCached：上一次提交已回滚或
	// 结果未知，内存相对 Redis 已过期，先恢复权威状态再进业务。
	cacheReload bool
	// gate 提交通道：缓存提交窗口内的下行先暂存，提交成功才刷出（R2.5）。
	// 提交策略集中在这里，业务与流程都不再感知「延迟发送」的存在。
	gate commitGate

	// self 自己的 PID（Receive 时记录一次）：异步落库完成后要往自己信箱
	// 回投结果，Actor 内部拿不到 PID，只能在消息处理时留一份。
	self *actor.PID
	// post 自投通道（组装层注入 sys.Send）。为空时（单测直接调用）
	// 同步发货退化为内联执行，保持旧语义。
	post func(pid *actor.PID, msg any) error
	// syncBusy 有一笔 A 类落库在跑；syncQueue 是它后面排队的。
	// 同一玩家的同步发货必须串行：并发事务会各自用内存里的 applied_grants
	// 覆盖对方，幂等键丢一个就可能在重投时二次发放。
	syncBusy  bool
	syncQueue []*GrantItemsMsg
}

// NewActor 创建玩家 Actor。
func NewActor(playerID int64, registry *module.Registry, logger *log.Logger, repo *Repository, onDirty func(int64), sendFunc SendFunc) *Actor {
	state := NewState(playerID)
	// 任何变更立刻标记落库（12.4.1 ①）：不这样的话在线玩家数据只在离线时才写，
	// 进程崩溃会丢掉全部在线会话的进度。
	if onDirty != nil {
		// 闭包捕获 playerID：State 会被整体替换（setState），但回调永远
		// 指向同一个玩家，不随 State 实例轮换而失效。
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

// WithPreloader 注入预加载器（12.6）。
func (a *Actor) WithPreloader(p *Preloader) *Actor {
	a.preloader = p
	return a
}

// State 返回玩家状态。
func (a *Actor) State() *State { return a.state }

// Receive 实现 actor.Actor。
//
// 分发逻辑：缓存权威模式下有三条路径——
//   - 纯下行消息（Push/ScenePush/心跳）：不改状态，直接进 receive；
//   - 无状态模块的协议（见 module.StatelessAware）：同样直接进 receive，
//     跳过 clone/CAS（榜单查询、聊天转发这类不碰 State 的请求占相当比例，
//     而 clone 一份含背包/属性/Ext 的 State 并不便宜）；
//   - 其余消息进 receiveCached 的「克隆→业务→CAS」流程。
//
// Go 知识点：Receive 由 actor.System 的分片 goroutine 调用，同一 Actor 的
// 消息在 mailbox channel 上 FIFO 串行——这是「业务无锁」的前提；类型
// switch 的 default 分支兜住未知消息，避免新增消息类型时静默丢包。
func (a *Actor) Receive(ctx *actor.Context) {
	// 记下自己的 PID：异步落库结果要靠它回投（见 syncgrant.go）。
	a.self = ctx.PID()
	// 落库结果不参与「克隆快照 → 提交」流程：它只更新版本、重建缓存、回执。
	if done, ok := ctx.Message().(*grantDoneMsg); ok {
		a.finishSyncGrant(done)
		return
	}
	if a.repo.CacheEnabled() {
		switch msg := ctx.Message().(type) {
		case *PushMsg, *ScenePush, *sceneHeartbeatTick:
			// 纯下行消息不参与「克隆快照 → 提交」流程：它们只读状态、不改状态。
		case *PacketMsg:
			if a.canBypassCache(msg.MsgID) {
				a.receive(ctx)
				return
			}
			a.receiveCached(ctx)
			return
		default:
			a.receiveCached(ctx)
			return
		}
	}
	a.receive(ctx)
}

// canBypassCache 判断一条协议能否跳过缓存快照流程。
//
// 三个前提缺一不可：模块声明无状态（不写 State）；状态已加载（否则处理器
// 读到空背包）；没有待重载（cacheReload 必须走完整流程才能拿回一致版本）。
func (a *Actor) canBypassCache(msgID uint32) bool {
	return a.loaded && !a.cacheReload &&
		a.registry != nil && a.registry.IsStateless(msgID)
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
	case KickMsg:
		if a.onKick != nil {
			a.logger.Info("kicked by remote node", "from", msg.FromNode)
			a.onKick(a.playerID, msg.FromNode)
		}
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
// 失败即回 BindResult{Code:1}：半成品 Actor 上线比拒绝登录更危险——
// 空状态上一条业务包就能写坏数据，且没有任何报错。
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
//
// Go 知识点：自调度用 ScheduleOnce 消息而不是 time.Ticker goroutine——
// 心跳状态活在 Actor 里，不需要额外 goroutine，也就不存在「Actor 停了
// ticker  goroutine 忘记回收」的泄漏路径；间隔到期前的取消由信箱丢弃兜底。
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
//
// 三条路径的差别只在「怎么读」：读到的都是全新 State，统一经 setState 挂回
// dirtyHook；cacheVersion 是后续 CAS 的期望值起点，必须与读到的版本一致。
func (a *Actor) preload(ctx *actor.Context) error {
	if a.repo.CacheEnabled() {
		loadCtx, cancel := context.WithTimeout(context.Background(), DefaultPreloadTimeout)
		defer cancel()
		// 先取会话令牌再读快照：令牌是「我有权写」的凭据，读失败也要先失败，
		// 不能出现「读到了状态但没有令牌」的半就绪态。
		token, err := a.repo.cache.AcquireLease(loadCtx, a.playerID)
		if err != nil {
			return fmt.Errorf("acquire lease: %w", err)
		}
		state, version, err := a.repo.LoadCached(loadCtx, a.playerID)
		if err != nil {
			return err
		}
		a.token = token
		a.setState(state)
		a.cacheVersion = version
		a.loaded = true
		return nil
	}
	if a.preloader != nil {
		state, _, err := a.preloader.Load(context.Background(), a.playerID)
		if err != nil {
			return err
		}
		a.setState(state)
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
//
// BizID 非空时走「与资产同快照」的 A 类幂等；空 BizID（B 类，主动领取）
// 由业务自身保证只领一次，键表不记录——否则表会被领取记录无限撑大。
func (a *Actor) onGrantItems(ctx *actor.Context, msg *GrantItemsMsg) {
	if msg.BizID != "" {
		// A 类幂等（R2.1 §2.2）：键 = source:bizID，与快照同一次 CAS、
		// 同落库到 applied_grants，进程重启后依然有效——充值回调/Kafka
		// 重投不会导致二次发奖。B 类（主动领取）禁止塞这里，会持续膨胀。
		key := msg.Source + ":" + msg.BizID
		// 经 ClaimBiz 而不是直接写 map：它负责「检查 + 置位 + 标脏」三件事，
		// 直接写会漏掉标脏——分字段提交时 grants 字段就不会被写回 Redis，
		// 幂等键丢失，Kafka 重投会二次发奖。
		if !a.state.ClaimBiz(key) {
			// 幂等命中：货已发过。但可能是「上次 DB 提交成功、删缓存失败」
			// 导致的重投——补一次清理，让它自愈。
			if msg.Sync {
				// 自愈：上次「库已提交、删缓存失败」会留下旧副本，必须清掉重写。
				// 只删不重建的话，下一次提交按脏字段拼出来的快照是残缺的
				// （见 resyncCache 的说明）。
				if err := a.rebuildCacheFromDB(); err != nil {
					a.replyGrant(msg, err)
					return
				}
			}
			a.replyGrant(msg, nil)
			return
		}
	}

	for _, it := range msg.Items {
		if it.Count <= 0 {
			continue
		}
		a.state.AddItem(it.ItemID, it.Count)
	}
	if a.onDirty != nil {
		a.onDirty(a.playerID)
	}
	a.logger.Info("grant items", "source", msg.Source, "count", len(msg.Items), "sync", msg.Sync)

	// A 类数据（充值/交易）：MySQL 提交后才回执，不等异步落库窗口。
	if msg.Sync {
		a.grantSync(msg)
		return
	}
	a.replyGrant(msg, nil)
}

// replyGrant 回执发货结果。
//
// 两条路必须分开：异步路径走提交通道（gate），等快照提交成功再回执，
// 避免「回了成功但没落库」的假成功；同步路径自己已经把数据写进
// MySQL，没有任何待提交项，走进 gate 反而会永远等不到刷出。
func (a *Actor) replyGrant(msg *GrantItemsMsg, err error) {
	if msg.Done == nil {
		return
	}
	if msg.Sync {
		msg.Done <- err
		return
	}
	_ = a.gate.Send(func() error { msg.Done <- err; return nil })
}

// resyncCache 用当前内存重建热快照（Drop 后立刻 Seed 一整份）。
//
// 为什么不能只 Drop：Drop 之后的下一次提交只写「脏字段」（cache_actor 的
// DirtyFields），于是 hash 会以「有 profile、没 items」的残缺形态重建。
// 残缺比缺失更难发现——跨玩家只读（PeerReader）读到的是空背包，而不是
// 可回源的 miss。重建一整份的代价只有一次 HSET 批量，换来副本永远完整。
//
// 失败语义：先 Drop 后 Seed，Seed 失败时 key 仍处于删除态（下一次加载走
// 冷启动从库读，跨玩家读回源库），不会留下半份数据。
func (a *Actor) resyncCache(version int64) error {
	if a.repo == nil || a.repo.cache == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := a.repo.cache.Drop(ctx, a.playerID); err != nil {
		return err
	}
	rec, err := a.repo.cache.Seed(ctx, a.state.snapshot(), version)
	if err != nil {
		return err
	}
	a.cacheVersion = rec.Version
	return nil
}

// rebuildCacheFromDB 读库里的已落库版本，再用内存重建快照。
//
// 用于「上次同步落库成功、但删缓存失败」的自愈路径：本地的 cacheVersion
// 可能落后于库，以库为准重建才不会让下一次加载反复判定「副本落后」。
func (a *Actor) rebuildCacheFromDB() error {
	if a.repo == nil || a.repo.cache == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ver, err := a.repo.writebehindVersion(ctx, a.playerID)
	if err != nil {
		return err
	}
	return a.resyncCache(ver)
}

// onSave 执行保存并回执真实结果；没有 Done 时保持 fire-and-forget。
func (a *Actor) onSave(ctx *actor.Context, msg *SaveMsg) {
	replySave(msg.Done, a.savePlayer(ctx))
}

// savePlayer 真正执行落库，返回第一个错误（模块钩子或 SQL 失败）。
//
// 内存权威：这里就是所有玩家数据的落库入口，不管缓存开没开。数据源是
// Actor 内存（a.state），Redis 副本落后于内存也不影响——落库以内存为准。
func (a *Actor) savePlayer(ctx *actor.Context) error {
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
//
// Go 知识点：select + default 的非阻塞发送。调用方超时离开后通道无人读，
// 若用阻塞发送，这条迟到回执会把 Actor 的 goroutine 卡死在下一个消息上。
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
	mctx.AddExp = a.addExp
	mctx.CacheEnabled = a.repo.CacheEnabled()
	if a.repo != nil {
		// 跨玩家只读：模块拿到的只有 View/ViewMany，拿不到别人的 State
		mctx.Peers = a.repo
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
//
// bufferReplies 置位时（缓存提交窗口）消息不直发，先入 queuedReplies——
// 顺序即产生顺序，CAS 成功后再按序补发（cache_actor.go）。
func (a *Actor) send(msgID uint32, body []byte) error {
	if !a.gate.Active() {
		return a.directSend(msgID, body)
	}
	// 入队必须拷贝：业务层的 body 多来自可复用的解码缓冲，
	// 不拷贝的话窗口内后续消息会把内容改写。
	cp := append([]byte(nil), body...)
	return a.gate.Send(func() error { return a.directSend(msgID, cp) })
}

// directSend 立即写回网关（sessionID 为 0 时静默丢弃）。
func (a *Actor) directSend(msgID uint32, body []byte) error {
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
