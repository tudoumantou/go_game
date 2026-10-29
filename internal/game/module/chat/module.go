// internal/game/module/chat/chat.go —— 聊天模块（私聊全链路，17.3）
//
// 架构职责：私聊的「发送 → 投递 → 历史」全链路，对应设计文档 17.3。
//  1. 发送：onSend 校验 → 生成全局 msgID → 本端落 out 历史 → 按对端在线状态三路投递；
//  2. 投递：deliver 三路——本服在线直推 / 跨服走 Kafka / 离线 Redis 暂存；
//  3. 历史：onHistory 倒序查库、正序回包；跨服来讯由 DeliverInbound 落 in 历史。
//
// Ext 槽位：无。聊天没有玩家快照内状态——历史是 MySQL 直写（Repo），
// 在线判定/暂存走 Redis（Messenger），模块本身只做编排，不持有状态。
//
// 事件订阅：无（聊天不参与升级/任务等事件流）。
//
// 配置表关系：无（频道开放范围是代码常量 channelPrivate，P6 仅私聊）。
//
// 懒初始化：不适用（无 Ext 行概念；DB 缺失时 repo 可为 nil，模块降级为只校验不回包）。
package chat

import (
	"context"
	"time"

	"game_study/internal/game/module"
	"game_study/pkg/log"
	"game_study/pkg/ratelimit"
)

// chatRepo 是模块对历史仓储的全部依赖（接口隔离）。
//
// 抽成接口只为一件事：让「频道校验」「三路投递」「方向还原」这些纯编排逻辑
// 能在内存里测，不必为一条断言拉起 MySQL。*Repo 隐式实现它，装配层不变。
type chatRepo interface {
	// Ready 数据库是否可用；无 DB 环境时模块降级回包（见 module.Shared）。
	Ready() bool
	Insert(ctx context.Context, ownerID, peerID, msgID int64, direction int32, channel int32, content string) error
	ListRecent(ctx context.Context, ownerID, peerID int64, limit int) ([]ChatLine, error)
}

// Module 聊天模块。
//
// 无玩家态：所有状态在 MySQL（历史）与 Redis（在线/暂存），模块只做编排，
// 因此 OnMessage 在玩家 Actor 内执行但不读写任何共享结构，无需加锁。

// Module 聊天模块。
//
// 无玩家态：所有状态在 MySQL（历史）与 Redis（在线/暂存），模块只做编排，
// 因此 OnMessage 在玩家 Actor 内执行但不读写任何共享结构，无需加锁。
type Module struct {
	*module.Thin

	// repo 历史仓储；nil 表示无 DB 环境（降级：只校验 + 投递，不留历史）
	repo      chatRepo
	messenger Messenger
	logger    *log.Logger
	// chatLimiter 发言频控（按 playerID 分桶，nil = 不限流）。
	//
	// 为什么需要独立于网关的包级限流：包限流是「总量」，聊天是「单条成本高」
	// ——每条要写 MySQL、推给对手、进 Kafka。一个玩家每秒 1 条、连发一小时，
	// 包限流完全放行，但已经写了 3600 行库、推了 3600 次。
	chatLimiter *ratelimit.Keyed
}

// ready 数据库是否可用。
//
// 两种「不可用」都要算：仓储压根没注入（无 DB 环境的模块级降级，repo 是
// nil 接口），以及仓储在但没配连接池（NewRepo(nil)，Ready() 为 false）。
// 只判前者会让后者变成 nil deref，只判后者会对 nil 接口调用方法——
// 所以这里是唯一一处同时判两者的地方，业务只写 if !m.ready()。
func (m *Module) ready() bool { return m.repo != nil && m.repo.Ready() }

// SetChatLimiter 覆盖默认频控（装配期调用；nil 表示关闭）。
//
// 默认在 New 里建（1 条/秒、突发 3）：即使装配层忘了配，也有基本防护。
func (m *Module) SetChatLimiter(l *ratelimit.Keyed) { m.chatLimiter = l }

// New 创建聊天模块（messenger 可为 nil：单机降级为只落库）。
func New(repo chatRepo, messenger Messenger, logger *log.Logger) *Module {
	m := &Module{
		repo:      repo,
		messenger: messenger,
		logger:    logger,
		// 默认 1 条/秒、突发 3：连发三句还来得及，第四句开始挡。
		chatLimiter: ratelimit.NewKeyed(1, 3, 10*time.Minute),
	}
	m.Thin = module.NewThin("chat",
		module.ThinLogger(logger),
		module.ThinLogout(m.onLogout),
	)
	module.Handle(m.Thin, m.onSend)
	module.Handle(m.Thin, m.onHistory)
	return m
}

// onLogout 下线时触发一次常规 flush 周期。
//
// 聊天无 Ext 快照，这里 MarkDirty 只是让 Persister 走一次常规周期
// （保持与其他模块的生命周期行为一致，无害）；真正要落的历史在 MySQL，
// 发送时已直写，不依赖登出触发。
func (m *Module) onLogout(ctx *module.Context) error {
	ctx.State.MarkDirty("chat")
	return nil
}

// DeliverInbound 投递一条「收到」方向的跨服私聊（Kafka 消费调用）。
//
// 由 chat_cross_consumer 在「本地性判定 + msg_id 幂等」通过后调用，
// 职责只有落 in 方向历史；repo 为 nil（无 DB 环境）时静默跳过——
// 历史是尽力而为的旁路数据，不因缺库阻断 Kafka 消费。
