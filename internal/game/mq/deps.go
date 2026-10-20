// internal/game/mq/deps.go —— 游戏服 Kafka 消费者的共享依赖（依赖注入点）
//
// 架构职责：把消费者需要的全部外部能力收拢成一个 Deps 值对象，由
// group.go 在装配时从 server.go 的组件里摘出来。
//
// 协作关系：server.go 的 Start ⑤ 构造 Deps 并传入 gamemq.NewGroup；
// 各消费者只认识这里声明的窄接口，不认识 player.Manager 等具体类型。
//
// 设计取舍：
//   - 为什么 Presence/PlayerRouter/Mailer 是接口而 Chat 是具体类型：
//     前三者在测试里要换内存实现（无 Redis/无 DB 环境跑消费者），
//     chat 模块的投递路径与 DB 强耦合，测试直接建真模块更省事；
//   - 编译期断言（var _ PlayerRouter = ...）是防御性契约：player.Manager
//     若改了签名，这里编译期就炸，而不是运行期消费者才发现。
package mq

import (
	"context"
	"time"

	"game_study/internal/game/module/chat"
	"game_study/internal/game/module/mail"
	"game_study/internal/game/player"
	"game_study/pkg/actor"
	"game_study/pkg/db"
	"game_study/pkg/log"
	pkgmq "game_study/pkg/mq"
)

// Presence 在线状态接口（*cache.Presence 实现；测试可注入内存实现）。
type Presence interface {
	IsOnline(ctx context.Context, playerID int64) (nodeID string, ok bool)
	PendPush(ctx context.Context, biz string, playerID int64, value []byte, maxLen int64, ttl time.Duration) error
}

// PlayerRouter 玩家 Actor 路由接口（*player.Manager 实现）。
type PlayerRouter interface {
	Get(playerID int64) (*actor.PID, bool)
	GetOrSpawn(playerID int64) (*actor.PID, error)
}

// 编译期断言：*player.Manager 实现 PlayerRouter。
var _ PlayerRouter = (*player.Manager)(nil)

// Mailer 邮件模块能力（*mail.Module 实现；测试可注入内存实现）。
//
// Deliver 投递 Kafka 邮件；DeliverOffline 用于 Tier-1 发放（充值到账）在玩家
// 离线或不在本服时落一封 biz_id 幂等的邮件，保证奖励不因不在线而丢。
type Mailer interface {
	Deliver(ctx context.Context, msg *pkgmq.MailMsg) (int64, error)
	DeliverOffline(ctx context.Context, playerID int64, bizID, title, content string, items []mail.Attachment) (bool, error)
}

// Deps 消费者共享依赖。
//
// 语义分层：Deduper/DB/Presence/Producer 是基础设施；Players/Sys 是 Actor
// 路由；Mail/Chat 是业务模块出口；Logger/NodeID 是身份与观测。
// 未配置的项为 nil（如无 Redis时 Presence=nil），各消费者自行判空降级。
type Deps struct {
	Deduper  pkgmq.Deduper
	DB       *db.DB
	Presence Presence
	Players  PlayerRouter
	Sys      *actor.System
	Producer *pkgmq.Producer
	Mail     Mailer
	Chat     *chat.Module
	Logger   *log.Logger
	NodeID   string // 本节点 ID（在线状态归属判定）
}
