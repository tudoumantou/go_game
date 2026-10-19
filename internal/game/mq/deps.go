// internal/game/mq/deps.go —— 游戏服 Kafka 消费者依赖
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
