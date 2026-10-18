// internal/game/mq/deps.go —— 游戏服 Kafka 消费者依赖
package mq

import (
	"context"
	"time"

	"game_study/internal/game/module/chat"
	"game_study/internal/game/module/mail"
	"game_study/internal/game/player"
	"game_study/pkg/actor"
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

// Deps 消费者共享依赖。
type Deps struct {
	Deduper  pkgmq.Deduper
	Presence Presence
	Players  PlayerRouter
	Sys      *actor.System
	Producer *pkgmq.Producer
	Mail     *mail.Module
	Chat     *chat.Module
	Logger   *log.Logger
	NodeID   string // 本节点 ID（在线状态归属判定）
}
