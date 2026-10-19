// internal/game/mq/loginlog_consumer.go —— 登录流水落库（gs.login.log，R3.2）
package mq

import (
	"context"
	"fmt"

	"game_study/pkg/db"
	"game_study/pkg/log"
	pkgmq "game_study/pkg/mq"
)

// LoginLogConsumer 把登录流水写入 login_log 表。
//
// 与其它消费者不同，它使用**集群共享 group**（group.go 里不带实例后缀）：
// 每实例独立消费会让同一行流水被所有游戏服实例各写一次。
// 也不做业务去重：流水是事实记录，重投产生重复行不影响玩家状态。
type LoginLogConsumer struct {
	consumer *pkgmq.Consumer
	db       *db.DB
	logger   *log.Logger
}

// NewLoginLogConsumer 创建消费者。
func NewLoginLogConsumer(consumer *pkgmq.Consumer, database *db.DB, logger *log.Logger) *LoginLogConsumer {
	return &LoginLogConsumer{consumer: consumer, db: database, logger: logger}
}

// Run 阻塞消费。
func (c *LoginLogConsumer) Run(ctx context.Context) error {
	return c.consumer.Run(ctx, c.handle)
}

// Close 关闭。
func (c *LoginLogConsumer) Close() error {
	if c.consumer == nil {
		return nil
	}
	return c.consumer.Close()
}

func (c *LoginLogConsumer) handle(ctx context.Context, env *pkgmq.Envelope) error {
	var msg pkgmq.LoginLogMsg
	if err := env.DecodePayload(&msg); err != nil {
		return fmt.Errorf("decode login log: %w", err)
	}
	if msg.AccountID <= 0 {
		return fmt.Errorf("invalid login log: account_id=%d", msg.AccountID)
	}
	if c.db == nil {
		return fmt.Errorf("login log consumer without database")
	}
	_, err := c.db.ExecContext(ctx,
		`INSERT INTO login_log (account_id, player_id, ip, device, result, reason) VALUES (?, ?, ?, ?, ?, ?)`,
		msg.AccountID, msg.PlayerID, msg.IP, msg.Device, msg.Result, msg.Reason)
	if err != nil {
		return fmt.Errorf("insert login_log: %w", err)
	}
	return nil
}
