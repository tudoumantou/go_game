// internal/login/mq/role_created_consumer.go —— 角色索引同步消费者（gs.role.created）
//
// 架构职责：把游戏服的创角事实写进登录服的 user_role 索引表，供选角界面
// 「按账号列出各服角色」。角色真源在游戏服库，本表是展示副本——查得到是
// 目标，查不到就是玩家投诉「我的角色没了」。
//
// 协作关系：游戏服 role_sync_outbox 投递协程生产；本消费者 UPSERT
// repo.UserRoleRepo（内部按 account_id%15 选分片并删缓存）。
//
// 设计取舍：
//   - 不用 Deduper：user_role 的主键 (account_id, player_id) + uk_player
//     本身就是跨实例、跨重启的幂等键。再加一层 Redis SETNX 只是把
//     「重复消息」换成「标记丢失后重复消息」——DB 唯一键比它更可靠，
//     而且少一个 Redis 依赖（本服 Redis 挂掉时选角索引照常同步）。
//   - 消费失败一律上抛：DB 抖动值得重试；解码失败重试无意义，但把它
//     留在这里判断会让「什么样的错该重试」散落到各处，交给
//     pkgmq.Consumer 的重试/死信策略统一处理。
//   - 单实例消费组（与 delivered 消费者同款 kafkaGroup）：user_role 只写
//     登录服库，多实例并发 UPSERT 虽然幂等，但会让分片内行锁竞争；
//     高可用靠 outbox 重投而非消费组扩并发。
package mq

import (
	"context"
	"fmt"

	"game_study/pkg/log"
	"game_study/pkg/model"
	pkgmq "game_study/pkg/mq"
)

// RoleIndexWriter 角色索引写入能力（*repo.UserRoleRepo 实现；测试注入内存实现）。
//
// 定成接口而不是直接依赖 *repo.UserRoleRepo：本包只关心「写一行索引」，
// 不关心分片路由与缓存删除——那是 repo 的职责，换成分片数/换缓存实现
// 都不该改到这里。
type RoleIndexWriter interface {
	Upsert(ctx context.Context, m *model.UserRole) error
}

// RoleCreatedConsumer 角色索引同步消费者。
type RoleCreatedConsumer struct {
	consumer *pkgmq.Consumer
	repo     RoleIndexWriter
	logger   *log.Logger
}

// NewRoleCreatedConsumer 创建消费者。
func NewRoleCreatedConsumer(consumer *pkgmq.Consumer, repo RoleIndexWriter, logger *log.Logger) *RoleCreatedConsumer {
	return &RoleCreatedConsumer{consumer: consumer, repo: repo, logger: logger}
}

// Run 阻塞消费直到 ctx 取消。
func (c *RoleCreatedConsumer) Run(ctx context.Context) error {
	return c.consumer.Run(ctx, c.handle)
}

// Close 关闭消费者（提交 offset 后退出）。
func (c *RoleCreatedConsumer) Close() error {
	if c.consumer == nil {
		return nil
	}
	return c.consumer.Close()
}

// handle 写入一条角色索引。
//
// 校验语义：PlayerId/AccountID 是「写到哪一行」的定位信息，缺一即非法消息，
// 返回 error 进死信留痕——静默跳过等于放弃一个玩家的角色可见性，
// 而且没有任何日志能定位到是谁。
func (c *RoleCreatedConsumer) handle(ctx context.Context, env *pkgmq.Envelope) error {
	var msg pkgmq.RoleCreatedMsg
	if err := env.DecodePayload(&msg); err != nil {
		return fmt.Errorf("decode role created: %w", err)
	}
	if msg.PlayerID <= 0 || msg.AccountID <= 0 {
		return fmt.Errorf("invalid role created payload: player=%d account=%d",
			msg.PlayerID, msg.AccountID)
	}
	if err := c.repo.Upsert(ctx, &model.UserRole{
		AccountId: msg.AccountID,
		PlayerId:  msg.PlayerID,
		ServerId:  msg.ServerID,
		Nickname:  msg.Nickname,
		Level:     msg.Level,
	}); err != nil {
		return fmt.Errorf("upsert user_role player=%d: %w", msg.PlayerID, err)
	}
	c.logger.Info("role index synced",
		"player_id", msg.PlayerID, "account_id", msg.AccountID, "server_id", msg.ServerID)
	return nil
}
