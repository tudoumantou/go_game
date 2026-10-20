// internal/game/mq/group.go —— 游戏服 Kafka 消费者的统一入口
//
// 架构职责：按配置创建全部消费者，统一管理它们的并发启停
// （Run 起 goroutine、Close 等待全部退出）。
//
// 协作关系：由 server.go 的 Start ⑤ 创建并 Run、Shutdown 阶段 Close；
// 消费者实现分布在本目录（activity/chat_cross 等）。
//
// 设计取舍：
//   - 消费组语义按 topic 分两种：业务 topic（充值/邮件/私聊/活动调度）用
//     **每实例独立 group**（groupID 带 hostname+pid 后缀）→ 广播语义，
//     每个节点都收到全量再自行本地性判定；登录流水 topic 用**集群共享
//     group** → 同一行日志只被一个实例写一次。混用两种语义是刻意的：
//     前者要求「人人可见」，后者要求「恰好一次落库」；
//   - 无 Kafka 配置时返回空 Group 而非错误：单测/演示环境不起消费者
//     是合法状态，调用方（server.go）无需分支处理。
package mq

import (
	"context"
	"fmt"
	"os"
	"sync"

	activitymod "game_study/internal/game/module/activity"
	"game_study/pkg/config"
	"game_study/pkg/log"
	pkgmq "game_study/pkg/mq"
)

// Consumer 消费者统一接口。
//
// Run 阻塞、Close 幂等：Group 对两者都只调一次，但 Close 在
// 「Run 已自行退出」的场景也要安全（Close 内部已判 nil）。
type Consumer interface {
	Run(ctx context.Context) error
	Close() error
}

// Group 消费者组。
type Group struct {
	consumers []Consumer
	logger    *log.Logger
	// wg 跟踪 Run 起的全部 goroutine：Close 必须等它们真正退出，
	// 否则「关闭完成」后仍有 consumer 在读 Kafka
	wg sync.WaitGroup
}

// NewGroup 按配置创建全部消费者（每实例独立 group → 广播语义，13.3）。
func NewGroup(cfg *config.Config, deps Deps) (*Group, error) {
	if len(cfg.Kafka.Brokers) == 0 {
		return &Group{logger: deps.Logger}, nil
	}

	instanceID := instanceID()
	groupPrefix := cfg.Kafka.ConsumerGroup
	if groupPrefix == "" {
		groupPrefix = "game-server"
	}
	// 闭包捕获 instanceID/groupPrefix：每个 topic 生成一个全局唯一的
	// group ID——这是「广播消费」的全部机制，Kafka 侧无需任何特殊配置
	groupID := func(topic string) string {
		// 每个 topic 独立 group，且带实例后缀 → 每实例都收到全量消息
		return fmt.Sprintf("%s-%s-%s", groupPrefix, topic, instanceID)
	}

	producer := deps.Producer
	mkConsumer := func(topic string) *pkgmq.Consumer {
		return pkgmq.NewConsumer(pkgmq.Config{
			Brokers: cfg.Kafka.Brokers,
			// 注意：单实例内部广播语义要求 group 唯一，故不使用共享 ConsumerGroup
		}, topic, groupID(topic), producer)
	}

	g := &Group{logger: deps.Logger}
	g.consumers = append(g.consumers,
		NewRechargeConsumer(mkConsumer(pkgmq.TopicRechargeOrder), deps),
		NewMailConsumer(mkConsumer(pkgmq.TopicMail), deps),
		NewChatConsumer(mkConsumer(pkgmq.TopicChatCross), deps),
		// 活动实例调度：写入进程级注册表，供活动模块校验开放时间
		NewActivityConsumer(mkConsumer(pkgmq.TopicActivitySchedule), activitymod.DefaultRegistry(), deps.Logger),
	)
	// 登录流水用集群共享 group：每实例独立消费会让同一行日志被写多份。
	if deps.DB != nil {
		loginLog := pkgmq.NewConsumer(pkgmq.Config{Brokers: cfg.Kafka.Brokers},
			pkgmq.TopicLoginLog, groupPrefix+"-login-log", producer)
		g.consumers = append(g.consumers, NewLoginLogConsumer(loginLog, deps.DB, deps.Logger))
	}
	return g, nil
}

// Run 并发启动所有消费者。
//
// Go 知识点：
//   - c := c 是循环变量快照（Go 1.22 前必需；本仓库按老语义写，保持
//     一致性更稳）——没有它，所有 goroutine 会共享同一个循环变量，
//     可能全部启动最后一个消费者；
//   - wg.Add 在 go 语句之前、循环体内调用：若放进 goroutine，会有
//     「Wait 已返回、Add 才执行」的竞态导致 panic（Add during wait）。
func (g *Group) Run(ctx context.Context) {
	for _, c := range g.consumers {
		c := c
		g.wg.Add(1)
		go func() {
			defer g.wg.Done()
			if err := c.Run(ctx); err != nil && ctx.Err() == nil {
				g.logger.Error("consumer stopped", "err", err)
			}
		}()
	}
}

// Close 关闭所有消费者。
//
// 顺序：先逐个 Close（触发 Kafka 消费者退出），再 wg.Wait（等所有
// goroutine 真正返回）——反过来的话 Wait 会永远完不成。
func (g *Group) Close() error {
	var firstErr error
	for _, c := range g.consumers {
		if err := c.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	g.wg.Wait()
	return firstErr
}

// Len 消费者数量。
func (g *Group) Len() int { return len(g.consumers) }

// instanceID 本实例标识（hostname + pid），用作 group 后缀保证全局唯一。
func instanceID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	return fmt.Sprintf("%s-%d", host, os.Getpid())
}
