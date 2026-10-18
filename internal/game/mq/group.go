// internal/game/mq/group.go —— 游戏服消费者统一入口
package mq

import (
	"context"
	"fmt"
	"os"
	"sync"

	"game_study/pkg/config"
	"game_study/pkg/log"
	pkgmq "game_study/pkg/mq"
)

// Consumer 消费者统一接口。
type Consumer interface {
	Run(ctx context.Context) error
	Close() error
}

// Group 消费者组。
type Group struct {
	consumers []Consumer
	logger    *log.Logger
	wg        sync.WaitGroup
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
	)
	return g, nil
}

// Run 并发启动所有消费者。
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

func instanceID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	return fmt.Sprintf("%s-%d", host, os.Getpid())
}
