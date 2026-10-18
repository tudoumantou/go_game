// pkg/cache/idem.go —— 基于 Redis SETNX 的消费幂等实现（pkg/mq.Deduper）
package cache

import (
	"context"
	"fmt"
	"time"

	"game_study/pkg/mq"

	"github.com/redis/go-redis/v9"
)

// RedisDeduper Redis 幂等去重器。
type RedisDeduper struct {
	client redis.UniversalClient
}

// NewRedisDeduper 创建去重器。
func NewRedisDeduper(client redis.UniversalClient) *RedisDeduper {
	return &RedisDeduper{client: client}
}

// 编译期断言：RedisDeduper 实现 mq.Deduper。
var _ mq.Deduper = (*RedisDeduper)(nil)

// FirstTime 实现 mq.Deduper（SET NX）。
func (d *RedisDeduper) FirstTime(ctx context.Context, topic, bizID string, ttl time.Duration) (bool, error) {
	if bizID == "" {
		return false, fmt.Errorf("biz_id is empty")
	}
	if ttl <= 0 {
		ttl = mq.DefaultIdemTTL
	}
	ok, err := d.client.SetNX(ctx, mq.IdemKey(topic, bizID), 1, ttl).Result()
	if err != nil {
		return false, fmt.Errorf("idem setnx %s: %w", bizID, err)
	}
	return ok, nil
}

// 编译期断言：实现 mq.Forgetter。
var _ mq.Forgetter = (*RedisDeduper)(nil)

// Forget 删除幂等标记（处理失败回滚，允许重投）。
func (d *RedisDeduper) Forget(ctx context.Context, topic, bizID string) error {
	return d.client.Del(ctx, mq.IdemKey(topic, bizID)).Err()
}
