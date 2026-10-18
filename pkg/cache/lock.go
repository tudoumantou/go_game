// pkg/cache/lock.go —— 基于 Redis 的分布式锁
package cache

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Lock 简单分布式锁（SET NX + 过期时间）。
type Lock struct {
	client redis.UniversalClient
	key    string
	token  string
}

// NewLock 创建锁对象。
func NewLock(client redis.UniversalClient, resource string, token string) *Lock {
	return &Lock{
		client: client,
		key:    LockKey(resource),
		token:  token,
	}
}

// TryLock 尝试加锁，成功返回 true。
func (l *Lock) TryLock(ctx context.Context, ttl time.Duration) (bool, error) {
	ok, err := l.client.SetNX(ctx, l.key, l.token, ttl).Result()
	if err != nil {
		return false, fmt.Errorf("lock %s: %w", l.key, err)
	}
	return ok, nil
}

// Unlock 释放锁（仅当 token 匹配）。
func (l *Lock) Unlock(ctx context.Context) error {
	script := `
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
else
    return 0
end`
	return l.client.Eval(ctx, script, []string{l.key}, l.token).Err()
}
