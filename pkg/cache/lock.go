// pkg/cache/lock.go —— 基于 Redis 的分布式锁
//
// 架构职责：跨进程互斥（如全服结算、排行榜归档、发奖批处理），保证
// 同一时刻只有一个实例执行临界区。
// 协作关系：key 由 keygen.LockKey 生成；与 idem.go 同出 SETNX 思路，
// 但多了 token 归属校验与显式 Unlock。
// 关键取舍：SETNX + TTL（非 Redlock 完整算法）——TTL 兜底防持锁者
// 崩溃导致死锁；token 保证「谁加的锁谁才能解」。已知边界：非可重入、
// 无自动续期，临界区长于 TTL 会失效——当前业务场景（短临界区）够用，
// 需要续期时应上门狗（watchdog）机制。
package cache

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Lock 简单分布式锁（SET NX + 过期时间）。
//
// 实例轻量（三个字段），但一个 Lock 对象只代表「一次锁竞争」：
// TryLock 成功后本对象持有 token，Unlock 必须用**同一个实例**——
// token 存在实例里，换实例就解不了这把锁。
type Lock struct {
	client redis.UniversalClient
	key    string
	token  string
}

// NewLock 创建锁对象。
//
// resource 是业务资源名（如 "season:reward:2026S1"），token 是持锁者
// 标识（建议实例ID+随机数）——解锁时校验，防止解掉别人的锁。
func NewLock(client redis.UniversalClient, resource string, token string) *Lock {
	return &Lock{
		client: client,
		key:    LockKey(resource),
		token:  token,
	}
}

// TryLock 尝试加锁，成功返回 true。
//
// 非阻塞：抢不到立即返回 false（调用方选择退出/稍后重试），不轮询
// 等待——把等待策略留给调用方，锁本身保持简单。
// ttl 必须显式设置：持锁者若崩溃，TTL 到期后锁自动释放，否则这条
// resource 将永远锁死（分布式锁最常见的故障模式）。
// Go/Redis 知识点：SET key val NX PX ttl 是单命令完成「不存在才写 +
// 过期时间」，原子性由 Redis 单线程执行模型保证，不需要 Lua。
func (l *Lock) TryLock(ctx context.Context, ttl time.Duration) (bool, error) {
	ok, err := l.client.SetNX(ctx, l.key, l.token, ttl).Result()
	if err != nil {
		return false, fmt.Errorf("lock %s: %w", l.key, err)
	}
	return ok, nil
}

// Unlock 释放锁（仅当 token 匹配）。
//
// 为什么用 Lua 而不是 GET 判断 + DEL：两条命令之间有时延，锁可能已
// 过期并被别人重新拿到，裸 DEL 会解掉新持锁者的锁；Lua 脚本在 Redis
// 里**单线程原子执行**，「比对与删除」之间无插入窗口。
// token 不匹配时脚本返回 0（静默不解锁）——不是错误，是设计：说明
// 锁已过期易主，调用方本就不该再解。
func (l *Lock) Unlock(ctx context.Context) error {
	script := `
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
else
    return 0
end`
	return l.client.Eval(ctx, script, []string{l.key}, l.token).Err()
}
