// pkg/cache/idem.go —— 基于 Redis SETNX 的消费幂等实现（pkg/mq.Deduper）
//
// 架构职责：给消息消费者提供「这条业务消息我处理过没有」的判断，防止
// Kafka at-least-once 投递导致的重复到账/重复发奖。
// 协作关系：实现 pkg/mq 的 Deduper/Forgetter 接口（接口定义在 mq 包，
// 本包只依赖接口——消费者注入实现，单测可替换）；幂等 key 与默认 TTL
// 也来自 mq 包（IdemKey/DefaultIdemTTL），与 Forget 的调用方共用约定。
// 关键取舍：SETNX + TTL 的单命令原子占位——不需要 Lua，也不需要
// 「先 GET 再 SETNX」的两步（两步之间有竞态窗口，重复消息会漏进去）。
package cache

import (
	"context"
	"fmt"
	"time"

	"game_study/pkg/mq"

	"github.com/redis/go-redis/v9"
)

// RedisDeduper Redis 幂等去重器。
//
// 无状态薄封装：只持有 client，可被多个消费 goroutine 并发使用——
// go-redis client 的连接池自身并发安全，SETNX 又是 Redis 单命令执行，
// 「抢注」的原子性由 Redis 单线程模型保证。
type RedisDeduper struct {
	client redis.UniversalClient
}

// NewRedisDeduper 创建去重器。
//
// client 一般为 cache.Client.GetClient() 的返回值——复用同一连接池，
// 不为去重单独建连接。
func NewRedisDeduper(client redis.UniversalClient) *RedisDeduper {
	return &RedisDeduper{client: client}
}

// 编译期断言：RedisDeduper 实现 mq.Deduper。
//
// Go 知识点：var _ Interface = (*T)(nil) 是惯用的编译期接口检查——赋值
// 给空标识符不产生任何运行期代码，接口未实现时编译直接失败，比「运行
// 到一半才发现没实现」便宜得多。
var _ mq.Deduper = (*RedisDeduper)(nil)

// FirstTime 实现 mq.Deduper（SET NX）。
//
// 语义：bizID 首次出现返回 true（调用方应执行业务），已存在返回 false
// （重复消息，调用方应确认丢弃）。
// 参数契约：topic 隔离业务域，bizID 是业务唯一键（如订单号）；ttl 是
// 幂等窗口，<=0 时取 mq.DefaultIdemTTL。
// 窗口取舍：窗口内重复被拦、窗口外允许重投（异常重试最终能通过），
// 窗口长度需大于「消息最大重投间隔」，否则重投会被误判为重复。
// 错误语义：Redis 故障返回 error——false 与 error 必须区分：false 是
// 「确定重复」，error 是「不知道」，调用方的处理策略完全不同。
func (d *RedisDeduper) FirstTime(ctx context.Context, topic, bizID string, ttl time.Duration) (bool, error) {
	// bizID 为空 = 调用方 bug：空键会把所有 bizID 为空的消息判为重复，
	// 且 key 形态异常难以排查，直接拒绝。
	if bizID == "" {
		return false, fmt.Errorf("biz_id is empty")
	}
	if ttl <= 0 {
		ttl = mq.DefaultIdemTTL
	}
	ok, err := d.client.SetNX(ctx, mq.IdemKey(topic, bizID), 1, ttl).Result()
	if err != nil {
		// %w 包装底层错误：调用方可用 errors.Is 判 Redis 错误类型
		return false, fmt.Errorf("idem setnx %s: %w", bizID, err)
	}
	return ok, nil
}

// 编译期断言：实现 mq.Forgetter。
var _ mq.Forgetter = (*RedisDeduper)(nil)

// Forget 删除幂等标记（处理失败回滚，允许重投）。
//
// 调用时机：业务处理**失败**时——否则重投会被 FirstTime 判重而永久
// 跳过，消息丢失。因此「什么算处理失败」必须由调用方严格定义。
// 注意竞态：Forget 之后、重投到达之前，另一条携带相同 bizID 的重复
// 消息可能抢先 FirstTime 成功——本工具解决的是「串行重投」场景，
// 并发双投需要业务层额外兜底。
func (d *RedisDeduper) Forget(ctx context.Context, topic, bizID string) error {
	return d.client.Del(ctx, mq.IdemKey(topic, bizID)).Err()
}
