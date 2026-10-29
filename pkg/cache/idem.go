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

// FirstTime 原子占位：key 不存在时写入并返回 true（首次），已存在返回 false。
//
// # 为什么单独抽成函数
//
// 「SETNX 抢一个标记」在项目里有三处：Kafka 消费去重（RedisDeduper）、
// 排行榜结算去重（center/rank）、里程碑展开去重（center/milestone）。
// 三处都写 `SetNX(ctx, key, 1, ttl)`，但错误语义各不相同——一处打 Warn 丢掉
// 上报，一处 `err != nil || !ok { continue }` 把「Redis 故障」和「确定重复」
// 混成一件事。
//
// 混在一起的后果不对称：Redis 故障时按「重复」处理，等于这条业务永久跳过；
// 而按「失败」处理至少还能靠重投补回来。所以本函数把两种情况分开返回，
// 调用方必须显式决定故障时的策略。
//
// 参数：key 由调用方按各自领域构造（三处的 key 格式不同，且短期内不能统一
// ——rank 的 key 改了会让线上正在生效的去重窗口重置，重复加分无法自动修正）。
// ttl <= 0 时不设过期（不推荐：key 会永久堆积，只适合调用方另有清理机制的场景）。
//
// 返回：true = 首次（调用方执行业务）；false = 已存在（确定重复）；
// error = 不知道（Redis 故障，不是重复）。
func FirstTime(ctx context.Context, client redis.UniversalClient, key string, ttl time.Duration) (bool, error) {
	ok, err := client.SetNX(ctx, key, 1, ttl).Result()
	if err != nil {
		// %w 包装：调用方可 errors.Is 判底层 Redis 错误类型（超时/连接）。
		return false, fmt.Errorf("setnx %s: %w", key, err)
	}
	return ok, nil
}

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
	// 与 rank/milestone 共用同一个原语：占位语义只有一处实现，
	// 「false 是确定重复、error 是不知道」这条区分也只有一处定义。
	return FirstTime(ctx, d.client, mq.IdemKey(topic, bizID), ttl)
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
