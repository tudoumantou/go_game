// pkg/mq/idempotent.go —— 消费幂等工具（设计文档 13.4）
// 约定：消费端先按 biz_id 去重，已处理直接 ack，避免「至少一次」语义下的重复应用。
//
// 架构职责：定义去重的接口契约（Deduper/Forgetter）与键格式，Redis 具体实现
// 由 internal 层注入——本包不 import redis，保持可测可替换。
// 协作关系：game 服的邮件/充值消费者在业务处理前调 FirstTime，失败时调
// Forget 回滚标记；pkg/mq 的 Consumer 只负责重试与死信，不感知去重。
// 设计取舍：
//   - 为什么用 Redis 而不是进程内 map：Kafka 至少一次语义下重投可能落在
//     任意实例，去重状态必须跨进程共享；进程内 map 多实例各去各的，等于没去。
//     Redis 的 SETNX + TTL 天然适合：原子判定、自动过期、重启不丢。
//   - 为什么接口而不是具体类型：本地开发/单测用 NoopDeduper 免依赖 Redis，
//     生产换存储（如 MySQL 唯一键）时业务代码零改动。
//   - at-least-once + 幂等 vs at-most-once：Kafka 手动提交只能二选一，
//     项目选「宁可重投、不可丢失」，重复由本层消除——金融类业务（充值）
//     丢消息的代价远大于重复处理的代价。
package mq

import (
	"context"
	"fmt"
	"time"
)

// DefaultIdemTTL 幂等键保留时长（13.4：24h）。
// 取值依据：消费重试 + 死信补偿 + 人工重投的窗口通常远小于 24h，覆盖即可；
// 同时 24h 后自动过期，避免 idem:* 键无限堆积占内存。
const DefaultIdemTTL = 24 * time.Hour

// Deduper 幂等去重接口。
//
// 契约：FirstTime 必须是「判定 + 占位」的原子操作（Redis SETNX 语义），
// 返回 true 表示调用方获得处理权；并发下同一 biz_id 只能有一个 true。
// 实现方出现存储故障时返回 error（而非 false），让消费路径走重试而非跳过。
type Deduper interface {
	// FirstTime 首次处理返回 true；已处理返回 false。
	// ttl 由调用方按 topic 传入（默认 DefaultIdemTTL），便于不同业务
	// 设置不同保留期；实现方应把 ttl 透传给存储的过期时间。
	FirstTime(ctx context.Context, topic, bizID string, ttl time.Duration) (bool, error)
}

// Forgetter 幂等键回滚（处理失败时删除标记，允许重投重新处理）。
// 独立成接口而非并进 Deduper：NoopDeduper 不需要实现它，
// 接口拆分（ISP）让「可选能力」通过 Forget 里的类型断言按需发现。
type Forgetter interface {
	Forget(ctx context.Context, topic, bizID string) error
}

// Forget 若实现 Forgetter 则回滚幂等标记。
//
// 用于业务处理失败的补偿路径：删掉 FirstTime 写下的标记，让重投的消息
// 能被重新判定为「首次」并再次处理。
// 时序很关键：标记写在业务前，业务失败若不删，重投会被 FirstTime 挡在门外，
// 消息等于永久丢失——这正是「至少一次」语义下必须配的手动回滚。
func Forget(ctx context.Context, d Deduper, topic, bizID string) {
	if f, ok := d.(Forgetter); ok {
		// 回滚失败不向上传染：标记最坏也会随 TTL 自然过期，
		// 不会把「可恢复的清理失败」升级成消费中断。
		_ = f.Forget(ctx, topic, bizID)
	}
}

// NoopDeduper 不去重实现（本地开发/单测用）。
// 空结构体零分配；值 receiver 即可满足接口，调用方传 NoopDeduper{} 不用取地址。
type NoopDeduper struct{}

// FirstTime 实现 Deduper。
// 恒定返回 true：本地开发/单测场景不去重，重复投递会重复执行业务，
// 换来的是不依赖 Redis 也能跑通链路。
// _ = xxx 显式丢弃参数：表明「故意不用」而非漏写，也让 lint 闭嘴。
func (NoopDeduper) FirstTime(ctx context.Context, topic, bizID string, ttl time.Duration) (bool, error) {
	_ = ctx
	_ = topic
	_ = bizID
	_ = ttl
	return true, nil
}

// IdemKey 生成幂等键：idem:{topic}:{biz_id}。
// 带 topic 前缀的原因：不同 topic 的 biz_id 空间相互独立（订单号 vs 邮件号
// 可能撞车），隔离后互不串扰；统一前缀方便 Redis 侧按模式扫描/巡检/清理。
func IdemKey(topic, bizID string) string {
	return fmt.Sprintf("idem:%s:%s", topic, bizID)
}
