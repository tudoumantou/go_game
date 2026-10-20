// pkg/mq/idempotent.go —— 消费幂等工具（设计文档 13.4）
// 约定：消费端先按 biz_id 去重，已处理直接 ack，避免「至少一次」语义下的重复应用。
package mq

import (
	"context"
	"fmt"
	"time"
)

// DefaultIdemTTL 幂等键保留时长（13.4：24h）。
const DefaultIdemTTL = 24 * time.Hour

// Deduper 幂等去重接口。
type Deduper interface {
	// FirstTime 首次处理返回 true；已处理返回 false。
	FirstTime(ctx context.Context, topic, bizID string, ttl time.Duration) (bool, error)
}

// Forgetter 幂等键回滚（处理失败时删除标记，允许重投重新处理）。
type Forgetter interface {
	Forget(ctx context.Context, topic, bizID string) error
}

// Forget 若实现 Forgetter 则回滚幂等标记。
//
// 用于业务处理失败的补偿路径：删掉 FirstTime 写下的标记，让重投的消息
// 能被重新判定为「首次」并再次处理。
func Forget(ctx context.Context, d Deduper, topic, bizID string) {
	if f, ok := d.(Forgetter); ok {
		// 回滚失败不向上传染：标记最坏也会随 TTL 自然过期，
		// 不会把「可恢复的清理失败」升级成消费中断。
		_ = f.Forget(ctx, topic, bizID)
	}
}

// NoopDeduper 不去重实现（本地开发/单测用）。
type NoopDeduper struct{}

// FirstTime 实现 Deduper。
func (NoopDeduper) FirstTime(ctx context.Context, topic, bizID string, ttl time.Duration) (bool, error) {
	// 恒定返回 true：本地开发/单测场景不去重，重复投递会重复执行业务，
	// 换来的是不依赖 Redis 也能跑通链路。
	_ = ctx
	_ = topic
	_ = bizID
	_ = ttl
	return true, nil
}

// IdemKey 生成幂等键：idem:{topic}:{biz_id}。
func IdemKey(topic, bizID string) string {
	return fmt.Sprintf("idem:%s:%s", topic, bizID)
}
