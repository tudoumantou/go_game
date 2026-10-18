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
func Forget(ctx context.Context, d Deduper, topic, bizID string) {
	if f, ok := d.(Forgetter); ok {
		_ = f.Forget(ctx, topic, bizID)
	}
}

// NoopDeduper 不去重实现（本地开发/单测用）。
type NoopDeduper struct{}

// FirstTime 实现 Deduper。
func (NoopDeduper) FirstTime(ctx context.Context, topic, bizID string, ttl time.Duration) (bool, error) {
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
