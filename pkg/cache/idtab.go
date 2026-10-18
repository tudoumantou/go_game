// pkg/cache/idtab.go —— ID 分配表（redis INCR）
package cache

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// IDTab 基于 Redis INCR 的 ID 分配器。
//
// 注意与 idtable.go 的 IDTable 区分：
//   - IDTab  —— **分配**新 ID（Redis String INCR，key = seq:{name}）
//   - IDTable —— **存在性**判断（Redis Set，key = idtab:{table}）
//
// 两者 key 前缀不同，避免 Redis 类型冲突（String vs Set）。
type IDTab struct {
	client redis.UniversalClient
	name   string
	start  int64
}

// NewIDTab 创建 ID 分配器。name 是 ID 域（player/account/...）。
func NewIDTab(client redis.UniversalClient, name string, start int64) *IDTab {
	return &IDTab{client: client, name: name, start: start}
}

// Next 分配下一个 ID。
func (t *IDTab) Next(ctx context.Context) (int64, error) {
	key := SeqKey(t.name)
	// 首次分配时用 start 初始化（SETNX 保证并发安全）
	if t.start > 0 {
		_ = t.client.SetNX(ctx, key, t.start-1, 0).Err()
	}
	id, err := t.client.Incr(ctx, key).Result()
	if err != nil {
		return 0, fmt.Errorf("idtab %s: %w", t.name, err)
	}
	return id, nil
}
