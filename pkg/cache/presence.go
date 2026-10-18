// pkg/cache/presence.go —— 在线状态与离线暂存（设计文档 13.3 模式① / ②）
package cache

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Presence 在线状态与离线暂存。
type Presence struct {
	client redis.UniversalClient
}

// NewPresence 创建 Presence。
func NewPresence(client redis.UniversalClient) *Presence {
	return &Presence{client: client}
}

// SetOnline 标记玩家在线（value = 所在节点 ID）。
func (p *Presence) SetOnline(ctx context.Context, playerID int64, nodeID string, ttl time.Duration) error {
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	return p.client.Set(ctx, OnlineKey(playerID), nodeID, ttl).Err()
}

// RefreshOnline 续期在线状态。
func (p *Presence) RefreshOnline(ctx context.Context, playerID int64, ttl time.Duration) error {
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	return p.client.Expire(ctx, OnlineKey(playerID), ttl).Err()
}

// IsOnline 查询玩家在线节点；ok=false 表示离线或状态过期。
func (p *Presence) IsOnline(ctx context.Context, playerID int64) (nodeID string, ok bool) {
	val, err := p.client.Get(ctx, OnlineKey(playerID)).Result()
	if err != nil {
		return "", false
	}
	return val, val != ""
}

// SetOffline 清除在线状态。
func (p *Presence) SetOffline(ctx context.Context, playerID int64) error {
	return p.client.Del(ctx, OnlineKey(playerID)).Err()
}

// PendPush 离线暂存（List，capped maxLen，TTL 7d）。
func (p *Presence) PendPush(ctx context.Context, biz string, playerID int64, value []byte, maxLen int64, ttl time.Duration) error {
	if maxLen <= 0 {
		maxLen = 200
	}
	if ttl <= 0 {
		ttl = 7 * 24 * time.Hour
	}
	key := pendKey(biz, playerID)
	pipe := p.client.TxPipeline()
	pipe.RPush(ctx, key, value)
	pipe.LTrim(ctx, key, -maxLen, -1)
	pipe.Expire(ctx, key, ttl)
	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("pend push %s: %w", key, err)
	}
	return nil
}

// PendDrain 取出并清空暂存消息（登录 drain）。
func (p *Presence) PendDrain(ctx context.Context, biz string, playerID int64) ([][]byte, error) {
	key := pendKey(biz, playerID)
	pipe := p.client.TxPipeline()
	rangeCmd := pipe.LRange(ctx, key, 0, -1)
	pipe.Del(ctx, key)
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, fmt.Errorf("pend drain %s: %w", key, err)
	}
	vals, err := rangeCmd.Result()
	if err != nil {
		return nil, err
	}
	out := make([][]byte, 0, len(vals))
	for _, v := range vals {
		out = append(out, []byte(v))
	}
	return out, nil
}

// PendLen 暂存数量（运维/监控用）。
func (p *Presence) PendLen(ctx context.Context, biz string, playerID int64) (int64, error) {
	return p.client.LLen(ctx, pendKey(biz, playerID)).Result()
}

func pendKey(biz string, playerID int64) string {
	return fmt.Sprintf("pend:%s:%d", biz, playerID)
}
