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

// tryOnlineScript 原子占位：旧值存在且属于其它节点时拒绝，否则写入本节点。
//
// 用 GET + SET 的管道做不到「判断与写入」原子，两台游戏服同时登录同一角色时
// 会双双成功、各自认为是权威会话。这里用 Lua 把两步合成一次执行。
var tryOnlineScript = redis.NewScript(`
local cur = redis.call('GET', KEYS[1])
if cur and cur ~= ARGV[1] then
  return {0, cur}
end
redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
return {1, ARGV[1]}
`)

// TrySetOnline 原子占用在线归属。
//
// 返回 ok=false 时 owner 是当前持有该角色的节点；同节点重连（旧会话未过期）
// 视为成功，允许本节点顶掉自己的旧连接。
func (p *Presence) TrySetOnline(ctx context.Context, playerID int64, nodeID string, ttl time.Duration) (ok bool, owner string, err error) {
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	res, err := tryOnlineScript.Run(ctx, p.client, []string{OnlineKey(playerID)}, nodeID, ttl.Milliseconds()).Slice()
	if err != nil {
		return false, "", err
	}
	if len(res) != 2 {
		return false, "", fmt.Errorf("unexpected presence result: %v", res)
	}
	// ok 由 0/1 表示；owner 可能是 string 或 []byte（不同客户端实现）。
	code, _ := res[0].(int64)
	switch v := res[1].(type) {
	case string:
		owner = v
	case []byte:
		owner = string(v)
	}
	return code == 1, owner, nil
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
