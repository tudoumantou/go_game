// presence_test.go —— R3.1 跨服在线归属的原子占位
package cache

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func newTestPresence(t *testing.T) (*Presence, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })
	return NewPresence(rdb), mr
}

// TestTrySetOnlineFirstClaim 空状态可以被任意节点占用。
func TestTrySetOnlineFirstClaim(t *testing.T) {
	p, _ := newTestPresence(t)
	ok, owner, err := p.TrySetOnline(context.Background(), 1, "game-1", time.Minute)
	require.NoError(t, err, fmt.Sprintf("first claim: ok=%v owner=%q err=%v", ok, owner, err))
	require.True(t, ok)
	require.EqualValues(t, "game-1", owner)
	node, online := p.IsOnline(context.Background(), 1)
	require.True(t, online, fmt.Sprintf("presence not set: %q %v", node, online))
	require.EqualValues(t, "game-1", node)
}

// TestTrySetOnlineBlocksOtherNode 别的节点持有时拒绝，并返回真实持有者。
func TestTrySetOnlineBlocksOtherNode(t *testing.T) {
	p, _ := newTestPresence(t)
	ctx := context.Background()
	ok, _, err := p.TrySetOnline(ctx, 1, "game-1", time.Minute)
	require.NoError(t, err, "setup claim failed")
	require.True(t, ok)
	ok, owner, err := p.TrySetOnline(ctx, 1, "game-2", time.Minute)
	require.NoError(t, err)
	require.False(t, ok || owner != "game-1", fmt.Sprintf("other node must be rejected: ok=%v owner=%q", ok, owner))
	// 归属没有被改写
	node, _ := p.IsOnline(ctx, 1)
	require.EqualValues(t, "game-1", node, fmt.Sprintf("ownership overwritten: %q", node))
}

// TestTrySetOnlineAllowsSameNodeReconnect 同节点重连（旧会话未过期）必须放行，
// 否则玩家掉线重连会被自己的残留在线状态挡住。
func TestTrySetOnlineAllowsSameNodeReconnect(t *testing.T) {
	p, _ := newTestPresence(t)
	ctx := context.Background()
	ok, _, err := p.TrySetOnline(ctx, 1, "game-1", time.Minute)
	require.NoError(t, err, "setup claim failed")
	require.True(t, ok)
	ok, owner, err := p.TrySetOnline(ctx, 1, "game-1", time.Minute)
	require.NoError(t, err, fmt.Sprintf("same node reclaim: ok=%v owner=%q err=%v", ok, owner, err))
	require.True(t, ok)
	require.EqualValues(t, "game-1", owner)
}

// TestTrySetOnlineAfterExpiry 持有者过期后可以被其它节点接手。
func TestTrySetOnlineAfterExpiry(t *testing.T) {
	p, mr := newTestPresence(t)
	ctx := context.Background()
	ok, _, err := p.TrySetOnline(ctx, 1, "game-1", time.Second)
	require.NoError(t, err, "setup claim failed")
	require.True(t, ok)
	mr.FastForward(2 * time.Second)
	ok, _, err = p.TrySetOnline(ctx, 1, "game-2", time.Minute)
	require.NoError(t, err, fmt.Sprintf("expired ownership should be claimable: ok=%v err=%v", ok, err))
	require.True(t, ok)
}
