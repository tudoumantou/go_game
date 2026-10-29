package cache

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// newTestClient 起一个内存 Redis 并返回连上它的 client。
func newTestClient(t *testing.T) (*miniredis.Miniredis, redis.UniversalClient) {
	t.Helper()
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })
	return mr, c
}

// TestFirstTimeOnlyOnce 首次 true、重复 false。
func TestFirstTimeOnlyOnce(t *testing.T) {
	_, c := newTestClient(t)
	ctx := context.Background()

	ok, err := FirstTime(ctx, c, "test:once", time.Minute)
	require.NoError(t, err)
	require.True(t, ok, "第一次应为首次")

	ok, err = FirstTime(ctx, c, "test:once", time.Minute)
	require.NoError(t, err)
	require.False(t, ok, "第二次应为重复")
}

// TestFirstTimeExpires 过期后同一个键重新变成「首次」。
//
// 这是幂等窗口的定义：窗口内重复被拦，窗口外允许重投——窗口必须长于
// 消息最大重投间隔，否则重投会被误判成重复。
func TestFirstTimeExpires(t *testing.T) {
	mr, c := newTestClient(t)
	ctx := context.Background()

	ok, err := FirstTime(ctx, c, "test:ttl", time.Minute)
	require.NoError(t, err)
	require.True(t, ok)

	mr.FastForward(2 * time.Minute)

	ok, err = FirstTime(ctx, c, "test:ttl", time.Minute)
	require.NoError(t, err)
	require.True(t, ok, "TTL 过后应重新可占位")
}

// TestFirstTimeDistinguishesErrorFromDuplicate Redis 故障必须返回 error，
// 不能返回 (false, nil)。
//
// 这条是本函数存在的理由：调用方按 false 处理是「确定重复，跳过」，
// 按 error 处理是「不知道，可重试」。混成 false 会让一次 Redis 抖动
// 变成一串静默丢弃的业务。
func TestFirstTimeDistinguishesErrorFromDuplicate(t *testing.T) {
	mr, c := newTestClient(t)
	mr.Close() // 关掉服务端：后续命令连接失败

	ok, err := FirstTime(context.Background(), c, "test:down", time.Minute)
	require.Error(t, err, "Redis 不可用必须返回 error")
	require.False(t, ok)
}
