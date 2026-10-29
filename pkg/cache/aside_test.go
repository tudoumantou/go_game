// pkg/cache/aside_test.go —— cache-aside 的四个判断
//
// 这四条用例对应 aside.go 顶部列的四个判断。它们不是「功能测试」而是
// 「语义测试」：三个仓储共用一个实现之后，这些语义只需要在一处钉死。
package cache

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// asidePayload 测试用载荷（任意可 JSON 化的结构）。
type asidePayload struct {
	Name string `json:"name"`
}

// TestAsideMissLoadsAndBackfills 未命中 → 回源 → 回填；第二次不再回源。
func TestAsideMissLoadsAndBackfills(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	var loads int
	load := func() (*asidePayload, bool, error) {
		loads++
		return &asidePayload{Name: "db"}, true, nil
	}

	got, err := Aside(ctx, rdb, "acc:1", time.Minute, load)
	require.NoError(t, err)
	require.Equal(t, "db", got.Name)
	require.Equal(t, 1, loads)

	got, err = Aside(ctx, rdb, "acc:1", time.Minute, load)
	require.NoError(t, err)
	require.Equal(t, "db", got.Name)
	require.Equal(t, 1, loads, "第二次必须命中缓存，不再回源")

	require.True(t, mr.Exists("acc:1"), "回源结果必须回填")
}

// TestAsideCorruptCacheFallsBackToLoad 坏缓存当 miss，不把读变成错误。
//
// 这是最容易写反的一条：把反序列化失败当错误返回，会让「缓存里一条脏
// 数据」升级成「这个页面对所有人 500」，而且要等 TTL 过期才自愈。
func TestAsideCorruptCacheFallsBackToLoad(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()
	require.NoError(t, mr.Set("acc:1", "not-json"))

	got, err := Aside(ctx, rdb, "acc:1", time.Minute,
		func() (*asidePayload, bool, error) { return &asidePayload{Name: "db"}, true, nil })
	require.NoError(t, err, "坏缓存不能变成读失败")
	require.Equal(t, "db", got.Name)
}

// TestAsideNilRDBReadsThrough 没有 Redis 时退化为直读，不 panic。
func TestAsideNilRDBReadsThrough(t *testing.T) {
	got, err := Aside[int](context.Background(), nil, "k", time.Minute,
		func() (int, bool, error) { return 42, true, nil })
	require.NoError(t, err)
	require.Equal(t, 42, got)
}

// TestAsideLoadErrorNotCached 回源失败原样上抛，且不写缓存。
//
// 把失败当成「查到空值」缓存起来，会让一次 DB 抖动变成 TTL 之内的
// 持续空结果——比直接报错难发现得多。
func TestAsideLoadErrorNotCached(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	boom := errors.New("db down")
	_, err := Aside(ctx, rdb, "acc:1", time.Minute,
		func() (*asidePayload, bool, error) { return nil, true, boom })
	require.ErrorIs(t, err, boom)
	require.False(t, mr.Exists("acc:1"), "回源失败不能留下缓存")
}

// TestAsideCacheableFalseSkipsBackfill 空结果按调用方意愿决定是否回填。
//
// 「懒得缓存空值」和「空值就是答案」是两种业务语义，helper 不替调用方猜：
// cacheable=false 时不写，true 时写。
func TestAsideCacheableFalseSkipsBackfill(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	_, err := Aside(ctx, rdb, "acc:1", time.Minute,
		func() (*asidePayload, bool, error) { return nil, false, nil })
	require.NoError(t, err)
	require.False(t, mr.Exists("acc:1"), "cacheable=false 时不应回填")

	_, err = Aside(ctx, rdb, "acc:2", time.Minute,
		func() (*asidePayload, bool, error) { return &asidePayload{}, true, nil })
	require.NoError(t, err)
	require.True(t, mr.Exists("acc:2"), "cacheable=true 时空值也要回填")
}
