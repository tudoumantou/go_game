// pkg/gamedata/watch_test.go —— 热更入口（目录监听）的行为测试
//
// tables_test.go 守的是「快照机制」对不对（Replace/LoadDir/Rollback 的语义），
// 本文件守的是「有没有人真的去调用它」——这条链路此前完全没有测试，
// 因为入口根本不存在（见 R16）。
package gamedata

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// validItemJSON 合法的一行 item 配置（字段与 data.go 里的生成结构对齐）。
func validItemJSON(name string) string {
	return `[{"id":1001,"col1":"` + name + `","col2":1,"col3":1}]`
}

// waitFor 轮询等待条件成立；超时即 Fatal。
//
// 热更是异步的（事件 → 去抖 → 加载），不能靠 sleep 猜时间：
// 轮询把「等待多久」变成实现细节，测试只关心最终状态。
func waitFor(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("超时未满足条件：%s", msg)
}

// startWatch 在后台跑 Watch，等监听就绪后返回取消函数。
//
// 必须等就绪：Watch 是异步启动的，若测试在 w.Add 之前就改文件，事件会
// 在注册前发完——那是「漏事件」，不是配置写错，会让用例偶发失败。
func startWatch(t *testing.T, opt WatchOptions) context.CancelFunc {
	t.Helper()
	ready := make(chan struct{})
	inner := opt.OnReady
	opt.OnReady = func() {
		if inner != nil {
			inner()
		}
		close(ready)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Watch(ctx, opt) }()
	select {
	case <-ready:
	case <-time.After(2 * time.Second):
		cancel()
		t.Fatal("监听器未在 2s 内就绪")
	}
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			require.NoError(t, err, "Watch 正常退出应返回 nil")
		case <-time.After(2 * time.Second):
			t.Error("Watch 未在 ctx 取消后退出")
		}
	})
	return cancel
}

// TestWatchAppliesFileChange 端到端：改文件 → 事件 → 去抖 → 新快照生效。
func TestWatchAppliesFileChange(t *testing.T) {
	resetSnapshot(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "item.json")
	require.NoError(t, os.WriteFile(path, []byte(validItemJSON("原始配置")), 0o644))

	before := Version()
	var reloads atomic.Int32
	startWatch(t, WatchOptions{
		Dir:      dir,
		Debounce: 20 * time.Millisecond, // 测试里缩短去抖，避免每用例多等 200ms
		OnReload: func(string, time.Duration) { reloads.Add(1) },
	})

	require.NoError(t, os.WriteFile(path, []byte(validItemJSON("热更后")), 0o644))

	waitFor(t, 3*time.Second, func() bool {
		v, ok := GetItemCfg(1001)
		return ok && v.Col1 == "热更后"
	}, "文件改动后应生效")

	require.NotEqualValues(t, before, Version(), "版本号必须随热更变化")
	require.Positive(t, reloads.Load(), "OnReload 回调应被触发")
}

// TestWatchKeepsSnapshotOnBadJSON 写坏文件是热更最危险的事故：
// 必须保留旧快照、版本不变、并给出错误回调（而不是静默）。
func TestWatchKeepsSnapshotOnBadJSON(t *testing.T) {
	resetSnapshot(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "item.json")
	require.NoError(t, os.WriteFile(path, []byte(validItemJSON("稳定配置")), 0o644))

	var errs atomic.Int32
	startWatch(t, WatchOptions{
		Dir:      dir,
		Debounce: 20 * time.Millisecond,
		OnError:  func(error) { errs.Add(1) },
	})

	beforeVersion := Version()
	beforeName := func() string { v, _ := GetItemCfg(1001); return v.Col1 }()

	require.NoError(t, os.WriteFile(path, []byte(`{这不是合法数组`), 0o644))

	waitFor(t, 3*time.Second, func() bool { return errs.Load() > 0 }, "坏 JSON 应触发 OnError")
	require.EqualValues(t, beforeVersion, Version(), "失败的热更不能改变版本")
	require.EqualValues(t, beforeName, func() string { v, _ := GetItemCfg(1001); return v.Col1 }(),
		"失败的热更不能改变当前快照")
}

// TestWatchIgnoresNonJSONFile 目录里允许混放 README/.bak/.swp：
// 非 .json 的变化不能触发热更（否则每写一行说明就重载一次 33 张表）。
func TestWatchIgnoresNonJSONFile(t *testing.T) {
	resetSnapshot(t)
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "item.json"), []byte(validItemJSON("配置")), 0o644))

	var reloads atomic.Int32
	startWatch(t, WatchOptions{
		Dir:      dir,
		Debounce: 20 * time.Millisecond,
		OnReload: func(string, time.Duration) { reloads.Add(1) },
	})

	require.NoError(t, os.WriteFile(filepath.Join(dir, "README.md"), []byte("说明文件"), 0o644))
	time.Sleep(300 * time.Millisecond)
	require.Zero(t, reloads.Load(), "非 .json 文件不应触发重载")
}

// TestWatchCoalescesRapidWrites 连续多次保存应被合并成少量加载。
//
// 断言用「≤ 写入次数」而不是「= 1」：事件到达时序由内核决定，写成等值断言
// 就是给自己埋一个偶发失败。这里要证明的是「有合并」，不是「合并成一次」。
func TestWatchCoalescesRapidWrites(t *testing.T) {
	resetSnapshot(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "item.json")
	require.NoError(t, os.WriteFile(path, []byte(validItemJSON("v0")), 0o644))

	var reloads atomic.Int32
	startWatch(t, WatchOptions{
		Dir:      dir,
		Debounce: 50 * time.Millisecond,
		OnReload: func(string, time.Duration) { reloads.Add(1) },
	})

	const writes = 5
	for i := 1; i <= writes; i++ {
		require.NoError(t, os.WriteFile(path, []byte(validItemJSON("v"+string(rune('0'+i)))), 0o644))
		time.Sleep(5 * time.Millisecond)
	}

	waitFor(t, 3*time.Second, func() bool { return reloads.Load() > 0 }, "至少应加载一次")
	time.Sleep(300 * time.Millisecond) // 给多余的加载留出发生的时间
	require.LessOrEqual(t, reloads.Load(), int32(writes), "去抖应把多次保存合并")
}

// TestWatchReturnsNilOnCancel 关服时必须能立刻退出，不能拖住关服预算。
func TestWatchReturnsNilOnCancel(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "item.json"), []byte(validItemJSON("配置")), 0o644))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Watch(ctx, WatchOptions{Dir: dir, Debounce: time.Second}) }()
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("ctx 取消后 Watch 未退出（去抖等待没响应取消）")
	}
}

// TestWatchBadDirReturnsError 目录不存在/不可监听：返回错误让调用方降级，
// 而不是 panic 或永久阻塞。
func TestWatchBadDirReturnsError(t *testing.T) {
	err := Watch(context.Background(), WatchOptions{Dir: filepath.Join(t.TempDir(), "nope")})
	require.Error(t, err, "不存在的目录应返回错误")
}

// TestWatchEmptyDirIsNoop 未配置目录时 Watch 直接返回 nil（不注册监听）。
func TestWatchEmptyDirIsNoop(t *testing.T) {
	require.NoError(t, Watch(context.Background(), WatchOptions{}))
}
