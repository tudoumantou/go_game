//go:build integration

// test/integration/leader_test.go —— 中心服选主的真实 failover
//
// 选主是「错了会写坏数据」的那一类：两个实例同时自认主，归档/赛季结算
// 会双写。所以这里用真 etcd 跑完整链路，而不是打桩：
//   - 先启动的实例成主，后启动的从属于它；
//   - 让位（Resign / ctx 取消）后另一个接管；
//   - 状态翻转只回调一次（上层任务循环以回调为唯一切换信号）。
//
// TTL 取 2s 让 failover 在秒级完成；每个用例用独立 key，互不干扰。
package integration

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"game_study/internal/center/leader"
	"game_study/pkg/log"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// startEtcd 启动一个单节点 etcd 容器并返回客户端。
func startEtcd(t *testing.T) *clientv3.Client {
	t.Helper()
	ctx := context.Background()

	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "quay.io/coreos/etcd:v3.5.10",
			ExposedPorts: []string{"2379/tcp"},
			Cmd: []string{
				"/usr/local/bin/etcd",
				"--advertise-client-urls=http://0.0.0.0:2379",
				"--listen-client-urls=http://0.0.0.0:2379",
			},
			WaitingFor: wait.ForListeningPort("2379/tcp").WithStartupTimeout(60 * time.Second),
		},
		Started: true,
	})
	require.NoError(t, err, "start etcd")
	t.Cleanup(func() { _ = c.Terminate(context.Background()) })

	host, err := c.Host(ctx)
	require.NoError(t, err)
	port, err := c.MappedPort(ctx, "2379/tcp")
	require.NoError(t, err)

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{fmt.Sprintf("%s:%s", host, port.Port())},
		DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err, "connect etcd")
	t.Cleanup(func() { _ = cli.Close() })
	return cli
}

// recorder 记录选主回调（回调可能来自任意 goroutine）。
type recorder struct {
	mu     sync.Mutex
	events []bool
}

func (r *recorder) onChange(v bool) {
	r.mu.Lock()
	r.events = append(r.events, v)
	r.mu.Unlock()
}

func (r *recorder) snapshot() []bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]bool(nil), r.events...)
}

// TestLeaderFailover 先到者成主，让位后另一个接管。
func TestLeaderFailover(t *testing.T) {
	cli := startEtcd(t)
	logger := log.New("leader-test", log.WithLevel("error"))
	key := fmt.Sprintf("/test/leader/%d", time.Now().UnixNano())

	recA, recB := &recorder{}, &recorder{}
	a := leader.New(cli, leader.Config{Key: key, SelfID: "A", TTL: 2, OnChange: recA.onChange}, logger)
	b := leader.New(cli, leader.Config{Key: key, SelfID: "B", TTL: 2, OnChange: recB.onChange}, logger)

	ctxA, cancelA := context.WithCancel(context.Background())
	defer cancelA()
	ctxB, cancelB := context.WithCancel(context.Background())
	defer cancelB()

	go func() { _ = a.Run(ctxA) }()
	require.Eventually(t, a.IsLeader, 15*time.Second, 50*time.Millisecond, "A 应先成为主")

	// A 确认是主之后再让 B 参选，避免「谁先到」的时序歧义
	go func() { _ = b.Run(ctxB) }()
	require.Never(t, b.IsLeader, 1500*time.Millisecond, 100*time.Millisecond,
		"A 在位期间 B 不能自认主")

	id, err := a.LeaderID(context.Background())
	require.NoError(t, err)
	require.Equal(t, "A", id, "LeaderID 应是当前主的 SelfID")

	// 让位：ctx 取消 → campaign 内部 Resign，其他实例应立刻接管
	// （不必等满租约，这正是「主动退出要 Resign 而不是干等 TTL」的意义）
	cancelA()
	require.Eventually(t, b.IsLeader, 15*time.Second, 50*time.Millisecond, "A 退出后 B 应接管")
	require.Eventually(t, func() bool { return !a.IsLeader() }, 5*time.Second, 50*time.Millisecond,
		"A 退出后不能再自称主")

	require.Equal(t, []bool{true, false}, recA.snapshot(), "A 的回调必须是「成主→失主」各一次")
	require.Equal(t, []bool{true}, recB.snapshot(), "B 只经历一次成主")

	cancelB()
	require.Eventually(t, func() bool { return !b.IsLeader() }, 5*time.Second, 50*time.Millisecond,
		"B 退出后应让位")
}

// TestResignHandsOverImmediately 主动 Resign 不等租约：另一个实例秒级接管。
//
// 这条是优雅重启的关键：滚动升级时每个实例关停都 Resign，才不会出现
// 「旧实例已退出、新实例还在等 TTL」的有主任务空窗。
func TestResignHandsOverImmediately(t *testing.T) {
	cli := startEtcd(t)
	logger := log.New("leader-test", log.WithLevel("error"))
	key := fmt.Sprintf("/test/leader/resign/%d", time.Now().UnixNano())

	a := leader.New(cli, leader.Config{Key: key, SelfID: "A", TTL: 10}, logger) // TTL 故意拉长
	b := leader.New(cli, leader.Config{Key: key, SelfID: "B", TTL: 10}, logger)

	ctxA, cancelA := context.WithCancel(context.Background())
	defer cancelA()
	ctxB, cancelB := context.WithCancel(context.Background())
	defer cancelB()

	go func() { _ = a.Run(ctxA) }()
	require.Eventually(t, a.IsLeader, 15*time.Second, 50*time.Millisecond, "A 应先成为主")

	go func() { _ = b.Run(ctxB) }()
	require.Never(t, b.IsLeader, time.Second, 100*time.Millisecond)

	start := time.Now()
	require.NoError(t, a.Resign(context.Background()))
	require.Eventually(t, b.IsLeader, 5*time.Second, 50*time.Millisecond, "Resign 后 B 应立即接管")
	require.Less(t, time.Since(start), 10*time.Second,
		"接管耗时应远小于 TTL（10s）——否则说明走的是租约过期而不是主动让位")
}

// TestSingleInstance 单实例：成主、LeaderID 是自己、Stop 后让位。
func TestSingleInstance(t *testing.T) {
	cli := startEtcd(t)
	logger := log.New("leader-test", log.WithLevel("error"))
	key := fmt.Sprintf("/test/leader/single/%d", time.Now().UnixNano())

	e := leader.New(cli, leader.Config{Key: key, SelfID: "solo", TTL: 2}, logger)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- e.Run(ctx) }()

	require.Eventually(t, e.IsLeader, 15*time.Second, 50*time.Millisecond)

	id, err := e.LeaderID(context.Background())
	require.NoError(t, err)
	require.Equal(t, "solo", id)

	e.Stop()
	select {
	case err := <-done:
		require.NoError(t, err, "Stop 后 Run 应正常返回 nil，而不是报错")
	case <-time.After(5 * time.Second):
		t.Fatal("Stop 后 Run 没有退出")
	}
	require.False(t, e.IsLeader())

	// Stop 后再 Run 应立即返回（stopCh 已关闭）
	require.NoError(t, e.Run(context.Background()), "已 Stop 的实例 Run 应立即返回 nil")
}
