// internal/game/player/manager_resolve_test.go —— Resolve/GetOrSpawn 跨节点解析单测（P0）
//
// 覆盖三条路径：
//  1. 本地命中直接返回（不碰 presence，热路径零 Redis）；
//  2. 本地未命中 + 归属表显示在别的节点 → 返回远程 PID（不本地 Spawn，
//     双 Actor 双写分叉是跨节点第一事故）；
//  3. 本地未命中 + 不在线 → ok=false；presence 未注入时同样退化为
//     纯本地查询（单机部署/测试环境不炸）。
package player

import (
	"context"
	"errors"
	"testing"

	"game_study/pkg/actor"
	"game_study/pkg/log"

	"github.com/stretchr/testify/require"
)

// fakePresence 内存在线归属表（测试替身）。
type fakePresence struct {
	// owner 玩家 → 节点；不在表内即离线。
	owner map[int64]string
	// hits 记录 IsOnline 被调用的次数，用来证明「本地命中不问 presence」。
	hits int
	// err 非 nil 时所有查询返回错误（降级路径：视为不在线）。
	err error
}

func (f *fakePresence) IsOnline(_ context.Context, playerID int64) (string, bool) {
	f.hits++
	if f.err != nil {
		return "", false
	}
	node, ok := f.owner[playerID]
	return node, ok
}

func newTestManager(t *testing.T) (*Manager, *actor.System) {
	t.Helper()
	sys := actor.NewSystem(actor.Config{ShardCount: 1, MailboxSize: 8})
	m := NewManager(sys, nil, log.New("test"), nil, nil, nil)
	t.Cleanup(sys.Stop)
	return m, sys
}

// TestResolveLocalHit 本地命中时不问 presence（热路径零 IO）。
func TestResolveLocalHit(t *testing.T) {
	m, _ := newTestManager(t)
	local := &actor.PID{ID: "player:42"}
	m.mu.Lock()
	m.index[42] = local
	m.mu.Unlock()

	presence := &fakePresence{owner: map[int64]string{42: "game-2"}}
	m.SetNodeID("game-1")
	m.SetPresence(presence)

	pid, ok := m.Resolve(context.Background(), 42)
	require.True(t, ok, "本地命中应解析成功")
	require.Same(t, local, pid, "本地命中应返回本地 PID")
	require.Zero(t, presence.hits, "本地命中不应查询 presence")
}

// TestResolveRemoteNode 本地未命中 + 在线在别节点 → 远程 PID。
func TestResolveRemoteNode(t *testing.T) {
	m, _ := newTestManager(t)
	m.SetNodeID("game-1")
	m.SetPresence(&fakePresence{owner: map[int64]string{42: "game-2"}})

	pid, ok := m.Resolve(context.Background(), 42)
	require.True(t, ok, "玩家在线在 game-2，应解析成功")
	require.Equal(t, "game-2", pid.Node, "PID.Node 应为归属节点")
	require.Equal(t, playerPID(42), pid.ID, "PID.ID 应与本地 Spawn key 一致")
}

// TestResolveOffline 不在线任何节点 → ok=false。
func TestResolveOffline(t *testing.T) {
	m, _ := newTestManager(t)
	m.SetNodeID("game-1")
	m.SetPresence(&fakePresence{owner: map[int64]string{}})

	_, ok := m.Resolve(context.Background(), 42)
	require.False(t, ok, "玩家不在线，Resolve 应返回 ok=false")
}

// TestResolveNoPresence presence 未注入时退化为纯本地查询（不 panic）。
func TestResolveNoPresence(t *testing.T) {
	m, _ := newTestManager(t)
	_, ok := m.Resolve(context.Background(), 42)
	require.False(t, ok, "无 presence 且本地未命中，应返回 ok=false")
}

// TestGetOrSpawnRemoteGuard 本地未命中 + 在线在别节点 → 返回远程 PID
// 且不本地 Spawn（防双 Actor 双写分叉）。
func TestGetOrSpawnRemoteGuard(t *testing.T) {
	m, _ := newTestManager(t)
	m.SetNodeID("game-1")
	m.SetPresence(&fakePresence{owner: map[int64]string{42: "game-2"}})

	pid, err := m.GetOrSpawn(context.Background(), 42)
	require.NoError(t, err)
	require.Equal(t, "game-2", pid.Node, "应返回远程 PID")
	require.Zero(t, m.Count(), "远程归属时不得本地 Spawn")
}

// TestGetOrSpawnPresenceError presence 查询报错时按「不在线」处理：
// 宁可本地拉起一个可能已存在的 Actor（旧行为），也不阻断登录。
func TestGetOrSpawnPresenceError(t *testing.T) {
	m, _ := newTestManager(t)
	m.SetNodeID("game-1")
	m.SetPresence(&fakePresence{err: errors.New("redis down")})

	// presence 故障 → 守卫放行 → 走本地 Spawn。这里 repo 为 nil 不会炸：
	// Spawn 只登记 Actor，真正加载发生在首条消息（Bind）时。
	pid, err := m.GetOrSpawn(context.Background(), 42)
	require.NoError(t, err, "presence 故障应降级为本地 Spawn")
	require.Empty(t, pid.Node, "降级路径应 Spawn 本地 Actor")
	require.Equal(t, 1, m.Count(), "降级后应持有本地 Actor")
}
