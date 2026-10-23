// r93_scene_test.go —— R9.3：重连主动同步与心跳
package player

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"game_study/pkg/actor"
	"github.com/stretchr/testify/require"
)

// fakeSceneLink 记录玩家 Actor 发起的场景动作。
type fakeSceneLink struct {
	mu         sync.Mutex
	heartbeats []int32 // 心跳携带的 sceneID
	resyncs    []int32
}

func (f *fakeSceneLink) Heartbeat(playerID int64, sceneID int32) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.heartbeats = append(f.heartbeats, sceneID)
}

func (f *fakeSceneLink) Resync(playerID int64, sceneID int32, pid *actor.PID) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.resyncs = append(f.resyncs, sceneID)
}

func (f *fakeSceneLink) counts() (int, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.resyncs), len(f.heartbeats)
}

func (f *fakeSceneLink) lastResync() int32 {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.resyncs) == 0 {
		return -1
	}
	return f.resyncs[len(f.resyncs)-1]
}

// 注意：System 的定时器按 100ms 粒度扫描（timerLoop 的 ticker），
// 因此测试里的心跳间隔不能小于 100ms，否则永远等不到触发。生产用 30s 不受影响。

// TestBindTriggersResyncAndHeartbeat 绑定时主动同步一次，并按间隔持续心跳。
//
// 主动同步的意义：断线期间视野里可能有人进出，客户端并不知道；
// 等客户端发现通知断档再拉，会先经历一段「看不到人」的窗口。
func TestBindTriggersResyncAndHeartbeat(t *testing.T) {
	rig := newSpendRig(t, nil)
	link := &fakeSceneLink{}
	rig.actor.WithSceneLink(link)
	rig.actor.heartbeatEvery = 100 * time.Millisecond
	rig.actor.state.SetSceneID(3)

	err := rig.sys.Send(rig.pid, &BindMsg{SessionID: 1, PlayerID: 7})
	require.NoError(t, err, fmt.Sprintf("send bind: %v", err))

	// 等 Resync（绑定路径是同步执行的，这里给一点余量）。
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if n, _ := link.counts(); n > 0 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	n, _ := link.counts()
	require.EqualValues(t, 1, n, fmt.Sprintf("resync 次数 = %d, want 1", n))
	got := link.lastResync()
	require.EqualValues(t, 3, got, fmt.Sprintf("resync sceneID = %d, want 3", got))

	// 100ms 一次，350ms 内至少应有 2 次心跳。
	time.Sleep(350 * time.Millisecond)
	_, hb := link.counts()
	require.False(t, hb < 2, fmt.Sprintf("心跳次数 = %d, want >= 2", hb))
}

// TestNoHeartbeatWithoutScene 未进场景时不发心跳、不做同步。
func TestNoHeartbeatWithoutScene(t *testing.T) {
	rig := newSpendRig(t, nil)
	link := &fakeSceneLink{}
	rig.actor.WithSceneLink(link)
	rig.actor.heartbeatEvery = 100 * time.Millisecond

	err := rig.sys.Send(rig.pid, &BindMsg{SessionID: 1, PlayerID: 7})
	require.NoError(t, err, fmt.Sprintf("send bind: %v", err))
	time.Sleep(250 * time.Millisecond)

	resyncs, hb := link.counts()
	require.EqualValues(t, 0, resyncs, fmt.Sprintf("未进场景不应触发同步/心跳，got resync=%d heartbeat=%d", resyncs, hb))
	require.EqualValues(t, 0, hb)
}

// TestHeartbeatStopsWhenOffline 下线后不再心跳（避免给已离场的场景发无用消息）。
func TestHeartbeatStopsWhenOffline(t *testing.T) {
	rig := newSpendRig(t, nil)
	link := &fakeSceneLink{}
	rig.actor.WithSceneLink(link)
	rig.actor.heartbeatEvery = 100 * time.Millisecond
	rig.actor.state.SetSceneID(1)

	err := rig.sys.Send(rig.pid, &BindMsg{SessionID: 1, PlayerID: 7})
	require.NoError(t, err, fmt.Sprintf("send bind: %v", err))
	time.Sleep(250 * time.Millisecond)
	before, _ := link.counts()
	require.NotEqualValues(t, 0, before, "绑定后应有心跳")

	// 解绑：之后即使定时器还在跑，也不应再发心跳。
	err = rig.sys.Send(rig.pid, &UnbindMsg{SessionID: 1})
	require.NoError(t, err, fmt.Sprintf("send unbind: %v", err))
	// 等一条可能在途的心跳落地，再取基线。
	time.Sleep(150 * time.Millisecond)
	_, hbAfter := link.counts()
	time.Sleep(250 * time.Millisecond)
	_, hbLater := link.counts()
	require.EqualValues(t, hbAfter, hbLater)
}
