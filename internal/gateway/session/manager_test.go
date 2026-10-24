// internal/gateway/session/manager_test.go —— 会话索引与清理
//
// 网关只维护三张表（SID / 数值 ID / 连接）。这里刻意没有「角色 → 会话」：
// 那份映射属于游戏服（players2sess 是唯一权威），网关存副本会在顶号、
// 换角时出现两份不一致的真相——踢人踢到旧会话就是这么来的。
package session

import (
	"fmt"
	"net"
	"testing"
	"time"

	"game_study/internal/gateway/conn"
	"game_study/pkg/codec"
	"game_study/pkg/log"

	"github.com/stretchr/testify/require"
)

type nopHandler struct{}

func (nopHandler) OnPacket(*conn.Conn, codec.Packet) {}
func (nopHandler) OnClose(*conn.Conn, error)         {}

// newRig 造一个 Manager 与 n 个会话（numID 从 1 递增）。
func newRig(t *testing.T, n int) (*Manager, []*Session) {
	t.Helper()
	logger := log.New("session-test", log.WithLevel("error"))
	m := NewManager(logger)

	sessions := make([]*Session, 0, n)
	for i := 1; i <= n; i++ {
		client, server := net.Pipe()
		t.Cleanup(func() {
			_ = client.Close()
			_ = server.Close()
		})
		c := conn.New(int64(i), server, nopHandler{}, logger)
		sessions = append(sessions, m.NewSession(c))
	}
	return m, sessions
}

// TestRemoveClearsAllIndexes 断开时三张表都要摘干净。
//
// 漏一张的表现：byConn 残留 → 同一连接 ID 复用后旧会话被查到；
// byNum 残留 → 下行帧发往已死的流（静默失败）。
func TestRemoveClearsAllIndexes(t *testing.T) {
	m, ss := newRig(t, 1)
	s := ss[0]

	m.Remove(s)

	require.Nil(t, m.GetBySID(s.ID()))
	require.Nil(t, m.GetByNumID(s.NumID()))
	require.Nil(t, m.GetByConn(s.Conn().ID()))
	require.EqualValues(t, 0, m.Count())
}

// TestRemoveIsIdempotent 重复移除是空操作（OnClose 与 GC 都会触发）。
func TestRemoveIsIdempotent(t *testing.T) {
	m, ss := newRig(t, 1)

	m.Remove(ss[0])
	require.NotPanics(t, func() { m.Remove(ss[0]) })
	require.EqualValues(t, 0, m.Count())
}

// TestGCReclaimsClosedSessions 已关闭的会话由 GC 摘掉（连接层已清理、索引还在）。
func TestGCReclaimsClosedSessions(t *testing.T) {
	m, ss := newRig(t, 2)
	ss[0].Close(fmt.Errorf("test"))

	m.GC(time.Now())

	require.Nil(t, m.GetBySID(ss[0].ID()))
	require.NotNil(t, m.GetBySID(ss[1].ID()), "在线会话不能被 GC 误删")
	require.EqualValues(t, 1, m.Count())
}

// TestGCReclaimsUnauthedSessions 长时间不鉴权的半开连接要回收（扫描器/僵死客户端）。
//
// 时间由调用方注入，测试不需要真的等 TTL。
func TestGCReclaimsUnauthedSessions(t *testing.T) {
	m, ss := newRig(t, 2)
	ss[1].SetAuth("tok", 100) // 已鉴权，不参与回收

	future := time.Now().Add(time.Hour)
	m.GC(future)

	require.Nil(t, m.GetBySID(ss[0].ID()), "未鉴权会话超过 TTL 必须回收")
	require.NotNil(t, m.GetBySID(ss[1].ID()), "已鉴权会话的生命周期由连接事件驱动")
}

// TestSetAuthStoresAccountNotRole 鉴权只带来账号 ID；角色映射由游戏服维护。
func TestSetAuthStoresAccountNotRole(t *testing.T) {
	_, ss := newRig(t, 1)

	ss[0].SetAuth("tok", 9001)

	require.EqualValues(t, 9001, ss[0].AccountID())
	require.Equal(t, "tok", ss[0].Token())
	require.Equal(t, StateAuthed, ss[0].State())
}

// TestCloseAllEmptiesIndexes 优雅停机：摘表后逐个关闭，索引清空。
func TestCloseAllEmptiesIndexes(t *testing.T) {
	m, ss := newRig(t, 3)

	m.CloseAll()

	require.EqualValues(t, 0, m.Count())
	require.Nil(t, m.GetByNumID(ss[0].NumID()))
	// 关闭回调（OnClose → Remove）在空表上重入，必须安全
	require.NotPanics(t, func() { m.Remove(ss[0]) })
}
