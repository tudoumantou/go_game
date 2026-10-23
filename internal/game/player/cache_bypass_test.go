// internal/game/player/cache_bypass_test.go —— 无状态消息的快路径判定
//
// canBypassCache 是「跳过 clone/CAS」的唯一闸门，四个前提缺一不可。
// 判错方向的后果不对称：漏判只多付一次快照开销；误放行会让
// cachePending（结果未知的旧快照）或 cacheReload（待重载）被跳过，
// 从而在错误的版本基线上继续写——所以这里逐条锁死。
package player

import (
	"context"
	"testing"

	"game_study/internal/game/module"
	"game_study/pkg/writebehind"

	"github.com/stretchr/testify/require"
)

const bypassTestMsgID uint32 = 990101

// fakeStatelessMod 最小 StatelessAware 模块（只为构造注册表）。
type fakeStatelessMod struct{ id uint32 }

func (m *fakeStatelessMod) Name() string                                    { return "fake_stateless" }
func (m *fakeStatelessMod) MsgIDs() []uint32                                { return []uint32{m.id} }
func (m *fakeStatelessMod) Init(*module.Context) error                      { return nil }
func (m *fakeStatelessMod) OnMessage(*module.Context, uint32, []byte) error { return nil }
func (m *fakeStatelessMod) OnSave(context.Context) error                    { return nil }
func (m *fakeStatelessMod) OnLogin(*module.Context) error                   { return nil }
func (m *fakeStatelessMod) OnLogout(*module.Context) error                  { return nil }
func (m *fakeStatelessMod) IsStateless(uint32) bool                         { return true }

var _ module.StatelessAware = (*fakeStatelessMod)(nil)

func newBypassTestActor() *Actor {
	reg := module.NewRegistry()
	reg.Register(&fakeStatelessMod{id: bypassTestMsgID})
	return &Actor{registry: reg, loaded: true}
}

// TestCanBypassCacheHappyPath 已加载 + 无未决提交 + 无待重载 + 模块声明无状态。
func TestCanBypassCacheHappyPath(t *testing.T) {
	a := newBypassTestActor()
	require.True(t, a.canBypassCache(bypassTestMsgID), "满足四个前提时应放行")
}

// TestCanBypassCacheRequiresLoaded 未加载时必须走完整流程（先 preload）。
func TestCanBypassCacheRequiresLoaded(t *testing.T) {
	a := newBypassTestActor()
	a.loaded = false
	require.False(t, a.canBypassCache(bypassTestMsgID), "未加载不得跳过")
}

// TestCanBypassCacheBlocksOnPending 上次提交结果未知时必须先 finishPending。
func TestCanBypassCacheBlocksOnPending(t *testing.T) {
	a := newBypassTestActor()
	a.cachePending = &writebehind.Record{Version: 1}
	require.False(t, a.canBypassCache(bypassTestMsgID), "存在未决提交不得跳过")
}

// TestCanBypassCacheBlocksOnReload 待重载时必须先 LoadCached 拿新版本。
func TestCanBypassCacheBlocksOnReload(t *testing.T) {
	a := newBypassTestActor()
	a.cacheReload = true
	require.False(t, a.canBypassCache(bypassTestMsgID), "待重载不得跳过")
}

// TestCanBypassCacheRejectsStatefulMsg 有状态模块的协议永远不走快路径。
func TestCanBypassCacheRejectsStatefulMsg(t *testing.T) {
	a := newBypassTestActor()
	require.False(t, a.canBypassCache(999999), "未注册/有状态协议不得跳过")
}

// TestCanBypassCacheNilRegistry 无注册表（极端装配错误）时保守返回 false。
func TestCanBypassCacheNilRegistry(t *testing.T) {
	a := &Actor{loaded: true}
	require.False(t, a.canBypassCache(bypassTestMsgID))
}
