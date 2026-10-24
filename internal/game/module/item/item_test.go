// internal/game/module/item/item_test.go —— 道具模块单测
//
// 道具的实际使用入口在 bag（协议 3001），item 只是「道具域」的落点：
// 无协议、无状态、只挂两个观测钩子。测试同样锁的是「保持为空」这个约定，
// 避免有人把道具逻辑塞进来又绕开 bag 的扣减校验。
package item

import (
	"testing"

	"game_study/internal/game/module/moduletest"
	"game_study/pkg/log"
	"github.com/stretchr/testify/require"
)

func newTestModule() (*Module, *moduletest.State, *moduletest.Recorder) {
	st := moduletest.NewState(1)
	rec := moduletest.NewRecorder()
	return New(log.New("item-test", log.WithLevel("error"))), st, rec
}

// TestLifecycleHooks 登录不产生提交，下线标脏。
func TestLifecycleHooks(t *testing.T) {
	m, st, rec := newTestModule()
	ctx := moduletest.NewContext(st, rec)

	require.NoError(t, m.OnLogin(ctx))
	require.Empty(t, st.DirtyModules())
	require.Zero(t, rec.Total())

	require.NoError(t, m.OnLogout(ctx))
	require.Contains(t, st.DirtyModules(), "item")
}

// TestNoProtocolsRegistered 道具使用走 bag 模块；item 不占协议号。
//
// 道具增减必须经过 bag 的库存校验（否则就是无限刷道具的入口），
// 所以这里刻意锁死「item 没有协议」。
func TestNoProtocolsRegistered(t *testing.T) {
	m, st, rec := newTestModule()
	ctx := moduletest.NewContext(st, rec)

	require.Error(t, m.OnMessage(ctx, 3099, nil), "item 不应注册任何协议")
	require.Zero(t, rec.Total())
}

// TestNilLoggerTolerated 演示环境可能传 nil logger。
func TestNilLoggerTolerated(t *testing.T) {
	m := New(nil)
	ctx := moduletest.NewContext(moduletest.NewState(1), moduletest.NewRecorder())
	require.NotPanics(t, func() { _ = m.OnLogin(ctx) })
	require.NoError(t, m.OnLogin(ctx))
}
