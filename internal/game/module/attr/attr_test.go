// internal/game/module/attr/attr_test.go —— 属性模块单测
//
// attr 当前没有协议、没有私有状态，只有登录/下线两个观测钩子。
// 这类「登记模块位」的模块最容易被顺手改坏（有人加协议、有人在这里
// 改状态），所以测试锁的是「它就是空的」这件事，而不是空转覆盖率。
package attr

import (
	"testing"

	"game_study/internal/game/module/moduletest"
	"game_study/pkg/log"
	"github.com/stretchr/testify/require"
)

func newTestModule(t *testing.T) (*Module, *moduletest.State, *moduletest.Recorder) {
	t.Helper()
	st := moduletest.NewState(1)
	rec := moduletest.NewRecorder()
	return New(log.New("attr-test", log.WithLevel("error"))), st, rec
}

// TestLifecycleHooks attr 只挂观测钩子：登录不产生提交，下线标脏。
func TestLifecycleHooks(t *testing.T) {
	m, st, rec := newTestModule(t)
	ctx := moduletest.NewContext(st, rec)

	require.NoError(t, m.OnLogin(ctx))
	require.Empty(t, st.DirtyModules(), "登录不该产生提交")
	require.Zero(t, rec.Total(), "登录不该下发任何包")

	require.NoError(t, m.OnLogout(ctx))
	require.Contains(t, st.DirtyModules(), "attr", "下线要提示 flusher 关注")
}

// TestNoProtocolsRegistered 属性查询随 PlayerInfo 下发，本模块不该占协议号。
//
// 这条断言是「锁定现状」而非「验证功能」：如果将来真的要给 attr 加协议，
// 应当同时更新本用例——它存在的意义是让那次改动显式发生，而不是被
// 顺手加一行 m.Handle 就悄悄生效。
func TestNoProtocolsRegistered(t *testing.T) {
	m, st, rec := newTestModule(t)
	ctx := moduletest.NewContext(st, rec)

	err := m.OnMessage(ctx, 2999, nil)
	require.Error(t, err, "attr 不应注册任何协议")
	require.Zero(t, rec.Total(), "未知消息不能产生下行")
}

// TestNilLoggerTolerated 组装层在测试/演示环境可能传 nil logger，
// 模块不能因此 panic（New 里已对 logger 判空，这里锁住该约定）。
func TestNilLoggerTolerated(t *testing.T) {
	m := New(nil)
	st := moduletest.NewState(1)
	ctx := moduletest.NewContext(st, moduletest.NewRecorder())

	require.NotPanics(t, func() { _ = m.OnLogin(ctx) })
	require.NoError(t, m.OnLogin(ctx))
}
