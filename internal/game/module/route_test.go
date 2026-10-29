// internal/game/module/route_test.go —— 统一注册入口（registrar）与 Bound 基座能力
//
// 这组测试钉的是「一个注册函数服务所有基座」这件事本身：
// 以前 module.Handle 只吃 *Thin、HandlePlain 只吃 *Plain，两边各有一套
// 测试；现在只剩一个入口，就只需要证明它对每个基座都成立。
package module

import (
	"testing"
	"time"

	"game_study/pkg/codec"
	gamev1 "game_study/pkg/pb/game/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// boundData 测试用状态结构（Bound 需要具体类型做泛型参数）。
type boundData struct{ Hits int32 }

// TestHandleRegistersOnBothBases 同一个 module.Handle 对 Thin 与 Bound 都成立。
func TestHandleRegistersOnBothBases(t *testing.T) {
	var thinHits, boundHits int
	thin := NewThin("t")
	Handle(thin, func(_ *Context, _ *gamev1.SigninInfoReq, _ *gamev1.SigninInfoResp) error {
		thinHits++
		return nil
	})
	bound := Define[boundData]("b")
	Handle(bound, func(_ *Context, _ *gamev1.SigninInfoReq, _ *gamev1.SigninInfoResp) error {
		boundHits++
		return nil
	})

	thinCtx, thinReplies := newThinFixture(thin)
	require.NoError(t, thin.OnMessage(thinCtx, codecSigninInfoReq(t), nil))
	boundCtx, boundReplies := newThinFixture(bound.Thin)
	require.NoError(t, bound.OnMessage(boundCtx, codecSigninInfoReq(t), nil))

	require.Equal(t, 1, thinHits, "Thin 基座的处理器应被调用")
	require.Equal(t, 1, boundHits, "Bound 基座的处理器应被调用")
	// 两个基座都声明了响应：自动回包各发一条。
	require.Len(t, *thinReplies, 1)
	require.Len(t, *boundReplies, 1)
}

// TestBoundStatelessFlag 只有显式声明 Stateless() 的模块才跳过快照流程。
func TestBoundStatelessFlag(t *testing.T) {
	plain := Define[NoState]("stateless").Stateless()
	stateful := Define[NoState]("stateful")

	require.True(t, plain.IsStateless(1), "声明后应恒为 true")

	// 未声明的模块仍实现 StatelessAware（返回 false），Registry 按普通模块处理。
	// 这里刻意不改成「不实现接口」：判据是能力，不是类型断言的成功与否。
	require.False(t, stateful.IsStateless(1), "未声明时不得声称无状态")
}

// TestBoundErrCodeBuildsReply 缓存不可用时的业务错误回包由 ErrCode + 响应声明构造。
func TestBoundErrCodeBuildsReply(t *testing.T) {
	b := Define[NoState]("rank").ErrCode(6702)
	Handle(b, func(_ *Context, _ *gamev1.SigninInfoReq, _ *gamev1.SigninInfoResp) error {
		return nil
	})

	respID, raw, ok := b.CacheFailureReply(codecSigninInfoReq(t))
	require.True(t, ok, "声明了 ErrCode 与响应后应能构造回包")
	require.Equal(t, codecSigninInfoResp(t), respID)

	var resp gamev1.SigninInfoResp
	require.NoError(t, proto.Unmarshal(raw, &resp))
	require.Equal(t, int32(6702), resp.Code)
}

// TestBoundWithoutErrCodeNoReply 未设 ErrCode 时不给业务回包（走网关通用错误）。
func TestBoundWithoutErrCodeNoReply(t *testing.T) {
	b := Define[NoState]("bare")
	Handle(b, func(_ *Context, _ *gamev1.SigninInfoReq, _ *gamev1.SigninInfoResp) error {
		return nil
	})
	_, _, ok := b.CacheFailureReply(codecSigninInfoReq(t))
	require.False(t, ok)
}

// TestBoundRouteCarriesRequest 带请求体的路由：解包后交给处理器。
func TestBoundRouteCarriesRequest(t *testing.T) {
	b := Define[boundData]("r")
	var got *gamev1.UseItemReq
	Route(b, &gamev1.UseItemReq{}, func(_ *Ctx[boundData], req *gamev1.UseItemReq) error {
		got = req
		return nil
	})

	body, err := proto.Marshal(&gamev1.UseItemReq{ItemUid: 7, Count: 3})
	require.NoError(t, err)
	require.NoError(t, b.OnMessage(&Context{State: &thinState{id: 1}}, codecUseItemReq(t), body))
	require.NotNil(t, got)
	require.Equal(t, int64(7), got.GetItemUid())
	require.Equal(t, int32(3), got.GetCount())
}

// TestBoundHandleRouteNoBody 无请求体的路由：请求语义只由身份与服务端状态决定。
func TestBoundHandleRouteNoBody(t *testing.T) {
	b := Define[boundData]("q")
	called := 0
	b.HandleRoute(&gamev1.SigninInfoReq{}, func(_ *Ctx[boundData]) error {
		called++
		return nil
	})
	require.NoError(t, b.OnMessage(&Context{State: &thinState{id: 1}}, codecSigninInfoReq(t), nil))
	require.Equal(t, 1, called)
}

// TestBoundUnknownMsgID 未注册的协议号返回错误，而不是静默丢弃。
func TestBoundUnknownMsgID(t *testing.T) {
	b := Define[NoState]("u")
	require.Error(t, b.OnMessage(&Context{State: &thinState{id: 1}}, 123456, nil))
}

// TestBoundMsgIDsIsCopy MsgIDs 返回副本：调用方改动不得影响内部状态。
func TestBoundMsgIDsIsCopy(t *testing.T) {
	b := Define[boundData]("m")
	Route(b, &gamev1.UseItemReq{}, func(_ *Ctx[boundData], _ *gamev1.UseItemReq) error {
		return nil
	})
	ids := b.MsgIDs()
	require.Len(t, ids, 1)
	ids[0] = 0
	require.Equal(t, codecUseItemReq(t), b.MsgIDs()[0])
}

// TestBoundClockReachesAllHooks 注入的时钟在路由之外的钩子里也生效。
//
// 曾经 SetClock 只在 addRoute 里挂 ctx.Clock，登录/下线/事件三个钩子拿到的
// 还是系统时间。后果不是「少一个测试便利」——loginreward 为此在模块里自己
// 又持了一个 now 字段绕开框架，长成了第五种模块形态。注入点漏一个是骨架
// 分裂的起点，所以这条用例覆盖全部四个入口。
func TestBoundClockReachesAllHooks(t *testing.T) {
	fixed := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return fixed }

	b := Define[boundData]("clock").
		SetClock(clock).
		WithLogin(func(c *Ctx[boundData]) error {
			require.Equal(t, fixed, c.Now(), "登录钩子应看到注入时钟")
			return nil
		}).
		WithLogout(func(c *Ctx[boundData]) error {
			require.Equal(t, fixed, c.Now(), "下线钩子应看到注入时钟")
			return nil
		}).
		WithEvent(func(c *Ctx[boundData], _ Event) {
			require.Equal(t, fixed, c.Now(), "事件钩子应看到注入时钟")
		})
	b.HandleRoute(&gamev1.SigninInfoReq{}, func(c *Ctx[boundData]) error {
		require.Equal(t, fixed, c.Now(), "路由应看到注入时钟")
		return nil
	})

	// 四个入口各用一个新的 Context：真实运行时每条消息也是新 Context，
	// 复用会让「ctx.Clock 非空则不覆盖」把后续注入吃掉。
	newCtx := func() *Context { return &Context{State: &thinState{id: 1}, CacheEnabled: true} }
	require.NoError(t, b.OnMessage(newCtx(), codecSigninInfoReq(t), nil))
	require.NoError(t, b.OnLogin(newCtx()))
	require.NoError(t, b.OnLogout(newCtx()))
	b.OnEvent(newCtx(), Event{Type: "kill", Delta: 1})
}

// 协议号反查失败的 message 类型在测试里只出现一次，抽成小函数避免重复。
func codecSigninInfoReq(t *testing.T) uint32 {
	t.Helper()
	return protoIDOf(t, &gamev1.SigninInfoReq{})
}

func codecSigninInfoResp(t *testing.T) uint32 {
	t.Helper()
	return protoIDOf(t, &gamev1.SigninInfoResp{})
}

func codecUseItemReq(t *testing.T) uint32 {
	t.Helper()
	return protoIDOf(t, &gamev1.UseItemReq{})
}

// protoIDOf 反查协议号（注册路径用的就是它，测试保持同一来源）。
func protoIDOf(t *testing.T, m proto.Message) uint32 {
	t.Helper()
	return codec.ProtoID(m)
}
