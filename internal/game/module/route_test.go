// internal/game/module/route_test.go —— 路由注册与 Bound 基座能力
//
// 这组测试钉的是「注册入口只有两个」这件事本身：
//
//	module.Route(b, &Req{}, onX)   带请求体
//	b.HandleRoute(&Req{}, onX)     无请求体
//
// 处理器签名统一是 func(*Ctx[D], req) error——回包由处理器自己做。
// 历史上那套「框架替你回 resp」的 module.Handle 已删除（v3.25）。
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
	Route(b, &gamev1.SigninInfoReq{}, func(_ *Ctx[NoState], _ *gamev1.SigninInfoReq) error {
		return nil
	})
	b.Resp(&gamev1.SigninInfoReq{}, &gamev1.SigninInfoResp{})

	respID, raw, ok := b.CacheFailureReply(codecSigninInfoReq(t))
	require.True(t, ok, "声明了 ErrCode 与响应后应能构造回包")
	require.Equal(t, codecSigninInfoResp(t), respID)

	var resp gamev1.SigninInfoResp
	require.NoError(t, proto.Unmarshal(raw, &resp))
	require.Equal(t, int32(6702), resp.Code)
}

// TestBoundWithoutErrCodeNoReply 未设 ErrCode 时不给业务回包（走网关通用错误）。
//
// 第二个断言同样重要：即使声明了响应原型，没有 ErrCode 就不回包——
// 否则会发出 code=0 的包，客户端会当成「成功」，而服务端一条都没执行。
func TestBoundWithoutErrCodeNoReply(t *testing.T) {
	b := Define[NoState]("bare")
	Route(b, &gamev1.SigninInfoReq{}, func(_ *Ctx[NoState], _ *gamev1.SigninInfoReq) error {
		return nil
	})
	b.Resp(&gamev1.SigninInfoReq{}, &gamev1.SigninInfoResp{})
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
