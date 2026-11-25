package center

import (
	"context"
	"errors"
	"testing"
	"time"

	"game_study/pkg/breaker"
	"game_study/pkg/config"
	"game_study/pkg/trace"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestClientBreakerLifecycle(t *testing.T) {
	c := NewClient("game-1", "127.0.0.1:9200", nil, nil)
	require.Equal(t, breaker.Closed, c.BreakerState(), "默认熔断关闭状态（即放行）")

	// 关闭熔断：连续失败也不打开。
	c.SetBreaker(breaker.Options{Threshold: -1})
	for i := 0; i < 20; i++ {
		c.cb.Failure()
	}
	require.Equal(t, breaker.Closed, c.BreakerState())

	// 重配后新参数生效（跨重连的 breaker 挂在 Client 上，SetBreaker 换的是同一个字段）。
	c.SetBreaker(breaker.Options{Threshold: 1, Cooldown: time.Millisecond, Probes: 1})
	c.cb.Failure()
	require.Equal(t, breaker.Open, c.BreakerState())
}

func TestBreakerOptionsFrom(t *testing.T) {
	opts := BreakerOptionsFrom(config.BreakerConfig{Threshold: 7, Cooldown: "2s", Probes: 3})
	require.Equal(t, 7, opts.Threshold)
	require.Equal(t, 2*time.Second, opts.Cooldown)
	require.Equal(t, 3, opts.Probes)

	// 配置写错的 cooldown 不致命：留空让 breaker 取默认值。
	bad := BreakerOptionsFrom(config.BreakerConfig{Cooldown: "not-a-duration"})
	require.Zero(t, bad.Cooldown)
}

// 指标未初始化时 emit 函数是 no-op，这里验证拦截器不改变 err 语义。
func TestMetricsInterceptorPassesError(t *testing.T) {
	c := NewClient("game-1", "127.0.0.1:9200", nil, nil)
	want := errors.New("down")
	invoker := func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
		return want
	}
	err := c.metricsInterceptor(context.Background(), "/center.CenterService/CrossEnroll",
		nil, nil, nil, invoker)
	require.ErrorIs(t, err, want)
}

// ---- 方法级超时 + deadline 传播 ----

func TestSetTimeoutsNormalize(t *testing.T) {
	c := NewClient("game-1", "addr", nil, nil)
	// 零装配：未调 SetTimeouts，字段为 0，rpcCtx 内部回落到默认值。
	require.Equal(t, time.Duration(0), c.timeouts.CrossEnroll)
	require.Equal(t, time.Duration(0), c.timeouts.QueryRank)

	// 只配一个：另一个补默认，不留 0。
	c.SetTimeouts(RPCTimeoutConfig{CrossEnroll: 8 * time.Second})
	require.Equal(t, 8*time.Second, c.timeouts.CrossEnroll)
	require.Equal(t, DefaultRPCTimeout, c.timeouts.QueryRank)

	// 负数按未配处理。
	c.SetTimeouts(RPCTimeoutConfig{CrossEnroll: -1, QueryRank: 5 * time.Second})
	require.Equal(t, DefaultRPCTimeout, c.timeouts.CrossEnroll)
	require.Equal(t, 5*time.Second, c.timeouts.QueryRank)
}

func TestRpcCtxDeadlineTakesMin(t *testing.T) {
	c := NewClient("game-1", "addr", nil, nil)
	c.SetTimeouts(RPCTimeoutConfig{CrossEnroll: 10 * time.Second})

	// 上游 deadline 更紧：取上游。
	upstream, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ctx, done := c.rpcCtx(upstream, c.timeouts.CrossEnroll)
	defer done()
	got, ok := ctx.Deadline()
	require.True(t, ok)
	require.WithinDuration(t, time.Now().Add(2*time.Second), got, 500*time.Millisecond)

	// 上游 deadline 更松：取本地配置。
	upstream2, cancel2 := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel2()
	ctx2, done2 := c.rpcCtx(upstream2, c.timeouts.CrossEnroll)
	defer done2()
	got2, _ := ctx2.Deadline()
	require.WithinDuration(t, time.Now().Add(10*time.Second), got2, 500*time.Millisecond)
}

func TestRpcCtxDropsCancelKeepsValues(t *testing.T) {
	c := NewClient("game-1", "addr", nil, nil)
	type key struct{}
	// 保留 Value：这是 trace span / metadata 能穿过来的前提。
	upstream := context.WithValue(context.Background(), key{}, "span")
	ctx, done := c.rpcCtx(upstream, 3*time.Second)
	defer done()
	require.Equal(t, "span", ctx.Value(key{}))

	// 丢掉取消信号：上游 cancel 后 ctx 仍存活（不能掐断中心服的写）。
	upstream2, cancel := context.WithCancel(context.WithValue(context.Background(), key{}, "v"))
	ctx2, done2 := c.rpcCtx(upstream2, 3*time.Second)
	defer done2()
	cancel()
	select {
	case <-ctx2.Done():
		t.Fatal("上游取消不应传导到 RPC ctx")
	default:
	}
	require.Equal(t, "v", ctx2.Value(key{}))
	// RPC 自己的超时仍然生效。
	require.WithinDuration(t, time.Now().Add(3*time.Second), mustDeadline(t, ctx2), 500*time.Millisecond)
}

func TestRpcCtxNilParentFallsBack(t *testing.T) {
	c := NewClient("game-1", "addr", nil, nil)
	ctx, done := c.rpcCtx(nil, 0)
	defer done()
	require.NotNil(t, ctx)
	require.WithinDuration(t, time.Now().Add(DefaultRPCTimeout), mustDeadline(t, ctx), 500*time.Millisecond)
}

func mustDeadline(t *testing.T, ctx context.Context) time.Time {
	t.Helper()
	dl, ok := ctx.Deadline()
	require.True(t, ok, "ctx 必须有 deadline")
	return dl
}

// TestRpcCtxKeepsSpanChain 验证 actor.packet → center.rpc 的 span 父子链：
// rpcCtx 经 WithoutCancel 后仍保留 span，Start 出来的中心服 span 挂在其下。
// 这正是改造前用 context.Background() 会断掉的地方。
func TestRpcCtxKeepsSpanChain(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exp),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	old := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { otel.SetTracerProvider(old) })

	// 模拟 Actor 在 packet span 内注入的父 ctx。
	parent, endParent := trace.Start(context.Background(), "actor.packet",
		attribute.Int("msg_id", 6801))
	c := NewClient("game-1", "addr", nil, nil)
	rpcCtx, done := c.rpcCtx(parent, 3*time.Second)

	// 模拟 center.rpc 拦截器在 rpcCtx 上起子 span。
	_, endRPC := trace.Start(rpcCtx, "center.rpc", attribute.String("rpc.method", "CrossEnroll"))
	endRPC()
	endParent()
	done()

	spans := exp.GetSpans()
	require.Len(t, spans, 2, "应有 packet 与 rpc 两个 span")
	var packet, rpc tracetest.SpanStub
	for _, s := range spans {
		switch s.Name {
		case "actor.packet":
			packet = s
		case "center.rpc":
			rpc = s
		}
	}
	require.NotNil(t, packet)
	require.NotNil(t, rpc)
	require.Equal(t, packet.SpanContext.TraceID(), rpc.SpanContext.TraceID(),
		"两个 span 必须同一条 trace")
	require.Equal(t, packet.SpanContext.SpanID(), rpc.Parent.SpanID(),
		"center.rpc 的父必须是 actor.packet")
}
