// pkg/trace/trace_test.go —— 链路 ID 的生成、进程内传递与跨进程传递
package trace

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/metadata"
)

func TestNewProducesValidID(t *testing.T) {
	id := New()
	require.True(t, Valid(id), "id 必须是 32 位十六进制: %q", id)
	require.NotEqual(t, id, New(), "两次生成不能碰撞")
}

// 外部传进来的脏数据必须被拒（Valid 是防注入的唯一入口）。
func TestValidRejectsGarbage(t *testing.T) {
	require.False(t, Valid(""))
	require.False(t, Valid("xyz"))
	require.False(t, Valid(strings.Repeat("g", 32)))
	require.False(t, Valid(strings.Repeat("a", 31)), "长度不足")
	require.True(t, Valid(strings.Repeat("a", 32)))
}

// ctx 传递（进程内，例如从 gRPC 入口传到 Actor 绑定）。
func TestContextRoundTrip(t *testing.T) {
	require.Empty(t, IDFrom(context.Background()))

	ctx := WithID(context.Background(), "abc")
	require.Equal(t, "abc", IDFrom(ctx))

	// 空 id 不写入：否则下游会拿到一个假的「有 trace」状态
	require.Empty(t, IDFrom(WithID(context.Background(), "")))
}

// 跨进程传递：出站 metadata → 入站 metadata。
func TestOutgoingToIncoming(t *testing.T) {
	out, id := NewOutgoing(context.Background())
	require.Equal(t, id, IDFrom(out))

	md, ok := metadata.FromOutgoingContext(out)
	require.True(t, ok)
	require.Equal(t, []string{id}, md.Get(MetadataKey))

	// 模拟对端：把出站 metadata 当入站读
	in := metadata.NewIncomingContext(context.Background(), md)
	require.Equal(t, id, FromIncoming(in))

	// 没有 metadata / 值非法时返回空串而不是脏值
	require.Empty(t, FromIncoming(context.Background()))
	bad := metadata.NewIncomingContext(context.Background(), metadata.Pairs(MetadataKey, "not-a-trace"))
	require.Empty(t, FromIncoming(bad))
}

// traceparent 供后续接入 otel/链路系统使用。
func TestTraceparentFormat(t *testing.T) {
	id := strings.Repeat("1", 32)
	require.Equal(t, "00-"+id+"-"+strings.Repeat("0", 16)+"-01", Traceparent(id))
	require.Empty(t, Traceparent("bad"))
}

// ---- span 级升级：双发双收、父 span 保留、子 span 生成 ----

func TestOutgoingSendsBothTraceHeaders(t *testing.T) {
	out, id := NewOutgoing(context.Background())
	md, ok := metadata.FromOutgoingContext(out)
	require.True(t, ok)
	require.Equal(t, []string{id}, md.Get(MetadataKey))

	tp := md.Get(W3CHeader)
	require.Len(t, tp, 1)
	tid, sid, flags, ok := ParseTraceparent(tp[0])
	require.True(t, ok)
	require.Equal(t, id, tid, "W3C 头里的 trace_id 必须和老字段一致")
	require.Len(t, sid, 16)
	require.NotEqual(t, strings.Repeat("0", 16), sid, "新头要带真实 span_id")
	require.True(t, flags.IsSampled())
}

func TestFromIncomingWithContextKeepsRemoteParent(t *testing.T) {
	tid := strings.Repeat("a", 32)
	sid := strings.Repeat("b", 16)
	tp := "00-" + tid + "-" + sid + "-01"
	in := metadata.NewIncomingContext(context.Background(), metadata.Pairs(W3CHeader, tp))

	ctx, id := FromIncomingWithContext(in)
	require.Equal(t, tid, id)
	sc := oteltrace.SpanFromContext(ctx).SpanContext()
	require.Equal(t, tid, sc.TraceID().String())
	require.Equal(t, sid, sc.SpanID().String(), "必须保留上游 span_id，否则子 span 断链")
	require.True(t, sc.IsRemote())
}

func TestFromIncomingFallsBackToLegacyHeader(t *testing.T) {
	id := strings.Repeat("c", 32)
	in := metadata.NewIncomingContext(context.Background(), metadata.Pairs(MetadataKey, id))
	ctx, got := FromIncomingWithContext(in)
	require.Equal(t, id, got)
	require.True(t, oteltrace.SpanFromContext(ctx).SpanContext().IsValid(),
		"老字段也要派生出合法父 span")
}

func TestParseTraceparentRejectsGarbage(t *testing.T) {
	good := "00-" + strings.Repeat("a", 32) + "-" + strings.Repeat("b", 16) + "-01"
	_, _, _, ok := ParseTraceparent(good)
	require.True(t, ok)

	for _, bad := range []string{
		"", "xyz", good + "-extra", "01-" + good[3:],
		"00-" + strings.Repeat("g", 32) + "-" + strings.Repeat("b", 16) + "-01",
		"00-" + strings.Repeat("a", 32) + "-" + strings.Repeat("0", 16) + "-01",
	} {
		_, _, _, ok := ParseTraceparent(bad)
		require.False(t, ok, "非法 traceparent 必须丢弃: %q", bad)
	}
}

// Start 生成的子 span 必须继承入站的 trace_id / span_id。
func TestStartCreatesChildSpanUnderIncoming(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exp),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { otel.SetTracerProvider(sdktrace.NewTracerProvider()) })

	tid := strings.Repeat("d", 32)
	sid := strings.Repeat("e", 16)
	in := metadata.NewIncomingContext(context.Background(),
		metadata.Pairs(W3CHeader, "00-"+tid+"-"+sid+"-01"))
	parent, _ := FromIncomingWithContext(in)

	ctx, end := Start(parent, "unit.child", attribute.String("k", "v"))
	end()
	require.NotNil(t, ctx)

	spans := exp.GetSpans()
	require.Len(t, spans, 1)
	require.Equal(t, "unit.child", spans[0].Name)
	require.Equal(t, tid, spans[0].SpanContext.TraceID().String())
	require.Equal(t, sid, spans[0].Parent.SpanID().String(), "父 span 必须是入站的那个")
}
