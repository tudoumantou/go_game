// pkg/trace/trace_test.go —— 链路 ID 的生成、进程内传递与跨进程传递
package trace

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
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
