// pkg/trace —— 链路追踪：从会话级 trace_id 平滑升级到 span 级 OTel
//
// 历史取舍（保留说明，避免后来者误删）：早期只需要「玩家说卡了 3 秒，
// 把网关和游戏服的日志串起来」，一个会话级 trace_id 落日志字段就够，
// 引 OTel SDK + exporter + collector 三件套显得太重。
//
// 现在项目上了跨服多进程，问题变成「卡在网关、Actor 还是中心服 RPC」，
// 会话级 id 回答不了——它不知道每一跳花了多久。所以本包做了两件事：
//
//  1. 内部换成 OTel 的 SpanContext（trace_id 仍是 32 位 W3C 十六进制，
//     与旧 id 完全同构），新增 Start 产出真正的 span 与耗时；
//  2. 传输同时写 `x-trace-id`（老字段）和 W3C `traceparent`（带 span_id），
//     入站优先读 traceparent、回退 x-trace-id——新老节点混部时链路不断。
//
// 兼容性承诺：原有 New/Valid/WithID/IDFrom/NewOutgoing/FromIncoming/
// Traceparent 全部保持原签名与语义，调用点零改动；SDK 的初始化在
// pkg/trace/otelx，未初始化时 otel 全局是 noop，开销可忽略。
package trace

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	oteltrace "go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/metadata"
)

const (
	// MetadataKey gRPC metadata 的老键（小写，gRPC 会强制小写）。
	MetadataKey = "x-trace-id"
	// W3CHeader W3C 标准头；与老键双发双收，保证混部兼容。
	W3CHeader = "traceparent"

	// tracerName 出现在 OTel 资源属性里，用于区分 span 来源。
	tracerName = "game_study"
)

// New 生成一个新的 trace_id（16 字节 → 32 位小写十六进制）。
//
// 随机源不可用时退回「纳秒 + 进程内计数」：trace_id 只用于关联日志，
// 不承担安全职责，宁可退化成弱唯一也不要返回空串（空串会让整条链路
// 失去关联）。格式与 W3C trace-id 完全一致，可直接转 otel.TraceID。
func New() string {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err == nil {
		return hex.EncodeToString(buf[:])
	}
	return fmt.Sprintf("%016x%016x", time.Now().UnixNano(), atomic.AddUint64(&fallbackSeq, 1))
}

// fallbackSeq 随机源失效时的进程内序号。
var fallbackSeq uint64

// Valid 判断是否是合法的 32 位十六进制 id（防外部注入脏数据）。
func Valid(id string) bool {
	if len(id) != 32 {
		return false
	}
	_, err := hex.DecodeString(id)
	return err == nil
}

type ctxKey struct{}

// WithID 把 trace_id 放进 context（进程内传递）。
//
// 同时构造一个 OTel SpanContext 供 Start 作为父 span：id 里只有 trace_id
// 没有 span_id，这里用 trace_id 的前 8 字节派生一个稳定的父 span_id。
// 若 ctx 已有合法 span（FromIncomingWithContext 解析过 traceparent），
// 不覆盖它——否则会丢掉上游真实的 span_id，链路在这一点断裂。
func WithID(ctx context.Context, id string) context.Context {
	if id == "" || ctx == nil {
		return ctx
	}
	ctx = context.WithValue(ctx, ctxKey{}, id)
	if oteltrace.SpanFromContext(ctx).SpanContext().IsValid() {
		return ctx
	}
	tid, err := oteltrace.TraceIDFromHex(id)
	if err != nil {
		return ctx
	}
	var sid oteltrace.SpanID
	copy(sid[:], tid[:8])
	sc := oteltrace.NewSpanContext(oteltrace.SpanContextConfig{
		TraceID:    tid,
		SpanID:     sid,
		TraceFlags: oteltrace.FlagsSampled,
		Remote:     true,
	})
	return oteltrace.ContextWithSpanContext(ctx, sc)
}

// IDFrom 从 context 取 trace_id（没有则空串）。
func IDFrom(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if id, ok := ctx.Value(ctxKey{}).(string); ok {
		return id
	}
	return ""
}

// Start 开启一个子 span，返回带 span 的 ctx 和结束函数（调用方 defer end()）。
//
// 用法：
//
//	ctx, end := trace.Start(ctx, "actor.packet", attribute.Int("msg_id", id))
//	defer end()
//
// 未初始化 OTel SDK 时全局是 noop tracer，Start 返回的是 noop span，
// end() 也无操作——业务代码无需条件判断。
func Start(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, func()) {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, span := otel.Tracer(tracerName).Start(ctx, name, oteltrace.WithAttributes(attrs...))
	return ctx, func() { span.End() }
}

// NewOutgoing 生成 trace_id 并写入 gRPC metadata；返回带 id 的 ctx。
//
// 只能在建流（client.Channel）之前调用：metadata 在流建立时一次性发出，
// 之后无法追加——这也正是「会话级 id」这个取舍的来源。
//
// 双发两个头：老节点只读 x-trace-id，新节点优先读 traceparent（能拿到
// 本进程的 span_id 作为父，链路才完整）。
func NewOutgoing(ctx context.Context) (context.Context, string) {
	id := New()
	ctx = WithID(ctx, id)
	sc := oteltrace.SpanFromContext(ctx).SpanContext()
	tp := Traceparent(id)
	if sc.SpanID().IsValid() {
		tp = "00-" + id + "-" + sc.SpanID().String() + "-01"
	}
	return metadata.AppendToOutgoingContext(ctx, MetadataKey, id, W3CHeader, tp), id
}

// FromIncoming 从 gRPC metadata 取 trace_id；不合法一律当没有。
//
// 优先 W3C traceparent，回退老字段 x-trace-id。只返回 id——需要链路
// 继续向下游传 span 时用 FromIncomingWithContext。
func FromIncoming(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	if vals := md.Get(W3CHeader); len(vals) > 0 {
		if id, _, _, ok := ParseTraceparent(vals[0]); ok {
			return id
		}
	}
	if vals := md.Get(MetadataKey); len(vals) > 0 && Valid(vals[0]) {
		return vals[0]
	}
	return ""
}

// FromIncomingWithContext 取入站 trace 信息并把父 span 装回 ctx。
//
// 与 FromIncoming 的区别：解析 traceparent 时保留上游 span_id，Start 产出的
// 子 span 才有正确的 parent，链路在网关↔游戏服之间不断。仅拿到老字段时
// 退化为 WithID（用 trace_id 派生父 span_id）。
func FromIncomingWithContext(ctx context.Context) (context.Context, string) {
	md, ok := metadata.FromIncomingContext(ctx)
	if ok {
		if vals := md.Get(W3CHeader); len(vals) > 0 {
			if id, sidHex, flags, valid := ParseTraceparent(vals[0]); valid {
				tid, _ := oteltrace.TraceIDFromHex(id)
				sid, _ := oteltrace.SpanIDFromHex(sidHex)
				sc := oteltrace.NewSpanContext(oteltrace.SpanContextConfig{
					TraceID: tid, SpanID: sid, TraceFlags: flags, Remote: true,
				})
				ctx = oteltrace.ContextWithSpanContext(ctx, sc)
				return context.WithValue(ctx, ctxKey{}, id), id
			}
		}
	}
	id := FromIncoming(ctx)
	if id != "" {
		ctx = WithID(ctx, id)
	}
	return ctx, id
}

// Traceparent 拼 W3C traceparent 头（兼容老调用：span_id 固定全 0）。
//
// 格式：version-traceid-spanid-flags。带真实 span_id 的版本由 NewOutgoing
// 内部生成——老接口保持不变，是因为它是公开 API 且已有测试。
func Traceparent(id string) string {
	if !Valid(id) {
		return ""
	}
	return "00-" + id + "-" + strings.Repeat("0", 16) + "-01"
}

// ParseTraceparent 解析 W3C traceparent，返回 trace_id / span_id / flags。
//
// 只接受 version=00、两段 id 长度正确且非全 0 的值；其余按无效丢弃，
// 不把脏数据带进 OTel。
func ParseTraceparent(tp string) (traceID, spanID string, flags oteltrace.TraceFlags, ok bool) {
	parts := strings.Split(tp, "-")
	if len(parts) != 4 || parts[0] != "00" {
		return "", "", 0, false
	}
	if len(parts[1]) != 32 || len(parts[2]) != 16 || len(parts[3]) != 2 {
		return "", "", 0, false
	}
	if !Valid(parts[1]) || parts[2] == strings.Repeat("0", 16) {
		return "", "", 0, false
	}
	if _, err := hex.DecodeString(parts[2]); err != nil {
		return "", "", 0, false
	}
	tid, err1 := oteltrace.TraceIDFromHex(parts[1])
	sid, err2 := oteltrace.SpanIDFromHex(parts[2])
	if err1 != nil || err2 != nil || !tid.IsValid() || !sid.IsValid() {
		return "", "", 0, false
	}
	b, err := hex.DecodeString(parts[3])
	if err != nil {
		return "", "", 0, false
	}
	return parts[1], parts[2], oteltrace.TraceFlags(b[0]), true
}
