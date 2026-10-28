// pkg/trace —— 最小链路追踪：一个 trace_id 从网关贯穿到玩家 Actor
//
// 为什么不用 OpenTelemetry SDK：本项目的诊断诉求只有一条——「玩家说卡了
// 3 秒，把这条请求在网关和游戏服的日志串起来」。引入 otel 需要 SDK +
// exporter + collector 三件套，对一个 10 行日志字段的需求来说太重。
//
// 折中做法：
//   - id 采用 W3C traceparent 的 32 位十六进制格式，需要升级时可以在
//     不改调用点的前提下换成 otel 的 SpanContext；
//   - 传输走 gRPC metadata（每个会话建流时传一次），不改 protobuf，
//     也就不需要重新生成 pb；
//   - 落点是日志字段，用 grep/日志平台按 trace_id 聚合。
//
// 生命周期是「会话级」：一条网关↔游戏服的流一个 id。同一个玩家的所有
// 请求共享它，配合已有的 player_id / seq 足以定位到具体一次操作。
package trace

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/metadata"
)

// MetadataKey gRPC metadata 的键（小写，gRPC 会强制小写）。
const MetadataKey = "x-trace-id"

// New 生成一个新的 trace_id（16 字节 → 32 位小写十六进制）。
//
// 随机源不可用时退回「纳秒 + 进程内计数」：trace_id 只用于关联日志，
// 不承担安全职责，宁可退化成弱唯一也不要返回空串（空串会让整条链路
// 失去关联）。
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
func WithID(ctx context.Context, id string) context.Context {
	if id == "" || ctx == nil {
		return ctx
	}
	return context.WithValue(ctx, ctxKey{}, id)
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

// NewOutgoing 生成 trace_id 并写入 gRPC metadata；返回带 id 的 ctx。
//
// 只能在建流（client.Channel）之前调用：metadata 在流建立时一次性发出，
// 之后无法追加——这也正是「会话级 id」这个取舍的来源。
func NewOutgoing(ctx context.Context) (context.Context, string) {
	id := New()
	ctx = WithID(ctx, id)
	return metadata.AppendToOutgoingContext(ctx, MetadataKey, id), id
}

// FromIncoming 从 gRPC metadata 取 trace_id；不合法一律当没有。
func FromIncoming(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	vals := md.Get(MetadataKey)
	if len(vals) == 0 {
		return ""
	}
	if id := vals[0]; Valid(id) {
		return id
	}
	return ""
}

// Traceparent 拼 W3C traceparent 头（接入 otel/链路系统时直接用）。
//
// 格式：version-traceid-spanid-flags；span_id 固定 0 表示「本进程未再分片」。
func Traceparent(id string) string {
	if !Valid(id) {
		return ""
	}
	return "00-" + id + "-" + strings.Repeat("0", 16) + "-01"
}
