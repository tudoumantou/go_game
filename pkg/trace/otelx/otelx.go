// Package otelx —— OTel SDK 初始化（OTLP gRPC exporter，对接 Tempo / Jaeger）
//
// 与 pkg/trace 的分工：trace 只提供「打点 API + W3C 传播」，不依赖 SDK；
// 本包负责进程启动时装配 SDK、exporter、采样器，并设置全局 TracerProvider。
// 未调用 Init 时，otel 全局是 noop，trace.Start 的开销接近零——这让单测
// 和 CLI 无需任何链路设施也能跑。
//
// 为什么选 OTLP/gRPC 而不是 Jaeger thrift：Tempo 与 Jaeger 1.35+ 都原生
// 收 OTLP，一个 exporter 两家都能用；gRPC 复用项目已有的 grpc 依赖，
// 不额外引 HTTP 栈。
package otelx

import (
	"context"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
)

// Config OTel 初始化参数。
type Config struct {
	// Enabled 是否启用 SDK；false 时不做任何初始化（noop）。
	Enabled bool
	// Endpoint OTLP gRPC 地址，如 "tempo:4317"；空等价于 Enabled=false。
	Endpoint string
	// ServiceName 资源属性 service.name，链路系统按它筛选服务。
	ServiceName string
	// SampleRatio 采样率 [0,1]；0 = 不采样，1 = 全采；越界按 1 处理。
	SampleRatio float64
}

// noopShutdown 未启用时返回的空关闭函数（调用方无条件 defer 即可）。
func noopShutdown(context.Context) error { return nil }

// Init 初始化 OTel 链路 SDK，返回关闭函数（进程退出前调用以 flush 剩余 span）。
//
// 失败语义：exporter 建不出来时返回 error 让启动方决定——通常是告警并
// 降级为 noop，而不是让游戏服起不来。链路是可观测性设施，不是业务前提。
func Init(ctx context.Context, cfg Config) (func(context.Context) error, error) {
	if !cfg.Enabled || strings.TrimSpace(cfg.Endpoint) == "" {
		return noopShutdown, nil
	}
	endpoint := strings.TrimPrefix(strings.TrimPrefix(cfg.Endpoint, "https://"), "http://")

	exp, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(endpoint),
		otlptracegrpc.WithInsecure(), // 内网采集，TLS 由 mesh/网关负责
		otlptracegrpc.WithTimeout(3*time.Second),
	)
	if err != nil {
		return nil, err
	}
	return initProvider(ctx, exp, cfg, false)
}

// initProvider 装配 provider；拆出来是为了让测试注入 in-memory exporter。
func initProvider(ctx context.Context, exp sdktrace.SpanExporter, cfg Config, sync bool) (func(context.Context) error, error) {
	name := cfg.ServiceName
	if name == "" {
		name = "unknown"
	}
	res, err := resource.New(ctx,
		resource.WithAttributes(semconv.ServiceName(name)),
	)
	if err != nil {
		return nil, err
	}
	ratio := cfg.SampleRatio
	if ratio <= 0 || ratio > 1 {
		ratio = 1
	}
	var processor sdktrace.TracerProviderOption
	if sync {
		processor = sdktrace.WithSyncer(exp) // 测试用：span 立即可读
	} else {
		processor = sdktrace.WithBatcher(exp) // 生产用：异步批量，减少 exporter 调用
	}
	tp := sdktrace.NewTracerProvider(
		processor,
		sdktrace.WithResource(res),
		// ParentBased：上游（网关）已决定不采样时下游跟随，避免采出断链。
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))),
	)
	otel.SetTracerProvider(tp)
	return tp.Shutdown, nil
}
