package otelx

import (
	"context"
	"testing"

	"game_study/pkg/trace"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestInitDisabledIsNoop(t *testing.T) {
	// 不要恢复 GetTracerProvider() 的返回值：那是全局 delegate，设回自身会
	// 让后续 SetTracerProvider 失效。用一个干净的 noop provider 收尾即可。
	t.Cleanup(func() { otel.SetTracerProvider(sdktrace.NewTracerProvider()) })

	shutdown, err := Init(context.Background(), Config{Enabled: false, Endpoint: "tempo:4317"})
	require.NoError(t, err)
	require.NotNil(t, shutdown)
	require.NoError(t, shutdown(context.Background()))
}

func TestInitProviderCollectsSpans(t *testing.T) {
	// 不要恢复 GetTracerProvider() 的返回值：那是全局 delegate，设回自身会
	// 让后续 SetTracerProvider 失效。用一个干净的 noop provider 收尾即可。
	t.Cleanup(func() { otel.SetTracerProvider(sdktrace.NewTracerProvider()) })

	exp := tracetest.NewInMemoryExporter()
	shutdown, err := initProvider(context.Background(), exp,
		Config{ServiceName: "unit-test", SampleRatio: 1}, true)
	require.NoError(t, err)

	ctx, end := trace.Start(context.Background(), "unit.span", attribute.String("case", "otelx"))
	end()
	require.NotNil(t, ctx)
	// InMemoryExporter.Shutdown 会清空缓存，断言必须在 shutdown 前做。
	spans := exp.GetSpans()
	require.Len(t, spans, 1, "Start 产生的 span 应被 exporter 收到")
	require.NoError(t, shutdown(context.Background()))
	require.Equal(t, "unit.span", spans[0].Name)
	attrs := attribute.NewSet(spans[0].Resource.Attributes()...)
	svc, ok := attrs.Value("service.name")
	require.True(t, ok)
	require.Equal(t, "unit-test", svc.AsString())
}

// 未初始化 SDK 时 Start 也必须可用（noop），业务代码不需要判空。
func TestStartWithoutSDK(t *testing.T) {
	old := otel.GetTracerProvider()
	otel.SetTracerProvider(sdktrace.NewTracerProvider())
	t.Cleanup(func() { otel.SetTracerProvider(old) })

	ctx, end := trace.Start(context.Background(), "noop.span")
	require.NotNil(t, ctx)
	end()
}
