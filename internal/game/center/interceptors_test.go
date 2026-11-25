package center

import (
	"context"
	"errors"
	"testing"
	"time"

	"game_study/pkg/breaker"
	"game_study/pkg/config"

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
