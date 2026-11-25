// internal/game/center/interceptors.go —— 出向 Unary RPC 的 RED 观测拦截器
//
// 与熔断器的分工：熔断决定「这条请求发不发」，本拦截器只记录「发出了、
// 结果如何、花了多久」。两者叠在同一个 Unary 链上，Report 流不经过。
//
// 指标按 method 拆分但 method 是 proto 里的封闭集合，不会高基数。
package center

import (
	"context"
	"time"

	"game_study/pkg/breaker"
	"game_study/pkg/config"
	"game_study/pkg/metrics"
	"game_study/pkg/trace"

	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/grpc"
)

// BreakerOptionsFrom 把配置段转成熔断器参数。
//
// 解析失败的 cooldown 留空交给 breaker.normalize 取默认值——配置写错
// 不该让服务起不来，熔断只是保护手段，降级到默认参数仍然有效。
func BreakerOptionsFrom(cfg config.BreakerConfig) breaker.Options {
	opts := breaker.Options{Threshold: cfg.Threshold, Probes: cfg.Probes}
	if cfg.Cooldown != "" {
		if d, err := config.ParseDuration(cfg.Cooldown); err == nil {
			opts.Cooldown = d
		}
	}
	return opts
}

// metricsInterceptor 记录单次 Unary RPC 的在途数、结果与耗时。
func (c *Client) metricsInterceptor(ctx context.Context, method string, req, reply any,
	cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	on := metrics.Enabled()
	if on {
		metrics.RPCInflightInc()
		defer metrics.RPCInflightDec()
	}
	// span 覆盖整次调用（含熔断快速失败）：报告「这一跳花了多久」。
	ctx, endSpan := trace.Start(ctx, "center.rpc", attribute.String("rpc.method", method))
	defer endSpan()
	start := time.Now()
	err := invoker(ctx, method, req, reply, cc, opts...)
	if on {
		result := "ok"
		switch {
		case err == nil:
		case breaker.IsRejected(err):
			result = "rejected"
		default:
			result = "error"
		}
		metrics.RPCRequest(method, result)
		metrics.RPCObserve(method, time.Since(start))
	}
	return err
}
