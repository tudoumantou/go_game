// pkg/breaker/interceptor.go —— gRPC Unary 客户端熔断拦截器
//
// 只拦 Unary RPC：常驻 Report 流有自己的退避重连，流上的失败由 Send 的
// 错误逐条兜底，套熔断反而会把「一条上报失败」误算成「整个中心服故障」。
// 同步裁决类 RPC（CrossEnroll / QueryRank）才需要「中心服抖了就别再塞
// 慢请求进玩家链路」这道闸。
//
// 业务拒绝不算故障：中心服返回「非报名期」（InvalidArgument /
// FailedPrecondition 等）恰恰说明它很健康。把这些算进去会让正常业务
// 流量把熔断器打穿。判定可由 IsFailure 覆盖。
package breaker

import (
	"context"
	"errors"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// rejectedPrefix 熔断拒绝的 gRPC 文案前缀（IsRejected 靠它识别）。
const rejectedPrefix = "breaker open"

// InterceptorOptions 拦截器参数。
type InterceptorOptions struct {
	// IsFailure 判断一次 RPC 错误是否算服务故障；nil 用默认规则。
	IsFailure func(error) bool
	// OnReject 每次被熔断挡下时调用（指标/日志），可为 nil。
	OnReject func(method string)
}

// DefaultIsFailure 默认故障判定：只有「服务侧异常」算故障。
//
// 业务类 code 不算——它们是中心服正常工作时给的裁决结果；
// Unavailable / DeadlineExceeded / Internal / Unknown / ResourceExhausted
// 才代表对端不可用或过载，这类错误堆积起来才该打开熔断。
func DefaultIsFailure(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrOpen) {
		return false // 自己挡下的请求不参与统计，否则拒绝会自我放大
	}
	st, ok := status.FromError(err)
	if !ok {
		return true // 非 gRPC 错误（超时/网络）按故障处理
	}
	switch st.Code() {
	case codes.InvalidArgument, codes.NotFound, codes.AlreadyExists,
		codes.FailedPrecondition, codes.PermissionDenied, codes.Unauthenticated,
		codes.OutOfRange, codes.Canceled:
		return false
	default:
		return true
	}
}

// UnaryInterceptor 返回一个绑定 b 的 gRPC 客户端拦截器。
//
// 生命周期注意：b 必须在 Client 结构上而不是每次建连时新建——重连会
// 重建 ClientConn，但中心服的故障状态应该跨重连保留，否则每次重连都
// 给对端一轮全量探测。
func UnaryInterceptor(b *Breaker, opts InterceptorOptions) grpc.UnaryClientInterceptor {
	isFailure := opts.IsFailure
	if isFailure == nil {
		isFailure = DefaultIsFailure
	}
	return func(ctx context.Context, method string, req, reply any,
		cc *grpc.ClientConn, invoker grpc.UnaryInvoker, callOpts ...grpc.CallOption) error {
		if !b.Allow() {
			if opts.OnReject != nil {
				opts.OnReject(method)
			}
			return status.Error(codes.Unavailable, rejectedPrefix+": "+b.Name())
		}
		err := invoker(ctx, method, req, reply, cc, callOpts...)
		if isFailure(err) {
			b.Failure()
		} else {
			b.Success()
		}
		return err
	}
}

// IsRejected 判断错误是否由本拦截器熔断挡下（区别于对端真的返回 Unavailable）。
//
// 调用方用它区分「中心服挂了」和「本闸关着」：两者都是 Unavailable，
// 但前者要告警，后者只需按降级路径走。
func IsRejected(err error) bool {
	if err == nil {
		return false
	}
	st, ok := status.FromError(err)
	return ok && st.Code() == codes.Unavailable && strings.HasPrefix(st.Message(), rejectedPrefix)
}
