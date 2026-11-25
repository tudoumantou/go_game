package breaker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestStateMachineOpensAndRecovers(t *testing.T) {
	var notices []State
	b := New(Options{Threshold: 2, Cooldown: 10 * time.Millisecond, Probes: 1,
		OnChange: func(s State) { notices = append(notices, s) }})

	require.Equal(t, Closed, b.State())
	b.Observe(errors.New("boom"))
	require.Equal(t, Closed, b.State())
	b.Observe(nil) // 成功清零
	require.Equal(t, 0, b.Failures())

	b.Observe(errors.New("boom"))
	b.Observe(errors.New("boom"))
	require.Equal(t, Open, b.State())
	require.False(t, b.Allow())
	require.Equal(t, []State{Open}, notices, "打开只通知一次")

	time.Sleep(20 * time.Millisecond)
	require.True(t, b.Allow(), "冷却到期放行探测")
	require.Equal(t, HalfOpen, b.State())
	b.Observe(nil)
	require.Equal(t, Closed, b.State())
	require.Equal(t, []State{Open, Closed}, notices)
}

func TestStateMachineProbeLimitAndReopen(t *testing.T) {
	b := New(Options{Threshold: 1, Cooldown: 10 * time.Millisecond, Probes: 2})
	b.Observe(errors.New("boom"))
	time.Sleep(20 * time.Millisecond)
	require.True(t, b.Allow())
	require.True(t, b.Allow())
	require.False(t, b.Allow(), "探测名额用尽后拒绝")

	b.Failure()
	require.Equal(t, Open, b.State(), "探测失败重新打开")
	require.False(t, b.Allow(), "冷却重新计时")
}

func TestStateMachineLateSuccessKeepsOpen(t *testing.T) {
	b := New(Options{Threshold: 1, Cooldown: time.Minute})
	b.Observe(errors.New("boom"))
	b.Observe(nil)
	require.Equal(t, Open, b.State(), "在途成功不该关掉刚打开的熔断")
}

func TestStateMachineDisabled(t *testing.T) {
	b := New(Options{Threshold: -1})
	for i := 0; i < 100; i++ {
		b.Failure()
	}
	require.Equal(t, Closed, b.State())
	require.True(t, b.Allow())
}

func TestStateMachineReset(t *testing.T) {
	b := New(Options{Threshold: 1})
	b.Failure()
	require.Equal(t, Open, b.State())
	b.Reset()
	require.Equal(t, Closed, b.State())
	require.Equal(t, 0, b.Failures())
}

func TestStateMachineConcurrent(t *testing.T) {
	b := New(Options{Threshold: 50, Cooldown: time.Millisecond})
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				if b.Allow() {
					if i%2 == 0 {
						b.Failure()
					} else {
						b.Success()
					}
				}
			}
		}(i)
	}
	wg.Wait()
	st := b.State()
	require.Contains(t, []State{Closed, Open, HalfOpen}, st)
	require.GreaterOrEqual(t, b.Failures(), 0)
}

// ---- 拦截器 ----

func TestInterceptorBusinessErrorDoesNotTrip(t *testing.T) {
	b := New(Options{Threshold: 1, Name: "center"})
	called := 0
	inv := UnaryInterceptor(b, InterceptorOptions{})
	bizErr := status.Error(codes.FailedPrecondition, "not in enroll phase")
	for i := 0; i < 10; i++ {
		err := inv(context.Background(), "/center.CrossEnroll", nil, nil, nil,
			func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
				called++
				return bizErr
			})
		require.ErrorIs(t, err, bizErr)
	}
	require.Equal(t, 10, called, "业务拒绝不该熔断，后续请求仍要放行")
	require.Equal(t, Closed, b.State())
}

func TestInterceptorFailsFastAfterThreshold(t *testing.T) {
	b := New(Options{Threshold: 2, Cooldown: time.Minute, Name: "center"})
	var rejected int
	called := 0
	inv := UnaryInterceptor(b, InterceptorOptions{OnReject: func(string) { rejected++ }})
	invoker := func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
		called++
		return status.Error(codes.Unavailable, "down")
	}

	require.Error(t, inv(context.Background(), "/m", nil, nil, nil, invoker))
	require.Error(t, inv(context.Background(), "/m", nil, nil, nil, invoker))
	require.Equal(t, Open, b.State())

	err := inv(context.Background(), "/m", nil, nil, nil, invoker)
	require.Equal(t, 2, called, "熔断打开后 invoker 不该再被调用")
	require.True(t, IsRejected(err), "拒绝错误要能被识别")
	require.Equal(t, 1, rejected)
	require.Equal(t, codes.Unavailable, status.Code(err))
}

func TestInterceptorSuccessAfterFailureResets(t *testing.T) {
	b := New(Options{Threshold: 3, Name: "center"})
	inv := UnaryInterceptor(b, InterceptorOptions{})
	fail := func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
		return status.Error(codes.DeadlineExceeded, "slow")
	}
	ok := func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
		return nil
	}
	_ = inv(context.Background(), "/m", nil, nil, nil, fail)
	require.Equal(t, 1, b.Failures())
	_ = inv(context.Background(), "/m", nil, nil, nil, ok)
	require.Equal(t, 0, b.Failures())
}

func TestIsRejectedDistinguishesRealUnavailable(t *testing.T) {
	require.False(t, IsRejected(nil))
	require.False(t, IsRejected(status.Error(codes.Unavailable, "real network error")))
	require.False(t, IsRejected(errors.New("plain")))
	b := New(Options{Threshold: 1, Name: "center"})
	b.Failure()
	inv := UnaryInterceptor(b, InterceptorOptions{})
	err := inv(context.Background(), "/m", nil, nil, nil,
		func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error { return nil })
	require.True(t, IsRejected(err))
}
