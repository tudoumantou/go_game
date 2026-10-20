// pkg/rpc/server.go —— gRPC Server 启动/关闭封装
//
// 架构职责：组装 net.Listener + grpc.Server + keepalive 策略，提供阻塞式
// Start（跟随 ctx）与有界优雅停止；业务 service 经 GRPCServer() 挂载。
// 协作关系：center/game 进程启动尾部 Start；shutdown 编排在 STOP 阶段
// 调 Stop；registry/etcd 在 Start 成功后注册节点（顺序：先监听、后注册，
// 确保 etcd 里的节点一定可拨通）。
// 设计取舍：
//   - 有界优雅期（Stop 默认 10s）：grpc.GracefulStop 等所有活跃 RPC 自然
//     结束，而双向流（GameService.Channel / CrossService.Channel）在客户端
//     不主动断开时永不结束——无界等待会把关服卡到被 SIGKILL。
//     10s = 普通请求收尾的富余 + 容器 terminationGracePeriod 之内的预算。
//   - keepalive  enforcement 放宽（MinTime 30s、PermitWithoutStream=true）：
//     防止客户端过频 ping 拖死服务端，同时允许无流探测及时清死连接。
package rpc

import (
	"context"
	"fmt"
	"net"
	"time"

	"game_study/pkg/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

// Server gRPC 服务端封装。
//
// 字段契约：gs/ln 在 Start 成功后非 nil；logger 用于监听/关闭日志；
// addr 是配置的监听地址（可能带 :0 让 OS 选端口，实际端口经 ln.Addr() 取）。
type Server struct {
	addr   string
	logger *log.Logger
	gs     *grpc.Server
	ln     net.Listener
}

// NewServer 创建 gRPC Server。
// opts 由调用方传入（拦截器/限流/凭据等），后置 append 可覆盖默认 keepalive
// 策略——框架默认值只服务「没传 opts」的多数场景，特殊需求不被绑架。
func NewServer(addr string, logger *log.Logger, opts ...grpc.ServerOption) *Server {
	// 默认放宽 keepalive 策略（客户端 60s 心跳，服务端最小间隔 30s）
	base := []grpc.ServerOption{
		// MinTime：客户端 ping 间隔小于 30s 视为滥用，服务端回 GOAWAY
		// 踢断——防某个配置错误的客户端高频 ping 拖垮 event loop。
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             30 * time.Second,
			PermitWithoutStream: true,
		}),
	}
	base = append(base, opts...)
	return &Server{
		addr:   addr,
		logger: logger,
		gs:     grpc.NewServer(base...),
	}
}

// GRPCServer 返回底层 grpc.Server。
// 契约：RegisterXxxServiceServer 必须在 Start 之前完成——Serve 开始后
// 再注册服务会 panic（gRPC 不允许运行中改服务表）。
func (s *Server) GRPCServer() *grpc.Server { return s.gs }

// Start 阻塞启动，直到 ctx 取消。
//
// 阻塞语义与 net.TCPServer.Start 一致：返回即监听终止，调用方按返回值
// 区分正常关服（ctx.Err()）与 Serve 故障。
// listener 的所有权在返回后移交给 gs（Serve 持有），关闭经
// GracefulStop/Stop 连带完成，调用方不要再单独 Close ln。
func (s *Server) Start(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", s.addr, err)
	}
	s.ln = ln
	s.logger.Info("grpc server listening", "addr", s.addr)

	// Serve 异步跑：主协程留在 select 上等信号，ctx 取消时直接返回
	// （GracefulStop 由 Stop 另行触发，不在这里耦合）。
	errCh := make(chan error, 1)
	go func() {
		if err := s.gs.Serve(ln); err != nil {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errCh:
		return err
	}
}

// Stop 优雅关闭（默认最多等 10s，超时强制断开）。
//
// 背景：grpc 的 GracefulStop 会等所有活跃 RPC 自然结束，而双向流（GameService.Channel、
// CrossService.Channel）在客户端不主动断开时**永远不会结束** —— 直接调用会永久阻塞，
// 线上表现为「优雅关闭卡住，直到 K8s terminationGracePeriodSeconds 到期被 SIGKILL」。
// 因此这里给一个有限的优雅期：期内让正常请求收尾，超期强制关闭。
func (s *Server) Stop() {
	s.StopWithTimeout(10 * time.Second)
}

// StopWithTimeout 带超时的优雅关闭。
// d<=0 表示「不优雅」：直接 Stop（立即断所有连接），供紧急场景使用。
func (s *Server) StopWithTimeout(d time.Duration) {
	if s.gs == nil {
		return
	}
	if d <= 0 {
		s.gs.Stop()
		return
	}

	// GracefulStop 放子 goroutine：它可能永远不返回（双向流），主协程
	// 必须留在 select 上掌控超时；done 用 close 广播「正常结束」——
	// close 会唤醒全部等待者，这里只有一个接收方，但语义比 send 更稳
	// （不存在「发了没人收」的悬挂）。
	done := make(chan struct{})
	go func() {
		s.gs.GracefulStop()
		close(done)
	}()

	timer := time.NewTimer(d)
	// defer Stop：超时分支走到时 timer 已触发无需 Stop，但正常分支
	// 必须显式停表释放 runtime timer，否则要等 d 自然到期才回收。
	defer timer.Stop()

	select {
	case <-done:
	case <-timer.C:
		// 超时：强制关闭所有连接与活跃 stream
		s.gs.Stop()
		// 等 GracefulStop 也返回：被 Stop 打断后它才会退出，等它保证
		// 子 goroutine 收敛、其引用的资源（回调/stream）不再被触碰后，
		// 本函数才返回——调用方下一步 Close 其它组件时才安全。
		<-done
	}
}
