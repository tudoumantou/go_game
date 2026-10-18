// pkg/rpc/server.go —— gRPC Server 启动/关闭封装
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
type Server struct {
	addr   string
	logger *log.Logger
	gs     *grpc.Server
	ln     net.Listener
}

// NewServer 创建 gRPC Server。
func NewServer(addr string, logger *log.Logger, opts ...grpc.ServerOption) *Server {
	// 默认放宽 keepalive 策略（客户端 60s 心跳，服务端最小间隔 30s）
	base := []grpc.ServerOption{
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
func (s *Server) GRPCServer() *grpc.Server { return s.gs }

// Start 阻塞启动，直到 ctx 取消。
func (s *Server) Start(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", s.addr, err)
	}
	s.ln = ln
	s.logger.Info("grpc server listening", "addr", s.addr)

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
func (s *Server) StopWithTimeout(d time.Duration) {
	if s.gs == nil {
		return
	}
	if d <= 0 {
		s.gs.Stop()
		return
	}

	done := make(chan struct{})
	go func() {
		s.gs.GracefulStop()
		close(done)
	}()

	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-done:
	case <-timer.C:
		// 超时：强制关闭所有连接与活跃 stream
		s.gs.Stop()
		<-done
	}
}
