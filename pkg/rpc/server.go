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

// Stop 优雅关闭。
func (s *Server) Stop() {
	if s.gs != nil {
		s.gs.GracefulStop()
	}
}
