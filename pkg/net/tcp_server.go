// pkg/net/tcp_server.go —— 通用 TCP 服务端
package net

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"
)

// Handler 处理新连接。
type Handler func(conn net.Conn)

// TCPServer TCP 服务端。
type TCPServer struct {
	addr     string
	listener net.Listener
	handler  Handler
	wg       sync.WaitGroup
	closeOnce sync.Once
	closeErr error
}

// NewTCPServer 创建 TCP 服务端。
func NewTCPServer(addr string, handler Handler) *TCPServer {
	return &TCPServer{
		addr:    addr,
		handler: handler,
	}
}

// Start 阻塞启动监听，直到 ctx 取消或发生监听错误。
func (s *TCPServer) Start(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", s.addr, err)
	}
	s.listener = ln

	acceptErrCh := make(chan error, 1)
	go s.acceptLoop(acceptErrCh)

	select {
	case <-ctx.Done():
		_ = s.Stop()
		return ctx.Err()
	case err := <-acceptErrCh:
		_ = s.Stop()
		return err
	}
}

func (s *TCPServer) acceptLoop(errCh chan<- error) {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			select {
			case errCh <- err:
			default:
			}
			return
		}

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handler(conn)
		}()
	}
}

// Stop 优雅关闭监听，等待已有连接 handler 返回（不强制关连接）。
func (s *TCPServer) Stop() error {
	s.closeOnce.Do(func() {
		if s.listener != nil {
			s.closeErr = s.listener.Close()
		}
	})
	s.wg.Wait()
	return s.closeErr
}

// Addr 返回实际监听地址。
func (s *TCPServer) Addr() net.Addr {
	if s.listener == nil {
		return nil
	}
	return s.listener.Addr()
}

// SetKeepAlive 设置 TCP keepalive。
func SetKeepAlive(conn net.Conn, period time.Duration) error {
	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		return nil
	}
	if err := tcpConn.SetKeepAlive(true); err != nil {
		return err
	}
	return tcpConn.SetKeepAlivePeriod(period)
}
