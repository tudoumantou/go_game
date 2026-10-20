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
//
// 约定：实现方自行负责该连接的读写循环与关闭；TCPServer 只保证
// 「一个连接一个 goroutine」，并在 Stop 时等待全部 handler 返回。
type Handler func(conn net.Conn)

// TCPServer TCP 服务端。
//
// closeOnce/closeErr 保证 Stop 幂等并缓存 listener.Close 的错误；
// wg 跟踪在途连接 handler，Stop 等待其全部返回（不强制断连，
// 主动断连由 handler 自行决定）。
type TCPServer struct {
	addr      string
	listener  net.Listener
	handler   Handler
	wg        sync.WaitGroup
	closeOnce sync.Once
	closeErr  error
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
	// accept 循环：listener 关闭（Stop 触发）时正常退出；
	// 其余错误只上报一次——容量 1 的 channel + default 分支保证
	// 错误风暴也不会让本 goroutine 卡在 channel 写上。
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
		// 连接处理与 accept 解耦：单个连接的读写阻塞不会拖住接受新连接；
		// wg 让 Stop 能等待全部 handler 收敛后再返回。
		go func() {
			defer s.wg.Done()
			s.handler(conn)
		}()
	}
}

// Stop 优雅关闭监听，等待已有连接 handler 返回（不强制关连接）。
//
// 幂等：Start 与外部调用可能同时触发，closeOnce 保证 listener 只关一次；
// wg.Wait 不设超时——强制断连的责任在 handler 侧。
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
//
// 非 *net.TCPConn（如测试用的 net.Pipe）时静默返回 nil：
// keepalive 是可选增强，不该让连接处理器为它写类型分支。
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
