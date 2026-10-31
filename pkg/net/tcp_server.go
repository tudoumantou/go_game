// pkg/net/tcp_server.go —— 通用 TCP 服务端
//
// 架构职责：监听端口、accept 连接、为每个连接起一个 goroutine 交给 Handler，
// 并在关闭时等待全部连接处理收敛。不解析任何协议。
// 协作关系：gateway 进程持有本服务端接客户端；Stop 可由 shutdown 编排
// 或 Start 内部（ctx 取消）触发，两条路径经 closeOnce 收敛为一次。
// 设计取舍：
//   - 「先拒新、后排空」的关闭顺序：Stop 先关 listener（新连接被拒），
//     再 wg.Wait() 等存量连接自然结束——反序（先等后关）会无限期接受
//     新连接，优雅退出永远等不到头。
//   - 不强制断连：TCP 连接上可能还有未读完的业务消息，强断等于丢请求；
//     主动断连的责任在 Handler（读超时/协议错误/ctx 取消）。
package net

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// Handler 处理新连接。
//
// 约定：实现方自行负责该连接的读写循环与关闭；TCPServer 只保证
// 「一个连接一个 goroutine」，并在 Stop 时等待全部 handler 返回。
// 契约要点：handler 必须能响应「对端关闭/读超时」而返回，否则 Stop 的
// wg.Wait 会一直等它（TCPServer 不强制断连，见 Stop 注释）。
type Handler func(conn net.Conn)

// TCPServer TCP 服务端。
//
// closeOnce/closeErr 保证 Stop 幂等并缓存 listener.Close 的错误；
// wg 跟踪在途连接 handler，Stop 等待其全部返回（不强制断连，
// 主动断连由 handler 自行决定）。
// 线程契约：Start 只能调一次；Stop 可重复/并发调用；addr/handler 构造后只读。
type TCPServer struct {
	addr string
	// listener 接口类型：测试可注入 fake listener
	listener net.Listener
	handler  Handler
	// wg 跟踪在途连接 goroutine
	wg sync.WaitGroup
	// closeOnce 保证 listener 只关一次
	closeOnce sync.Once
	// closeErr 缓存首次关闭错误，重复 Stop 返回同一值
	closeErr error
	// mu 保护 listener / stopCh：Start 写入、Stop 与 Addr 读取，三者可能
	// 来自不同 goroutine（shutdown 编排可以在 Start 阻塞期间调 Stop）。
	// 不加锁就是数据竞争——-race 下必现，且可能读到半写的接口值。
	mu sync.Mutex
	// stopCh 由 Stop 关闭：让阻塞在 select 里的 Start 立刻返回。
	// 没有它时「外部 Stop 但 ctx 未取消」会让 Start 一直挂着，
	// 表现为关服编排在等一个永远不会返回的 Start。
	stopCh chan struct{}
	// stopped 标记 Stop 已经跑过。Start 在 listen 之后必须再看一眼：
	// Stop 先于 Start 到达时 closeOnce 关掉的是 nil，不补这一判，
	// 端口会被监听起来且再没人关它（优雅关闭永远等不到）。
	stopped atomic.Bool
}

// NewTCPServer 创建 TCP 服务端。
// 此时尚未监听（无 net.Listen 调用），addr 校验推迟到 Start——提前 listen
// 会把「端口占用」错误从 Start 提前到构造函数，破坏启动顺序的可编排性。
func NewTCPServer(addr string, handler Handler) *TCPServer {
	return &TCPServer{
		addr:    addr,
		handler: handler,
		stopCh:  make(chan struct{}),
	}
}

// Start 阻塞启动监听，直到 ctx 取消或发生监听错误。
//
// 阻塞语义是刻意的：上层 shutdown 编排需要「服务已就绪」与「服务已退出」
// 两个明确信号，Start 返回即意味着监听已终止（无论正常还是异常），
// 调用方按返回值区分：ctx.Err() = 正常关服，其他 = 故障。
func (s *TCPServer) Start(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", s.addr, err)
	}
	s.mu.Lock()
	s.listener = ln
	s.mu.Unlock()

	// Stop 先于 Start 到达：closeOnce 已经执行过（关的是 nil），
	// 这里必须自己关，否则端口被监听起来而没有任何人会再关它。
	// 返回 nil 而非错误：外部已经要求关闭，这属于正常收敛，不是启动失败。
	if s.stopped.Load() {
		_ = ln.Close()
		return nil
	}

	// 容量 1：只关心「第一个错误」，后续 accept 错误直接走 default 丢弃
	// （见 acceptLoop），这里不需要缓冲更多。
	acceptErrCh := make(chan error, 1)
	go s.acceptLoop(acceptErrCh)

	select {
	case <-ctx.Done():
		// 关服信号：先自行 Stop 收敛 listener 与在途连接，再上报取消原因。
		_ = s.Stop()
		return ctx.Err()
	case <-s.stopSignal():
		// 外部 Stop（ctx 未取消）：监听已收敛，按正常关服返回 nil。
		// 若这里也返回错误，调用方会把一次「主动关服」当成启动失败。
		return nil
	case err := <-acceptErrCh:
		// 监听级错误（非连接级）：同样先收敛再原样上报。
		_ = s.Stop()
		return err
	}
}

// stopSignal 返回 stopCh（惰性创建，兼容零值构造）。
//
// 直接用字段会让零值 TCPServer 的 select 永久阻塞在 nil channel 上；
// 测试与手工构造都走这里，保证「Stop 一定能唤醒 Start」这条契约恒成立。
// 返回双向 channel：Stop 要 close 它，Start 只读——用同一个函数避免
// 两处各自加锁创建的窗口。
func (s *TCPServer) stopSignal() chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopCh == nil {
		s.stopCh = make(chan struct{})
	}
	return s.stopCh
}

// acceptLoop 接受连接循环（内部 goroutine）。
func (s *TCPServer) acceptLoop(errCh chan<- error) {
	// accept 循环：listener 关闭（Stop 触发）时正常退出；
	// 其余错误只上报一次——容量 1 的 channel + default 分支保证
	// 错误风暴也不会让本 goroutine 卡在 channel 写上。
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			// errors.Is 而非 ==：listener 被 Close 后 Accept 返回的
			// net.ErrClosed 可能被包装，解包比较才可靠。
			if errors.Is(err, net.ErrClosed) {
				return
			}
			// 非关闭原因（fd 耗尽等）：上报后退出循环，由 Start 决定进程命运。
			// default 分支：errCh 已有一个未读错误时直接丢弃，本 goroutine 不阻塞。
			select {
			case errCh <- err:
			default:
			}
			return
		}

		// wg.Add 必须在 go 之前：若放 goroutine 内，Stop 的 wg.Wait 可能
		// 在 Add 执行前就跑完，漏等这个连接（经典 Add/Wait 竞态）。
		s.wg.Add(1)
		// 连接处理与 accept 解耦：单个连接的读写阻塞不会拖住接受新连接；
		// wg 让 Stop 能等待全部 handler 收敛后再返回。
		go func() {
			// defer 保证 handler panic 时计数也能归还，否则 Stop 永远等不到。
			defer s.wg.Done()
			s.handler(conn)
		}()
	}
}

// Stop 优雅关闭监听，等待已有连接 handler 返回（不强制关连接）。
//
// 幂等：Start 与外部调用可能同时触发，closeOnce 保证 listener 只关一次；
// wg.Wait 不设超时——强制断连的责任在 handler 侧。
// 顺序即策略：先关 listener（拒新）再 Wait（排空），与「先拒新后排空」对应。
func (s *TCPServer) Stop() error {
	s.closeOnce.Do(func() {
		// 先置标记：Start 若正在 listen 与 accept 之间，会在 listen 之后
		// 看到它并自行关闭，不会留下「关了还在听」的窗口。
		s.stopped.Store(true)
		s.mu.Lock()
		ln := s.listener
		s.mu.Unlock()
		// 顺序：关 listener（拒新）→ 关 stopCh（唤醒 Start）。
		if ln != nil {
			s.closeErr = ln.Close()
		}
		close(s.stopSignal())
	})
	s.wg.Wait()
	return s.closeErr
}

// Addr 返回实际监听地址。
// Start 前调用返回 nil（listener 未创建）：调用方需判空，
// 端口 0 场景下（让 OS 选端口）只有监听后才知道真实端口。
func (s *TCPServer) Addr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener == nil {
		return nil
	}
	return s.listener.Addr()
}

// SetKeepAlive 设置 TCP keepalive。
//
// 非 *net.TCPConn（如测试用的 net.Pipe）时静默返回 nil：
// keepalive 是可选增强，不该让连接处理器为它写类型分支。
// 为什么需要：对端宕机/网络中断时不会发 FIN，连接呈「半开」状态，
// 依赖 OS 默认 keepalive（通常 2 小时）才发现太慢；这里主动 30s 周期探测，
// 让网关尽快回收死连接、触发会话重连逻辑。
func SetKeepAlive(conn net.Conn, period time.Duration) error {
	// 类型断言：net.Conn 是接口，具体类型可能是 *net.TCPConn 或测试替身
	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		return nil
	}
	if err := tcpConn.SetKeepAlive(true); err != nil {
		return err
	}
	return tcpConn.SetKeepAlivePeriod(period)
}
