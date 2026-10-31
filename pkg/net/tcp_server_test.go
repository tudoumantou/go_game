// pkg/net/tcp_server_test.go —— TCPServer 的生命周期契约测试
//
// 覆盖四条契约（都在 tcp_server.go 的注释里写着，此前没有测试守）：
//
//  1. Start 阻塞直到 ctx 取消或 Stop 被调用，正常关服返回 nil；
//  2. Stop 幂等且并发安全，重复调用返回同一个错误值；
//  3. Stop 先关 listener（拒新连接），再等存量 handler（不强制断连）；
//  4. Stop 先于 Start 到达时不能让端口被监听起来。
//
// 本文件同时是 -race 的回归：listener 字段曾被 Start 写、Stop/Addr 无锁读，
// 只有真正并发跑起来才能暴露。
package net

import (
	"context"
	"io"
	stdnet "net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// startServer 起一个监听 127.0.0.1:0 的服务，返回 server 与实际地址。
//
// 用 :0 让操作系统选端口：固定端口在 CI 上会与并行任务撞车，且「端口被占用」
// 这个错误应该由专门的用例覆盖，不该污染其余用例。
func startServer(t *testing.T, handler Handler) (*TCPServer, string) {
	t.Helper()
	s := NewTCPServer("127.0.0.1:0", handler)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Start(ctx) }()
	t.Cleanup(func() {
		cancel()
		_ = s.Stop()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("Start 未在 2s 内返回")
		}
	})
	return s, waitAddr(t, s)
}

// waitAddr 等监听就绪。Addr 在 Start 完成 listen 之前返回 nil，
// 所以首个连接不能立刻拨号——那是测试里最常见的偶发失败来源。
func waitAddr(t *testing.T, s *TCPServer) string {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if a := s.Addr(); a != nil {
			return a.String()
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("监听地址在 2s 内未就绪")
	return ""
}

func TestAddrBeforeStartIsNil(t *testing.T) {
	s := NewTCPServer("127.0.0.1:0", func(stdnet.Conn) {})
	require.Nil(t, s.Addr(), "Start 之前不应有监听地址")
}

// TestServeConnection 一条连接的完整往返：handler 收到字节、回写、连接关闭。
func TestServeConnection(t *testing.T) {
	got := make(chan string, 1)
	_, addr := startServer(t, func(conn stdnet.Conn) {
		defer conn.Close()
		buf := make([]byte, 4)
		if _, err := io.ReadFull(conn, buf); err != nil {
			return
		}
		got <- string(buf)
		_, _ = conn.Write([]byte("pong"))
	})

	conn, err := stdnet.Dial("tcp", addr)
	require.NoError(t, err)
	defer conn.Close()

	_, err = conn.Write([]byte("ping"))
	require.NoError(t, err)
	require.Equal(t, "ping", <-got, "handler 应收到完整 4 字节")

	require.NoError(t, conn.SetReadDeadline(time.Now().Add(2*time.Second)))
	resp := make([]byte, 4)
	_, err = io.ReadFull(conn, resp)
	require.NoError(t, err)
	require.Equal(t, "pong", string(resp))
}

// TestStopIsIdempotentUnderConcurrency Stop 可能被 shutdown 编排与 Start
// 内部路径同时触发：并发调 10 次必须全部返回且不 panic、返回值一致。
func TestStopIsIdempotentUnderConcurrency(t *testing.T) {
	s, _ := startServer(t, func(conn stdnet.Conn) { _ = conn.Close() })

	const n = 10
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = s.Stop()
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		require.NoError(t, err, "第 %d 次 Stop 返回错误", i)
	}
	// closeOnce 缓存首次结果：再次调用仍是同一个值，不会变成 nil。
	require.NoError(t, s.Stop())
}

// TestStopRejectsNewConnection 关服的「先拒新」：Stop 之后新连接必须失败，
// 否则排空永远等不到头（新连接会不断进来）。
func TestStopRejectsNewConnection(t *testing.T) {
	s, addr := startServer(t, func(conn stdnet.Conn) { _ = conn.Close() })
	require.NoError(t, s.Stop())

	conn, err := stdnet.DialTimeout("tcp", addr, 200*time.Millisecond)
	if err == nil {
		_ = conn.Close()
		t.Fatal("Stop 之后仍能建立连接：listener 没有被关闭")
	}
}

// TestStopWaitsForInflightHandler 关服的「后排空」：在途 handler 没返回时
// Stop 必须阻塞，返回后 Stop 才能结束。
func TestStopWaitsForInflightHandler(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	s, addr := startServer(t, func(conn stdnet.Conn) {
		close(entered)
		<-release
		_ = conn.Close()
	})

	conn, err := stdnet.Dial("tcp", addr)
	require.NoError(t, err)
	defer conn.Close()
	<-entered // handler 已经进来，此时它在等 release

	stopDone := make(chan error, 1)
	go func() { stopDone <- s.Stop() }()

	select {
	case <-stopDone:
		t.Fatal("Stop 在在途连接未结束时返回了：wg.Wait 没起作用")
	case <-time.After(100 * time.Millisecond):
		// 预期：仍在等
	}

	close(release)
	select {
	case err := <-stopDone:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("在途连接结束后 Stop 仍未返回")
	}
}

// TestStopUnblocksStart 外部 Stop（ctx 未取消）必须让 Start 返回 nil。
// 修复前：Stop 关掉 listener 后 acceptLoop 静默退出，Start 的 select 只在
// ctx.Done / acceptErrCh 上等，于是永久挂起——关服编排会卡在这里。
func TestStopUnblocksStart(t *testing.T) {
	s := NewTCPServer("127.0.0.1:0", func(conn stdnet.Conn) { _ = conn.Close() })
	done := make(chan error, 1)
	go func() { done <- s.Start(context.Background()) }()
	waitAddr(t, s)

	require.NoError(t, s.Stop())

	select {
	case err := <-done:
		// 返回 nil 而非 context.Canceled：这是「外部要求关闭」，
		// 不是启动失败，也不是 ctx 到期。
		require.NoError(t, err, "外部 Stop 应让 Start 正常返回")
	case <-time.After(2 * time.Second):
		t.Fatal("Stop 未能打断 Start：它还在等 ctx")
	}
}

// TestStopBeforeStartDoesNotListen Stop 先于 Start 到达时，closeOnce 关的是
// nil；若 Start 不补判一次，端口会被监听起来且再没人关它。
func TestStopBeforeStartDoesNotListen(t *testing.T) {
	s := NewTCPServer("127.0.0.1:0", func(conn stdnet.Conn) { _ = conn.Close() })
	require.NoError(t, s.Stop(), "Start 之前 Stop 不应报错")

	// ctx 永不取消：若实现有洞，这里会永久阻塞并让测试超时。
	done := make(chan error, 1)
	go func() { done <- s.Start(context.Background()) }()

	select {
	case err := <-done:
		require.NoError(t, err, "Stop 先到时 Start 应正常收敛")
	case <-time.After(2 * time.Second):
		t.Fatal("Stop 先于 Start 时 Start 没有收敛：端口被监听起来且无人关闭")
	}

	// 端口必须真的关掉了。
	if addr := s.Addr(); addr != nil {
		conn, err := stdnet.DialTimeout("tcp", addr.String(), 200*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			t.Error("Stop 先于 Start 时端口仍可连接")
		}
	}
}

// TestStartReturnsListenError 监听失败必须以 error 返回（调用方据此退出），
// 不能静默返回 nil。
func TestStartReturnsListenError(t *testing.T) {
	s := NewTCPServer("256.0.0.1:1", func(stdnet.Conn) {})
	err := s.Start(context.Background())
	require.Error(t, err, "非法地址应返回错误")
	require.Contains(t, err.Error(), "listen", "错误应带上下文：%v", err)
}

// TestSetKeepAlive 非 *net.TCPConn（net.Pipe）静默返回 nil：keepalive 是
// 可选增强，不该让 handler 为它写类型分支；真实 TCP 连接必须设置成功。
func TestSetKeepAlive(t *testing.T) {
	c1, c2 := stdnet.Pipe()
	defer c1.Close()
	defer c2.Close()
	require.NoError(t, SetKeepAlive(c1, time.Second), "net.Pipe 应静默跳过")

	ln, err := stdnet.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()
	go func() {
		c, err := ln.Accept()
		if err == nil {
			_ = c.Close()
		}
	}()
	conn, err := stdnet.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)
	defer conn.Close()
	require.NoError(t, SetKeepAlive(conn, 30*time.Second))
}
