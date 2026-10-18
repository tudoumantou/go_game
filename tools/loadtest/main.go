// tools/loadtest/main.go —— 网关连接压测与冒烟（设计文档 23.4 / 19.5）
//
// 两个子命令：
//   -conn   连接保持压测：批量建立 TCP 连接并保持心跳，验证网关 10W 连接承载
//   -smoke  E2E 冒烟：登录 → 握手 → 创角/进游戏 → 拉角色信息（19.5 节第 6 条）
//
// 用法：
//
//	go run ./tools/loadtest -conn -n 100000 -gateway 127.0.0.1:9001 -hold 5m
//	go run ./tools/loadtest -smoke -login http://127.0.0.1:8080 -gateway 127.0.0.1:9001
//
// 说明：
//   - 压测只建立 TCP 连接 + 完成握手，不进入游戏（不进 Actor，避免压到玩家逻辑）；
//     如需全链路压测，先跑 -smoke 校验正确性，再逐步加 -conn 的并发。
//   - 连接数受本机 ulimit -n 限制，10W 连接需要 `ulimit -n 200000`。
package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"game_study/test/robot"
)

func main() {
	var (
		mode       = flag.String("mode", "smoke", "运行模式：conn | smoke")
		conns      = flag.Int("n", 1000, "并发连接数（conn 模式）")
		batch      = flag.Int("batch", 500, "每批建连数，批间 sleep 让网关喘口气")
		batchDelay = flag.Duration("batch-delay", 200*time.Millisecond, "批间间隔")
		hold       = flag.Duration("hold", 30*time.Second, "连接保持时长")
		gateway    = flag.String("gateway", "127.0.0.1:9001", "网关 TCP 地址")
		loginURL   = flag.String("login", "http://127.0.0.1:8080", "登录服 HTTP 地址")
		devices    = flag.Int("devices", 1, "smoke 模式的角色数")
	)
	flag.Parse()

	var err error
	switch *mode {
	case "conn":
		err = runConn(*conns, *batch, *batchDelay, *hold, *gateway)
	case "smoke":
		err = runSmoke(*loginURL, *gateway, *devices)
	default:
		err = fmt.Errorf("unknown mode %q (want conn|smoke)", *mode)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "FAIL: %v\n", err)
		os.Exit(1)
	}
}

// runConn 批量建立并保持 TCP 连接（23.4 表第 1 行：单网关 10W 连接保持）。
func runConn(total, batchSize int, batchDelay, hold time.Duration, addr string) error {
	if total <= 0 {
		return fmt.Errorf("-n must be > 0")
	}
	if batchSize <= 0 {
		batchSize = total
	}
	// 软限制提示：10W 连接至少要 2 倍 fd（socket + epoll 等）
	fmt.Printf("目标 %d 连接 → %s（保持 %s，每批 %d）\n", total, addr, hold, batchSize)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	var (
		established atomic.Int64
		failed      atomic.Int64
		mu          sync.Mutex
		conns       []net.Conn
	)

	start := time.Now()
	for i := 0; i < total; i += batchSize {
		if ctx.Err() != nil {
			break
		}
		n := batchSize
		if i+n > total {
			n = total - i
		}
		var wg sync.WaitGroup
		wg.Add(n)
		chunk := make([]net.Conn, n)
		for j := 0; j < n; j++ {
			go func(idx int) {
				defer wg.Done()
				d := net.Dialer{Timeout: 5 * time.Second}
				c, err := d.DialContext(ctx, "tcp", addr)
				if err != nil {
					failed.Add(1)
					return
				}
				chunk[idx] = c
				established.Add(1)
			}(j)
		}
		wg.Wait()
		mu.Lock()
		for _, c := range chunk {
			if c != nil {
				conns = append(conns, c)
			}
		}
		mu.Unlock()

		if elapsed := time.Since(start); elapsed > 0 {
			fmt.Printf("\r已建立 %d / 目标 %d（失败 %d，%.0f conn/s）",
				established.Load(), total, failed.Load(),
				float64(established.Load())/elapsed.Seconds())
		}
		select {
		case <-ctx.Done():
		case <-time.After(batchDelay):
		}
	}

	fmt.Printf("\n建连完成：成功 %d，失败 %d，耗时 %s\n",
		established.Load(), failed.Load(), time.Since(start).Round(time.Millisecond))

	// 保持阶段：定期检查连接是否被网关踢掉
	tick := time.NewTicker(5 * time.Second)
	defer tick.Stop()
	holdTimer := time.NewTimer(hold)
	defer holdTimer.Stop()
	alive := established.Load()

loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case <-holdTimer.C:
			break loop
		case <-tick.C:
			// 用一次非阻塞读探测对端是否已关闭：SetReadDeadline(now) + Read
			var dead int64
			for _, c := range conns {
				_ = c.SetReadDeadline(time.Now())
				buf := make([]byte, 1)
				if _, err := c.Read(buf); err != nil {
					if ne, ok := err.(net.Error); ok && ne.Timeout() {
						continue // 超时 = 连接存活
					}
					dead++
				}
			}
			alive = established.Load() - dead
			fmt.Printf("保持中：存活 %d / %d（掉线 %d）\n", alive, established.Load(), dead)
		}
	}

	// 收尾
	mu.Lock()
	for _, c := range conns {
		_ = c.Close()
	}
	mu.Unlock()
	fmt.Printf("压测结束：峰值连接 %d，最终存活 %d\n", established.Load(), alive)
	if failed.Load() > 0 {
		return fmt.Errorf("%d 个连接建立失败", failed.Load())
	}
	return nil
}

// runSmoke E2E 冒烟（19.5 第 6 条五步）：登录 → 握手 → 创角 → 进游戏 → 角色信息。
func runSmoke(loginURL, gatewayAddr string, devices int) error {
	if devices <= 0 {
		devices = 1
	}
	for i := 0; i < devices; i++ {
		deviceID := fmt.Sprintf("loadtest-%d-%d", time.Now().UnixNano(), i)
		roleName := fmt.Sprintf("压测%d", time.Now().UnixNano()%1000000)

		c := robot.New(loginURL, gatewayAddr)
		if err := c.GuestLogin(deviceID); err != nil {
			return fmt.Errorf("[%d] guest login: %w", i, err)
		}
		if err := c.Connect(); err != nil {
			return fmt.Errorf("[%d] connect: %w", i, err)
		}
		if err := c.Handshake(); err != nil {
			c.Close()
			return fmt.Errorf("[%d] handshake: %w", i, err)
		}
		hs, err := c.SendHandshakeReq("1.0.0", "loadtest")
		if err != nil {
			c.Close()
			return fmt.Errorf("[%d] handshake req: %w", i, err)
		}
		if hs.Code != 0 || hs.AccountId == 0 {
			c.Close()
			return fmt.Errorf("[%d] handshake code=%d account=%d", i, hs.Code, hs.AccountId)
		}

		// 无角色 → 2003（首次登录的正常路径）
		eg, err := c.EnterGame()
		if err != nil {
			c.Close()
			return fmt.Errorf("[%d] enter game: %w", i, err)
		}
		if eg.Code == 2003 {
			cr, err := c.CreateRole(roleName, 1)
			if err != nil {
				c.Close()
				return fmt.Errorf("[%d] create role: %w", i, err)
			}
			if cr.Code != 0 || cr.RoleId == 0 {
				c.Close()
				return fmt.Errorf("[%d] create role code=%d role=%d", i, cr.Code, cr.RoleId)
			}
		} else if eg.Code != 0 {
			c.Close()
			return fmt.Errorf("[%d] enter game code=%d", i, eg.Code)
		}

		info, err := c.PlayerInfo()
		c.Close()
		if err != nil {
			return fmt.Errorf("[%d] player info: %w", i, err)
		}
		if info.RoleId == 0 || info.Level == 0 {
			return fmt.Errorf("[%d] player info incomplete: %+v", i, info)
		}
		fmt.Printf("smoke[%d] OK  role=%d name=%s level=%d exp=%d\n",
			i, info.RoleId, info.Name, info.Level, info.Exp)
	}
	fmt.Printf("冒烟通过：%d 个角色完成「登录→握手→创角→进游戏→角色信息」\n", devices)
	return nil
}
