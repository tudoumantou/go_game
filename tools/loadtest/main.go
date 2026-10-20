// tools/loadtest/main.go —— 网关连接压测与冒烟（设计文档 23.4 / 19.5）
//
// 工程链位置：对应 Makefile target `make loadtest`（默认 conn 模式，参数可被
// N/HOLD 覆盖）与 `make bench`（scripts/bench.sh 驱动 login/biz/conn 三模式，
// 产物汇总到 reports/）；smoke 模式对应 19.5 节第 6 条 E2E 冒烟。
//
// 四个子命令：
//
//	-mode conn   连接保持压测：批量建立 TCP 连接并保持，验证网关 10W 连接承载
//	-mode login  登录吞吐压测：HTTP 游客登录 + RSA 握手 + 加密登录请求
//	-mode biz    业务消息压测：进游戏后混合发「背包/角色信息/聊天」，测 Actor 全链路延迟
//	-mode smoke  E2E 冒烟：登录 → 握手 → 创角/进游戏 → 拉角色信息（19.5 节第 6 条）
//
// 用法：
//
//	go run ./tools/loadtest -mode conn  -n 100000 -gateway 127.0.0.1:9001 -hold 5m
//	go run ./tools/loadtest -mode login -n 2000 -c 50 -json /tmp/login.json
//	go run ./tools/loadtest -mode biz   -n 200 -c 50 -rounds 20 -kind bag -json /tmp/biz.json
//	go run ./tools/loadtest -mode smoke -login http://127.0.0.1:8080 -gateway 127.0.0.1:9001
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

// main 入口：按 -mode 分发到 conn / login / biz / smoke 四种压测。
//
// 退出码约定：login/biz 模式只要有失败计数（或写 JSON 失败）就以非零码退出，
// 便于 CI 与 scripts/bench.sh 判定；统计结果同时打印到控制台并落 -json。
//
// Go 知识点：flag 参数的默认值即「make loadtest 的常用档位」（-n 1000
// 对应文档常规压测量级）；time.Duration 型参数直接接受 "30s"/"5m" 字面量，
// 无需自己解析。错误统一走 `fmt.Fprintf(os.Stderr, ...) + os.Exit(1)`：
// 诊断信息进 stderr，与 stdout 上的统计报告分流，bench.sh 可分别捕获。
func main() {
	var (
		mode        = flag.String("mode", "smoke", "运行模式：conn | login | biz | smoke")
		conns       = flag.Int("n", 1000, "连接数 / 登录次数 / 玩家数（视模式而定）")
		concurrency = flag.Int("c", 50, "并发数（login/biz 模式）")
		rounds      = flag.Int("rounds", 20, "每个玩家的消息轮数（biz 模式）")
		kind        = flag.String("kind", "all", "消息类型：all | bag | profile | chat（biz 模式）")
		batch       = flag.Int("batch", 500, "每批建连数，批间 sleep 让网关喘口气")
		batchDelay  = flag.Duration("batch-delay", 200*time.Millisecond, "批间间隔")
		hold        = flag.Duration("hold", 30*time.Second, "连接保持时长")
		gateway     = flag.String("gateway", "127.0.0.1:9001", "网关 TCP 地址")
		loginURL    = flag.String("login", "http://127.0.0.1:8080", "登录服 HTTP 地址")
		devices     = flag.Int("devices", 1, "smoke 模式的角色数")
		jsonOut     = flag.String("json", "", "结果写入 JSON（供 bench.sh 汇总）")
	)
	flag.Parse()

	var err error
	switch *mode {
	case "conn":
		err = runConn(*conns, *batch, *batchDelay, *hold, *gateway, *jsonOut)
	case "login":
		s := runLogin(*loginURL, *gateway, *conns, *concurrency)
		s.Print()
		if werr := s.WriteJSON(*jsonOut); werr != nil {
			err = werr
		}
		if s.Errors > 0 {
			err = fmt.Errorf("%d 次登录失败（成功率 %.1f%%）",
				s.Errors, 100*float64(s.Count)/float64(s.Count+int(s.Errors)))
		}
	case "biz":
		s := runBiz(*loginURL, *gateway, *conns, *rounds, *kind)
		s.Print()
		if werr := s.WriteJSON(*jsonOut); werr != nil {
			err = werr
		}
		if s.Errors > 0 {
			err = fmt.Errorf("%d 次业务请求失败", s.Errors)
		}
	case "smoke":
		err = runSmoke(*loginURL, *gateway, *devices)
	default:
		err = fmt.Errorf("unknown mode %q (want conn|login|biz|smoke)", *mode)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "FAIL: %v\n", err)
		os.Exit(1)
	}
}

// runConn 批量建立并保持 TCP 连接（23.4 表第 1 行：单网关 10W 连接保持）。
//
// 契约：
//   - 返回 error 只表示「压测本身失败」（参数非法 / 写 JSON 失败 / 有连接
//     建立失败），由 main 决定退出码；hold 阶段掉线不计入失败；
//   - 统计口径：吞吐只算建连阶段（rec.Stop() 之后 hold 不再计入），
//     延迟样本是单次 dial 的耗时；
//   - 连接在返回前全部 Close：让服务端看到正常 FIN 而非进程退出的 RST。
func runConn(total, batchSize int, batchDelay, hold time.Duration, addr string, jsonOut string) error {
	if total <= 0 {
		return fmt.Errorf("-n must be > 0")
	}
	if batchSize <= 0 {
		batchSize = total
	}
	// 软限制提示：10W 连接至少要 2 倍 fd（socket + epoll 等）
	fmt.Printf("目标 %d 连接 → %s（保持 %s，每批 %d）\n", total, addr, hold, batchSize)

	// signal.NotifyContext 把「收到 SIGINT/SIGTERM」变成一个可取消的 ctx：
	// 压测中途 Ctrl-C 时，DialContext 与各 select 都能体面退出，
	// 不会把上万个连接丢在半路；defer cancel 释放 ctx 关联的通知 goroutine
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	var (
		// 计数器用 atomic.Int64（Go 1.19+ 的类型化原子）：dial goroutine 之间
		// 无锁累加；conns 切片的追加仍需要 mu —— append 可能重新分配底层数组，
		// 这与「单个计数器自增」不是一回事，不能只靠原子操作保护
		established atomic.Int64
		failed      atomic.Int64
		mu          sync.Mutex
		conns       []net.Conn
	)
	rec := NewRecorder()

	// 分批建连 + 批间 sleep：一次性全量 dial 会打满网关 accept 队列、
	// 耗尽本机 ephemeral 端口，且失败率虚高，测不出真实承载
	start := time.Now()
	for i := 0; i < total; i += batchSize {
		if ctx.Err() != nil {
			break
		}
		n := batchSize
		if i+n > total {
			n = total - i
		}
		// 每批内并发 dial；chunk 按下标独占写入、goroutine 之间不共享同一元素，
		// 因此汇总时无需对 chunk 加锁（conns 切片本身才需要 mu）
		var wg sync.WaitGroup
		wg.Add(n)
		chunk := make([]net.Conn, n)
		for j := 0; j < n; j++ {
			go func(idx int) {
				defer wg.Done()
				d := net.Dialer{Timeout: 5 * time.Second}
				t0 := time.Now()
				c, err := d.DialContext(ctx, "tcp", addr)
				if err != nil {
					failed.Add(1)
					rec.AddError()
					return
				}
				rec.Add(time.Since(t0))
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

	rec.Stop() // 建连阶段结束：后面的 hold 不算吞吐
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
			// 用一次非阻塞读探测对端是否已关闭：SetReadDeadline(now) + Read。
			// 技巧：deadline 设为「now」时，若连接存活且无数据，Read 立刻返回
			// timeout（net.Error.Timeout() == true，当作存活信号）；若对端已发
			// FIN，Read 返回 EOF —— 不需要额外协议报文就能判断生死
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

	// 收尾：此刻建连 goroutine 已全部 wg.Wait 过，本可无锁访问 conns；
	// 保持 Lock 是为了与建连阶段同一套访问约定（将来若收尾前启动探测
	// goroutine，不会因为「这里曾经不用锁」而踩坑）
	mu.Lock()
	for _, c := range conns {
		_ = c.Close()
	}
	mu.Unlock()
	fmt.Printf("压测结束：峰值连接 %d，最终存活 %d\n", established.Load(), alive)

	s := rec.Summarize(fmt.Sprintf("建连（目标 %d，批次 %d）", total, batchSize))
	s.Print()
	if err := s.WriteJSON(jsonOut); err != nil {
		return err
	}
	if failed.Load() > 0 {
		return fmt.Errorf("%d 个连接建立失败", failed.Load())
	}
	return nil
}

// runSmoke E2E 冒烟（19.5 第 6 条五步）：登录 → 握手 → 创角 → 进游戏 → 角色信息。
//
// 串行执行 devices 个角色：冒烟验证的是「正确性」而不是吞吐，串行失败时
// 错误信息直指具体步骤，不会与并发失败纠缠。2003 = 无角色（首次登录的
// 正常路径）走创角分支；其余非零码即失败。
func runSmoke(loginURL, gatewayAddr string, devices int) error {
	if devices <= 0 {
		devices = 1
	}
	// device_id 带时间戳：每次冒烟都是新账号，避免复用历史角色把「创角」步骤绕过去
	for i := 0; i < devices; i++ {
		deviceID := fmt.Sprintf("loadtest-%d-%d", time.Now().UnixNano(), i)
		roleName := fmt.Sprintf("压测%d", time.Now().UnixNano()%1000000)

		c := robot.New(loginURL, gatewayAddr)
		// 错误分支逐个显式 Close，而不是在循环里 defer c.Close()：
		// 循环内的 defer 会全部堆积到函数返回才执行，期间连接一直挂着；
		// 显式关闭让失败路径立即释放，意图与时机都可预期
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
		// 无论 PlayerInfo 成败都先 Close 再判错：连接的使命已经完成，
		// 错误处理路径不该继续持有它；Close 只断连接，不影响已返回的 info
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
