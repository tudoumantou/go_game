// tools/loadtest/login.go —— 登录吞吐压测（设计文档 23.4「登录吞吐 ≥2000 TPS」）
//
// 工程链位置：`make bench`（scripts/bench.sh）的 login 子模式，产物
// reports/*.json 由 stats.go 的 WriteJSON 写出。
//
// 压的是完整登录链路：HTTP 游客登录 → TCP 建连 → RSA 握手 → 加密登录请求。
// 这段是 CPU 最密集的路径（RSA 解密 + HKDF + AES 初始化），也是扩容时最先到瓶颈的地方。
package main

import (
	"fmt"
	"sync"
	"time"

	"game_study/test/robot"
)

// runLogin 并发执行 total 次完整登录。
//
// 每个登录会创建一个 guest 账号（device_id 唯一），这是登录链路的真实开销 ——
// 只压握手不压账号创建会显著低估实际成本。
//
// 并发模型：buffered channel 当信号量（sem := make(chan struct{}, concurrency)）。
// Go 知识点：带缓冲 channel 是最轻量的并发度控制 —— 发送阻塞即「名额用完」，
// 比手写 worker 池简单；配合 sync.WaitGroup 等全部结束。注意「发送在主循环、
// 接收在 goroutine 的 defer 里」的顺序：即使 doLogin panic，名额也一定释放。
func runLogin(loginURL, gateway string, total, concurrency int) Summary {
	if concurrency <= 0 {
		concurrency = 50
	}
	rec := NewRecorder()
	// 信号量容量 = 并发上限（限流）；wg 计数 = 总任务数（等待完成）——
	// 两者分工不同，不能互相替代
	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup

	for i := 0; i < total; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(idx int) {
			defer wg.Done()
			defer func() { <-sem }()

			start := time.Now()
			if err := doLogin(loginURL, gateway, idx); err != nil {
				rec.AddError(err)
				return
			}
			rec.Add(time.Since(start))
		}(i)
	}
	wg.Wait()
	return rec.Summarize(fmt.Sprintf("登录吞吐（%d 次，并发 %d）", total, concurrency))
}

// doLogin 一次完整登录，成功后立即断开（不进入游戏）。
//
// 失败即返回 error 由 Recorder.AddError 记录，不重试：压测要的是失败率原样，
// 重试会把「服务端过载」洗成「偶发超时」，指标失真。
//
// defer c.Close() 覆盖所有返回路径（对比 smoke 模式的循环显式 Close）——
// 单次调用、非循环场景，defer 是最省心且不会累积的关闭方式。
func doLogin(loginURL, gateway string, idx int) error {
	deviceID := fmt.Sprintf("bench-login-%d-%d", time.Now().UnixNano(), idx)

	c := robot.New(loginURL, gateway)
	defer c.Close()

	if err := c.GuestLogin(deviceID); err != nil {
		return fmt.Errorf("guest login: %w", err)
	}
	if err := c.Connect(); err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	if err := c.Handshake(); err != nil {
		return fmt.Errorf("handshake: %w", err)
	}
	hs, err := c.SendHandshakeReq("1.0.0", "loadtest")
	if err != nil {
		return fmt.Errorf("handshake req: %w", err)
	}
	if hs.Code != 0 || hs.AccountId == 0 {
		return fmt.Errorf("handshake code=%d account=%d", hs.Code, hs.AccountId)
	}
	return nil
}
