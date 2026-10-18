// tools/loadtest/login.go —— 登录吞吐压测（设计文档 23.4「登录吞吐 ≥2000 TPS」）
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
func runLogin(loginURL, gateway string, total, concurrency int) Summary {
	if concurrency <= 0 {
		concurrency = 50
	}
	rec := NewRecorder()
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
