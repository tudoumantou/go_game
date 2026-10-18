// tools/loadtest/biz.go —— 混合业务消息压测（设计文档 23.4「单游戏服在线 5K，P99 < 50ms」）
//
// 与 login 模式的区别：login 压的是接入层（RSA/握手），biz 压的是
// 「网关 → gRPC 流 → 游戏服 Actor → 模块 → 回包」这条完整数据路径。
//
// 每轮随机挑一种消息（角色信息 / 背包 / 聊天），避免单一 handler 被 CPU 缓存过度优化，
// 也接近真实玩家的混合操作分布。
package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"game_study/test/robot"
)

// bizWeights 消息类型权重（近似真实玩家的操作分布：看背包 > 看角色 > 聊天）。
var bizWeights = []struct {
	name   string
	weight int
}{
	{"bag", 50},
	{"profile", 40},
	{"chat", 10},
}

// runBiz 并发压业务消息。
//
// 每个虚拟玩家：登录 → 建连 → 握手 → 进游戏（无角色则创建）→ 循环发 rounds 条消息。
func runBiz(loginURL, gateway string, clients, rounds int, kind string) Summary {
	if clients <= 0 {
		clients = 50
	}
	if rounds <= 0 {
		rounds = 20
	}
	weights := filterWeights(kind)

	rec := NewRecorder()
	var wg sync.WaitGroup

	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			if err := bizWorker(loginURL, gateway, idx, rounds, weights, rec); err != nil {
				// 一个虚拟玩家失败只记一次，不中断其他玩家
				rec.AddError(err)
			}
		}(i)
	}
	wg.Wait()
	label := "混合"
	if kind != "" && kind != "all" {
		label = kind
	}
	return rec.Summarize(fmt.Sprintf("业务消息（%d 玩家 × %d 轮，%s）", clients, rounds, label))
}

// filterWeights 按 kind 过滤消息类型（空或 all = 全部）。
//
// 分开跑的意义：混合负载里聊天要写库，会把 P99 拉到 100ms+；
// 只跑读接口能证明「Actor 主链路」本身是亚毫秒级，两者不该混在一起看。
func filterWeights(kind string) []struct {
	name   string
	weight int
} {
	if kind == "" || kind == "all" {
		return bizWeights
	}
	out := make([]struct {
		name   string
		weight int
	}, 0, 1)
	for _, w := range bizWeights {
		if w.name == kind {
			out = append(out, w)
		}
	}
	if len(out) == 0 {
		return bizWeights
	}
	return out
}

// bizWorker 单个虚拟玩家的完整生命周期。
func bizWorker(loginURL, gateway string, idx, rounds int, weights []struct {
	name   string
	weight int
}, rec *Recorder) error {
	// 固定 device_id：重复压测时复用同一角色，避免每次造号污染数据
	deviceID := fmt.Sprintf("bench-biz-%d", idx)
	roleName := fmt.Sprintf("压测%d", idx)

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
	if hs.Code != 0 {
		return fmt.Errorf("handshake code=%d", hs.Code)
	}

	// 进游戏：首次登录返回 2003（无角色）→ 创角后重进
	eg, err := c.EnterGame()
	if err != nil {
		return fmt.Errorf("enter game: %w", err)
	}
	if eg.Code == 2003 {
		cr, err := c.CreateRole(roleName, 1)
		if err != nil {
			return fmt.Errorf("create role: %w", err)
		}
		if cr.Code != 0 && cr.Code != 2004 { // 2004 = 重名（并发压测时可能撞名）
			return fmt.Errorf("create role code=%d", cr.Code)
		}
		if _, err := c.EnterGame(); err != nil {
			return fmt.Errorf("re-enter game: %w", err)
		}
	} else if eg.Code != 0 {
		return fmt.Errorf("enter game code=%d", eg.Code)
	}

	rnd := rand.New(rand.NewSource(time.Now().UnixNano() + int64(idx)))
	total := 0
	for _, w := range weights {
		total += w.weight
	}
	if total == 0 {
		total = 1
	}

	for i := 0; i < rounds; i++ {
		pick := rnd.Intn(total)
		start := time.Now()
		var err error
		acc := 0
		for _, w := range weights {
			acc += w.weight
			if pick < acc {
				err = sendBiz(c, w.name, rnd)
				break
			}
		}
		if err != nil {
			rec.AddError(err)
			continue
		}
		rec.Add(time.Since(start))
	}
	return nil
}

// sendBiz 按类型发一条业务消息并等回包。
func sendBiz(c *robot.Client, kind string, rnd *rand.Rand) error {
	switch kind {
	case "bag":
		_, err := c.BagList()
		return err
	case "profile":
		_, err := c.PlayerInfo()
		return err
	case "chat":
		// 发给自己（channel=1 世界/系统频道由服务端决定，这里只验证往返）
		_, err := c.ChatSend(1, 0, fmt.Sprintf("bench-%d", rnd.Int()));
		return err
	default:
		return fmt.Errorf("unknown biz kind %q", kind)
	}
}
