// Package benchmark 压测与验收（设计文档 23.4）
//
// P0 阶段为骨架：定义压测入口与验收口径，具体负载待对应模块落地后填充。
// 运行: go test ./test/benchmark/ -bench=. -benchmem -benchtime=10s
//
// 压测基线入库（19.5 清单第 7 步），作为扩容依据；每个里程碑复跑一次。
package benchmark

import (
	"testing"

	"game_study/pkg/config"
)

// BenchmarkConfigLoad 配置解析路径（登录 TPS 的依赖项之一，23.4 表第 3 行）
func BenchmarkConfigLoad(b *testing.B) {
	path := "../../configs/login/login-dev.yaml"
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cfg, err := config.Load(path)
		if err != nil {
			b.Fatal(err)
		}
		if cfg.Server.Name == "" {
			b.Fatal("empty server name")
		}
	}
}

// TestBaselineTargets 验收口径自检（23.4 目标值常量登记，防漂移）
func TestBaselineTargets(t *testing.T) {
	targets := map[string]string{
		"单网关连接":    ">=10W 长连接, RSS < 2GB",
		"单游戏服在线":   "5K 机器人, P99 消息延迟 < 50ms",
		"登录吞吐":     ">=2000 TPS",
		"落库吞吐":     "10 服 x 30 并发 = 300, 60s 周期",
		"Kafka 扇出": "世界聊天 1000 条/秒, 端到端 < 1s",
		"Actor 背压": "信箱满时拒绝而非 OOM",
	}
	for k, v := range targets {
		t.Logf("基线 %s: %s", k, v)
	}
	// TODO(P7): 接 test/benchmark 全连接压测（网关）、5K 机器人混合消息（游戏服）、
	//   集中登录 TPS、flusher 落库观察、Kafka 生产消费打点、慢消费者构造。
}

// TODO(P7) 压测项登记（23.4）：
//   - BenchmarkGatewayConns    全连接 + 保活 30 分钟（10W 连接, RSS<2GB）
//   - BenchmarkGameMixed       5K 机器人混合业务消息（聊天/道具/移动, P99<50ms）
//   - BenchmarkLoginTPS        集中登录（RSA 握手 + Token 签发, >=2000 TPS）
//   - BenchmarkFlusherThroughput 观察 flusher 指标 + MySQL 负载（300 并发, 60s）
//   - BenchmarkKafkaFanout     世界聊天 1000 条/秒, 端到端 < 1s
//   - BenchmarkActorBackpressure 构造慢消费者, 断言拒绝而非 OOM
