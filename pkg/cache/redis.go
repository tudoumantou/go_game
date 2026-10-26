// pkg/cache/redis.go —— Redis 客户端封装（go-redis/v9）
//
// 架构职责：本包唯一创建/持有 Redis 连接的地方——所有子模块
// （idem/idtab/idtable/lock/presence）共享这里产出的连接池。
// 协作关系：配置由 pkg/config 的 redis 段经 RedisConfig 传入；
// Client.GetClient() 把底层 UniversalClient 交给各子模块使用。
// 关键取舍：用 UniversalClient 统一单点/集群两种形态（按地址数量
// 自动选择），业务代码不感知部署拓扑；创建时 Ping 探活，配置错误
// 在启动期暴露而非第一个业务请求。
package cache

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisConfig Redis 连接配置。
//
// Addrs 一个 = 单点，多个 = 集群（自动识别）；DB 仅在单点模式有意义
// （集群模式不使用逻辑库）。
// PoolSize/MinIdle 是连接池容量：并发峰值决定 PoolSize，MinIdle 保底
// 热连接减少握手开销（必须 ≤ PoolSize，由 config.Validate 强校验）。
type RedisConfig struct {
	Addrs    []string
	Password string
	DB       int
	PoolSize int
	MinIdle  int
	// Breaker 熔断参数（见 breaker.go）；Threshold < 0 时关闭熔断。
	Breaker BreakerOptions
}

// Client Redis 客户端封装。
//
// 包一层是为了收敛依赖：业务只认 cache.Client，不直接 import
// go-redis——未来换客户端/加埋点只改本文件。
// 并发：创建后只读（内部 client 连接池自身并发安全），可作全局单例。
type Client struct {
	client  redis.UniversalClient
	breaker *Breaker // nil 表示熔断关闭
}

// New 根据配置创建 Redis 客户端（单点/哨兵/集群自动识别）。
//
// 启动即 Ping：连接失败直接返回 error，让进程快速失败——带着坏配置
// 继续跑，第一个业务请求才炸，排查成本高一个量级。
// Go 知识点：context.WithTimeout + defer cancel()——cancel 必须被
// 调用（defer 保证任何返回路径都执行），否则 context 泄漏、关联的
// 定时器资源不释放（go vet 的 lostcancel 检查项）。
func New(cfg RedisConfig) (*Client, error) {
	var client redis.UniversalClient
	opts := &redis.UniversalOptions{
		Addrs:        cfg.Addrs,
		Password:     cfg.Password,
		DB:           cfg.DB,
		PoolSize:     cfg.PoolSize,
		MinIdleConns: cfg.MinIdle,
	}
	// 多地址 = 集群部署；单地址走普通客户端（也兼容单点哨兵场景，
	// 由 go-redis 内部按 Addrs 形态二次判定）。
	switch {
	case len(cfg.Addrs) > 1:
		client = redis.NewClusterClient(opts.Cluster())
	default:
		client = redis.NewClient(opts.Simple())
	}

	// 熔断器挂在连接上（Hook）：所有命令都经过它，包括 pipeline 与 Lua。
	// 必须在 Ping 之前装好——Ping 本身就是第一条探测命令。
	var breaker *Breaker
	if !cfg.Breaker.disabled() {
		breaker = NewBreaker(cfg.Breaker)
		client.AddHook(breaker)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis ping: %w", err)
	}
	return &Client{client: client, breaker: breaker}, nil
}

// Close 关闭连接。
//
// nil 防御：Client 零值（未成功创建）也可安全 Close，方便 defer 模式
// `c, err := New(...); defer c.Close()`——New 失败时 c 为 nil 也不炸。
func (c *Client) Close() error {
	if c.client == nil {
		return nil
	}
	return c.client.Close()
}

// Breaker 熔断器（未启用时为 nil，供监控/测试读取状态）。
func (c *Client) Breaker() *Breaker {
	if c == nil {
		return nil
	}
	return c.breaker
}

// GetClient 暴露底层客户端（仅内部基础封装使用）。
//
// 逃生舱：通用能力走本包封装，特殊命令（Lua/Scan/位图等）经此直接
// 触达，避免封装无限膨胀。
func (c *Client) GetClient() redis.UniversalClient {
	return c.client
}
