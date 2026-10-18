// pkg/cache/redis.go —— Redis 客户端封装（go-redis/v9）
package cache

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisConfig Redis 连接配置。
type RedisConfig struct {
	Addrs    []string
	Password string
	DB       int
	PoolSize int
	MinIdle  int
}

// Client Redis 客户端封装。
type Client struct {
	client redis.UniversalClient
}

// New 根据配置创建 Redis 客户端（单点/哨兵/集群自动识别）。
func New(cfg RedisConfig) (*Client, error) {
	var client redis.UniversalClient
	opts := &redis.UniversalOptions{
		Addrs:        cfg.Addrs,
		Password:     cfg.Password,
		DB:           cfg.DB,
		PoolSize:     cfg.PoolSize,
		MinIdleConns: cfg.MinIdle,
	}
	switch {
	case len(cfg.Addrs) > 1:
		client = redis.NewClusterClient(opts.Cluster())
	default:
		client = redis.NewClient(opts.Simple())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis ping: %w", err)
	}
	return &Client{client: client}, nil
}

// Close 关闭连接。
func (c *Client) Close() error {
	if c.client == nil {
		return nil
	}
	return c.client.Close()
}

// GetClient 暴露底层客户端（仅内部基础封装使用）。
func (c *Client) GetClient() redis.UniversalClient {
	return c.client
}
