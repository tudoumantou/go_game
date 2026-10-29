// internal/login/repo/token_repo.go —— Token Redis 仓储
//
// 架构职责：login token 在 Redis 的存取删——「token → player_id」
// 映射的唯一存储。网关握手期经 server.go 的 validator 读同一个 key
// 完成校验。key 由 cache.TokenKey 生成：两个进程共用一处定义，
// 「改一处忘另一处导致全体掉线」的隐患从结构上消失。
// 协作关系：service.AuthService.GuestLogin 调 Set，ValidateToken
// 调 Get；登出/顶号场景（未来）调 Del。
// 设计取舍：① 用 Redis 而非 MySQL——token 校验在握手热路径，
// 要求亚毫秒级响应与高并发读，MySQL 撑不住这个量级；② TTL 由
// 上层传入（配置 token.ttl，缺省 24h）而非仓储硬编码——过期策略
// 是业务决策不是存储决策；③ value 直接存 account_id 数字——
// 读一次 Redis 就完成「token 是否有效 + 归属是谁」两个问题，
// 不需要二次查库。
package repo

import (
	"context"
	"fmt"
	"time"

	"game_study/pkg/cache"
	"github.com/redis/go-redis/v9"
)

// TokenRepo Token 存储。
// 依赖 redis.UniversalClient（单机/哨兵/集群通用接口）而非具体
// 客户端类型——部署形态变化时本文件零改动。
type TokenRepo struct {
	client redis.UniversalClient
}

// NewTokenRepo 创建 TokenRepo。
func NewTokenRepo(client redis.UniversalClient) *TokenRepo {
	return &TokenRepo{client: client}
}

// key 生成 redis key（定义在 pkg/cache，与网关校验器共用）。
func (r *TokenRepo) key(token string) string {
	return cache.TokenKey(token)
}

// Set 存储 token 与 player_id 映射，TTL 秒。
// TTL 即会话有效期：过期后 token 自动消失，网关校验自然失败
// （redis.Nil）——无需后台任务清理，Redis 的过期机制就是 GC。
func (r *TokenRepo) Set(ctx context.Context, token string, playerID int64, ttl time.Duration) error {
	return r.client.Set(ctx, r.key(token), playerID, ttl).Err()
}

// Get 查询 token。
// 知识点：redis.Nil 是「key 不存在」的哨兵错误，与连接失败等
// 真实错误区分——token 过期/不存在返回可读错误，驱动故障原样
// 上抛让上层按 5xx 处理；调用方（service.ValidateToken）把错误
// 直接当「token 无效」，过期与故障在这一层都被拒绝登录，语义安全。
func (r *TokenRepo) Get(ctx context.Context, token string) (int64, error) {
	val, err := r.client.Get(ctx, r.key(token)).Int64()
	if err == redis.Nil {
		return 0, fmt.Errorf("token not found")
	}
	if err != nil {
		return 0, err
	}
	return val, nil
}

// Del 删除 token。
// 顶号/登出/封禁时调用：删除后旧 token 立即失效（网关下一次
// 校验即失败），实现「服务端强制下线」而不必等 TTL 自然过期。
func (r *TokenRepo) Del(ctx context.Context, token string) error {
	return r.client.Del(ctx, r.key(token)).Err()
}
