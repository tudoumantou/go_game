// internal/login/repo/token_repo.go —— Token Redis 仓储
package repo

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// TokenRepo Token 存储。
type TokenRepo struct {
	client redis.UniversalClient
	prefix string
}

// NewTokenRepo 创建 TokenRepo。
func NewTokenRepo(client redis.UniversalClient) *TokenRepo {
	return &TokenRepo{client: client, prefix: "token:"}
}

// key 生成 redis key。
func (r *TokenRepo) key(token string) string {
	return r.prefix + token
}

// Set 存储 token 与 player_id 映射，TTL 秒。
func (r *TokenRepo) Set(ctx context.Context, token string, playerID int64, ttl time.Duration) error {
	return r.client.Set(ctx, r.key(token), playerID, ttl).Err()
}

// Get 查询 token。
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
func (r *TokenRepo) Del(ctx context.Context, token string) error {
	return r.client.Del(ctx, r.key(token)).Err()
}
