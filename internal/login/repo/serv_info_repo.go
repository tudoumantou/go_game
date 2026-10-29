// internal/login/repo/serv_info_repo.go —— 服务器列表仓储（全量预热，R 账号中心）
//
// 架构职责：serv_info 表的读写与 Redis 全量缓存。区服列表全局少量、低频读，
// 真源 MySQL，缓存 cache-aside 整表预热——启动时一次拉全量进 Redis，
// 之后读路径基本不命中 MySQL（设计文档：serv_info 全量预热）。
// 协作关系：登录服选服/区服列表接口经本 repo 取数；运营改配置后调
// Invalidate 失效缓存。
// 设计取舍：① 全量而非单键缓存——区服数量级小（个位数到几百），整表
// 预热成本极低、收益高，避免逐键回源；② Redis 为 nil 时自动退化为直读
// MySQL，便于无 Redis 环境（单测/本地）直接跑；③ 反序列化失败不当错误，
// 当 miss 回源重建——缓存是加速层，坏缓存不该阻断读。
package repo

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"game_study/pkg/cache"
	"game_study/pkg/db"
	"game_study/pkg/model"
	"game_study/pkg/sqlstore"
)

// servInfoCacheTTL 全量缓存有效期；区服配置变更低频，5 分钟足够，
// 运维改完也可主动 Invalidate 立即生效。
const servInfoCacheTTL = 5 * time.Minute

// ServInfoRepo 服务器列表仓储。
//
// 基座是 pkg/sqlstore.Base（与其余仓储同源）；rdb 可空，无 Redis 时退化为
// 直读 MySQL——缓存是加速层，不是正确性前提。
type ServInfoRepo struct {
	*sqlstore.Base
	rdb redis.UniversalClient
}

// NewServInfoRepo 创建仓储。
func NewServInfoRepo(database *db.DB, rdb redis.UniversalClient) *ServInfoRepo {
	return &ServInfoRepo{Base: sqlstore.New(database, "serv_info"), rdb: rdb}
}

// List 列出全部区服（cache-aside）。
// Redis 命中直接返回；miss 回源 MySQL 并回填整表快照。
func (r *ServInfoRepo) List(ctx context.Context) ([]*model.ServInfo, error) {
	// 缓存三步走 cache.Aside（与 acc_summary/user_role 同一套）。
	// 空区服列表也要回填：它是正确答案，缓存它省掉每次选服的回源。
	return cache.Aside(ctx, r.rdb, cache.ServInfoKey(), servInfoCacheTTL,
		func() ([]*model.ServInfo, bool, error) {
			out, err := r.loadAll(ctx)
			return out, err == nil, err
		})
}

// loadAll 从 MySQL 拉全量（ORDER BY id 稳定顺序，便于客户端展示）。
func (r *ServInfoRepo) loadAll(ctx context.Context) ([]*model.ServInfo, error) {
	const q = `SELECT id, name, ip, port, open_time, status, recommend, created_at, updated_at
	           FROM serv_info ORDER BY id`
	rows, err := r.DB().QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("list serv_info: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []*model.ServInfo
	for rows.Next() {
		var m model.ServInfo
		if err := rows.Scan(&m.Id, &m.Name, &m.Ip, &m.Port, &m.OpenTime,
			&m.Status, &m.Recommend, &m.CreatedAt, &m.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, &m)
	}
	return out, rows.Err()
}

// Invalidate 使全量缓存失效。运营改区服配置后调用，下次读回源重建。
func (r *ServInfoRepo) Invalidate(ctx context.Context) error {
	if r.rdb == nil {
		return nil
	}
	return r.rdb.Del(ctx, cache.ServInfoKey()).Err()
}
