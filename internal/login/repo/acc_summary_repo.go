// internal/login/repo/acc_summary_repo.go —— 账号摘要仓储（accuid%32 分片，R 账号中心）
//
// 架构职责：acc_summary 的读写与 Redis 缓存。控制单表 B+ 树规模，
// 按 accuid % 32 稳定路由到物理分片 acc_summary_0..31（见数据归属文档 §4）。
// 真源 MySQL，缓存 cache-aside（按 accuid 单键）。
// 协作关系：登录服查账号摘要（昵称/等级/头像）走这里；游戏服创角/升级
// 经 MQ 同步摘要时调 Upsert（设计文档：acc_summary 为展示副本）。
// 设计取舍：① 路由是 accuid 的确定性函数，不依赖节点内存——坐实
// 「登录服无状态、不预捞 id 进内存」；② 物理分片表名运行时拼接
// （fmt.Sprintf），与逻辑模板表 acc_summary 的列定义（gendb 模型）解耦，
// 不进 gendb 解析范围（同 login_log 分表范式）；③ Redis nil 退化为直读；
// ④ 坏缓存当 miss 处理。
package repo

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"game_study/pkg/cache"
	"game_study/pkg/db"
	"game_study/pkg/model"
	"game_study/pkg/sqlstore"
)

const (
	// accSummaryShards 分片数（钉死 32，2 的幂便于未来 reshard，见文档 §3）。
	accSummaryShards = 32
	accSummaryTTL    = 10 * time.Minute
)

// AccSummaryRepo 账号摘要仓储。
type AccSummaryRepo struct {
	*sqlstore.Base
	rdb redis.UniversalClient
}

// NewAccSummaryRepo 创建仓储。
func NewAccSummaryRepo(database *db.DB, rdb redis.UniversalClient) *AccSummaryRepo {
	return &AccSummaryRepo{Base: sqlstore.New(database, "acc_summary"), rdb: rdb}
}

// shardAccSummary 计算分片下标（与 DDL 物理表命名一致，0..31）。
func shardAccSummary(accuid int64) int {
	return int(accuid % accSummaryShards)
}

// accSummaryTable 物理分片表名（运行时拼接，不在 gendb 解析范围）。
func accSummaryTable(accuid int64) string {
	return fmt.Sprintf("acc_summary_%d", shardAccSummary(accuid))
}

// accSummaryKey Redis 单键（格式定义在 pkg/cache）。
func accSummaryKey(accuid int64) string {
	return cache.AccSummaryKey(accuid)
}

// Get 按 accuid 取摘要（cache-aside）。未命中返回 (nil, nil)——摘要可不存在。
func (r *AccSummaryRepo) Get(ctx context.Context, accuid int64) (*model.AccSummary, error) {
	if r.rdb != nil {
		if raw, err := r.rdb.Get(ctx, accSummaryKey(accuid)).Bytes(); err == nil {
			var m model.AccSummary
			if json.Unmarshal(raw, &m) == nil {
				return &m, nil
			}
		}
	}
	m, err := r.load(ctx, accuid)
	if err != nil {
		return nil, err
	}
	if m != nil && r.rdb != nil {
		if raw, mErr := json.Marshal(m); mErr == nil {
			_ = r.rdb.Set(ctx, accSummaryKey(accuid), raw, accSummaryTTL).Err()
		}
	}
	return m, nil
}

// load 回源分片表。ErrNoRows 翻译为「无摘要」（nil, nil）而非错误。
func (r *AccSummaryRepo) load(ctx context.Context, accuid int64) (*model.AccSummary, error) {
	const q = `SELECT accuid, nickname, avatar, level, last_login_at, created_at, updated_at
	           FROM %s WHERE accuid = ? LIMIT 1`
	row := r.DB().QueryRowContext(ctx, fmt.Sprintf(q, accSummaryTable(accuid)), accuid)
	var m model.AccSummary
	err := row.Scan(&m.Accuid, &m.Nickname, &m.Avatar, &m.Level,
		&m.LastLoginAt, &m.CreatedAt, &m.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("load acc_summary: %w", err)
	}
	return &m, nil
}

// Upsert 写入/更新某账号摘要到分片表，并失效其缓存。
// 幂等（ON DUPLICATE KEY UPDATE）；调用方（MQ 消费）重复投递安全。
func (r *AccSummaryRepo) Upsert(ctx context.Context, m *model.AccSummary) error {
	if m == nil || m.Accuid <= 0 {
		return fmt.Errorf("acc_summary upsert: invalid accuid")
	}
	const q = `INSERT INTO %s (accuid, nickname, avatar, level, last_login_at)
	           VALUES (?,?,?,?,?)
	           ON DUPLICATE KEY UPDATE nickname = VALUES(nickname), avatar = VALUES(avatar),
	                                 level = VALUES(level), last_login_at = VALUES(last_login_at)`
	_, err := r.DB().ExecContext(ctx, fmt.Sprintf(q, accSummaryTable(m.Accuid)),
		m.Accuid, m.Nickname, m.Avatar, m.Level, m.LastLoginAt)
	if err != nil {
		return fmt.Errorf("upsert acc_summary: %w", err)
	}
	if r.rdb != nil {
		_ = r.rdb.Del(ctx, accSummaryKey(m.Accuid)).Err()
	}
	return nil
}
