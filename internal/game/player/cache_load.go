// internal/game/player/cache_load.go —— 缓存权威模式的读取路径（R2.1 §3）
//
// 架构职责：LoadCached 是玩家状态在缓存模式下的唯一读入口，逻辑只有一句：
// **Redis 有就用 Redis，没有才回源 MySQL**。
//
// 协作关系：被 actor.go 的 preload 与 cache_actor.go 的 cacheReload 重载调用；
// 冷加载内部再委托各模块注册的 writebehind.Persister 读私有表。
//
// 设计取舍：
//   - 为什么 Redis 故障/损坏时不回退旧 SQL 数据：SQL 里可能是在线期间被
//     Redis 甩在后面的旧版本，用它冒充最新状态等于静默降级，后续 CAS 全部
//     基于错误版本——宁可登录失败，不喂脏数据；
//   - 冷加载用 RepeatableRead + ReadOnly 事务：四张表（receipts/player/
//     attr/item）必须读到同一时间点，否则可能拿到「新背包 + 旧等级」的缝合
//     状态；只读事务不拿写锁，不会阻塞在线写入。
package player

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"game_study/pkg/writebehind"
)

// LoadCached 只在热数据不存在时回源；Redis 故障或损坏时不使用旧 SQL 冒充最新状态。
//
// 返回值里的 version 是调用方后续 Commit 的 CAS 期望值起点，必须与读到的
// 快照同源，不能自行 +1。
func (r *Repository) LoadCached(ctx context.Context, id int64) (*State, int64, error) {
	record, err := r.cache.Load(ctx, id)
	if errors.Is(err, writebehind.ErrMiss) {
		cold, err := r.loadColdSnapshot(ctx, id)
		if err != nil {
			return nil, 0, err
		}
		record, err = r.cache.Seed(ctx, cold.State, cold.Version)
		if err != nil {
			return nil, 0, err
		}
	} else if err != nil {
		return nil, 0, err
	}
	return stateFromSnapshot(record.State), record.Version, nil
}

// SQL 只承担一致性冷启动读取，不参与在线签到查询、领取。
//
// 读到的记录带 version：首次 Seed 时把它作为版本 0 之后的基线，
// 保证「第一个 Commit 的 CAS 期望值」与 Seed 写入的版本一致。
func (r *Repository) loadColdSnapshot(ctx context.Context, id int64) (writebehind.Record, error) {
	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return writebehind.Record{}, err
	}
	defer func() { _ = tx.Rollback() }()
	out := writebehind.Record{State: writebehind.Snapshot{PlayerID: id, Items: make(map[int32]int64)}}
	var receipts sql.NullString
	err = tx.QueryRowContext(ctx, `SELECT version, applied_grants FROM player_writebehind WHERE player_id = ?`, id).Scan(&out.Version, &receipts)
	if err == nil && receipts.Valid {
		if err := json.Unmarshal([]byte(receipts.String), &out.State.AppliedGrants); err != nil {
			return out, err
		}
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return out, err
	}
	p := &out.State.Profile
	if err := tx.QueryRowContext(ctx, `SELECT name, level, exp, coin, scene_id FROM player WHERE id = ?`, id).Scan(&p.Name, &p.Level, &p.Exp, &p.Coin, &p.SceneID); err != nil {
		return out, err
	}
	a := &out.State.Attr
	err = tx.QueryRowContext(ctx, `SELECT career, hp, mp, atk, def, spd FROM player_attr WHERE player_id = ?`, id).Scan(&a.Career, &a.Hp, &a.Mp, &a.Atk, &a.Def, &a.Spd)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return out, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT item_id, count FROM player_item WHERE player_id = ?`, id)
	if err != nil {
		return out, err
	}
	for rows.Next() {
		var item int32
		var count int64
		if err := rows.Scan(&item, &count); err != nil {
			rows.Close()
			return out, err
		}
		out.State.Items[item] = count
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return out, err
	}
	// 模块私有表由各自的 Persister 读取，框架不再内建任何玩法的 SQL。
	if err := loadModuleStates(ctx, tx, &out.State); err != nil {
		return out, err
	}
	if err := out.State.Validate(); err != nil {
		return out, err
	}
	return out, tx.Commit()
}

// loadModuleStates 按注册表遍历各模块的冷启动读取。
//
// 顺序由 writebehind.Persisters() 保证稳定（按名字排序）；新增玩法只要注册
// Persister，就会自动被这里读到，不需要修改本文件。
func loadModuleStates(ctx context.Context, tx *sql.Tx, state *writebehind.Snapshot) error {
	for _, p := range writebehind.Persisters() {
		raw, err := p.Load(ctx, tx, state.PlayerID)
		if err != nil {
			// %w 保留原始错误链：调用方可用 errors.Is 定位具体模块失败，
			// 而 %q 保证模块名可读
			return fmt.Errorf("load module %q: %w", p.Name(), err)
		}
		if len(raw) == 0 {
			continue
		}
		if state.Ext == nil {
			state.Ext = make(map[string]json.RawMessage, 1)
		}
		state.Ext[p.Name()] = raw
	}
	return nil
}
