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

	"game_study/pkg/model"
	"game_study/pkg/writebehind"
)

// LoadCached 读取玩家状态（内存权威模式下的读入口）。
//
// 判定顺序：缓存命中且不落后于库 → 直接用；否则一律回源库重建。
//
// 与「缓存权威」的两处关键差异：
//   - 命中后要比较库里的 player_writebehind.version。同步落库（A 类）会推进
//     库版本而缓存副本可能还没跟上，落后时必须用库重建——否则玩家重登会
//     看到充值前的余额；
//   - Redis 故障不再等于登录失败。副本只是加速层，回源即可；回填失败也不
//     影响本次返回（下次加载再填）。
//
// 返回值里的 version 是调用方后续 Commit 的 CAS 期望值起点，必须与读到的
// 快照同源，不能自行 +1。
func (r *Repository) LoadCached(ctx context.Context, id int64) (*State, int64, error) {
	record, loadErr := r.cache.Load(ctx, id)
	switch {
	case loadErr == nil:
		// 副本可能落后于库：同步落库（A 类）会推进库版本而缓存没跟上。
		// 校验是「尽力而为」——查不到库版本（表缺行、查询失败）就信任副本，
		// 它是加分项而不是正确性前提，不能因为一次辅助查询拒绝登录。
		if dbVersion, verr := r.writebehindVersion(ctx, id); verr == nil && record.Version < dbVersion {
			// 落后：先清掉（否则 Seed 的「已存在即返回」会让重建写不进去），
			// 清不掉也不要紧——下面直接用冷快照返回，绝不返回旧副本。
			_ = r.cache.Drop(ctx, id)
			break
		}
		return stateFromSnapshot(record.State), record.Version, nil
	case errors.Is(loadErr, writebehind.ErrMiss):
		// 正常未命中，回源。
	default:
		// Redis 故障：副本不是必需品，回源库。这里不阻断——一次缓存抖动
		// 不该变成全服登录失败。观测靠连接/消息指标与登录耗时。
	}

	cold, err := r.loadColdSnapshot(ctx, id)
	if err != nil {
		return nil, 0, err
	}
	seeded, err := r.cache.Seed(ctx, cold.State, cold.Version)
	if err != nil {
		// 回填失败不影响登录：本次用冷快照，下次加载再填。
		return stateFromSnapshot(cold.State), cold.Version, nil
	}
	return stateFromSnapshot(seeded.State), seeded.Version, nil
}

// LoadCachedForce 强制从库重建，跳过缓存。
//
// 用于「内存可能处于半改状态」的恢复（panic）：此时不能拿可能落后的副本
// 覆盖内存。回填缓存失败不影响返回值。
func (r *Repository) LoadCachedForce(ctx context.Context, id int64) (*State, int64, error) {
	cold, err := r.loadColdSnapshot(ctx, id)
	if err != nil {
		return nil, 0, err
	}
	if seeded, seedErr := r.cache.Seed(ctx, cold.State, cold.Version); seedErr == nil {
		return stateFromSnapshot(seeded.State), seeded.Version, nil
	}
	return stateFromSnapshot(cold.State), cold.Version, nil
}

// writebehindVersion 读库里的已落库版本（判定副本是否落后的基准）。
//
// 缺行表示这个玩家还没有异步落库记录，视为 0——此时任何副本都不落后。
func (r *Repository) writebehindVersion(ctx context.Context, id int64) (int64, error) {
	w, err := model.FindPlayerWritebehindByPK(ctx, r.SQL(), id)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("read writebehind version: %w", err)
	}
	return w.Version, nil
}

// SQL 只承担一致性冷启动读取，不参与在线签到查询、领取。
//
// 读到的记录带 version：首次 Seed 时把它作为版本 0 之后的基线，
// 保证「第一个 Commit 的 CAS 期望值」与 Seed 写入的版本一致。
func (r *Repository) loadColdSnapshot(ctx context.Context, id int64) (writebehind.Record, error) {
	tx, err := r.DB().BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return writebehind.Record{}, err
	}
	defer func() { _ = tx.Rollback() }()
	out := writebehind.Record{State: writebehind.Snapshot{PlayerID: id, Items: make(map[int32]int64)}}
	// 幂等键与版本：缺行表示首次登录（版本 0、还没有任何已应用发放）
	w, err := model.FindPlayerWritebehindByPK(ctx, tx, id)
	switch {
	case err == nil:
		out.Version = w.Version
		if w.AppliedGrants.Valid {
			if err := json.Unmarshal([]byte(w.AppliedGrants.String), &out.State.AppliedGrants); err != nil {
				return out, err
			}
		}
	case errors.Is(err, sql.ErrNoRows):
		// 首次登录，正常路径
	default:
		return out, err
	}
	p := &out.State.Profile
	row, err := model.FindPlayerByPK(ctx, tx, id)
	if err != nil {
		return out, err
	}
	p.Name, p.Level, p.Exp, p.Coin, p.SceneID = row.Name, row.Level, row.Exp, row.Coin, row.SceneId

	attr, err := model.FindPlayerAttrByPK(ctx, tx, id)
	switch {
	case err == nil:
		out.State.Attr = writebehind.Attr{
			Career: attr.Career,
			Hp:     attr.Hp,
			Mp:     attr.Mp,
			Atk:    attr.Atk,
			Def:    attr.Def,
			Spd:    attr.Spd,
		}
	case errors.Is(err, sql.ErrNoRows):
		// 无属性行按空属性继续（创角事务异常时也能登录自愈）
	default:
		return out, err
	}

	items, err := model.ListPlayerItemByPlayer(ctx, tx, id)
	if err != nil {
		return out, err
	}
	for _, it := range items {
		out.State.Items[it.ItemId] = int64(it.Count)
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
