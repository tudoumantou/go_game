// internal/game/player/persist.go —— 直连 MySQL 的加载与落库（非缓存权威模式）
//
// 架构职责：Repository 是玩家持久化在 SQL 形态下的唯一出入口：
// Load 全量读、Save 增量写、CacheEnabled 决定走哪条权威链路。
//
// 协作关系：actor.go 的 preload/savePlayer 调用 Load/Save；
// cache_load.go 在缓存模式下只借它的 db 句柄做冷启动。
//
// 设计取舍：
//   - 为什么道具从「先删后插」改成「只写脏项 + 合批 UPSERT」：50 格背包
//     每次保存都全表重写，落库量随背包规模线性膨胀，而实际每轮通常只变
//     1-2 格；脏标记 diff 后，语句数从 1+N 降到 1+批次数；
//   - 为什么 SeedItem 不标脏：预加载灌数据是「把库搬进内存」，不是变更，
//     标脏会让每次登录都把全背包重写一遍；
//   - 失败保留脏标记：Save 失败时 dirtyItems 不清，下轮 flusher 重试同一
//     批数据，不会出现「保存失败但标记已清」的静默丢失。
package player

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"game_study/internal/game/module"
	"game_study/pkg/db"
	"game_study/pkg/flush"
	"game_study/pkg/model"
	"game_study/pkg/writebehind"
)

// Repository 玩家数据仓储。
type Repository struct {
	db    *db.DB
	cache *writebehind.Store
}

func (r *Repository) SetCache(store *writebehind.Store) { r.cache = store }

// CacheEnabled 是否启用缓存权威模式。
//
// 接收者为 nil 时返回 false：actor.go 在无 DB 的单测环境里会拿到 nil
// Repository，方法内先判 r != nil 是防御性写法（nil 接收者上调用方法是
// 合法的，只要不解引用字段）。
func (r *Repository) CacheEnabled() bool { return r != nil && r.cache != nil }

// NewRepository 创建仓储。
func NewRepository(database *db.DB) *Repository {
	return &Repository{db: database}
}

// Load 从 MySQL 加载玩家状态（角色信息 + 属性 + 道具）。
//
// ErrNoRows 不当错误：player/attr 缺行按空状态继续（创角事务异常时也能
// 登录自愈），只有其它 SQL 错误才中断加载。
func (r *Repository) Load(ctx context.Context, playerID int64) (*State, error) {
	state := NewState(playerID)

	// 角色基础信息
	row, err := model.FindPlayerByPK(ctx, r.db.DB, playerID)
	if err == nil {
		state.SetProfile(module.Profile{
			Name:    row.Name,
			Level:   row.Level,
			Exp:     row.Exp,
			Coin:    row.Coin,
			SceneID: row.SceneId,
		})
	} else if err != sql.ErrNoRows {
		return nil, fmt.Errorf("load player: %w", err)
	}

	// 属性
	attrRow, err := model.FindPlayerAttrByPK(ctx, r.db.DB, playerID)
	if err == nil {
		state.SetAttrStruct(Attr{
			Career: attrRow.Career,
			Hp:     attrRow.Hp,
			Mp:     attrRow.Mp,
			Atk:    attrRow.Atk,
			Def:    attrRow.Def,
			Spd:    attrRow.Spd,
		})
	} else if err != sql.ErrNoRows {
		return nil, fmt.Errorf("load attr: %w", err)
	}

	// 道具
	items, err := r.loadItems(ctx, playerID)
	if err != nil {
		return nil, err
	}
	for itemID, count := range items {
		// SeedItem 不标脏：预加载灌数据不应触发写回
		state.SeedItem(itemID, count)
	}
	return state, nil
}

func (r *Repository) loadItems(ctx context.Context, playerID int64) (map[int32]int64, error) {
	rows, err := model.ListPlayerItemByPlayer(ctx, r.db.DB, playerID)
	if err != nil {
		return nil, fmt.Errorf("query items: %w", err)
	}
	out := make(map[int32]int64, len(rows))
	for _, it := range rows {
		out[it.ItemId] = int64(it.Count)
	}
	return out, nil
}

// Save 落库玩家状态（事务：角色 + 属性 + 道具）。
//
// 道具从「先删后插全量重写」改为「只写变更项 + 合批 UPSERT」（12.4.4）：
//   - 只有 AddItem/RemoveItem 碰过的道具才写，50 格背包里通常只变 1-2 格；
//   - 多行合成一条 INSERT ... ON DUPLICATE KEY UPDATE（500 行/条）；
//   - 数量归零的道具合成一条 DELETE ... WHERE player_id=? AND item_id IN (...)。
//
// SQL 语句数从「1 + N（道具数）」降到「1 + 批次 + 批次」。
func (r *Repository) Save(ctx context.Context, state *State) error {
	_, err := r.save(ctx, state, false)
	return err
}

// SaveForce 同步全量落库，并把业务幂等键与资产写进同一事务（A 类数据）。
//
// 服务对象：充值、交易这类「回执必须在 MySQL 提交之后发出」的操作。
// 异步通道（writebehind）等的是一个落库窗口，Redis 掉电会连窗口一起丢，
// 对钱不可接受。
//
// 与 Save 的两点差别：
//  1. 同一事务内写 applied_grants：「发货」与「幂等键」原子生效，
//     Kafka 重投时幂等命中，不会二次发放；
//  2. 同一事务内推进 player_writebehind.version：异步落库与加载侧的
//     版本校验据此判定缓存副本过期，旧值不会覆盖新值。
//
// 返回推进后的版本号。调用方必须用它更新本地 cacheVersion，否则下一次
// Commit 的 CAS 期望值会落后于数据库，被判为冲突。
func (r *Repository) SaveForce(ctx context.Context, state *State) (int64, error) {
	return r.save(ctx, state, true)
}

func (r *Repository) save(ctx context.Context, state *State, withGrants bool) (int64, error) {
	dirty := state.DirtyItems()
	var version int64

	err := r.db.Tx(ctx, func(tx *sql.Tx) error {
		playerID := state.PlayerID()
		a := state.Attr()

		// 角色基础信息（name 不在此更新，改名走独立流程）。
		// 用生成的部分列更新而不是全列 UPDATE：account_id/name 由别的流程
		// 维护，落库时带进来会覆盖并发写入的结果。
		p := state.Profile()
		if _, err := model.UpdatePlayerProgressByPK(ctx, tx, &model.Player{
			Id:      playerID,
			Level:   p.Level,
			Exp:     p.Exp,
			Coin:    p.Coin,
			SceneId: p.SceneID,
		}); err != nil {
			return fmt.Errorf("update player: %w", err)
		}

		// 属性 upsert（事务内执行，保证与道具同事务）
		if _, err := model.UpsertPlayerAttrByPK(ctx, tx, &model.PlayerAttr{
			PlayerId: playerID,
			Career:   a.Career,
			Hp:       a.Hp,
			Mp:       a.Mp,
			Atk:      a.Atk,
			Def:      a.Def,
			Spd:      a.Spd,
		}); err != nil {
			return fmt.Errorf("upsert attr: %w", err)
		}

		if err := saveDirtyItems(ctx, tx, playerID, dirty); err != nil {
			return err
		}
		// 模块私有表：本方法只负责 player/attr/items 三张基础表，Ext 表由
		// 这里补上。内存权威的落库路径不经过 writebehind worker，缺这一步
		// 模块状态（签到/任务/装备…）永远不会入库。
		//
		// 只写**脏模块**：多数轮次玩家只动了金币/道具，把 9 张 Ext 表全量
		// UPSERT 一遍纯属浪费（量随在线人数线性增长）。
		if err := writebehind.PersistDirtyModules(ctx, tx, state.snapshot(), state.DirtyModules()); err != nil {
			return err
		}
		if !withGrants {
			return nil
		}
		return saveGrantsAndBumpVersion(ctx, tx, state, &version)
	})
	if err != nil {
		return 0, err // 失败保留脏标记，下轮重试
	}

	// 同一 Actor 线程内执行（onSave 走 Actor 消息），清标记无并发风险。
	// 两个标记都要清：道具增量（ClearItemsDirty）与模块脏（ClearDirtyModules），
	// 否则下一轮会把已经落过的模块再写一遍。
	state.ClearItemsDirty()
	state.ClearDirtyModules()
	return version, nil
}

// saveGrantsAndBumpVersion 在同一事务里写幂等键并推进版本号。
//
// 版本必须推进：缓存里的旧快照带着旧 version，异步落库的栅栏用
// 「快照 version > 数据库 version」判断是否采用。推进之后旧快照永远
// 不满足条件，从而不会被写回库——这是同步落库与异步通道共存的关键。
//
// 先读后写的两步都在同一事务内，且同一玩家只有一个 Actor 在跑
// SaveForce（跨进程由玩家归属保证），因此没有并发写版本的风险；
// 换来的好处是不再依赖 `version = version + 1` 的表达式更新（生成器
// 不支持表达式 SET），也不必再回读一次版本。
func saveGrantsAndBumpVersion(ctx context.Context, tx *sql.Tx, state *State, version *int64) error {
	raw, err := json.Marshal(state.snapshot().AppliedGrants)
	if err != nil {
		return fmt.Errorf("encode applied grants: %w", err)
	}
	playerID := state.PlayerID()
	w, err := model.FindPlayerWritebehindByPK(ctx, tx, playerID)
	switch {
	case err == nil:
		*version = w.Version + 1
	case errors.Is(err, sql.ErrNoRows):
		// 首次同步落库：与历史 INSERT 语义一致（初始版本为 1）
		*version = 1
	default:
		return fmt.Errorf("read writebehind version: %w", err)
	}
	if _, err := model.UpsertPlayerWritebehindByPK(ctx, tx, &model.PlayerWritebehind{
		PlayerId:      playerID,
		Version:       *version,
		AppliedGrants: sql.NullString{String: string(raw), Valid: true},
	}); err != nil {
		return fmt.Errorf("bump writebehind version: %w", err)
	}
	return nil
}

// saveDirtyItems 写本次变更的道具（合批 UPSERT + 合批 DELETE）。
//
// 拆两类：count>0 走 UPSERT、count<=0 走 DELETE（RemoveItem 归零时
// DirtyItems 返回 0，语义即「删行」）。
func saveDirtyItems(ctx context.Context, tx *sql.Tx, playerID int64, dirty map[int32]int64) error {
	if len(dirty) == 0 {
		return nil
	}

	upserts := make([][]any, 0, len(dirty))
	deletes := make([]any, 0, len(dirty))
	for itemID, count := range dirty {
		if count <= 0 {
			deletes = append(deletes, itemID)
			continue
		}
		upserts = append(upserts, []any{playerID, itemID, count})
	}

	// 列名取生成代码的常量：改列名时生成物变化 → 这里编译失败，
	// 而不是像字面量那样静默拼出一条 Unknown column 的 SQL。
	// 不含 expire_at：本路径只维护数量，过期时间由过期玩法单独管理，
	// 带上它会把已有过期时间覆盖成 NULL。
	itemCols := []string{model.ColPlayerItemPlayerId, model.ColPlayerItemItemId, model.ColPlayerItemCount}
	itemTable := (&model.PlayerItem{}).TableName()

	for _, batch := range flush.Chunk(upserts, flush.DefaultBatchRows, flush.DefaultBatchBytes) {
		q, args, err := flush.BuildBatchUpsert(itemTable, itemCols, batch)
		if err != nil {
			return fmt.Errorf("build item upsert: %w", err)
		}
		if _, err := tx.ExecContext(ctx, q, args...); err != nil {
			return fmt.Errorf("batch upsert items: %w", err)
		}
	}

	// 删除也合批：数量归零的道具一条语句删完
	const deleteChunk = flush.DefaultBatchRows
	for start := 0; start < len(deletes); start += deleteChunk {
		end := start + deleteChunk
		if end > len(deletes) {
			end = len(deletes)
		}
		q, args, err := flush.BuildBatchDeleteWhere(itemTable, "player_id = ?",
			[]any{playerID}, model.ColPlayerItemItemId, deletes[start:end])
		if err != nil {
			return fmt.Errorf("build item delete: %w", err)
		}
		if _, err := tx.ExecContext(ctx, q, args...); err != nil {
			return fmt.Errorf("batch delete items: %w", err)
		}
	}
	return nil
}
