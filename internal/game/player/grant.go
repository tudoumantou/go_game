// internal/game/player/grant.go —— 非缓存路径的事务发奖（直连 MySQL 形态）
//
// 架构职责：CommitItems 把「业务事实」与「背包变更」钉在同一个 SQL 事务里，
// ReloadItems 在 COMMIT 结果未知后恢复背包。二者只服务非缓存权威模式；
// 缓存模式的等价物是快照 CAS（cache_actor.go）。
//
// 协作关系：由 actor.go 的 moduleCtx 闭包调用（模块的 CommitItems 钩子）。
//
// 设计取舍：
//   - 为什么 SELECT ... FOR UPDATE：两个购买请求并发打到同一行背包时，
//     行锁让第二个事务等第一个提交后再读，避免「都读到旧数量、都写回
//     新数量」的丢失更新；锁范围只有 player 一行，不扩大；
//   - 为什么 Commit 失败要包成 ErrCommitUnknown：MySQL 在 COMMIT 报文
//     丢失时可能已提交，调用方无法区分「没成功」与「成功了但没收到确认」，
//     必须按「未知」处理——先重载再继续，宁可慢一步不能重复发奖；
//   - ReloadItems 只换背包：装备派生属性与其它模块的在线内存状态不属于
//     本次未知事务的范围，整体重载会把它们打回旧值。
package player

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"

	"game_study/internal/game/module"
	"game_study/pkg/model"
)

// ErrCommitUnknown 表示 COMMIT 返回错误，数据库可能已提交，必须重载后再处理消息。
//
// 哨兵错误：调用方用 errors.Is 判定（见 actor.go moduleCtx 闭包），不要
// 用字符串比较——包装链会随时间变化。
var ErrCommitUnknown = errors.New("transaction commit outcome unknown")

// CommitItems 由玩家 Actor 同步调用，业务事实与背包同事务持久化。
// 调用期间不会有该 Actor 的其他消息或 Write-Behind 保存交错执行。
//
// 前置校验（item/count/上溢）放在开事务前：非法参数不该浪费一次加锁。
func (r *Repository) CommitItems(ctx context.Context, state *State, grants []module.ItemGrant, business func(*sql.Tx) error) error {
	if r.CacheEnabled() {
		return fmt.Errorf("direct SQL grant disabled for cached player")
	}
	dirty := state.DirtyItems()
	counts := make(map[int32]int64)
	for _, grant := range grants {
		current, ok := counts[grant.ItemID]
		if !ok {
			current = state.GetItemCount(grant.ItemID)
		}
		if grant.ItemID <= 0 || grant.Count <= 0 || current < 0 || grant.Count > math.MaxInt32-current {
			return fmt.Errorf("invalid item grant: item=%d count=%d", grant.ItemID, grant.Count)
		}
		counts[grant.ItemID] = current + grant.Count
		dirty[grant.ItemID] = counts[grant.ItemID]
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	// 锁角色行（生成的 @lockby:id）：锁范围只有 player 一行，不扩大；
	// 行不存在（角色被删）时 ErrNoRows 直接上抛，与手写 SELECT 语义一致。
	if _, err := model.LockPlayerById(ctx, tx, state.PlayerID()); err != nil {
		return err
	}
	if err := business(tx); err != nil {
		return err
	}
	if err := saveDirtyItems(ctx, tx, state.PlayerID(), dirty); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		// %w 包装哨兵错误：errors.Is(err, ErrCommitUnknown) 仍为真
		return fmt.Errorf("%w: %v", ErrCommitUnknown, err)
	}
	for id, count := range counts {
		state.SeedItem(id, count)
	}
	state.ClearItemsDirty()
	return nil
}

// ReloadItems 在未知提交后获取相同角色行锁，等待旧发奖事务结束再读取背包。
// 只替换背包，保留装备派生属性和其他模块的在线内存状态。
//
// 行锁在这里的用途与 CommitItems 不同：不是为了互斥，而是为了**等待**——
// 上一个结果未知的事务可能还在提交中，FOR UPDATE 会让本事务排到它后面，
// 读到的背包必然包含那次发奖的结果。
func (r *Repository) ReloadItems(ctx context.Context, state *State) error {
	if r.CacheEnabled() {
		return fmt.Errorf("use Redis recovery for cached player")
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	// 锁角色行（生成的 @lockby:id）
	if _, err := model.LockPlayerById(ctx, tx, state.PlayerID()); err != nil {
		return err
	}
	rows, err := model.ListPlayerItemByPlayer(ctx, tx, state.PlayerID())
	if err != nil {
		return err
	}
	items := make(map[int32]int64, len(rows))
	for _, it := range rows {
		items[it.ItemId] = int64(it.Count)
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	state.mu.Lock()
	state.items = items
	clear(state.dirtyItems)
	state.mu.Unlock()
	return nil
}
