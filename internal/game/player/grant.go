// internal/game/player/grant.go —— 非缓存路径的背包重载
//
// 架构职责：ReloadItems 在「提交结果未知」后恢复背包——先拿同一行锁等待
// 在途事务结束，再重读道具表。只服务非缓存权威模式；缓存模式的等价物是
// 快照 CAS（cache_actor.go）。
//
// 历史注记（v3.13.2）：这里原本还有 CommitItems——把「业务事实」与「背包
// 变更」钉在同一个 SQL 事务里，并通过 module.Context.CommitItems 暴露给业务。
// 已删除，两个原因：
//  1. 它把 *sql.Tx 塞进业务上下文，等于给模块开了「直接写库」的后门；
//  2. 全仓零调用方——缓存模式下玩家的唯一写入口是快照 CAS，直连 SQL 只会
//     造成「库已扣、快照未扣」的双写分歧。
//
// 需要「业务事实与资产原子生效」的场景走 A 类同步发奖：GrantItemsMsg +
// SaveForce（幂等键与资产同一事务）。
package player

import (
	"context"
	"fmt"

	"game_study/pkg/model"
)

// ReloadItems 在未知提交后获取相同角色行锁，等待旧事务结束再读取背包。
// 只替换背包，保留装备派生属性和其他模块的在线内存状态。
//
// 行锁在这里的用途不是互斥，而是**等待**——上一个结果未知的事务可能还在
// 提交中，FOR UPDATE 会让本事务排到它后面，读到的背包必然包含那次写入。
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
