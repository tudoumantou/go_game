package store

// recover.go 实现一致性恢复读取（文档 16.1 / 14.4 LoadWorld）。
//
// 核心要求：**一次只读事务 = 一个一致性快照**。
//
// 为什么必须一个事务：如果对象、会话、战斗分别用三条独立查询，
// 期间刚好有一笔提交改了资源余量并新建了采集会话，恢复就会读到
// 「余量已扣但会话不存在」或「会话存在但部队没有对应 Phase」。
// 这类半新半旧的镜像会让世界带着不一致的引用进入 Ready，
// 之后每一次 Tick 都会在这个坏状态上继续放大。
//
// InnoDB 的实现细节：REPEATABLE READ 下，事务的读视图在**第一次**
// 一致性读时建立，之后所有普通 SELECT 都看同一个快照。READ COMMITTED
// 则是每条语句一个新快照，所以这里必须显式指定隔离级别。
// 只读事务还有一个额外好处：不会被误写成写事务，也不会持有行锁
// 拖慢正在恢复的世界。

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"game_study/internal/worldmap/domain"
)

// SceneState 是场景的最小恢复状态。
//
// 目前只有 revision。后续场景快照需要更多字段时在这里追加，
// RecoveredWorld 的结构不变——这样上层代码的改动面被限制在一个类型里。
type SceneState struct {
	Revision uint64
}

// RecoveredWorld 是一致性恢复读出的全部世界状态。
//
// 字段语义：
//   - Scenes：场景 revision，按 scene_id 升序读入（map 本身无序，
//     但读取顺序决定了 snapshot 的建立顺序，必须稳定）；
//   - Objects：全部未删除对象，按 object_id 升序；
//   - Sessions/Battles：需要继续推进的活跃会话与待裁决战斗；
//   - Queues：目标 → 排队部队（enqueue_seq 升序）；
//   - Deliveries：未完成交接单。
//
// 注意这里**不包含** occupancy：静态占位在恢复校验阶段按需单独加载
// （LoadOccupancy），不放进一致性快照的理由是它体量大且只用于校验，
// 放进 RecoveredWorld 会让每次恢复都复制一份全量占位到内存。
type RecoveredWorld struct {
	WorldID    domain.WorldID
	Epoch      uint64
	Scenes     map[domain.SceneID]SceneState
	Objects    []domain.ObjectSnapshot
	Sessions   []*domain.CollectSession
	Battles    []*domain.BattleRecord
	Queues     map[domain.ObjectID][]domain.ObjectID
	Deliveries []domain.Delivery
}

// RecoverWorld 读取世界的一致性快照。
//
// 严格只读：本函数不修改任何数据。
//
// 为什么「恢复」不负责递增 epoch / 置 NotReady：这两件事是**所有权**
// 语义（谁有权改），必须由拿到租约的进程在 Acquire 时完成。
// 如果 RecoverWorld 顺手写 epoch，它会变成一个「谁调用谁改代次」的
// 危险函数——两个进程同时调用就会互相把对方的写入栅栏掉。
// 因此本函数的契约是：只读、可重入、无副作用。
func (r *Repository) RecoverWorld(ctx context.Context, worldID domain.WorldID) (*RecoveredWorld, error) {
	if worldID == 0 {
		return nil, fmt.Errorf("%w: world_id 不能为 0", domain.ErrInvalidArgument)
	}

	// 用调用方的 ctx（通常带恢复预算），不再叠加短超时：
	// 大世界全量读取可能超过单个业务事务的 5s 预算。
	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
		ReadOnly:  true,
	})
	if err != nil {
		return nil, fmt.Errorf("开启恢复只读事务 world=%d: %w", worldID, err)
	}
	// 任何提前返回都回滚：只读事务回滚与提交等价，但不提交能确保
	// 「本函数绝不留下副作用」。
	finished := false
	defer func() {
		if !finished {
			_ = tx.Rollback()
		}
	}()

	out := &RecoveredWorld{
		WorldID: worldID,
		Scenes:  make(map[domain.SceneID]SceneState),
		Queues:  make(map[domain.ObjectID][]domain.ObjectID),
	}

	// 1) 世界行：epoch 是恢复结果的一部分（上层要拿它和租约里的 epoch 对齐）。
	if err := tx.QueryRowContext(ctx,
		`SELECT epoch FROM map_world WHERE world_id = ?`, int64(worldID)).Scan(&out.Epoch); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("%w: world_id=%d", ErrWorldNotFound, worldID)
		}
		return nil, fmt.Errorf("读取世界行 world=%d: %w", worldID, err)
	}

	// 2) 场景 revision，按 scene_id 升序。
	if err := r.recoverScenes(ctx, tx, worldID, out); err != nil {
		return nil, err
	}

	// 3) 全部对象（未删除）。
	objects, err := loadAllObjects(ctx, tx, worldID)
	if err != nil {
		return nil, fmt.Errorf("恢复对象: %w", err)
	}
	out.Objects = objects

	// 4) 活动采集会话。
	sessions, err := loadActiveSessions(ctx, tx, worldID)
	if err != nil {
		return nil, fmt.Errorf("恢复采集会话: %w", err)
	}
	out.Sessions = sessions

	// 5) 待裁决战斗。
	battles, err := loadPendingBattles(ctx, tx, worldID)
	if err != nil {
		return nil, fmt.Errorf("恢复战斗: %w", err)
	}
	out.Battles = battles

	// 6) 目标战斗队列。
	queues, err := loadBattleQueue(ctx, tx, worldID)
	if err != nil {
		return nil, fmt.Errorf("恢复战斗队列: %w", err)
	}
	out.Queues = queues

	// 7) 未完成交接单。
	deliveries, err := loadPendingDeliveries(ctx, tx, worldID)
	if err != nil {
		return nil, fmt.Errorf("恢复交接单: %w", err)
	}
	out.Deliveries = deliveries

	if err := tx.Commit(); err != nil {
		// 只读事务提交失败不会产生未知状态：没有任何写入。
		return nil, fmt.Errorf("结束恢复只读事务 world=%d: %w", worldID, err)
	}
	finished = true
	return out, nil
}

// recoverScenes 读取场景 revision。
//
// 为什么单独抽出来：场景行数是拓扑的固定值（默认 25），
// 读取逻辑简单但校验点集中——缺失的场景行必须报错，
// 不能「读多少算多少」，否则缺一个场景的世界会被当成完整世界 Ready。
func (r *Repository) recoverScenes(ctx context.Context, tx *sql.Tx, worldID domain.WorldID, out *RecoveredWorld) error {
	rows, err := tx.QueryContext(ctx,
		`SELECT scene_id, revision FROM map_scene WHERE world_id = ? ORDER BY scene_id ASC`,
		int64(worldID))
	if err != nil {
		return fmt.Errorf("查询场景行 world=%d: %w", worldID, err)
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var (
			sceneID  int32
			revision uint64
		)
		if err := rows.Scan(&sceneID, &revision); err != nil {
			return fmt.Errorf("扫描场景行: %w", err)
		}
		out.Scenes[domain.SceneID(sceneID)] = SceneState{Revision: revision}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("遍历场景行: %w", err)
	}
	return nil
}
