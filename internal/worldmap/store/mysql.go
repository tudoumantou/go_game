package store

// mysql.go 实现 runtime.Committer：把一个 domain.MutationPlan 原子地写进 InnoDB。
//
// 为什么整个文件只有「顺序 + 校验 + 错误分类」三件事：
// 文档 7.2 规定所有地图写事务共用一套加锁顺序，任何业务分支都不能
// 自己决定先锁谁。顺序一旦分叉，两个场景并发提交就会以不同顺序
// 请求同一批行锁，死锁率立刻上升。所以这里把顺序写成一条直线，
// 业务差异全部体现在 plan 的切片内容里，而不是体现在 SQL 的分支里。
//
// 本文件最重要的一条纪律（文档 7.3）：
//
//	COMMIT 报错 ≠ 事务失败。
//
// BEGIN/语句执行阶段的错误都是「确定失败」——此时 COMMIT 还没发出去，
// 服务端不可能已经提交；而 COMMIT 本身的错误（连接断开、context 超时、
// 服务端回包丢失）意味着「可能已提交、可能没提交」，属于**结果未知**。
// 把未知当失败会让调用方安全地重试……前提是事务真的没提交；
// 一旦它其实提交了，重试就会二次执行出征/结算，直接违反 INV-06/07。
// 因此 Unknown 是独立状态，本文件的任何错误分类都必须先回答
// 「COMMIT 发出去了吗」。

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"game_study/internal/worldmap/domain"

	mysqlDriver "github.com/go-sql-driver/mysql"
)

// MySQL 错误码。用常量而不是裸数字：错误码是 MySQL 的公开契约，
// 写名字能让 review 的人立刻看懂，也方便在注释里对应文档。
const (
	mysqlErrDupEntry        = 1062 // ER_DUP_ENTRY：唯一键/主键冲突
	mysqlErrLockWaitTimeout = 1205 // ER_LOCK_WAIT_TIMEOUT：行锁等待超时
	mysqlErrDeadlock        = 1213 // ER_LOCK_DEADLOCK：InnoDB 检测到死锁并回滚了本事务
)

// Commit 实现 runtime.Committer。
//
// 返回值语义（与 domain.CommitResult 的契约严格对齐）：
//   - Committed=true：确定提交成功；
//   - Unknown=true：结果不可解析，**禁止**当作失败重试；
//   - Err!=nil 且前两者为 false：确定回滚，可以按业务失败处理。
func (r *Repository) Commit(ctx context.Context, plan *domain.MutationPlan) domain.CommitResult {
	if plan == nil {
		return domain.CommitResult{Err: fmt.Errorf("%w: plan 不能为空", domain.ErrInvalidArgument)}
	}
	if plan.CommandID == "" {
		return domain.CommitResult{Err: fmt.Errorf("%w: 计划缺少 command_id", domain.ErrInvalidArgument)}
	}
	if plan.Key.WorldID == 0 {
		return domain.CommitResult{Err: fmt.Errorf("%w: 计划缺少 world_id", domain.ErrInvalidArgument)}
	}
	// 入口先看一次 ctx：已取消的上下文没必要去借连接。
	if err := ctx.Err(); err != nil {
		return domain.CommitResult{Err: err}
	}

	attempts := r.opts.MaxRetries + 1
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		res, err := r.commitOnce(ctx, plan)
		if err == nil {
			return res
		}
		// 结果未知优先返回：哪怕 err 是死锁形状，也不能重试——
		// 未知意味着「可能有写入生效」，重试等于拿事务去赌。
		if res.Unknown {
			return res
		}
		lastErr = err
		if !isRetryable(err) {
			break
		}
		if attempt+1 >= attempts {
			break
		}
		r.logger.Warn("map 事务可重试失败",
			"command_id", plan.CommandID, "attempt", attempt+1, "max_retries", r.opts.MaxRetries, "err", err)
		if err := sleepCtx(ctx, retryBackoff(attempt)); err != nil {
			// 退避期间被取消：这是确定失败（事务已经回滚）。
			return domain.CommitResult{Err: err}
		}
	}
	return domain.CommitResult{Err: lastErr}
}

// commitOnce 执行一次完整事务（可能被 Commit 重试多次）。
//
// 返回值：err==nil 表示已提交；err!=nil 且 res.Unknown 表示结果不可解析；
// 其余 err 表示确定回滚。object_id 全部来自 plan，所以重试天然沿用
// 已分配的 ID——绝不能在重试里重新调用 AllocObjectID，否则一旦首次
// 尝试其实已提交，第二次会用新 ID 再创建一遍对象（文档 14.2）。
func (r *Repository) commitOnce(ctx context.Context, plan *domain.MutationPlan) (domain.CommitResult, error) {
	// 每个事务一个独立超时：SRE 关注的不是「单个语句快」，而是
	// 「一笔事务不会占着行锁超过预算」。cancel 必须 defer，
	// 否则 context 树与定时器要等到超时才释放（连接池视角是泄漏）。
	txCtx := ctx
	var cancel context.CancelFunc
	if r.opts.TxTimeout > 0 {
		txCtx, cancel = context.WithTimeout(ctx, r.opts.TxTimeout)
		defer cancel()
	}

	// 1) BEGIN。
	//
	// 这里不用 pkg/db.Tx 模板：模板把「fn 返回错误就回滚」封装成黑盒，
	// 而本函数必须精确区分 COMMIT 的错误，还要在重试路径里复用同一个
	// plan。手写 BEGIN/COMMIT 多几行，换来的是错误分类完全可控。
	tx, err := r.db.BeginTx(txCtx, nil)
	if err != nil {
		return domain.CommitResult{}, fmt.Errorf("开启事务(world=%d): %w", plan.Key.WorldID, err)
	}
	committed := false
	// 兜底回滚：任何提前 return 都走这里。多次 Rollback 是安全的
	// （database/sql 返回 ErrTxDone 之后什么都不做）。
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	// 2) 世界行栅栏：owner + epoch（+ 租约）。
	lockedScenes, err := r.fenceWorld(txCtx, tx, plan)
	if err != nil {
		return domain.CommitResult{}, err
	}

	// 3) 场景行已在 fenceWorld 内按 scene_id 升序 FOR UPDATE 加锁，
	//    返回值 lockedScenes 是加锁后读到的**当前** revision。
	//    revision 的**比对**放到第 4 步之后，理由见 verifySceneRevisions。

	// 4) 幂等：同键同 hash 直接返回已提交，同键不同 hash 是客户端错误。
	if hit, err := r.lookupReceipt(txCtx, tx, plan); hit || err != nil {
		if err != nil {
			return domain.CommitResult{}, err
		}
		// 幂等命中：不写任何行，也不推进 revision，直接返回「已提交」。
		// 这里用 defer 的 Rollback 收尾而不是 Commit：本事务只做了读，
		// 回滚同样能释放行锁，而且不会引入「只读事务 COMMIT 失败」
		// 这种无意义的未知态（业务其实早已提交，冻结场景纯属误伤）。
		//
		// 注意返回的是「库里当前的 revision」而不是「期望+1」：
		// 首次提交已经把 revision 推进过了，再 +1 会让内存镜像比数据库
		// 多跑一代，下一次提交必然误判 revision 冲突。
		return domain.CommitResult{Committed: true, SceneRevisions: lockedScenes}, nil
	}

	// 4.5) 幂等未命中才做 revision 比对（顺序理由见 verifySceneRevisions）。
	if err := verifySceneRevisions(lockedScenes, plan.AffectedSceneRevisions); err != nil {
		return domain.CommitResult{}, err
	}

	// 5) 对象 upsert（object_id 升序）。
	upserts := sortedSnapshots(plan.ObjectsUpsert)
	expected := expectedObjects(plan.ExpectedObjectVersions)
	versions, err := r.writeObjects(txCtx, tx, plan, upserts, expected)
	if err != nil {
		return domain.CommitResult{}, err
	}

	// 6) 对象墓碑。
	if err := r.writeTombstones(txCtx, tx, plan, plan.ObjectsTombstone); err != nil {
		return domain.CommitResult{}, err
	}

	// 7) 静态占位：先声明后释放（声明冲突要整笔回滚）。
	if err := r.writeOccupancy(txCtx, tx, plan); err != nil {
		return domain.CommitResult{}, err
	}

	// 8) 采集会话：新建 + 结算（+ 资源流水）。
	if err := r.writeCollect(txCtx, tx, plan); err != nil {
		return domain.CommitResult{}, err
	}

	// 9) 战斗记录。
	if err := r.writeBattles(txCtx, tx, plan); err != nil {
		return domain.CommitResult{}, err
	}

	// 9b) 目标战斗队列（出队先于入队）。
	if err := r.writeBattleQueue(txCtx, tx, plan); err != nil {
		return domain.CommitResult{}, err
	}

	// 10) 回城交接单（先留单再清货）。
	if err := r.writeDeliveries(txCtx, tx, plan); err != nil {
		return domain.CommitResult{}, err
	}

	// 11) 首次命令回执 —— 它是「这条命令已生效」的提交标记。
	//     放在业务写入之后写，语义上就是「对象已落库，回执才存在」；
	//     反过来写会让崩溃后出现「回执存在但对象没落库」的中间态。
	if err := r.writeReceipt(txCtx, tx, plan); err != nil {
		return domain.CommitResult{}, err
	}

	// 12) outbox 投影事件（与业务同事务，保证不漏发）。
	if err := r.writeOutbox(txCtx, tx, plan); err != nil {
		return domain.CommitResult{}, err
	}

	// 13) 场景 revision +1（最后一步，与文档 7.2 的锁顺序一致）。
	result := domain.CommitResult{
		Committed:      true,
		SceneRevisions: make([]domain.SceneRevision, 0, len(lockedScenes)),
		ObjectVersions: versions,
	}
	for _, sr := range lockedScenes {
		if _, err := tx.ExecContext(txCtx,
			`UPDATE map_scene SET revision = revision + 1 WHERE world_id = ? AND scene_id = ?`,
			int64(plan.Key.WorldID), int32(sr.SceneID)); err != nil {
			return domain.CommitResult{}, fmt.Errorf("推进场景 revision(scene=%d): %w", sr.SceneID, err)
		}
		result.SceneRevisions = append(result.SceneRevisions,
			domain.SceneRevision{SceneID: sr.SceneID, Revision: sr.Revision + 1})
	}

	// 14) COMMIT —— 从这里开始，任何错误都是「结果未知」。
	if err := tx.Commit(); err != nil {
		unknown := fmt.Errorf("提交事务结果未知(command_id=%s): %w", plan.CommandID, err)
		// 必须打 Error 级别：这条日志是解析流程的唯一线索，
		// 运维要靠 command_id 去 map_command 里确认到底提交没有。
		r.logger.Error("map 事务 COMMIT 失败，结果未知，禁止按失败处理",
			"command_id", plan.CommandID, "world_id", uint64(plan.Key.WorldID), "err", err)
		return domain.CommitResult{Unknown: true, Err: unknown}, unknown
	}
	committed = true
	return result, nil
}

// fenceWorld 锁世界行、校验 owner/epoch/lease，再按 scene_id 升序
// 锁受影响场景行。返回加锁后读到的当前 revision（比对交给
// verifySceneRevisions，因为比对时机必须晚于幂等查询）。
//
// 为什么世界行也要 FOR UPDATE 而不是共享锁：文档 7.2 说「对 world 行使用
// 兼容的共享读锁使不同场景可并行提交，租约接管需要独占更新」。
// 本实现选排他锁是偏保守的取舍——同一 world 的所有提交在世界行上串行，
// 一致性最强、死锁面最小（世界行永远是第一把锁）。代价是同一世界的
// 并发度被压到 1；M2 单实例 + 短事务下这是可接受的，等实测吞吐不够
// 再换成 SELECT ... LOCK IN SHARE MODE，加锁顺序不变，属于局部改动。
func (r *Repository) fenceWorld(ctx context.Context, tx *sql.Tx, plan *domain.MutationPlan) ([]domain.SceneRevision, error) {
	var (
		owner      string
		epoch      uint64
		leaseUntil int64
	)
	err := tx.QueryRowContext(ctx,
		`SELECT owner_id, epoch, lease_until_ms FROM map_world WHERE world_id = ? FOR UPDATE`,
		int64(plan.Key.WorldID)).Scan(&owner, &epoch, &leaseUntil)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("%w: world_id=%d", ErrWorldNotFound, plan.Key.WorldID)
	}
	if err != nil {
		return nil, fmt.Errorf("锁定世界行 world=%d: %w", plan.Key.WorldID, err)
	}
	if r.opts.OwnerID != "" && owner != r.opts.OwnerID {
		return nil, fmt.Errorf("%w: 库中 owner=%q 本进程=%q", ErrLeaseLost, owner, r.opts.OwnerID)
	}
	if plan.Epoch != epoch {
		// 栅栏：epoch 不匹配说明计划来自旧代次（世界被重新加载过），
		// 旧代次的对象版本/revision 全都不可信，必须整笔拒绝。
		return nil, fmt.Errorf("%w: 计划 epoch=%d 当前=%d", ErrStaleEpoch, plan.Epoch, epoch)
	}
	// lease_until_ms=0 表示「尚未设置租约」（初始化/测试），不做过期判断。
	if leaseUntil > 0 && r.now() > leaseUntil {
		return nil, fmt.Errorf("%w: lease_until_ms=%d now=%d", ErrLeaseExpired, leaseUntil, r.now())
	}

	scenes := sortedSceneRevisions(plan.AffectedSceneRevisions)
	locked := make([]domain.SceneRevision, 0, len(scenes))
	for _, sr := range scenes {
		var rev uint64
		err := tx.QueryRowContext(ctx,
			`SELECT revision FROM map_scene WHERE world_id = ? AND scene_id = ? FOR UPDATE`,
			int64(plan.Key.WorldID), int32(sr.SceneID)).Scan(&rev)
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("%w: world_id=%d scene_id=%d", ErrSceneNotFound, plan.Key.WorldID, sr.SceneID)
		}
		if err != nil {
			return nil, fmt.Errorf("锁定场景行 scene=%d: %w", sr.SceneID, err)
		}
		// 这里只**读**当前 revision，不比对——比对接在幂等查询之后，
		// 原因见 verifySceneRevisions 的注释。
		locked = append(locked, domain.SceneRevision{SceneID: sr.SceneID, Revision: rev})
	}
	return locked, nil
}

// verifySceneRevisions 把加锁后读到的 revision 与计划期望值比对。
//
// 为什么比对必须发生在**幂等查询之后**（这条顺序来自文档 7.3）：
// 一条命令「上次提交成功但回包丢失」后重试时，它携带的期望 revision
// 是首次执行前的值，而库里已经 +1。如果先比对 revision，重试永远只能
// 拿到 ErrStaleVersion，永远读不到首次结果——幂等机制在最需要它的场景
// 下彻底失效。文档 7.2 的加锁顺序（世界 → 场景 → 对象）依然严格遵守，
// 改变的只是「比对」这个纯内存动作的位置。
func verifySceneRevisions(locked, expected []domain.SceneRevision) error {
	want := make(map[domain.SceneID]uint64, len(expected))
	for _, sr := range expected {
		want[sr.SceneID] = sr.Revision
	}
	for _, got := range locked {
		exp, ok := want[got.SceneID]
		if !ok {
			continue
		}
		if got.Revision != exp {
			// 乐观并发失败：场景在规划之后被别的命令改过。
			return fmt.Errorf("%w: scene=%d 期望 revision=%d 实际=%d",
				domain.ErrStaleVersion, got.SceneID, exp, got.Revision)
		}
	}
	return nil
}

// lookupReceipt 查询幂等键。
//
// 返回 (true, nil) 表示同键同 hash 已提交过；(false, nil) 表示首次执行。
//
// 为什么不加 FOR UPDATE：这一步只需要「读一眼有没有回执」。如果并发事务
// 正在插入同一个键，我们的 INSERT（第 11 步）会拿到 1062 或等待它提交，
// 两种结果都能被正确处理，不需要在这里提前加锁——提前加锁反而扩大了
// 持锁范围。
func (r *Repository) lookupReceipt(ctx context.Context, tx *sql.Tx, plan *domain.MutationPlan) (bool, error) {
	actor := actorKeyOf(plan)
	var (
		commandID string
		hash      []byte
		result    []byte
	)
	err := tx.QueryRowContext(ctx,
		`SELECT command_id, payload_hash, result_json FROM map_command
		 WHERE world_id = ? AND actor_key = ? AND request_id = ?`,
		int64(plan.Key.WorldID), actor, plan.Key.RequestID).Scan(&commandID, &hash, &result)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("查询命令回执: %w", err)
	}
	if !bytes.Equal(hash, plan.PayloadHash[:]) {
		// 同键不同参：不能再按新参数执行一遍，否则等于绕过幂等。
		return false, fmt.Errorf("%w: world=%d actor=%s request=%s",
			domain.ErrIdempotencyConflict, plan.Key.WorldID, actor, plan.Key.RequestID)
	}
	// 首次 result_json 已经随回执持久化。CommitResult 没有承载它的字段
	// （domain 不归本任务修改），因此这里记日志并用返回值表达「已提交」；
	// 调用方需要拿到首次结果时应走 FindReceipt / CommandResolution
	// （文档 7.3 步骤 2 的解析路径），这也是设计上唯一的权威来源。
	r.logger.Info("幂等命中，直接返回首次结果",
		"command_id", commandID, "request_id", plan.Key.RequestID, "result_json", string(result))
	return true, nil
}

// writeObjects 写入/更新对象快照，返回提交后的对象版本列表。
func (r *Repository) writeObjects(
	ctx context.Context, tx *sql.Tx, plan *domain.MutationPlan,
	snaps []domain.ObjectSnapshot, expected map[domain.ObjectID]uint64,
) ([]domain.ObjectVersion, error) {
	versions := make([]domain.ObjectVersion, 0, len(snaps))
	for _, snap := range snaps {
		var current uint64
		err := tx.QueryRowContext(ctx,
			`SELECT version FROM map_object WHERE world_id = ? AND object_id = ? FOR UPDATE`,
			int64(plan.Key.WorldID), uint64(snap.Key.ID)).Scan(&current)
		exists := true
		if errors.Is(err, sql.ErrNoRows) {
			exists = false
		} else if err != nil {
			return nil, fmt.Errorf("锁定对象 object_id=%d: %w", snap.Key.ID, err)
		}

		// 版本比对规则：
		//   - 行已存在：必须与期望版本逐位相等；
		//   - 行不存在：期望只能是 0（「我认为它不存在」）。期望非 0
		//     说明规划器的镜像里有个库里没有的对象——这是数据不一致，
		//     按 StaleVersion 拒绝，让上层走快照重建而不是盲目插入。
		if exp, ok := expected[snap.Key.ID]; ok {
			if (!exists && exp != 0) || (exists && current != exp) {
				return nil, fmt.Errorf("%w: object_id=%d 期望=%d 实际=%d(存在=%t)",
					domain.ErrStaleVersion, snap.Key.ID, exp, current, exists)
			}
		}

		payload, err := encodePayload(&snap)
		if err != nil {
			return nil, fmt.Errorf("编码对象 payload object_id=%d: %w", snap.Key.ID, err)
		}
		// ON DUPLICATE KEY UPDATE 而不是「先 SELECT 再 INSERT/UPDATE」：
		// 判断与写入在一条语句里完成，并发下不会出现两者之间的窗口。
		// 前面已经 FOR UPDATE 锁过该行，这条语句的作用主要是让插入与
		// 更新共用一条 SQL，减少分支。
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO map_object
			   (world_id, object_id, kind, owner_scene, version, schema_version, player_id, payload, deleted)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
			 ON DUPLICATE KEY UPDATE
			   kind = VALUES(kind), owner_scene = VALUES(owner_scene), version = VALUES(version),
			   schema_version = VALUES(schema_version), player_id = VALUES(player_id),
			   payload = VALUES(payload), deleted = VALUES(deleted)`,
			int64(plan.Key.WorldID), uint64(snap.Key.ID), int32(snap.Kind), int32(snap.OwnerScene),
			snap.Version, snap.SchemaVersion, snap.PlayerID, payload, boolToInt32(snap.Deleted),
		); err != nil {
			return nil, fmt.Errorf("写入对象 object_id=%d: %w", snap.Key.ID, err)
		}
		versions = append(versions, domain.ObjectVersion{Key: snap.Key, Version: snap.Version})
	}
	return versions, nil
}

// writeTombstones 写墓碑。
//
// 为什么不物理 DELETE：历史命令回执与被删对象是同一笔事实的两面，
// 审计要能解释「这里曾经有什么」。deleted=1 且 version+1 就够了，
// 读路径统一带 deleted=0 过滤（LoadScene 已经这么做）。
func (r *Repository) writeTombstones(ctx context.Context, tx *sql.Tx, plan *domain.MutationPlan, keys []domain.ObjectKey) error {
	for _, key := range sortedKeys(keys) {
		res, err := tx.ExecContext(ctx,
			`UPDATE map_object SET deleted = 1, version = version + 1
			 WHERE world_id = ? AND object_id = ?`,
			int64(plan.Key.WorldID), uint64(key.ID))
		if err != nil {
			return fmt.Errorf("写对象墓碑 object_id=%d: %w", key.ID, err)
		}
		// 0 行说明对象根本不存在：墓碑一个不存在的对象是规划错误。
		// 这里不做「静默成功」——静默会让「删不掉」在线上表现为
		// 「删了但其实还在」，更难排查。
		if n, err := res.RowsAffected(); err == nil && n == 0 {
			return fmt.Errorf("%w: 墓碑目标不存在 object_id=%d", domain.ErrObjectNotFound, key.ID)
		}
	}
	return nil
}

// writeOccupancy 写静态占位声明与释放。
//
// 关键：1062 在这里是**业务结论**而不是系统故障。两个并发创建在内存里
// 都判定「这是空格」，最终只能靠唯一键裁决，输的一方拿到 1062 后
// 返回 PositionOccupied（文档 5.3）。InnoDB 的重复键错误只回滚当前语句，
// 但业务语义要求整笔命令失败，所以这里返回错误让 defer 回滚整个事务。
func (r *Repository) writeOccupancy(ctx context.Context, tx *sql.Tx, plan *domain.MutationPlan) error {
	for _, claim := range plan.OccupancyClaims {
		_, err := tx.ExecContext(ctx,
			`INSERT INTO map_occupancy (world_id, cell_x, cell_y, layer, object_id) VALUES (?, ?, ?, ?, ?)`,
			int64(plan.Key.WorldID), claim.CellX, claim.CellY, claim.Layer, uint64(claim.ObjectKey.ID))
		if err != nil {
			if isMySQLCode(err, mysqlErrDupEntry) {
				return fmt.Errorf("%w: cell=(%d,%d) layer=%d object_id=%d",
					domain.ErrPositionOccupied, claim.CellX, claim.CellY, claim.Layer, claim.ObjectKey.ID)
			}
			return fmt.Errorf("占用格 (%d,%d): %w", claim.CellX, claim.CellY, err)
		}
	}
	for _, rel := range plan.OccupancyReleases {
		// 释放带 object_id 条件：只有「确实是这个对象占的格」才允许删，
		// 防止旧计划的释放误删新对象刚抢到的格子。
		if _, err := tx.ExecContext(ctx,
			`DELETE FROM map_occupancy
			 WHERE world_id = ? AND cell_x = ? AND cell_y = ? AND layer = ? AND object_id = ?`,
			int64(plan.Key.WorldID), rel.CellX, rel.CellY, rel.Layer, uint64(rel.ObjectKey.ID)); err != nil {
			return fmt.Errorf("释放格 (%d,%d): %w", rel.CellX, rel.CellY, err)
		}
	}
	return nil
}

// writeCollect 处理采集会话的新建与结算。
//
// 结算顺序照抄文档 14.3：锁会话 → 确认 ACTIVE → UPDATE 会话 →
// INSERT 资源流水。流水的 UNIQUE(world,command_id,resource_id,kind)
// 是「同一命令的同一动作只记一次账」的最终保证（文档 10.3）。
func (r *Repository) writeCollect(ctx context.Context, tx *sql.Tx, plan *domain.MutationPlan) error {
	for _, sess := range plan.CollectStart {
		// 新建会话必然是 ACTIVE（零值按 ACTIVE 兜底）：直接落 0 会让
		// 这条会话既不是 ACTIVE 也不是 SETTLED，恢复时读不出来。
		status := sess.Status
		if status == domain.CollectStatusUnspecified {
			status = domain.CollectStatusActive
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO map_collect
			   (world_id, collect_id, resource_id, troop_id, start_at_ms, end_at_ms,
			    planned_qty, rate_milli, status, settled_qty, rule_version)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			int64(plan.Key.WorldID), sess.ID, uint64(sess.ResourceID), uint64(sess.TroopID),
			sess.StartAtMS, sess.EndAtMS, sess.PlannedQty, sess.RateMilliItemsPerSec,
			int32(status), sess.SettledQty, sess.RuleVersion); err != nil {
			return fmt.Errorf("写入采集会话 %s: %w", sess.ID, err)
		}
	}

	for _, ch := range plan.CollectSettle {
		var (
			resourceID uint64
			status     int32
		)
		err := tx.QueryRowContext(ctx,
			`SELECT resource_id, status FROM map_collect
			 WHERE world_id = ? AND collect_id = ? FOR UPDATE`,
			int64(plan.Key.WorldID), ch.SessionID).Scan(&resourceID, &status)
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("%w: collect_id=%s", ErrCollectNotFound, ch.SessionID)
		}
		if err != nil {
			return fmt.Errorf("锁定采集会话 %s: %w", ch.SessionID, err)
		}
		if domain.CollectStatus(status) != domain.CollectStatusActive {
			// 已经结算过：结算本身是幂等的（文档 10.2「重复结算返回原结果」），
			// 重复落一次 UPDATE 会把 settled_qty 覆盖成新值，所以直接跳过。
			r.logger.Warn("采集会话已非 ACTIVE，跳过结算", "collect_id", ch.SessionID, "status", status)
			continue
		}
		res, err := tx.ExecContext(ctx,
			`UPDATE map_collect SET status = 2, settled_qty = ?
			 WHERE world_id = ? AND collect_id = ? AND status = 1`,
			ch.SettledQty, int64(plan.Key.WorldID), ch.SessionID)
		if err != nil {
			return fmt.Errorf("结算采集会话 %s: %w", ch.SessionID, err)
		}
		if n, err := res.RowsAffected(); err == nil && n == 0 {
			// 并发结算：另一笔事务已经把它改成 SETTLED。仍然继续写流水，
			// 由 uk_command 决定这条账是否已经记过。
			r.logger.Warn("采集会话结算未命中 ACTIVE 状态", "collect_id", ch.SessionID)
		}

		remaining, _ := resourceRemainingAfter(plan, domain.ObjectID(resourceID))
		eventID, err := newEventID()
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx,
			`INSERT INTO map_resource_ledger
			   (event_id, world_id, resource_id, command_id, kind, qty, remaining_after)
			 VALUES (?, ?, ?, ?, 2, ?, ?)`,
			eventID, int64(plan.Key.WorldID), resourceID, plan.CommandID, ch.SettledQty, remaining)
		if err != nil {
			// 1062 = 这条账已经记过（重试/重复结算）。流水是审计事实，
			// 重复插入必须被吞掉，否则重试永远成功不了。
			if isMySQLCode(err, mysqlErrDupEntry) {
				r.logger.Info("资源流水已存在，跳过", "collect_id", ch.SessionID, "command_id", plan.CommandID)
				continue
			}
			return fmt.Errorf("写资源流水 collect_id=%s: %w", ch.SessionID, err)
		}
	}
	return nil
}

// resourceRemainingAfter 从计划的资源点快照里取「结算后余量」。
//
// 流水表的 remaining_after 是审计字段，必须与对象快照同源；从库里
// 再查一次反而可能读到别的命令写入的更新值。找不到快照时返回 0 并
// 由调用方忽略 ok ——这是「计划与流水字段不同步」的降级路径，
// 不阻断结算（结算金额本身来自 SettledQty，审计余量缺失不影响正确性，
// 但会在对账时暴露）。
func resourceRemainingAfter(plan *domain.MutationPlan, resourceID domain.ObjectID) (int64, bool) {
	for _, snap := range plan.ObjectsUpsert {
		if snap.Key.ID == resourceID && snap.Resource != nil {
			return snap.Resource.Remaining, true
		}
	}
	return 0, false
}

// writeBattles 写战斗记录。
//
// ON DUPLICATE KEY UPDATE 只覆盖 status/result/applied_operation_id 三个
// 可变列：输入快照（input_hash、双方属性、seed）创建后永不修改，
// 否则「相同输入→相同结果」的对账基础就没了（文档 11.1）。
func (r *Repository) writeBattles(ctx context.Context, tx *sql.Tx, plan *domain.MutationPlan) error {
	for _, b := range plan.BattleUpsert {
		attacker, err := json.Marshal(combatToJSON(b.AttackerStats))
		if err != nil {
			return fmt.Errorf("编码攻方属性 %s: %w", b.BattleID, err)
		}
		defender, err := json.Marshal(combatToJSON(b.DefenderStats))
		if err != nil {
			return fmt.Errorf("编码守方属性 %s: %w", b.BattleID, err)
		}
		var result any
		if b.Result != nil {
			raw, err := json.Marshal(resultToJSON(b.Result))
			if err != nil {
				return fmt.Errorf("编码战斗结果 %s: %w", b.BattleID, err)
			}
			result = raw
		}
		ruleVersion := b.RuleVersion
		if ruleVersion == "" {
			ruleVersion = b.AttackerStats.RuleVersion
		}
		worldID := b.WorldID
		if worldID == 0 {
			worldID = plan.Key.WorldID
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO map_battle
			   (world_id, battle_id, target_id, attacker_id, defender_id, input_hash,
			    attacker_stats, defender_stats, rule_version, random_seed, status,
			    deadline_ms, result, applied_operation_id)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			 ON DUPLICATE KEY UPDATE
			   status = VALUES(status), result = VALUES(result),
			   applied_operation_id = VALUES(applied_operation_id)`,
			int64(worldID), b.BattleID, uint64(b.TargetID), uint64(b.AttackerID), uint64(b.DefenderID),
			b.InputHash[:], attacker, defender, ruleVersion, b.RandomSeed, int32(b.Status),
			b.DeadlineMS, result, b.AppliedOperationID,
		); err != nil {
			return fmt.Errorf("写入战斗 %s: %w", b.BattleID, err)
		}
	}
	return nil
}

// writeBattleQueue 维护目标战斗队列（文档 11.4）。
//
// 顺序：先把 Dequeue 落库，再写 Enqueue。两个切片表达的是**意图**
// 而不是全量状态，同一笔计划里常见「队首战斗结束、下一名顶上」：
// 先删后插才是因果顺序，反过来会短暂出现「两个成员同时排第一」。
//
// 入队用 ON DUPLICATE KEY UPDATE enqueue_seq = enqueue_seq 做 no-op：
// UNIQUE(world,target,troop) 的作用就是「同一支部队不能重复入队」，
// 重试/重复意图命中唯一键时保持原序号不变，不能把排队位置刷新到队尾
// ——否则玩家的队列顺序会被一次网络重试惩罚。
func (r *Repository) writeBattleQueue(ctx context.Context, tx *sql.Tx, plan *domain.MutationPlan) error {
	for _, e := range plan.QueueDequeue {
		// 按 (target,troop) 删，Seq 允许缺省：出队时序号没有语义，
		// 而 target+troop 有唯一键保证最多一行。
		if _, err := tx.ExecContext(ctx,
			`DELETE FROM map_battle_queue WHERE world_id = ? AND target_id = ? AND troop_id = ?`,
			int64(plan.Key.WorldID), uint64(e.TargetID), uint64(e.TroopID)); err != nil {
			return fmt.Errorf("战斗队列出队 target=%d troop=%d: %w", e.TargetID, e.TroopID, err)
		}
	}
	for _, e := range plan.QueueEnqueue {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO map_battle_queue (world_id, target_id, enqueue_seq, troop_id)
			 VALUES (?, ?, ?, ?)
			 ON DUPLICATE KEY UPDATE enqueue_seq = enqueue_seq`,
			int64(plan.Key.WorldID), uint64(e.TargetID), e.Seq, uint64(e.TroopID)); err != nil {
			return fmt.Errorf("战斗队列入队 target=%d troop=%d seq=%d: %w", e.TargetID, e.TroopID, e.Seq, err)
		}
	}
	return nil
}

// writeDeliveries 写回城交接单。
//
// 1062 视为「已创建」并跳过：UNIQUE(world,troop,settlement_no) 的作用
// 就是让重复事务只能有一张单（文档 9.5「先留单再清货」）。同一单号被
// 重复插入说明重试/并发，跳过即可；但绝不覆盖 payload——单子一旦创建
// 就不可修改，覆盖会让玩家侧的去重键与内容脱钩。
func (r *Repository) writeDeliveries(ctx context.Context, tx *sql.Tx, plan *domain.MutationPlan) error {
	for _, d := range plan.DeliveryCreate {
		cargo, err := json.Marshal(cargoToJSON(d.Cargo))
		if err != nil {
			return fmt.Errorf("编码交接单 cargo %s: %w", d.DeliveryID, err)
		}
		worldID := d.WorldID
		if worldID == 0 {
			worldID = plan.Key.WorldID
		}
		status := d.Status
		if status == 0 {
			status = domain.DeliveryStatusPending
		}
		createdAt := d.CreatedAtMS
		if createdAt == 0 {
			createdAt = r.now()
		}
		_, err = tx.ExecContext(ctx,
			`INSERT INTO map_delivery
			   (delivery_id, world_id, troop_id, player_id, payload_hash, cargo, status,
			    attempts, next_retry_ms, settlement_no, applied_player_version, created_at_ms)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?)`,
			d.DeliveryID, int64(worldID), uint64(d.TroopID), d.PlayerID, d.PayloadHash[:], cargo,
			int32(status), d.Attempts, d.NextRetryMS, d.SettlementNo, createdAt)
		if err != nil {
			if isMySQLCode(err, mysqlErrDupEntry) {
				r.logger.Info("交接单已存在，跳过创建", "delivery_id", d.DeliveryID, "settlement_no", d.SettlementNo)
				continue
			}
			return fmt.Errorf("写入交接单 %s: %w", d.DeliveryID, err)
		}
	}
	return nil
}

// writeReceipt 写首次命令回执。
func (r *Repository) writeReceipt(ctx context.Context, tx *sql.Tx, plan *domain.MutationPlan) error {
	rc := plan.CommandReceipt
	if rc == nil {
		return nil
	}
	// 回执的 command_id 以计划为准（计划 ID 是本事务的身份），
	// 只有当计划里没填时才退回到回执自身的字段。
	commandID := rc.CommandID
	if commandID == "" {
		commandID = plan.CommandID
	}
	resultJSON := rc.ResultJSON
	if len(resultJSON) == 0 {
		// JSON 列不接受空串/非法 JSON。空结果写 {} 而不是 NULL：
		// 列声明是 NOT NULL，且「有回执但结果体为空」在语义上等价于空对象。
		resultJSON = []byte("{}")
	}
	committedAt := rc.CommittedAtMS
	if committedAt == 0 {
		committedAt = r.now()
	}
	_, err := tx.ExecContext(ctx,
		`INSERT INTO map_command
		   (world_id, actor_key, request_id, command_id, payload_hash, result_code, result_json, committed_at_ms)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		int64(plan.Key.WorldID), actorKeyOf(plan), plan.Key.RequestID, commandID,
		rc.PayloadHash[:], rc.ResultCode, resultJSON, committedAt)
	if err != nil {
		if isMySQLCode(err, mysqlErrDupEntry) {
			// 并发同键提交：另一方先写了回执。此时本事务的写入必须整体
			// 放弃——同一幂等键只能有一个执行结果。
			return fmt.Errorf("%w: 回执已被并发事务写入 request_id=%s",
				domain.ErrIdempotencyConflict, plan.Key.RequestID)
		}
		return fmt.Errorf("写入命令回执 %s: %w", commandID, err)
	}
	return nil
}

// writeOutbox 写投影事件。
//
// outbox 与业务同事务是「不漏发」的唯一手段：进程崩溃后后台 worker 仍能
// 从这张表把事件拉出来。发布失败只重试，不回滚业务（文档 7.3）。
func (r *Repository) writeOutbox(ctx context.Context, tx *sql.Tx, plan *domain.MutationPlan) error {
	for _, ev := range plan.OutboxEvents {
		payload := ev.Payload
		if len(payload) == 0 {
			payload = []byte("{}")
		}
		eventID := ev.EventID
		if eventID == "" {
			// 计划没给 ID 时兜底生成：outbox 主键是事件身份，
			// 缺失就写不进去；这里补一个随机值并留日志。
			id, err := newEventID()
			if err != nil {
				return err
			}
			eventID = id
			r.logger.Warn("outbox 事件缺少 event_id，已生成", "event_id", eventID, "command_id", plan.CommandID)
		}
		worldID := ev.WorldID
		if worldID == 0 {
			worldID = plan.Key.WorldID
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO map_outbox
			   (event_id, world_id, scene_id, revision, kind, payload, status, next_retry_ms)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			eventID, int64(worldID), int32(ev.SceneID), ev.Revision, ev.Kind, payload,
			ev.Status, ev.NextRetryMS); err != nil {
			return fmt.Errorf("写入 outbox 事件 %s: %w", eventID, err)
		}
	}
	return nil
}

// ---------- 排序/映射辅助 ----------

// sortedSceneRevisions 复制并按 scene_id 升序，同时去重。
//
// 为什么要复制：plan 属于调用方（规划器），排序会就地修改底层数组，
// 并发场景下可能被另一个 goroutine 读到半排序状态。Go 的切片是
// 「指针+长度+容量」的视图，append 到 nil 会分配新底层数组，这是
// 最省事的深拷贝写法。
func sortedSceneRevisions(in []domain.SceneRevision) []domain.SceneRevision {
	out := append([]domain.SceneRevision(nil), in...)
	sort.Slice(out, func(i, j int) bool { return out[i].SceneID < out[j].SceneID })
	// 去重：同一 scene 出现两次时，第二次 FOR UPDATE 读到的是同一行，
	// 但它的期望 revision 与「已加锁后的校正」语义重复，容易误判。
	if len(out) < 2 {
		return out
	}
	dst := 1
	for i := 1; i < len(out); i++ {
		if out[i].SceneID == out[dst-1].SceneID {
			continue
		}
		out[dst] = out[i]
		dst++
	}
	return out[:dst]
}

// sortedSnapshots 复制并按 object_id 升序。
//
// 加锁顺序必须与主键顺序一致：所有事务都按 object_id 升序取行锁，
// 才不会出现 A 锁 1 等 2、B 锁 2 等 1 的经典死锁环。
func sortedSnapshots(in []domain.ObjectSnapshot) []domain.ObjectSnapshot {
	out := append([]domain.ObjectSnapshot(nil), in...)
	sort.SliceStable(out, func(i, j int) bool { return out[i].Key.ID < out[j].Key.ID })
	return out
}

// sortedKeys 复制并按 object_id 升序（墓碑同理）。
func sortedKeys(in []domain.ObjectKey) []domain.ObjectKey {
	out := append([]domain.ObjectKey(nil), in...)
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// expectedObjects 把期望版本切片转成 map 便于 O(1) 查找。
func expectedObjects(in []domain.ObjectVersion) map[domain.ObjectID]uint64 {
	if len(in) == 0 {
		return nil
	}
	m := make(map[domain.ObjectID]uint64, len(in))
	for _, ov := range in {
		m[ov.Key.ID] = ov.Version
	}
	return m
}

// isMySQLCode 判断驱动错误是否为指定 MySQL 错误码。
//
// Go 知识点：errors.As 会沿着 %w 链逐层做类型断言。go-sql-driver/mysql
// 把服务端错误表示为 *mysql.MySQLError，Number 就是 ER_* 码。
// 用 As 而不是自己拆字符串，是因为 database/sql 可能在外面再包一层
// （例如 "driver: bad connection"），As 能穿透包装。
func isMySQLCode(err error, code uint16) bool {
	var me *mysqlDriver.MySQLError
	if !errors.As(err, &me) || me == nil {
		return false
	}
	return me.Number == code
}

// isRetryable 判断错误是否值得重试（死锁 / 锁等待超时）。
//
// 这两类错误都意味着「本事务被 MySQL 主动回滚」，是**确定失败**，
// 所以可以安全地重开事务再试一次（文档 7.2：死锁只允许以同一
// command_id 有限重试）。两者区别：
//   - 1213 死锁：InnoDB 检测到环，立刻牺牲一个事务，重试通常成功；
//   - 1205 锁等待超时：只是等太久了，重试仍可能再等超时，
//     所以同样受 MaxRetries 限制，避免把超时变成忙等。
//
// context 取消/超时不在重试之列：ctx 已经死了，重试只会立刻再失败。
func isRetryable(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	return isMySQLCode(err, mysqlErrDeadlock) || isMySQLCode(err, mysqlErrLockWaitTimeout)
}

// boolToInt32 把 bool 转 TINYINT(1)。MySQL 没有原生 bool，
// database/sql 虽然支持传 bool，但显式转成 0/1 让写的值可预期。
func boolToInt32(b bool) int32 {
	if b {
		return 1
	}
	return 0
}
