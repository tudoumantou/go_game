package store

// lease.go 实现世界租约（设计文档 16.2）。
//
// 为什么租约必须落在 SQL 而不是只靠 etcd：
//   - etcd 只负责「发现」谁在服务这个 world，节点地址存在不代表它还能写库；
//   - 真正决定写权限的是 map_world 这一行的 owner_id + epoch + lease_until_ms。
//     进程 GC 停顿 40 秒后 etcd 租约会话可能还没过期，但它必须立刻失去写权限。
//
// 因此每次提交事务都会重新校验 owner/epoch（见 mysql.go 的 fenceWorld），
// 租约本身只是「让新进程知道什么时候可以接管」的协调数据。

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"game_study/internal/worldmap/domain"
)

// Acquire 取得世界所有权并返回新 epoch。
//
// 语义（文档 16.2）：
//   - 行不存在：创建一行，epoch=1，owner=ownerID，status=RECOVERING；
//   - 行存在且租约未过期且 owner 是别人：ErrLeaseHeld，不抢占；
//   - 否则（租约已过期，或 owner 就是自己）：epoch+1、写 owner 与新租约、
//     status=RECOVERING。
//
// 为什么 epoch 一定要 +1：新进程加载的是「数据库里的现状」，而旧进程
// 可能还握着内存里的旧镜像与在途事务。epoch 是它们之间唯一的栅栏——
// 旧事务重新提交时会在 fenceWorld 里被 ErrStaleEpoch 拒绝。
//
// 为什么 status 置 RECOVERING 而不是 READY：拿到租约只代表「可以开始
// 加载」，一致快照 + 到期业务补处理完成之前，世界不能接受新写。
// 置 READY 是上层恢复流程的责任（本包不替它做决定）。
func (r *Repository) Acquire(ctx context.Context, worldID domain.WorldID, ownerID string, ttl time.Duration) (uint64, error) {
	if worldID == 0 {
		return 0, fmt.Errorf("%w: world_id 不能为 0", domain.ErrInvalidArgument)
	}
	if ownerID == "" {
		return 0, fmt.Errorf("%w: owner_id 不能为空", domain.ErrInvalidArgument)
	}
	if ttl <= 0 {
		ttl = r.opts.LeaseTTL
	}
	until := r.now() + ttl.Milliseconds()

	txCtx, cancel := context.WithTimeout(ctx, r.opts.TxTimeout)
	defer cancel()

	tx, err := r.db.BeginTx(txCtx, nil)
	if err != nil {
		return 0, fmt.Errorf("取得世界租约(开启事务): %w", err)
	}
	// 与 commitOnce 不同，这里的错误都发生在 COMMIT 之前或 COMMIT 的非关键
	// 分支上；未提交即未生效，回滚即可，不存在「结果未知」的歧义。
	defer func() { _ = tx.Rollback() }()

	var (
		currentOwner string
		currentEpoch uint64
		leaseUntil   int64
	)
	// FOR UPDATE：租约接管必须是串行的。两个人同时判断「旧租约过期」
	// 然后各自 epoch+1，会让两个进程都以为自己赢了。
	err = tx.QueryRowContext(txCtx,
		`SELECT owner_id, epoch, lease_until_ms FROM map_world WHERE world_id = ? FOR UPDATE`,
		int64(worldID)).Scan(&currentOwner, &currentEpoch, &leaseUntil)

	switch {
	case errors.Is(err, sql.ErrNoRows):
		// 初次创建：拓扑版本/配置版本由初始化流程随后写入。这里留 0/'' 是
		// 诚实表达「还不知道」，而不是编一份默认配置进库。
		//
		// 不用 INSERT ... ON DUPLICATE KEY UPDATE 兜并发：上面的 FOR UPDATE
		// 已经在不存在的主键区间上加了间隙锁，另一笔并发创建会阻塞到
		// 本事务结束，因此这里的 1062 只可能是「锁粒度被调小」后的异常，
		// 按 ErrLeaseHeld 报出来让人看见，而不是悄悄改成抢锁。
		if _, err := tx.ExecContext(txCtx,
			`INSERT INTO map_world
			   (world_id, owner_id, epoch, lease_until_ms, topology_version, config_version, status)
			 VALUES (?, ?, 1, ?, 0, '', ?)`,
			int64(worldID), ownerID, until, WorldStatusRecovering); err != nil {
			if isMySQLCode(err, mysqlErrDupEntry) {
				return 0, fmt.Errorf("%w: 世界行被并发创建 world_id=%d", ErrLeaseHeld, worldID)
			}
			return 0, fmt.Errorf("创建世界行 world_id=%d: %w", worldID, err)
		}
		if err := tx.Commit(); err != nil {
			// 创建世界行的事务提交失败：没有既有状态被破坏，直接报错由上层重试。
			return 0, fmt.Errorf("创建世界行提交失败 world_id=%d: %w", worldID, err)
		}
		r.logger.Info("世界首次创建并取得租约", "world_id", uint64(worldID), "owner", ownerID, "epoch", 1)
		return 1, nil

	case err != nil:
		return 0, fmt.Errorf("查询世界租约 world_id=%d: %w", worldID, err)
	}

	held := leaseUntil > r.now() && currentOwner != "" && currentOwner != ownerID
	if held {
		return 0, fmt.Errorf("%w: world_id=%d owner=%q 租约至 %d(now=%d)",
			ErrLeaseHeld, worldID, currentOwner, leaseUntil, r.now())
	}

	newEpoch := currentEpoch + 1
	if _, err := tx.ExecContext(txCtx,
		`UPDATE map_world SET owner_id = ?, epoch = ?, lease_until_ms = ?, status = ?
		 WHERE world_id = ?`,
		ownerID, newEpoch, until, WorldStatusRecovering, int64(worldID)); err != nil {
		return 0, fmt.Errorf("接管世界租约 world_id=%d: %w", worldID, err)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("接管世界租约提交失败 world_id=%d: %w", worldID, err)
	}
	r.logger.Info("世界租约接管成功",
		"world_id", uint64(worldID), "owner", ownerID,
		"prev_owner", currentOwner, "prev_epoch", currentEpoch, "epoch", newEpoch)
	return newEpoch, nil
}

// Renew 续租。
//
// 带 epoch 条件不只是防御：进程被挂起超过 TTL 后，别的进程可能已经
// 递增 epoch 接管。如果续租只按 owner 匹配，被挂起的进程会把自己
// 「续活」，于是同一世界出现两个自认为合法的写入者。epoch 条件让
// 旧进程的续租必然失败 → 上层立即停止接单（文档 16.2）。
func (r *Repository) Renew(ctx context.Context, worldID domain.WorldID, ownerID string, epoch uint64, ttl time.Duration) error {
	if worldID == 0 {
		return fmt.Errorf("%w: world_id 不能为 0", domain.ErrInvalidArgument)
	}
	if ownerID == "" {
		return fmt.Errorf("%w: owner_id 不能为空", domain.ErrInvalidArgument)
	}
	if ttl <= 0 {
		ttl = r.opts.LeaseTTL
	}
	until := r.now() + ttl.Milliseconds()

	res, err := r.db.ExecContext(ctx,
		`UPDATE map_world SET lease_until_ms = ?
		 WHERE world_id = ? AND owner_id = ? AND epoch = ?`,
		until, int64(worldID), ownerID, epoch)
	if err != nil {
		return fmt.Errorf("续租 world_id=%d: %w", worldID, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("续租影响行数 world_id=%d: %w", worldID, err)
	}
	if n == 0 {
		// 注意：MySQL 的 RowsAffected 默认返回「真正改变的行数」。
		// 若 lease_until_ms 恰好等于新值（同一毫秒内连续续租），
		// 会得到 0。用 epoch 先查一遍太贵，接受这个极小概率的误报：
		// 上层收到 ErrLeaseLost 会降级并重试，最坏只多一次告警。
		return fmt.Errorf("%w: world_id=%d owner=%q epoch=%d", ErrLeaseLost, worldID, ownerID, epoch)
	}
	return nil
}

// Release 正常停止时释放世界（文档 16.3）。
//
// 顺序要求：调用方必须先停掉所有事务 worker 再释放租约。提前释放会让
// 新 owner 立刻接管并递增 epoch，而旧进程的在途事务还没结束——
// 那些事务会被栅栏拒绝（安全），但会以「未知结果」的形式制造一堆
// 需要人工解析的记录。
//
// 释放后 status=NOT_READY 且 owner 清空：下一个进程看到的就是
// 「无人持有、不可服务」，必须先走 Acquire+RecoverWorld。
func (r *Repository) Release(ctx context.Context, worldID domain.WorldID, ownerID string, epoch uint64) error {
	if worldID == 0 {
		return fmt.Errorf("%w: world_id 不能为 0", domain.ErrInvalidArgument)
	}
	res, err := r.db.ExecContext(ctx,
		`UPDATE map_world SET owner_id = '', lease_until_ms = 0, status = ?
		 WHERE world_id = ? AND owner_id = ? AND epoch = ?`,
		WorldStatusNotReady, int64(worldID), ownerID, epoch)
	if err != nil {
		return fmt.Errorf("释放世界租约 world_id=%d: %w", worldID, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return fmt.Errorf("%w: world_id=%d owner=%q epoch=%d", ErrLeaseLost, worldID, ownerID, epoch)
	}
	r.logger.Info("世界租约已释放", "world_id", uint64(worldID), "owner", ownerID, "epoch", epoch)
	return nil
}

// SetWorldStatus 设置世界状态（恢复流程用：一致性加载完成后置 READY）。
//
// 单独暴露而不是塞进 RecoverWorld：RecoverWorld 承诺「只读、不改数据」
// （文档 14.4），状态推进必须由上层显式发起，否则恢复中途崩溃会让
// 世界莫名变成 READY。
func (r *Repository) SetWorldStatus(ctx context.Context, worldID domain.WorldID, ownerID string, epoch uint64, status int32) error {
	res, err := r.db.ExecContext(ctx,
		`UPDATE map_world SET status = ?
		 WHERE world_id = ? AND owner_id = ? AND epoch = ?`,
		status, int64(worldID), ownerID, epoch)
	if err != nil {
		return fmt.Errorf("设置世界状态 world_id=%d: %w", worldID, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return fmt.Errorf("%w: world_id=%d owner=%q epoch=%d", ErrLeaseLost, worldID, ownerID, epoch)
	}
	return nil
}
