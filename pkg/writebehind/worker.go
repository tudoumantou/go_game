// pkg/writebehind/worker.go —— 快照落库 worker（Redis 队列 → MySQL 合批事务）
//
// 职责：定期从脏队列取玩家 → Load 最新快照 → 同一事务内合批写
// MySQL → Ack。可随游戏服进程跑（每 tick 顺带），也可独立进程跑
// （Run 方法即为此设计）。
//
// 可靠性设计：
//   - 至少一次：任何一步失败都不 Ack，玩家留在队列，下轮重试；
//   - 数据库版本栅栏兜底重投：多 worker/重试导致的重复落库，由
//     player_writebehind.version 的 SELECT ... FOR UPDATE 比较挡住，
//     旧版本记录直接跳过（幂等）；
//   - 单批单事务边界：一批玩家一个事务，整批提交或整批回滚，
//     模块间不会出现「A 模块落了 B 模块没落」的半状态。
package writebehind

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"game_study/pkg/flush"
)

// Worker 落库 worker。
//
// 字段说明：
//   - store：Redis 存取（队列消费 + 快照读取 + Ack）；
//   - db：MySQL 连接池（合批事务的承载）；
//   - batch：单批玩家数上限（同时限制 Redis 读取量与事务行数）。
//
// 无状态、可多实例部署：并发安全由数据库版本栅栏与 Redis 原子
// 脚本保证，不需要选主。
type Worker struct {
	store *Store
	db    *sql.DB
	batch int
}

// NewWorker 创建 worker。
//
// batch 被 clamp 到 (0, flush.DefaultBatchRows]：单批超过 500 行会让
// 事务过长、锁持有时间上升（见 batch.go 的合批预算）；
// 非法配置静默纠正而非报错——batch 只是吞吐旋钮，不影响正确性。
func NewWorker(store *Store, database *sql.DB, batch int) *Worker {
	if batch <= 0 || batch > flush.DefaultBatchRows {
		batch = flush.DefaultBatchRows
	}
	return &Worker{store: store, db: database, batch: batch}
}

// FlushBatch 先读取一个分区的多个玩家快照，合成 SQL 批次。失败保留 Redis 队列。
//
// 流程：Pending 取本批玩家 → 逐个 Load（任一失败整体放弃——
// 快照读不出来说明缓存/协议有恙，不该带着半批数据硬写）→
// Persist 同事务落库 → 全部 Ack。
// 返回处理的玩家数（0 = 队列空，调用方据此终止排空循环）。
//
// 顺序保证：先 Persist 后 Ack——Ack 只表示「已安全落库」，
// 提前 Ack 会在 MySQL 写入失败时丢数据。
func (w *Worker) FlushBatch(ctx context.Context, partition int) (int, error) {
	ids, err := w.store.Pending(ctx, partition, w.batch)
	if err != nil {
		return 0, err
	}
	if len(ids) == 0 {
		return 0, nil
	}
	// 按 ids 上界预分配，N 次 Load 的 append 零扩容
	records := make([]Record, 0, len(ids))
	for _, id := range ids {
		record, err := w.store.Load(ctx, id)
		if err != nil {
			return 0, err
		}
		records = append(records, record)
	}
	if err := w.Persist(ctx, records); err != nil {
		return 0, err
	}
	for _, record := range records {
		if err := w.store.Ack(ctx, record.State.PlayerID, record.Version); err != nil {
			return 0, err
		}
	}
	return len(records), nil
}

// FlushAll 循环排空所有分区的脏任务，直到没有待处理记录或 ctx 截止。
//
// 关闭时必须调用：缓存权威模式下玩家脏数据只在 Redis 队列里，进程内 worker
// 若只等下一次 ticker，退出时这批数据既没落库、也不在 flush.Flusher 的视野内
// （那条路径处理的是非缓存玩家），等于直接丢失。
//
// 错误语义：单个分区失败不阻断其它分区（能救的先救），所有错误用
// errors.Join 汇总返回（Go 1.20+ 的多错误合并，保留每条分区的独立
// 上下文，比只返回第一个错误更利于停机诊断）。
func (w *Worker) FlushAll(ctx context.Context) error {
	var errs []error
	for part := 0; part < Partitions; part++ {
		for {
			if err := ctx.Err(); err != nil {
				return errors.Join(append(errs, err)...)
			}
			n, err := w.FlushBatch(ctx, part)
			if err != nil {
				// 单个分区失败不挡住其它分区：能救的先救，错误一起上报。
				errs = append(errs, fmt.Errorf("partition %d: %w", part, err))
				break
			}
			if n == 0 {
				break
			}
		}
	}
	return errors.Join(errs...)
}

// Run 可由独立进程运行；首轮也等待定时器，不因一条签到触发同步 MySQL。
//
// 每 tick 顺序处理 10 个分区；每分区独立 30s 子超时——一个分区卡死
// （大事务/锁等待）不拖垮其余分区的落库节奏。
//
// Go 细节：
//   - ticker 必须 Stop：与 timer 同理，不 Stop 的 ticker 相关资源要等
//     GC/下次触发才释放，defer ticker.Stop() 是惯用保险；
//   - context.WithTimeout 在**循环体内**创建并立即 cancel()：若用
//     defer cancel 会把 cancel 推迟到 Run 返回，十次循环累积十个
//     未释放 context（经典 context 泄漏）；
//   - onError 回调而非内部日志：让调用方决定错误的去向（进程可
//     顺便做熔断/告警），框架不绑定日志实现。
func (w *Worker) Run(ctx context.Context, interval time.Duration, onError func(error)) error {
	if interval <= 0 {
		interval = time.Minute
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			for part := 0; part < Partitions; part++ {
				// 每分区独立超时；cancel 必须本轮结束即调用，不能 defer 到函数尾
				runCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
				_, err := w.FlushBatch(runCtx, part)
				cancel()
				if err != nil && onError != nil {
					onError(fmt.Errorf("partition %d: %w", part, err))
				}
			}
		}
	}
}

// Persist 在同一个 SQL 事务中保存多个玩家。版本行是数据库最终栅栏，
// 允许多个落库进程安全重投，也防止读取较早快照的 worker 倒写。
//
// 流程：
//  1. 本地 Validate + 版本校验 + 按玩家去重（同玩家取最高版本——
//     重投/多 Load 场景可能带同一玩家的多个版本，只写最新的）；
//  2. 按 player_id 排序：多进程并发写不同玩家子集时，行锁获取顺序
//     一致（SELECT ... FOR UPDATE 按同序加锁），规避死锁；
//  3. INSERT IGNORE 播种版本行（并发 worker 先到先建，不冲突）；
//  4. SELECT ... FOR UPDATE 读出版本栅栏：只接受 r.Version > 库版本
//     的记录，旧版本快照（过期 worker 的残留）直接跳过——这是
//     「至少一次投递」在数据库侧的幂等闸门；
//  5. persistSnapshots 写各表；成功 Commit。
//
// 失败不 Ack：提交失败时玩家仍在 Redis 脏队列，下轮重试；已成功的
// 提交即使 Ack 失败也安全（版本栅栏会挡掉重复写）。
//
// Go 细节：
//   - defer func(){ _ = tx.Rollback() }() 是 database/sql 的惯用保险：
//     Commit 成功后 Rollback 返回 ErrTxDone（无副作用），而任何
//     panic/提前 return 的路径都能回滚并释放连接——没有它，提前
//     return 会把连接「漏」在池里直到超时（对比 db.Tx 的无 defer 写法）；
//   - byID map 去重后重建 records：先按玩家聚合再排序，比排序后
//     去重简单且顺序确定。
func (w *Worker) Persist(ctx context.Context, records []Record) error {
	if len(records) == 0 {
		return nil
	}
	byID := make(map[int64]Record)
	for _, r := range records {
		if err := r.State.Validate(); err != nil {
			return err
		}
		if r.Version <= 0 {
			return fmt.Errorf("invalid dirty version")
		}
		old, ok := byID[r.State.PlayerID]
		// 同玩家多条记录取版本最高者（过期 Load 的残留）
		if !ok || old.Version < r.Version {
			byID[r.State.PlayerID] = r
		}
	}
	records = make([]Record, 0, len(byID))
	for _, r := range byID {
		records = append(records, r)
	}
	// 排序：与下面的 FOR UPDATE 加锁顺序一致，多进程并发时防死锁
	sort.Slice(records, func(i, j int) bool { return records[i].State.PlayerID < records[j].State.PlayerID })
	if len(records) > flush.DefaultBatchRows {
		return fmt.Errorf("snapshot batch exceeds limit")
	}
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	marks := make([]string, len(records))
	args := make([]any, len(records))
	seeds := make([]string, len(records))
	for i, r := range records {
		marks[i] = "?"
		args[i] = r.State.PlayerID
		seeds[i] = "(?, 0)"
	}
	// 播种版本行：并发 worker 先到先建，IGNORE 避免重复插入报错
	if _, err := tx.ExecContext(ctx, "INSERT IGNORE INTO player_writebehind (player_id, version) VALUES "+strings.Join(seeds, ","), args...); err != nil {
		return err
	}
	// FOR UPDATE：对选中行加排他锁直到事务提交——版本比较与后续写入
	// 之间不允许别的 worker 插足；ORDER BY 与排序一致 = 固定加锁顺序
	rows, err := tx.QueryContext(ctx, "SELECT player_id, version FROM player_writebehind WHERE player_id IN ("+strings.Join(marks, ",")+") ORDER BY player_id FOR UPDATE", args...)
	if err != nil {
		return err
	}
	versions := make(map[int64]int64)
	for rows.Next() {
		var id, version int64
		if err := rows.Scan(&id, &version); err != nil {
			rows.Close()
			return err
		}
		versions[id] = version
	}
	err = rows.Err()
	// Rows 必须 Close：底层连接要归还池，否则每批泄漏一个连接直到池耗尽
	rows.Close()
	if err != nil {
		return err
	}
	accepted := make([]Record, 0, len(records))
	for _, r := range records {
		version, exists := versions[r.State.PlayerID]
		if !exists {
			return fmt.Errorf("missing version fence")
		}
		// 版本栅栏：只写比数据库更新的快照；旧版本是过期 worker 的残留，跳过
		if r.Version > version {
			accepted = append(accepted, r)
		}
	}
	if len(accepted) > 0 {
		if err := persistSnapshots(ctx, tx, accepted); err != nil {
			return err
		}
	}
	return tx.Commit() // 出错不 Ack；下次按数据库版本判定是否已经提交。
}

// persistSnapshots 把一批快照写进各表（在调用方事务内）。
//
// 写序与内容：
//  1. player：CASE WHEN 批量 UPDATE level/exp/coin/scene_id（保留
//     账号归属、名字等 Tier-1 元信息不动）；
//  2. player COUNT 校验：元信息必须存在——已删角色的残留快照不允许
//     落成孤立背包/签到数据，宁可整批回滚；
//  3. player_attr：合批 UPSERT；
//  4. player_item：差异删除（NOT IN 保留集合）+ 合批 UPSERT——
//     不清表重写，保留道具 UID、过期时间、创建时间；
//  5. Ext 模块：按名字排序后交各自 Persister（顺序确定 = 复现/断言友好；
//     未注册模块直接报错，不静默丢数据）；
//  6. player_writebehind：版本 + applied_grants 栅栏行收尾。
//
// Go 细节：
//   - CASE id WHEN ? THEN ? ... END 把 N 行 UPDATE 合成 1 条语句，
//     参数按「列 → 玩家」嵌套顺序追加，与 SQL 文本中占位符顺序严格对应；
//   - itemIDs 排序：map 遍历随机，排序保证 items 参数序列确定，
//     同输入永远生成同 SQL（便于对账与测试断言）；
//   - strings.Builder 每列一个：builder 不可复用拷贝，按列构建
//     SET 片段后 Join，比整体一个大 builder 更清晰。
func persistSnapshots(ctx context.Context, tx *sql.Tx, records []Record) error {
	// 玩家基础信息合批 UPDATE，保留账号归属、名字等 Tier-1 元信息。
	columns := []string{"level", "exp", "coin", "scene_id"}
	sets := make([]string, 0, len(columns))
	args := make([]any, 0, len(records)*9)
	for _, column := range columns {
		var b strings.Builder
		b.WriteString(column + " = CASE id ")
		for _, r := range records {
			b.WriteString("WHEN ? THEN ? ")
			args = append(args, r.State.PlayerID)
			p := r.State.Profile
			switch column {
			case "level":
				args = append(args, p.Level)
			case "exp":
				args = append(args, p.Exp)
			case "coin":
				args = append(args, p.Coin)
			case "scene_id":
				args = append(args, p.SceneID)
			}
		}
		b.WriteString("ELSE " + column + " END")
		sets = append(sets, b.String())
	}
	ids := make([]any, 0, len(records))
	marks := make([]string, 0, len(records))
	for _, r := range records {
		ids = append(ids, r.State.PlayerID)
		marks = append(marks, "?")
	}
	args = append(args, ids...)
	result, err := tx.ExecContext(ctx, "UPDATE player SET "+strings.Join(sets, ", ")+" WHERE id IN ("+strings.Join(marks, ",")+")", args...)
	if err != nil {
		return err
	}
	_ = result
	// 元信息必须存在，避免被删除角色产生孤立背包与签到数据。
	var existing int
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM player WHERE id IN ("+strings.Join(marks, ",")+")", ids...).Scan(&existing); err != nil {
		return err
	}
	if existing != len(records) {
		return fmt.Errorf("player metadata missing")
	}
	attrs := make([][]any, 0, len(records))
	items := make([][]any, 0)
	fences := make([][]any, 0, len(records))
	// 模块私有状态按 Ext 键分组，交给各自的 Persister 合批写入。
	// 框架不再认识任何具体玩法的表名，新增玩法只需实现并注册 Persister。
	extGroups := make(map[string][]ModuleRow)
	for _, r := range records {
		s := r.State
		a := s.Attr
		attrs = append(attrs, []any{s.PlayerID, a.Career, a.Hp, a.Mp, a.Atk, a.Def, a.Spd})
		itemIDs := make([]int, 0, len(s.Items))
		for id := range s.Items {
			itemIDs = append(itemIDs, int(id))
		}
		// map 遍历顺序随机 → 排序保证 items 行序确定（同输入同 SQL）
		sort.Ints(itemIDs)
		for _, id := range itemIDs {
			if n := s.Items[int32(id)]; n > 0 {
				items = append(items, []any{s.PlayerID, int32(id), n})
			}
		}
		for name, raw := range s.Ext {
			extGroups[name] = append(extGroups[name], ModuleRow{PlayerID: s.PlayerID, Data: raw})
		}
		receipts, err := json.Marshal(s.AppliedGrants)
		if err != nil {
			return err
		}
		fences = append(fences, []any{s.PlayerID, r.Version, string(receipts)})
	}
	if err := upsert(ctx, tx, "player_attr", []string{"player_id", "career", "hp", "mp", "atk", "def", "spd"}, attrs); err != nil {
		return err
	}
	// 只删除快照中已不存在的项，保留现有道具 UID、过期时间和创建时间。
	deleteSQL := "DELETE FROM player_item WHERE player_id IN (" + strings.Join(marks, ",") + ")"
	deleteArgs := append([]any(nil), ids...)
	if len(items) > 0 {
		pairs := make([]string, 0, len(items))
		for _, item := range items {
			pairs = append(pairs, "(?,?)")
			deleteArgs = append(deleteArgs, item[0], item[1])
		}
		deleteSQL += " AND (player_id,item_id) NOT IN (" + strings.Join(pairs, ",") + ")"
	}
	if _, err := tx.ExecContext(ctx, deleteSQL, deleteArgs...); err != nil {
		return err
	}
	if err := upsert(ctx, tx, "player_item", []string{"player_id", "item_id", "count"}, items); err != nil {
		return err
	}
	// 模块扩展落库：顺序按名字固定，便于复现与断言。
	// 模块未注册 Persister 时立即报错 —— 宁可整批回滚，也不静默丢数据。
	names := make([]string, 0, len(extGroups))
	for name := range extGroups {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		p, ok := LookupPersister(name)
		if !ok {
			return fmt.Errorf("no persister registered for module %q", name)
		}
		if err := p.PersistBatch(ctx, tx, extGroups[name]); err != nil {
			return fmt.Errorf("module %q: %w", name, err)
		}
	}
	return upsert(ctx, tx, "player_writebehind", []string{"player_id", "version", "applied_grants"}, fences)
}

// upsert 合批 UPSERT 的薄封装（复用 flush 的切批与拼接）。
//
// 行数超 DefaultBatchRows 时自动切多条语句；单模块单表调用，
// table/cols 均为代码常量（白名单语义），值全部走占位符。
func upsert(ctx context.Context, tx *sql.Tx, table string, cols []string, rows [][]any) error {
	for _, batch := range flush.Chunk(rows, flush.DefaultBatchRows, flush.DefaultBatchBytes) {
		query, args, err := flush.BuildBatchUpsert(table, cols, batch)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, query, args...); err != nil {
			return err
		}
	}
	return nil
}
