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

type Worker struct {
	store *Store
	db    *sql.DB
	batch int
}

func NewWorker(store *Store, database *sql.DB, batch int) *Worker {
	if batch <= 0 || batch > flush.DefaultBatchRows {
		batch = flush.DefaultBatchRows
	}
	return &Worker{store: store, db: database, batch: batch}
}

// FlushBatch 先读取一个分片的多个玩家快照，合成 SQL 批次。失败保留 Redis 队列。
func (w *Worker) FlushBatch(ctx context.Context, partition int) (int, error) {
	ids, err := w.store.Pending(ctx, partition, w.batch)
	if err != nil {
		return 0, err
	}
	if len(ids) == 0 {
		return 0, nil
	}
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
		if !ok || old.Version < r.Version {
			byID[r.State.PlayerID] = r
		}
	}
	records = make([]Record, 0, len(byID))
	for _, r := range byID {
		records = append(records, r)
	}
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
	if _, err := tx.ExecContext(ctx, "INSERT IGNORE INTO player_writebehind (player_id, version) VALUES "+strings.Join(seeds, ","), args...); err != nil {
		return err
	}
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
