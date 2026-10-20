package store

// battle.go 承载战斗记录与目标战斗队列的读取（写入在 mysql.go 的
// writeBattles 里，因为战斗结果必须与对象版本推进同事务）。
//
// 恢复语义（文档 11.3）：重启后 PENDING 战斗必须按 11.3 用同一输入
// 重新裁决；已经终态的战斗只读回结果，绝不重算。因此这里把两边都读全，
// 由上层按 status 分流——store 不做业务判断。

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"game_study/internal/worldmap/domain"
)

// battleResultJSON 是 domain.BattleResult 的落库形态。
//
// Detail 用 json.RawMessage 而不是 []byte：[]byte 会被编码成 base64
// 字符串，而 DetailJSON 的内容本身就是一段 JSON（Resolver 产出）。
// 存成嵌套对象后，SQL 侧可以直接用 JSON 函数做对账查询；
// base64 字符串则完全不可读。
type battleResultJSON struct {
	WinnerID    uint64          `json:"winner_id"`
	LoserID     uint64          `json:"loser_id"`
	AppliedAtMS int64           `json:"applied_at_ms"`
	AttackerWon bool            `json:"attacker_won"`
	Detail      json.RawMessage `json:"detail,omitempty"`
}

// resultToJSON 把领域结果转成落库形态。
//
// DetailJSON 非法 JSON 时降级成字符串而不是报错：战斗结果已经裁决完毕，
// 因为「明细格式不对」而拒绝落库会让整场战斗丢失，代价远大于收益。
// 降级后仍然可读，只是不能再用 JSON 函数查询。
func resultToJSON(res *domain.BattleResult) battleResultJSON {
	out := battleResultJSON{
		WinnerID:    uint64(res.WinnerID),
		LoserID:     uint64(res.LoserID),
		AppliedAtMS: res.AppliedAtMS,
		AttackerWon: res.AttackerWon,
	}
	if len(res.DetailJSON) > 0 {
		if json.Valid(res.DetailJSON) {
			out.Detail = json.RawMessage(res.DetailJSON)
		} else if quoted, err := json.Marshal(string(res.DetailJSON)); err == nil {
			out.Detail = json.RawMessage(quoted)
		}
	}
	return out
}

// resultFromJSON 还原领域结果。Detail 的两种形态（对象/字符串）都要处理。
func resultFromJSON(raw []byte) (*domain.BattleResult, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var p battleResultJSON
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, fmt.Errorf("解析战斗结果: %w", err)
	}
	out := &domain.BattleResult{
		WinnerID:    domain.ObjectID(p.WinnerID),
		LoserID:     domain.ObjectID(p.LoserID),
		AppliedAtMS: p.AppliedAtMS,
		AttackerWon: p.AttackerWon,
	}
	if len(p.Detail) > 0 {
		// 复合字面量：先试对象形态，失败再当字符串解。
		var asString string
		if err := json.Unmarshal(p.Detail, &asString); err == nil {
			out.DetailJSON = []byte(asString)
		} else {
			out.DetailJSON = append([]byte(nil), p.Detail...)
		}
	}
	return out, nil
}

const battleColumns = `battle_id, target_id, attacker_id, defender_id, input_hash,
	attacker_stats, defender_stats, rule_version, random_seed, status, deadline_ms,
	result, applied_operation_id`

// scanBattle 把一行扫描结果组装成 BattleRecord。
//
// 注意 input_hash 是 BINARY(32)，Scan 到 []byte 后必须校验长度再拷贝：
// 长度不对说明列被迁移改过，直接 return 错误比悄悄截断安全。
func scanBattle(sc interface{ Scan(...any) error }, worldID domain.WorldID) (*domain.BattleRecord, error) {
	var (
		rec                      domain.BattleRecord
		hash                     []byte
		attackerRaw, defenderRaw []byte
		status                   int32
		resultRaw                []byte
	)
	if err := sc.Scan(&rec.BattleID, &rec.TargetID, &rec.AttackerID, &rec.DefenderID, &hash,
		&attackerRaw, &defenderRaw, &rec.RuleVersion, &rec.RandomSeed, &status, &rec.DeadlineMS,
		&resultRaw, &rec.AppliedOperationID); err != nil {
		return nil, err
	}
	rec.WorldID = worldID
	if len(hash) != len(rec.InputHash) {
		return nil, fmt.Errorf("%w: 战斗 %s input_hash 长度=%d 期望=32",
			domain.ErrInvalidState, rec.BattleID, len(hash))
	}
	copy(rec.InputHash[:], hash)

	var attacker, defender combatJSON
	if err := json.Unmarshal(attackerRaw, &attacker); err != nil {
		return nil, fmt.Errorf("解析攻方属性 %s: %w", rec.BattleID, err)
	}
	if err := json.Unmarshal(defenderRaw, &defender); err != nil {
		return nil, fmt.Errorf("解析守方属性 %s: %w", rec.BattleID, err)
	}
	rec.AttackerStats = combatFromJSON(attacker)
	rec.DefenderStats = combatFromJSON(defender)
	rec.Status = domain.BattleStatus(status)

	result, err := resultFromJSON(resultRaw)
	if err != nil {
		return nil, fmt.Errorf("解析战斗结果 %s: %w", rec.BattleID, err)
	}
	rec.Result = result
	return &rec, nil
}

// LoadPendingBattles 加载仍需裁决的战斗，按 deadline 升序。
//
// 按 deadline 排序而不是 battle_id：恢复后补处理必须优先处理最早超时的
// 战斗，否则长时间停机后会用错误的时间顺序裁决，产生「先到的攻击输给
// 后到的攻击」这类反直觉结果（文档 16.1 的补处理顺序要求）。
func (r *Repository) LoadPendingBattles(ctx context.Context, worldID domain.WorldID) ([]*domain.BattleRecord, error) {
	return loadPendingBattles(ctx, r.db, worldID)
}

// loadPendingBattles 是 LoadPendingBattles 的事务内版本（恢复流程用）。
func loadPendingBattles(ctx context.Context, q queryer, worldID domain.WorldID) ([]*domain.BattleRecord, error) {
	rows, err := q.QueryContext(ctx,
		`SELECT `+battleColumns+` FROM map_battle
		 WHERE world_id = ? AND status = ? ORDER BY deadline_ms ASC, battle_id ASC`,
		int64(worldID), int32(domain.BattleStatusPending))
	if err != nil {
		return nil, fmt.Errorf("查询待裁决战斗 world=%d: %w", worldID, err)
	}
	defer func() { _ = rows.Close() }()

	var out []*domain.BattleRecord
	for rows.Next() {
		rec, err := scanBattle(rows, worldID)
		if err != nil {
			return nil, fmt.Errorf("扫描战斗行: %w", err)
		}
		if !rec.Status.Valid() {
			return nil, fmt.Errorf("%w: 战斗 %s 状态非法 %d", domain.ErrInvalidState, rec.BattleID, int32(rec.Status))
		}
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("遍历战斗: %w", err)
	}
	return out, nil
}

// LoadBattle 按 battle_id 加载单条战斗记录。
func (r *Repository) LoadBattle(ctx context.Context, worldID domain.WorldID, battleID string) (*domain.BattleRecord, error) {
	rec, err := scanBattle(r.db.QueryRowContext(ctx,
		`SELECT `+battleColumns+` FROM map_battle WHERE world_id = ? AND battle_id = ?`,
		int64(worldID), battleID), worldID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("%w: battle_id=%s", domain.ErrObjectNotFound, battleID)
	}
	if err != nil {
		return nil, fmt.Errorf("查询战斗 %s: %w", battleID, err)
	}
	return rec, nil
}

// LoadBattleQueue 加载目标战斗队列。
//
// 返回 map[目标对象][]排队部队，切片内按 enqueue_seq 升序（FIFO）。
//
// 为什么用 enqueue_seq 而不是入队时间：时间是应用侧时钟，多实例下
// 可能回拨；自增/单调序号是数据库给出的全序，不会被时钟影响
// （文档 11.1「战斗记录按 battle_id 排序没有语义」）。
func (r *Repository) LoadBattleQueue(ctx context.Context, worldID domain.WorldID) (map[domain.ObjectID][]domain.ObjectID, error) {
	return loadBattleQueue(ctx, r.db, worldID)
}

// loadBattleQueue 是 LoadBattleQueue 的事务内版本（恢复流程用）。
func loadBattleQueue(ctx context.Context, q queryer, worldID domain.WorldID) (map[domain.ObjectID][]domain.ObjectID, error) {
	rows, err := q.QueryContext(ctx,
		`SELECT target_id, troop_id FROM map_battle_queue
		 WHERE world_id = ? ORDER BY target_id ASC, enqueue_seq ASC`,
		int64(worldID))
	if err != nil {
		return nil, fmt.Errorf("查询战斗队列 world=%d: %w", worldID, err)
	}
	defer func() { _ = rows.Close() }()

	queues := make(map[domain.ObjectID][]domain.ObjectID)
	for rows.Next() {
		var targetID, troopID uint64
		if err := rows.Scan(&targetID, &troopID); err != nil {
			return nil, fmt.Errorf("扫描队列行: %w", err)
		}
		queues[domain.ObjectID(targetID)] = append(queues[domain.ObjectID(targetID)], domain.ObjectID(troopID))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("遍历战斗队列: %w", err)
	}
	return queues, nil
}
