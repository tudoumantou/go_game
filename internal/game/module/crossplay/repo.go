// repo.go —— 三张表（cross_enroll / cross_player / cross_group）的访问层。
//
// 只做 SQL 与 JSON 编解码，不含业务判断：阶段校验、分数下限、归属合并
// 都在 registry.go 与 handler.go。db 为 nil 表示未配置 MySQL，一切操作
// 返回 ErrNoDB，由调用方决定降级还是失败——而不是悄悄当成成功。
package crossplay

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"game_study/pkg/db"
	"game_study/pkg/model"
	"game_study/pkg/sqlstore"
)

// 分批大小。
const (
	// playerBatch 玩家表批量写的单批行数。一服约 3300 个活动参与者，
	// 两批就写完；切太碎会让事务次数涨上去，切太大则单事务 undo 过大。
	playerBatch = 1000
	// groupBatch 组表批量写的单批行数。一组 100 人约 6KB JSON，
	// 200 组一批约 1.2MB，是落到 binlog 的合理粒度。
	groupBatch = 200
	// clearBatch 清表单批行数。用 DELETE ... LIMIT 而不是 TRUNCATE：
	// TRUNCATE 会锁表，挡住切期瞬间仍在跑的上报/查询。
	clearBatch = 5000
)

// Repo 跨服玩法三张表的仓储。
type Repo struct {
	*sqlstore.Base
}

// NewRepo 创建仓储。database 允许为 nil（未配 DB 的部署）。
func NewRepo(database *db.DB) *Repo {
	return &Repo{Base: sqlstore.New(database, "crossplay")}
}

// ── cross_enroll ────────────────────────────────────────────────────

// ListEnrolls 读出全部报名行（启动加载用）。
//
// 全表扫而不按玩家查：启动时本来就要把整张表装进内存，
// 分批查反而多几次往返。单服一期的报名量在万级，一次读完没有压力。
func (r *Repo) ListEnrolls(ctx context.Context) ([]model.CrossEnroll, error) {
	if !r.Ready() {
		return nil, sqlstore.ErrNoDB
	}
	rows, err := r.SQL().QueryContext(ctx,
		"SELECT player_id, server_id, power, info, enrolled_at FROM cross_enroll")
	if err != nil {
		return nil, fmt.Errorf("list cross_enroll: %w", err)
	}
	defer rows.Close()

	var out []model.CrossEnroll
	for rows.Next() {
		var m model.CrossEnroll
		if err := rows.Scan(&m.PlayerId, &m.ServerId, &m.Power, &m.Info, &m.EnrolledAt); err != nil {
			return nil, fmt.Errorf("scan cross_enroll: %w", err)
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// UpsertEnroll 写入一条报名（幂等）。
func (r *Repo) UpsertEnroll(ctx context.Context, m *model.CrossEnroll) error {
	if !r.Ready() {
		return sqlstore.ErrNoDB
	}
	_, err := model.UpsertCrossEnrollByPK(ctx, r.SQL(), m)
	if err != nil {
		return fmt.Errorf("upsert cross_enroll: %w", err)
	}
	return nil
}

// DeleteEnroll 删除一条报名（取消报名、切期清理）。
func (r *Repo) DeleteEnroll(ctx context.Context, playerID int64) error {
	if !r.Ready() {
		return sqlstore.ErrNoDB
	}
	if _, err := model.DeleteCrossEnrollByPK(ctx, r.SQL(), playerID); err != nil {
		return fmt.Errorf("delete cross_enroll: %w", err)
	}
	return nil
}

// ── cross_player ────────────────────────────────────────────────────

// ListPlayers 读出全部玩家行（启动加载用）。
func (r *Repo) ListPlayers(ctx context.Context) ([]model.CrossPlayer, error) {
	if !r.Ready() {
		return nil, sqlstore.ErrNoDB
	}
	rows, err := r.SQL().QueryContext(ctx,
		"SELECT player_id, group_id, server_id, power, score, info, updated_at FROM cross_player")
	if err != nil {
		return nil, fmt.Errorf("list cross_player: %w", err)
	}
	defer rows.Close()

	var out []model.CrossPlayer
	for rows.Next() {
		var m model.CrossPlayer
		if err := rows.Scan(&m.PlayerId, &m.GroupId, &m.ServerId, &m.Power, &m.Score, &m.Info, &m.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan cross_player: %w", err)
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// SavePlayers 批量写入玩家行（关服刷盘、定期落库）。
func (r *Repo) SavePlayers(ctx context.Context, rows []model.CrossPlayer) error {
	if len(rows) == 0 {
		return nil
	}
	if !r.Ready() {
		return sqlstore.ErrNoDB
	}
	for start := 0; start < len(rows); start += playerBatch {
		end := start + playerBatch
		if end > len(rows) {
			end = len(rows)
		}
		batch := rows[start:end]
		if err := r.Tx(ctx, func(tx *sql.Tx) error {
			return model.UpsertCrossPlayerBatch(ctx, tx, batch)
		}); err != nil {
			return fmt.Errorf("save cross_player [%d,%d): %w", start, end, err)
		}
	}
	return nil
}

// ── cross_group ─────────────────────────────────────────────────────

// ListGroups 读出全部组行（启动加载用）。
func (r *Repo) ListGroups(ctx context.Context) ([]model.CrossGroup, error) {
	if !r.Ready() {
		return nil, sqlstore.ErrNoDB
	}
	rows, err := r.SQL().QueryContext(ctx, "SELECT group_id, players, servers FROM cross_group")
	if err != nil {
		return nil, fmt.Errorf("list cross_group: %w", err)
	}
	defer rows.Close()

	var out []model.CrossGroup
	for rows.Next() {
		var m model.CrossGroup
		if err := rows.Scan(&m.GroupId, &m.Players, &m.Servers); err != nil {
			return nil, fmt.Errorf("scan cross_group: %w", err)
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// SaveGroups 批量写入组行（整列覆盖）。
//
// 游戏服侧的 cross_group 是**恢复副本**不是权威：权威在中心服，这里写它只是
// 为了让「活动期重启」能立刻显示排行，晚几分钟没有任何业务影响。
func (r *Repo) SaveGroups(ctx context.Context, rows []model.CrossGroup) error {
	if len(rows) == 0 {
		return nil
	}
	if !r.Ready() {
		return sqlstore.ErrNoDB
	}
	for start := 0; start < len(rows); start += groupBatch {
		end := start + groupBatch
		if end > len(rows) {
			end = len(rows)
		}
		batch := rows[start:end]
		if err := r.Tx(ctx, func(tx *sql.Tx) error {
			return model.UpsertCrossGroupBatch(ctx, tx, batch)
		}); err != nil {
			return fmt.Errorf("save cross_group [%d,%d): %w", start, end, err)
		}
	}
	return nil
}

// ── 切期清理 ────────────────────────────────────────────────────────

// ClearAll 清空三张表（「报名期开始清上一期」的落地动作）。
//
// 顺序：先删玩家与分组，最后删报名。反过来会出现「报名表已空、玩家表还在」
// 的中间态，那时候如果进程崩了，重启加载出来的是一份残缺状态。
//
// 用分批 DELETE 而不是 TRUNCATE：TRUNCATE 会拿表级锁，挡住切期瞬间仍在
// 跑的上报与查询；而且 TRUNCATE 不可回滚，中途失败会得到「删了一半」。
func (r *Repo) ClearAll(ctx context.Context) (int64, error) {
	if !r.Ready() {
		return 0, sqlstore.ErrNoDB
	}
	var total int64
	for _, table := range []string{"cross_player", "cross_group", "cross_enroll"} {
		n, err := r.clearTable(ctx, table)
		total += n
		if err != nil {
			return total, fmt.Errorf("clear %s: %w", table, err)
		}
	}
	return total, nil
}

// clearTable 分批删除一张表的全部行。
func (r *Repo) clearTable(ctx context.Context, table string) (int64, error) {
	var total int64
	for {
		res, err := r.SQL().ExecContext(ctx, "DELETE FROM "+table+" LIMIT ?", clearBatch)
		if err != nil {
			return total, err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return total, err
		}
		total += n
		if n < clearBatch {
			return total, nil
		}
		// 每批之间让出一次：连续 DELETE 会把 undo log 顶到上限，
		// 也会让同一时刻的读请求一直等锁。
		if err := ctx.Err(); err != nil {
			return total, err
		}
	}
}

// ── JSON 编解码 ─────────────────────────────────────────────────────

// playerJSON cross_group.players 列的元素。
//
// 字段名 snake_case 与中心服侧同一份契约：这一列会从中 心服下发时被整段
// 解析，两边名字不一致就会静默读出零值（JSON 解不出来不报错）。
type playerJSON struct {
	PlayerID  int64 `json:"player_id"`
	Power     int64 `json:"power"`
	Score     int64 `json:"score"`
	UpdatedAt int64 `json:"updated_at"`
}

// serverJSON cross_group.servers 列的元素。
//
// 存它是因为表结构定了要存（运营排查时想直接看「这一组里有哪些服」），
// 但游戏服**不读它**——本服要的「谁归哪个服管」是从下发报文的成员里
// 直接得到的，不需要反向索引。
type serverJSON struct {
	ServerID int32   `json:"server_id"`
	Players  []int64 `json:"players"`
}

// membersToJSON 组内成员 → players 列，并顺带生成 servers 列。
//
// 两列在同一次调用里生成：servers 是 players 的派生视图，分开算必然有
// 一天会对不上（加了人忘了更新索引）。
func membersToJSON(members []Entry) (playersCol, serversCol sql.NullString, err error) {
	if len(members) == 0 {
		return sql.NullString{}, sql.NullString{}, nil
	}
	pj := make([]playerJSON, 0, len(members))
	byServer := make(map[int32][]int64, 4)
	order := make([]int32, 0, 4)
	for _, m := range members {
		pj = append(pj, playerJSON{
			PlayerID:  m.PlayerID,
			Power:     m.Power,
			Score:     m.Score,
			UpdatedAt: m.UpdatedAt,
		})
		if _, seen := byServer[m.ServerID]; !seen {
			order = append(order, m.ServerID)
		}
		byServer[m.ServerID] = append(byServer[m.ServerID], m.PlayerID)
	}
	sj := make([]serverJSON, 0, len(order))
	for _, sid := range order {
		sj = append(sj, serverJSON{ServerID: sid, Players: byServer[sid]})
	}

	pb, err := json.Marshal(pj)
	if err != nil {
		return sql.NullString{}, sql.NullString{}, err
	}
	sb, err := json.Marshal(sj)
	if err != nil {
		return sql.NullString{}, sql.NullString{}, err
	}
	return sql.NullString{String: string(pb), Valid: true},
		sql.NullString{String: string(sb), Valid: true}, nil
}

// jsonToMembers 组表的两个 JSON 列 → 成员列表。
//
// **两列都要解**：players 列按用户的表设计只有 (player_id, power, score,
// updated_at)，没有 server_id。而「这个玩家归哪个服管」是跨服改分的前提
// （报文要发给他的服），所以归属必须从 servers 列恢复。
//
// 这也解释了为什么两列必须在同一次写入里生成（见 membersToJSON）：
// 分开算必然有一天会对不上，表现为「攻击发出去了，但发错了服」。
func jsonToMembers(playersCol, serversCol sql.NullString) ([]*Entry, error) {
	if !playersCol.Valid || playersCol.String == "" {
		return nil, nil
	}
	var pj []playerJSON
	if err := json.Unmarshal([]byte(playersCol.String), &pj); err != nil {
		return nil, err
	}

	// 反向索引：player_id → server_id
	serverOf := make(map[int64]int32, len(pj))
	if serversCol.Valid && serversCol.String != "" {
		var sj []serverJSON
		if err := json.Unmarshal([]byte(serversCol.String), &sj); err != nil {
			// servers 列坏了不该让整组读不出来：分数与排行还能用，
			// 只是这些玩家暂时不能被跨服攻击（ServerID=0 会在发送时被拒）。
			serverOf = nil
		} else {
			for _, sv := range sj {
				for _, pid := range sv.Players {
					serverOf[pid] = sv.ServerID
				}
			}
		}
	}

	out := make([]*Entry, 0, len(pj))
	for _, p := range pj {
		out = append(out, &Entry{
			PlayerID:  p.PlayerID,
			ServerID:  serverOf[p.PlayerID],
			Power:     p.Power,
			Score:     p.Score,
			UpdatedAt: p.UpdatedAt,
		})
	}
	return out, nil
}

// infoToJSON []KV → 表的 info 列。
func infoToJSON(info []KV) (sql.NullString, error) {
	if len(info) == 0 {
		return sql.NullString{}, nil
	}
	b, err := json.Marshal(info)
	if err != nil {
		return sql.NullString{}, err
	}
	return sql.NullString{String: string(b), Valid: true}, nil
}

// jsonToInfo 表的 info 列 → []KV。
func jsonToInfo(col sql.NullString) []KV {
	if !col.Valid || col.String == "" {
		return nil
	}
	var out []KV
	if err := json.Unmarshal([]byte(col.String), &out); err != nil {
		return nil // 坏数据按「没有展示信息」处理，不阻断加载
	}
	return out
}

// enrollRow 领域条目 → cross_enroll 表行。
func enrollRow(e *Entry, enrolledAt int64) *model.CrossEnroll {
	info, _ := infoToJSON(e.Info) // 序列化失败就留空：报名本身不该被展示信息拦住
	return &model.CrossEnroll{
		PlayerId:   e.PlayerID,
		ServerId:   e.ServerID,
		Power:      e.Power,
		Info:       info,
		EnrolledAt: enrolledAt,
	}
}
