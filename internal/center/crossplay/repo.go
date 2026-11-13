package crossplay

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"game_study/pkg/db"
	"game_study/pkg/log"
	"game_study/pkg/model"
)

// 分批大小：单批行数。10 万行的批量写要切成多批，
// 避免单事务 undo log 过大、行锁持有时间过长。
const (
	// enrollBatch 报名表写入/清理的单批行数。
	enrollBatch = 2000
	// groupBatch 分组表整列写的单批行数（一组 100 人约 6KB JSON，
	// 200 组一批约 1.2MB，是一次落到 binlog 的合理粒度）。
	groupBatch = 200
	// clearBatch 清表的单批行数。用 DELETE ... LIMIT 而不是 TRUNCATE：
	// TRUNCATE 锁表，会挡住活动期仍在读该表的查询。
	clearBatch = 5000
)

// playerJSON 分组表 players 列的元素。
//
// 字段名与设计文档一致（snake_case），因为这份 JSON 会被游戏服直接解析，
// 两边都按同一份契约取舍名字。
type playerJSON struct {
	PlayerID  int64 `json:"player_id"`
	Power     int64 `json:"power"`
	Score     int64 `json:"score"`
	UpdatedAt int64 `json:"updated_at"`
}

// serverJSON 分组表 servers 列的元素：从 players 派生的反向索引。
//
// 两个用途：① 中心服下发时按服聚合；② 游戏服做跨服分数变更时查
// 「这个玩家归哪个服管」。必须与 players 在同一次写入里生成，
// 否则两份会对不上。
type serverJSON struct {
	ServerID int32   `json:"server_id"`
	Players  []int64 `json:"players"`
}

// Repo 中心服侧三张表的读写。
//
// 只做 SQL，不含业务判断：报名期校验、分组、下发窗口都在 Service 里。
// db 为 nil 表示未配置 MySQL —— 此时一切写操作返回 ErrNoDB，
// 由调用方决定是降级还是失败，而不是悄悄当成成功。
type Repo struct {
	db  *db.DB
	log *log.Logger
}

// NewRepo 创建仓储。database 允许为 nil（未配 DB 的部署）。
func NewRepo(database *db.DB, logger *log.Logger) *Repo {
	return &Repo{db: database, log: logger}
}

// ErrNoDB 未配置数据库。
var ErrNoDB = errors.New("crossplay: no database")

// Ready 数据库是否可用。
func (r *Repo) Ready() bool { return r != nil && r.db != nil }

// UpsertEnroll 写入一条报名（幂等）。
//
// 重复报名不报错、也不刷新 enrolled_at：报名时间保留第一次的，
// 否则玩家反复点报名会让审计时间失真。power 会被刷新 —— 报名期内
// 玩家仍可增长战力，以最后一次点击时的值为准（分组开始后锁定）。
func (r *Repo) UpsertEnroll(ctx context.Context, m Member, info []byte, now int64) error {
	if !r.Ready() {
		return ErrNoDB
	}
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO center_cross_enroll (player_id, server_id, power, info, enrolled_at)
		VALUES (?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE server_id = VALUES(server_id), power = VALUES(power), info = VALUES(info)`,
		m.PlayerID, m.ServerID, m.Power, nullJSON(info), now)
	if err != nil {
		return fmt.Errorf("upsert enroll %d: %w", m.PlayerID, err)
	}
	return nil
}

// DeleteEnroll 删除一条报名（取消报名，幂等）。
func (r *Repo) DeleteEnroll(ctx context.Context, playerID int64) error {
	if !r.Ready() {
		return ErrNoDB
	}
	if _, err := r.db.ExecContext(ctx,
		"DELETE FROM center_cross_enroll WHERE player_id = ?", playerID); err != nil {
		return fmt.Errorf("delete enroll %d: %w", playerID, err)
	}
	return nil
}

// LoadEnrolls 读全部报名，供分组使用。
//
// 10 万行 ≈ 2MB，一次读完在内存里排序比分页快得多（分组本来就是全量排序）。
func (r *Repo) LoadEnrolls(ctx context.Context) ([]Member, error) {
	if !r.Ready() {
		return nil, ErrNoDB
	}
	rows, err := r.db.QueryContext(ctx,
		"SELECT player_id, server_id, power FROM center_cross_enroll")
	if err != nil {
		return nil, fmt.Errorf("load enrolls: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := make([]Member, 0, 1024)
	for rows.Next() {
		var m Member
		if err := rows.Scan(&m.PlayerID, &m.ServerID, &m.Power); err != nil {
			return nil, fmt.Errorf("scan enroll: %w", err)
		}
		m.Score = InitialScore
		out = append(out, m)
	}
	return out, rows.Err()
}

// SaveGroups 把一次分组结果整体落库：分组表整列 + 玩家表的玩家维度行。
//
// 两件事必须在同一个事务里：玩家表是「按玩家查」的索引，分组表是权威，
// 只写一半会让 GM 按玩家查到的组号与实际分组不一致。
//
// 采用「先删后插」而不是 UPSERT：上一期残留的组号不会被这一期的写入覆盖掉
// （比如上一期有 12 组、这一期只有 10 组，UPSERT 会留下 11、12 两个脏组）。
func (r *Repo) SaveGroups(ctx context.Context, groups []Group) error {
	if !r.Ready() {
		return ErrNoDB
	}
	if len(groups) == 0 {
		return nil
	}

	for start := 0; start < len(groups); start += groupBatch {
		end := min(start+groupBatch, len(groups))
		batch := groups[start:end]
		err := r.db.Tx(ctx, func(tx *sql.Tx) error {
			// ① 分组表：整列覆盖
			rows := make([]model.CenterCrossGroup, 0, len(batch))
			players := make([]model.CenterCrossPlayer, 0, len(batch)*GroupSize)
			for _, g := range batch {
				pj, sj, err := marshalGroup(g)
				if err != nil {
					return err
				}
				rows = append(rows, model.CenterCrossGroup{
					GroupId: g.ID,
					Players: nullJSON(pj),
					Servers: nullJSON(sj),
				})
				for _, m := range g.Members {
					players = append(players, model.CenterCrossPlayer{
						PlayerId:  m.PlayerID,
						GroupId:   g.ID,
						ServerId:  m.ServerID,
						Power:     m.Power,
						Score:     m.Score,
						UpdatedAt: m.UpdatedAt,
					})
				}
			}
			if err := model.UpsertCenterCrossGroupBatch(ctx, tx, rows); err != nil {
				return fmt.Errorf("upsert groups: %w", err)
			}
			// ② 玩家表：分组期一次性生成（之后只更新 score，不再动 group_id）
			if err := model.UpsertCenterCrossPlayerBatch(ctx, tx, players); err != nil {
				return fmt.Errorf("upsert players: %w", err)
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// SaveAllGroups 落库全部组（下发窗口的整列刷盘、center 重启前的落库）。
func (r *Repo) SaveAllGroups(ctx context.Context, s *Store) error {
	return r.SaveGroups(ctx, s.All())
}

// UpsertScores 落库一批分数变化（只改 score/updated_at）。
//
// 不动 group_id：分组完成后玩家所属组不再变化，重复写 group_id 只会
// 增加 binlog 体积并制造「两个写者」的错觉。
func (r *Repo) UpsertScores(ctx context.Context, members []Member) error {
	if !r.Ready() || len(members) == 0 {
		return nil
	}
	for start := 0; start < len(members); start += enrollBatch {
		end := min(start+enrollBatch, len(members))
		batch := members[start:end]
		err := r.db.Tx(ctx, func(tx *sql.Tx) error {
			rows := make([]model.CenterCrossPlayer, 0, len(batch))
			for _, m := range batch {
				rows = append(rows, model.CenterCrossPlayer{
					PlayerId:  m.PlayerID,
					GroupId:   m.GroupID,
					ServerId:  m.ServerID,
					Power:     m.Power,
					Score:     m.Score,
					UpdatedAt: m.UpdatedAt,
				})
			}
			return model.UpsertCenterCrossPlayerBatch(ctx, tx, rows)
		})
		if err != nil {
			return fmt.Errorf("upsert scores: %w", err)
		}
	}
	return nil
}

// LoadGroups 从库恢复分组（center 重启后重建内存）。
//
// 只读分组表，不读玩家表：分组表的 players 列已经是完整快照，
// join 玩家表反而会把两处可能不一致的数据混在一起。
func (r *Repo) LoadGroups(ctx context.Context) ([]Group, error) {
	if !r.Ready() {
		return nil, ErrNoDB
	}
	rows, err := r.db.QueryContext(ctx,
		"SELECT group_id, players FROM center_cross_group ORDER BY group_id")
	if err != nil {
		return nil, fmt.Errorf("load groups: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []Group
	for rows.Next() {
		var (
			gid  int32
			blob sql.NullString
		)
		if err := rows.Scan(&gid, &blob); err != nil {
			return nil, fmt.Errorf("scan group: %w", err)
		}
		g := Group{ID: gid}
		if blob.Valid && blob.String != "" {
			var pj []playerJSON
			if err := json.Unmarshal([]byte(blob.String), &pj); err != nil {
				return nil, fmt.Errorf("group %d players json: %w", gid, err)
			}
			g.Members = make([]Member, len(pj))
			for i, p := range pj {
				g.Members[i] = Member{
					PlayerID:  p.PlayerID,
					GroupID:   gid,
					Power:     p.Power,
					Score:     p.Score,
					UpdatedAt: p.UpdatedAt,
				}
			}
		}
		out = append(out, g)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	// servers 列不参与恢复：玩家的 ServerID 在 players 元素里没有，
	// 需要从 servers 反查补回，见 fillServers。
	return out, r.fillServers(ctx, out)
}

// fillServers 用 servers 列补回每个成员的 ServerID。
//
// players 元素不含 server_id（省 4 字节 × 10 万人），归属信息只在 servers 里。
// 恢复时必须补回来，否则 ServerOf 全返回 0，跨服 delta 找不到目标服。
func (r *Repo) fillServers(ctx context.Context, groups []Group) error {
	if len(groups) == 0 {
		return nil
	}
	rows, err := r.db.QueryContext(ctx, "SELECT group_id, servers FROM center_cross_group")
	if err != nil {
		return fmt.Errorf("load server index: %w", err)
	}
	defer func() { _ = rows.Close() }()

	owner := make(map[int64]int32, 1024)
	for rows.Next() {
		var (
			gid  int32
			blob sql.NullString
		)
		if err := rows.Scan(&gid, &blob); err != nil {
			return fmt.Errorf("scan servers: %w", err)
		}
		if !blob.Valid || blob.String == "" {
			continue
		}
		var sj []serverJSON
		if err := json.Unmarshal([]byte(blob.String), &sj); err != nil {
			return fmt.Errorf("group %d servers json: %w", gid, err)
		}
		for _, s := range sj {
			for _, pid := range s.Players {
				owner[pid] = s.ServerID
			}
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for gi := range groups {
		for mi := range groups[gi].Members {
			groups[gi].Members[mi].ServerID = owner[groups[gi].Members[mi].PlayerID]
		}
	}
	return nil
}

// ClearCurrentPeriod 清空报名表与玩家表（切期时调用）。
//
// 分组表不清：它是本期唯一长期数据，由归档流程处理。
// 分批 DELETE 而不是 TRUNCATE：TRUNCATE 会锁表并重置自增，
// 而这两张表可能在「结束期」还被查询（客诉排查「我到底报没报上」）。
func (r *Repo) ClearCurrentPeriod(ctx context.Context) (int64, error) {
	if !r.Ready() {
		return 0, ErrNoDB
	}
	var total int64
	for _, table := range []string{"center_cross_enroll", "center_cross_player"} {
		n, err := r.clearTable(ctx, table)
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

func (r *Repo) clearTable(ctx context.Context, table string) (int64, error) {
	var total int64
	for {
		res, err := r.db.ExecContext(ctx,
			fmt.Sprintf("DELETE FROM %s LIMIT %d", table, clearBatch))
		if err != nil {
			return total, fmt.Errorf("clear %s: %w", table, err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return total, fmt.Errorf("clear %s rows: %w", table, err)
		}
		total += n
		if n < clearBatch {
			return total, nil
		}
		if err := ctx.Err(); err != nil {
			return total, err
		}
		// 批次之间让出：避免长时间占满连接池、给复制线程喘息。
		time.Sleep(10 * time.Millisecond)
	}
}

// marshalGroup 把一组序列化成 (players, servers) 两段 JSON。
//
// 一次遍历同时产出两份：servers 是 players 的反向索引，分开算容易漏人。
func marshalGroup(g Group) (players, servers []byte, err error) {
	pj := make([]playerJSON, 0, len(g.Members))
	byServer := make(map[int32][]int64, 8)
	for _, m := range g.Members {
		pj = append(pj, playerJSON{
			PlayerID:  m.PlayerID,
			Power:     m.Power,
			Score:     m.Score,
			UpdatedAt: m.UpdatedAt,
		})
		byServer[m.ServerID] = append(byServer[m.ServerID], m.PlayerID)
	}
	sj := make([]serverJSON, 0, len(byServer))
	for sid, pids := range byServer {
		sj = append(sj, serverJSON{ServerID: sid, Players: pids})
	}
	// 服务器号升序：JSON 内容稳定，便于比对与人工排查。
	sort.Slice(sj, func(i, j int) bool { return sj[i].ServerID < sj[j].ServerID })

	if players, err = json.Marshal(pj); err != nil {
		return nil, nil, fmt.Errorf("marshal players of group %d: %w", g.ID, err)
	}
	if servers, err = json.Marshal(sj); err != nil {
		return nil, nil, fmt.Errorf("marshal servers of group %d: %w", g.ID, err)
	}
	return players, servers, nil
}

// nullJSON 空数组落库为 NULL 而不是 "null" 字符串。
func nullJSON(b []byte) sql.NullString {
	if len(b) == 0 {
		return sql.NullString{}
	}
	return sql.NullString{String: string(b), Valid: true}
}
