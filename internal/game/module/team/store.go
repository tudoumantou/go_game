// store.go —— team / team_member 两张表的访问层。
//
// 建队与入队都要在事务里完成两件事：改队伍行 + 改成员行。拆开就会出现
// 「队建好了但队长不在成员表里」这种孤儿队伍。
package team

import (
	"context"
	"database/sql"
	"errors"

	"game_study/internal/game/module"
	"game_study/pkg/db"
	"game_study/pkg/model"
)

// Repo 组队仓储。
//
// db 可为 nil（无 DB 环境）：模块层入口判空后按「降级成功/不在队」回包，
// 保证无依赖环境下协议演示不中断。
type Repo struct {
	*module.Shared
}

// NewRepo 创建仓储。
func NewRepo(database *db.DB) *Repo {
	return &Repo{Shared: module.NewShared(database, "team")}
}

// FindByPlayer 查询玩家所在队伍。
//
// 返回 (nil, nil) 表示「不在队」：sql.ErrNoRows 翻译成 nil 而非错误。
// status != 2 过滤必须显式写：解散是软删，team_member 行还在，
// 不过滤会让已解散队伍的「幽灵成员」照样查到队伍。
func (r *Repo) FindByPlayer(ctx context.Context, playerID int64) (*Team, error) {
	const q = `SELECT t.id, t.leader_id, t.copy_id, t.status
	           FROM team t JOIN team_member m ON m.team_id = t.id
	           WHERE m.player_id = ? AND t.status != 2 LIMIT 1`
	var t Team
	err := r.SQL().QueryRowContext(ctx, q, playerID).Scan(&t.ID, &t.LeaderID, &t.CopyID, &t.Status)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &t, nil
}

// Create 创建队伍并把队长加入。
//
// 两条 INSERT 必须同事务：只建队伍行不插队长成员行，中间态下队长
// 自己都查不到这个队（与 guild.Create 同构）。status=0 显式写入，
// 不依赖数据库 DEFAULT——SQL 可读性优先。
func (r *Repo) Create(ctx context.Context, leaderID int64, copyID int32) (int64, error) {
	var teamID int64
	err := r.Tx(ctx, func(tx *sql.Tx) error {
		res, err := model.InsertTeam(ctx, tx, &model.Team{
			LeaderId: leaderID,
			CopyId:   copyID,
			Status:   0,
		})
		if err != nil {
			return err
		}
		teamID, err = res.LastInsertId()
		if err != nil {
			return err
		}
		_, err = model.InsertTeamMember(ctx, tx, &model.TeamMember{
			TeamId:   teamID,
			PlayerId: leaderID,
			Ready:    0,
		})
		return err
	})
	return teamID, err
}

// Join 加入队伍（事务：人数校验 + 插入，防并发超员）。
//
// 与 guild.Join 同构但有两处差异：
//  1. FOR UPDATE 锁 team 行（而非 count 行）——队伍没有冗余计数，
//     锁主行即可让随后的 COUNT(*) 看到一致快照；
//  2. COUNT(*) 实时统计而非读冗余字段——队伍上限只有 5 人，
//     锁内 count 的成本可忽略，换来少一个需要维护的计数器
//     （公会动辄几百人才值得用冗余 member_count）。
//
// status==2 的软删行与不存在的行同判 ErrTeamNotExist：对加入者而言
// 已解散的队伍就是不存在。
func (r *Repo) Join(ctx context.Context, teamID, playerID int64) error {
	return r.Tx(ctx, func(tx *sql.Tx) error {
		// 锁队伍主行（生成的 @lockby:id）：让随后的 COUNT(*) 看到一致快照
		team, err := model.LockTeamById(ctx, tx, teamID)
		if errors.Is(err, sql.ErrNoRows) || (err == nil && team.Status == 2) {
			return ErrTeamNotExist
		}
		if err != nil {
			return err
		}
		count, err := model.CountTeamMemberByTeamId(ctx, tx, teamID)
		if err != nil {
			return err
		}
		if count >= int64(maxMembers()) {
			return ErrTeamFull
		}
		if _, err := model.InsertTeamMember(ctx, tx, &model.TeamMember{
			TeamId:   teamID,
			PlayerId: playerID,
			Ready:    0,
		}); err != nil {
			// 唯一键冲突 = 重复加入（uk_team_player）。这是双端并发加入同一队伍的
			// 兜底（模块层前置查询挡掉常见情况）；判定走结构化的 1062，不看错误文本。
			if db.IsDuplicate(err) {
				return ErrAlreadyIn
			}
			return err
		}
		return nil
	})
}

// Leave 退出队伍（队长退出则解散）。
//
// 解散路径：删全部成员行 + 软删队伍行（status=2）——物理行保留，
// 与 guild.Quit 的硬删解散刻意不同（见文件头说明）。
// JOIN 查询同样带 status != 2：已解散队伍的残留成员行视同不在队，
// 不会重复触发解散逻辑。
func (r *Repo) Leave(ctx context.Context, playerID int64) error {
	return r.Tx(ctx, func(tx *sql.Tx) error {
		var teamID, leaderID int64
		err := tx.QueryRowContext(ctx,
			`SELECT t.id, t.leader_id FROM team t JOIN team_member m ON m.team_id = t.id
			 WHERE m.player_id = ? AND t.status != 2 LIMIT 1`, playerID).Scan(&teamID, &leaderID)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotInTeam
		}
		if err != nil {
			return err
		}
		if _, err := model.DeleteTeamMemberByTeamIdPlayerId(ctx, tx, teamID, playerID); err != nil {
			return err
		}
		if leaderID == playerID {
			if _, err := model.DeleteTeamMemberByTeamId(ctx, tx, teamID); err != nil {
				return err
			}
			if _, err := model.UpdateTeamStatusByPK(ctx, tx, &model.Team{Id: teamID, Status: 2}); err != nil {
				return err
			}
		}
		return nil
	})
}

// Members 查询队伍成员（联 player 取名字）。
//
// LEFT JOIN player 而非 INNER：玩家行可能缺失（注销/数据迁移期），
// INNER 会因一个成员的 player 行丢失把整队查不出来；LEFT JOIN +
// COALESCE 保证「队还在，名字为空」的降级展示。
// ORDER BY m.id：自增 id 即入队顺序，队内展示稳定且复用主键索引。
// IsLeader 用 JOIN 结果里的 leader_id 逐行折算：不用再查一次 team 表，
// 也避免「两次查询之间队长变更」的读数分歧。
func (r *Repo) Members(ctx context.Context, teamID int64) ([]Member, error) {
	const q = `SELECT m.player_id, COALESCE(p.name, ''), m.ready, t.leader_id
	           FROM team_member m
	           JOIN team t ON t.id = m.team_id
	           LEFT JOIN player p ON p.id = m.player_id
	           WHERE m.team_id = ? ORDER BY m.id`
	rows, err := r.SQL().QueryContext(ctx, q, teamID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var out []Member
	for rows.Next() {
		var m Member
		var leaderID int64
		if err := rows.Scan(&m.PlayerID, &m.Name, &m.Ready, &leaderID); err != nil {
			return nil, err
		}
		m.IsLeader = m.PlayerID == leaderID
		out = append(out, m)
	}
	return out, rows.Err()
}

// 仓储错误。
//
// 哨兵 error：模块层只认 errors.Is，仓储层裸返回不 %w 包装，
// 保证哨兵链可读（与 guild 同一约定）。
