// internal/game/module/team/team.go —— 组队模块（6201-6208）
//
// 架构职责：队伍的信息/创建/加入/退出，对应设计文档 4.9.3。
//  1. 信息：onInfo 查队伍 + 成员（联 player 表取昵称）；
//  2. 创建：onCreate 事务建队（队长自动入队）；
//  3. 加入/退出：onJoin/onLeave，人数上限在事务内加锁判定；
//  4. 掉线：OnLogout 自动退队——队伍是临时关系，防挂机占位。
//
// Ext 槽位：无。队伍数据全在 MySQL（team / team_member 两表），
// 与公会/好友同属「MySQL 直写模块」——不过 Persister 快照链路。
//
// 事件订阅：无。
//
// 配置表关系：team_limit.xml（第一行 Col1=队伍人数上限），
// 经 gamedata.TeamLimitCfgList() 读取，见 maxMembers。
//
// 懒初始化：不适用（无 Ext 行；队伍行由创建动作显式产生）。
//
// 跨切约定：仓储调用一律传 context.Background()——请求 ctx 在回包后
// 即取消，沿用会把这些 DB 读写误杀。
//
// 与公会模块的关键差异：公会解散是硬删（DELETE 行），队伍解散是
// 软删（status=2，查询统一过滤）——保留行供对账排查，且自增 id
// 不复用，旧客户端拿着旧 teamId 不会蒙到一支新队伍。
package team

import (
	"context"
	"database/sql"
	"errors"

	"game_study/internal/common/errcode"
	"game_study/internal/game/module"
	"game_study/pkg/db"
	"game_study/pkg/gamedata"
	"game_study/pkg/log"
	"game_study/pkg/model"

	gamev1 "game_study/pkg/pb/game/v1"
)

// maxMembers 队伍上限（team_limit.xml 第一行 col1；缺配置回退 5）。
//
// 与 guild.maxMembers 的差异：队伍上限是全局唯一配置（取第一行），
// 公会是按等级查表（Col2 匹配）。双层 if 先查存在再查有效性，
// 防御「表为空」和「首行 Col1=0」两种脏配置。
func maxMembers() int {
	if len(gamedata.TeamLimitCfgList()) > 0 {
		if n := gamedata.TeamLimitCfgList()[0].Col1; n > 0 {
			return int(n)
		}
	}
	return 5
}

// Member 队伍成员。
type Member struct {
	// PlayerID 成员 player_id
	PlayerID int64
	// Name 昵称（LEFT JOIN player + COALESCE，玩家行缺失时回空串）
	Name string
	// Ready 是否已准备（开房 readiness，demo 期只存不校验）
	Ready bool
	// IsLeader 是否队长（由 leader_id 逐行折算，见 Members）
	IsLeader bool
}

// Team 队伍信息。
//
// 字段语义：Status 0=正常，2=已解散（软删，查询一律过滤）；
// CopyID 是队伍要打的副本配置 ID。
type Team struct {
	// ID 队伍自增主键
	ID int64
	// LeaderID 队长 player_id（队长退队即触发解散）
	LeaderID int64
	// CopyID 目标副本配置 id（创建时指定，决定成员匹配意图）
	CopyID int32
	// Status 0=正常 2=已解散（软删标记，物理行保留）
	Status int32
}

// Repo 组队仓储。
//
// db 可为 nil（无 DB 环境）：模块层入口判空后按「降级成功/不在队」回包，
// 保证无依赖环境下协议演示不中断。
type Repo struct {
	db *db.DB
}

// NewRepo 创建仓储。
func NewRepo(database *db.DB) *Repo { return &Repo{db: database} }

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
	err := r.db.QueryRowContext(ctx, q, playerID).Scan(&t.ID, &t.LeaderID, &t.CopyID, &t.Status)
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
	err := r.db.Tx(ctx, func(tx *sql.Tx) error {
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
	return r.db.Tx(ctx, func(tx *sql.Tx) error {
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
	return r.db.Tx(ctx, func(tx *sql.Tx) error {
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
	rows, err := r.db.QueryContext(ctx, q, teamID)
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
var (
	ErrTeamNotExist = errors.New("team not exist")
	ErrTeamFull     = errors.New("team full")
	ErrAlreadyIn    = errors.New("already in team")
	ErrNotInTeam    = errors.New("not in team")
)

// teamRepo 是模块对组队仓储的全部依赖（接口隔离）。
//
// 队伍的业务规则大多在 SQL 事务里（人数上限、队长解散），但「谁在队里
// 决定能不能创建」「仓储错误怎么翻译成业务码」这些编排逻辑在模块层，
// 需要能脱离 MySQL 验证。*Repo 隐式实现它，装配层不变。
type teamRepo interface {
	FindByPlayer(ctx context.Context, playerID int64) (*Team, error)
	Create(ctx context.Context, leaderID int64, copyID int32) (int64, error)
	Join(ctx context.Context, teamID, playerID int64) error
	Leave(ctx context.Context, playerID int64) error
	Members(ctx context.Context, teamID int64) ([]Member, error)
}

// Module 组队模块。
//
// 无内存态、无锁：全部状态走 MySQL 事务，OnMessage 在玩家 Actor 内
// 串行执行；跨玩家的并发安全由数据库（唯一键 + FOR UPDATE）保证。
type Module struct {
	*module.Thin

	// repo 仓储；nil 表示无 DB 环境，入口降级为「直接成功/不在队」回包
	repo   teamRepo
	logger *log.Logger
}

// New 创建组队模块。
func New(repo teamRepo, logger *log.Logger) *Module {
	m := &Module{repo: repo, logger: logger}
	m.Thin = module.NewThin("team",
		module.ThinLogger(logger),
		module.ThinLogout(m.onLogout),
	)
	module.Handle(m.Thin, m.onInfo)
	module.Handle(m.Thin, m.onCreate)
	module.Handle(m.Thin, m.onJoin)
	module.Handle(m.Thin, m.onLeave)
	return m
}

// onLogout 掉线自动退队。
//
// 与公会的关键语义差异：公会掉线不退会（持久社交关系，防误退），
// 队伍掉线退队（临时关系，防挂机占位坑队友）——同一生命周期钩子
// 在不同模块承载相反的产品决策。
// 注意：这是与 Thin 的日志器、守卫同处一个基座的钩子，Thin 只负责
// 在 OnLogout 时调用它；错误被忽略——登出流程不能被 DB 抖动卡住。
func (m *Module) onLogout(ctx *module.Context) error {
	if m.repo == nil {
		return nil
	}
	_ = m.repo.Leave(context.Background(), ctx.State.PlayerID())
	return nil
}

// onInfo 查询当前队伍信息；不在队回 int32(errcode.ErrTeamNotIn)。
//
// 两次查询（队伍 → 成员）：先查队伍，t==nil 时省掉第二次；
// 队伍上限 5 人，分两次查的代价可接受（合并成一条 JOIN 会把
// 「不在队」和「在队无成员」两种语义搅在一起）。
func (m *Module) onInfo(ctx *module.Context, _ *gamev1.TeamInfoReq, resp *gamev1.TeamInfoResp) error {
	if m.repo == nil {
		// 无 DB 环境按「不在队」回：与 (nil, nil) 同码
		return ctx.FailCode(resp, errcode.ErrTeamNotIn)
	}
	t, err := m.repo.FindByPlayer(context.Background(), ctx.State.PlayerID())
	if err != nil {
		return err
	}
	if t == nil {
		return ctx.FailCode(resp, errcode.ErrTeamNotIn)
	}
	members, err := m.repo.Members(context.Background(), t.ID)
	if err != nil {
		return err
	}
	resp.Code = int32(errcode.OK)
	resp.TeamId = t.ID
	resp.CopyId = t.CopyID
	for _, mb := range members {
		resp.Members = append(resp.Members, &gamev1.TeamMemberInfo{
			PlayerId: mb.PlayerID,
			Name:     mb.Name,
			Ready:    mb.Ready,
			IsLeader: mb.IsLeader,
		})
	}
	return nil
}

// onCreate 创建队伍；repo 缺失（单机/测试）时按成功降级。
//
// 前置 FindByPlayer 挡「已有队伍再创建」：仓储层只管建队，
// 「一个人只能在一队」的约束在模块层显式声明（与 guild 同构）。
func (m *Module) onCreate(ctx *module.Context, req *gamev1.TeamCreateReq, resp *gamev1.TeamCreateResp) error {
	if m.repo == nil {
		resp.Code = int32(errcode.OK)
		return nil
	}
	if t, err := m.repo.FindByPlayer(context.Background(), ctx.State.PlayerID()); err != nil {
		return err
	} else if t != nil {
		return ctx.FailCode(resp, errcode.ErrTeamAlreadyIn)
	}
	teamID, err := m.repo.Create(context.Background(), ctx.State.PlayerID(), req.CopyId)
	if err != nil {
		return err
	}
	resp.Code = int32(errcode.OK)
	resp.TeamId = teamID
	return nil
}

// onJoin 加入指定队伍；已在别的队先拒绝，超员/不存在由仓储错误码转换。
//
// 前置查询的意义：Join 仓储层只保证「进这个队」的原子性，
// 「你原来在别的队」需要先退再进——模块层显式拒绝跨队直加，
// demo 期不做自动换队（避免两个队伍的成员行竞态）。
func (m *Module) onJoin(ctx *module.Context, req *gamev1.TeamJoinReq, resp *gamev1.TeamJoinResp) error {
	if m.repo == nil {
		resp.Code = int32(errcode.OK)
		return nil
	}
	if t, err := m.repo.FindByPlayer(context.Background(), ctx.State.PlayerID()); err != nil {
		return err
	} else if t != nil {
		return ctx.FailCode(resp, errcode.ErrTeamAlreadyIn)
	}
	if err := m.repo.Join(context.Background(), req.TeamId, ctx.State.PlayerID()); err != nil {
		switch {
		case errors.Is(err, ErrTeamNotExist):
			return ctx.FailCode(resp, errcode.ErrTeamNotExist)
		case errors.Is(err, ErrTeamFull):
			return ctx.FailCode(resp, errcode.ErrTeamFull)
		case errors.Is(err, ErrAlreadyIn):
			return ctx.FailCode(resp, errcode.ErrTeamAlreadyIn)
		default:
			return err
		}
	}
	resp.Code = int32(errcode.OK)
	return nil
}

// onLeave 主动退队；队长退队 = 解散（仓储层保证）。
//
// 解散的原子性在仓储事务内：删队长行 → 删全员 → 软删队伍，
// 任一步失败整体回滚，不会出现「队长退了、队伍还在」的半解散态。
func (m *Module) onLeave(ctx *module.Context, _ *gamev1.TeamLeaveReq, resp *gamev1.TeamLeaveResp) error {
	if m.repo == nil {
		resp.Code = int32(errcode.OK)
		return nil
	}
	if err := m.repo.Leave(context.Background(), ctx.State.PlayerID()); err != nil {
		if errors.Is(err, ErrNotInTeam) {
			return ctx.FailCode(resp, errcode.ErrTeamNotIn)
		}
		return err
	}
	resp.Code = int32(errcode.OK)
	return nil
}
