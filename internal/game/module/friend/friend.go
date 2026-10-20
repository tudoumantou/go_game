// Package friend 好友最小闭环（R5）。
//
// 关系存 MySQL 双向两行（friend 表 uk_pair），申请存 friend_apply（uk_from_to）：
//
//   - 好友是**双人状态**，不能像任务/签到那样放进单玩家快照——一份快照无法表达
//     「两边都在或都不在」，也无法约束唯一性。所以这里直写事务，不走 write-behind。
//   - 同意/删除都在一个事务里完成「两行都写」或「两行都删」，任一步失败整体回滚，
//     不会出现单边好友。
//   - 缓存只用于在线状态展示，不参与关系判定：Redis 挂了顶多显示离线，关系仍正确。
//
// 架构职责：好友关系全生命周期（申请/同意/拒绝/删除）。
// Ext 槽位：无——关系数据在 MySQL，玩家快照不存好友列表。
// 事件订阅：无。
// 配置表：friend_limit.xml（col1=好友上限，col2=待处理申请上限）。
// 懒初始化：不适用（关系型数据，无「空行」概念）；
// 每个请求带 3s 超时的 DB context，慢 SQL 不会拖死玩家 Actor。
package friend

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"game_study/internal/game/module"
	"game_study/pkg/codec"
	"game_study/pkg/db"
	"game_study/pkg/gamedata"
	"game_study/pkg/log"

	gamev1 "game_study/pkg/pb/game/v1"
	"google.golang.org/protobuf/proto"
)

const (
	msgIDListReq    = codec.MsgFriendListReq
	msgIDListResp   = codec.MsgFriendListResp
	msgIDApplyReq   = codec.MsgFriendApplyReq
	msgIDApplyResp  = codec.MsgFriendApplyResp
	msgIDHandleReq  = codec.MsgFriendHandleReq
	msgIDHandleResp = codec.MsgFriendHandleResp
	msgIDDeleteReq  = codec.MsgFriendDeleteReq
	msgIDDeleteResp = codec.MsgFriendDeleteResp
)

// 业务码。
//
// 好友段从 6801 起：与背包/装备等错开，客户端按号段定位玩法。
const (
	codeOK             int32 = 0
	codeSelf           int32 = 6801 // 不能加自己
	codeTargetNotFound int32 = 6802 // 目标玩家不存在
	codeAlreadyFriend  int32 = 6803 // 已是好友
	codeFriendFull     int32 = 6804 // 好友已满
	codeApplyExists    int32 = 6805 // 已申请，等待对方处理
	codeApplyNotFound  int32 = 6806 // 申请不存在/已处理/已过期
	codeApplyFull      int32 = 6807 // 待处理申请过多
	codeApplyExpired   int32 = 6808 // 申请已过期
)

// applyTTL 申请有效期：超过后同意会被拒绝，需重新申请。
//
// 选 7 天：足够覆盖「对方隔几天上线」，又不至于让申请列表永久堆积。
// 过期判定在同意时做，不依赖定时清理任务——少一个后台作业，
// 少一类「任务没跑」的故障模式。
const applyTTL = 7 * 24 * time.Hour

// onlineChecker 在线状态查询（*cache.Presence 实现；测试可注入）。
//
// 接口而非具体类型：单测可注入假实现，也隔离了 Redis 故障面。
type onlineChecker interface {
	IsOnline(ctx context.Context, playerID int64) (string, bool)
}

// Module 好友模块。
type Module struct {
	repo   *Repo
	online onlineChecker
	logger *log.Logger
}

// New 创建模块；online 可为 nil（列表全部显示离线）。
func New(database *db.DB, online onlineChecker, logger *log.Logger) *Module {
	return &Module{repo: NewRepo(database), online: online, logger: logger}
}

// Name 模块名（= 落库归属标签；好友关系本身直写 MySQL，不进快照）。
func (*Module) Name() string { return "friend" }

// MsgIDs 好友四件套：列表 / 申请 / 处理 / 删除。
func (*Module) MsgIDs() []uint32 {
	return []uint32{msgIDListReq, msgIDApplyReq, msgIDHandleReq, msgIDDeleteReq}
}

// Init 无模块级状态要初始化。
func (*Module) Init(*module.Context) error { return nil }

// 好友关系直接落库，没有快照旁路数据。
func (*Module) OnSave(context.Context) error   { return nil }
func (*Module) OnLogin(*module.Context) error  { return nil }
func (*Module) OnLogout(*module.Context) error { return nil }

// OnMessage 协议分发；所有数据库访问都带 3s 超时，避免玩家 Actor 被慢 SQL 拖住。
func (m *Module) OnMessage(ctx *module.Context, msgID uint32, body []byte) error {
	switch msgID {
	case msgIDListReq:
		return m.onList(ctx)
	case msgIDApplyReq:
		return m.onApply(ctx, body)
	case msgIDHandleReq:
		return m.onHandle(ctx, body)
	case msgIDDeleteReq:
		return m.onDelete(ctx, body)
	default:
		return fmt.Errorf("friend: unexpected msg %d", msgID)
	}
}

// onList 好友 + 待处理申请列表；在线状态只作展示，不影响关系判定。
func (m *Module) onList(ctx *module.Context) error {
	// Go 知识点：context.WithTimeout 派生带超时的 ctx，
	// defer cancel() 保证函数返回时释放定时器资源（取消会沿 context
	// 树传播到 database/sql 的驱动层，中断慢查询）。
	ctxDB, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	friends, applies, err := m.repo.List(ctxDB, ctx.State.PlayerID())
	if err != nil {
		return fmt.Errorf("list friends: %w", err)
	}

	resp := &gamev1.FriendListResp{Code: codeOK, Limit: friendLimit()}
	for _, f := range friends {
		online := false
		if m.online != nil {
			if _, ok := m.online.IsOnline(ctxDB, f.PlayerID); ok {
				online = true
			}
		}
		resp.Friends = append(resp.Friends, &gamev1.FriendEntry{
			PlayerId:  f.PlayerID,
			Name:      f.Name,
			Level:     f.Level,
			Online:    online,
			CreatedAt: f.CreatedAt.Unix(),
		})
	}
	for _, a := range applies {
		resp.Applies = append(resp.Applies, &gamev1.FriendApplyEntry{
			PlayerId:  a.FromID,
			Name:      a.Name,
			Level:     a.Level,
			CreatedAt: a.CreatedAt.Unix(),
		})
	}
	return reply(ctx, msgIDListResp, resp)
}

// onApply 发起好友申请；自加/非法目标在模块层拦截，不发 SQL。
//
// 参数校验前置：非法的请求（PlayerId<=0、加自己）连库都不查，
// 既省一次往返，也避免把无意义查询打到 DB。
func (m *Module) onApply(ctx *module.Context, body []byte) error {
	var req gamev1.FriendApplyReq
	if err := proto.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("decode FriendApplyReq: %w", err)
	}
	resp := &gamev1.FriendApplyResp{}
	if req.PlayerId <= 0 {
		resp.Code = codeTargetNotFound
		return reply(ctx, msgIDApplyResp, resp)
	}
	if req.PlayerId == ctx.State.PlayerID() {
		resp.Code = codeSelf
		return reply(ctx, msgIDApplyResp, resp)
	}

	ctxDB, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	code, err := m.repo.Apply(ctxDB, ctx.State.PlayerID(), req.PlayerId)
	if err != nil {
		return fmt.Errorf("apply friend: %w", err)
	}
	resp.Code = code
	return reply(ctx, msgIDApplyResp, resp)
}

// onHandle 同意/拒绝申请；过期在仓储层按 TTL 判定并顺手置为已拒绝。
func (m *Module) onHandle(ctx *module.Context, body []byte) error {
	var req gamev1.FriendHandleReq
	if err := proto.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("decode FriendHandleReq: %w", err)
	}
	resp := &gamev1.FriendHandleResp{}
	if req.PlayerId <= 0 || req.PlayerId == ctx.State.PlayerID() {
		resp.Code = codeApplyNotFound
		return reply(ctx, msgIDHandleResp, resp)
	}

	ctxDB, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	code, err := m.repo.Handle(ctxDB, ctx.State.PlayerID(), req.PlayerId, req.Accept, applyTTL)
	if err != nil {
		return fmt.Errorf("handle friend apply: %w", err)
	}
	resp.Code = code
	return reply(ctx, msgIDHandleResp, resp)
}

// onDelete 删除好友；幂等——本来不是好友也回成功（双向两行一起删）。
func (m *Module) onDelete(ctx *module.Context, body []byte) error {
	var req gamev1.FriendDeleteReq
	if err := proto.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("decode FriendDeleteReq: %w", err)
	}
	resp := &gamev1.FriendDeleteResp{}
	if req.PlayerId <= 0 {
		resp.Code = codeTargetNotFound
		return reply(ctx, msgIDDeleteResp, resp)
	}

	ctxDB, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := m.repo.Delete(ctxDB, ctx.State.PlayerID(), req.PlayerId); err != nil {
		return fmt.Errorf("delete friend: %w", err)
	}
	resp.Code = codeOK // 删除幂等：本来不是好友也视为成功
	return reply(ctx, msgIDDeleteResp, resp)
}

// reply 序列化 proto 并经 ctx.Reply 下发。
func reply(ctx *module.Context, msgID uint32, msg proto.Message) error {
	raw, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal friend resp: %w", err)
	}
	return ctx.Reply(msgID, raw)
}

// friendLimit 好友上限（friend_limit.xml col1），缺配置回退 50。
func friendLimit() int32 {
	if len(gamedata.FriendLimitCfgList()) == 0 {
		return 50
	}
	cfg := gamedata.FriendLimitCfgList()[0]
	if cfg.Col1 <= 0 {
		return 50
	}
	return int32(cfg.Col1)
}

// applyLimit 待处理申请上限（col2），缺配置回退 30。
func applyLimit() int {
	if len(gamedata.FriendLimitCfgList()) == 0 {
		return 30
	}
	cfg := gamedata.FriendLimitCfgList()[0]
	if cfg.Col2 <= 0 {
		return 30
	}
	return int(cfg.Col2)
}

// FriendRow 好友列表行。
type FriendRow struct {
	PlayerID  int64
	Name      string
	Level     int32
	CreatedAt time.Time
}

// ApplyRow 待处理申请行。
type ApplyRow struct {
	FromID    int64
	Name      string
	Level     int32
	CreatedAt time.Time
}

// Repo 好友仓储：所有写操作都在事务里完成，保证双边一致。
type Repo struct {
	db *db.DB
}

// NewRepo 创建仓储。
func NewRepo(database *db.DB) *Repo { return &Repo{db: database} }

// List 读取好友与待我处理的申请。
func (r *Repo) List(ctx context.Context, playerID int64) ([]FriendRow, []ApplyRow, error) {
	var friends []FriendRow
	rows, err := r.db.DB.QueryContext(ctx, `
		SELECT f.friend_id, p.name, p.level, f.created_at
		FROM friend f JOIN player p ON p.id = f.friend_id
		WHERE f.player_id = ? ORDER BY f.created_at DESC`, playerID)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var f FriendRow
		if err := rows.Scan(&f.PlayerID, &f.Name, &f.Level, &f.CreatedAt); err != nil {
			return nil, nil, err
		}
		friends = append(friends, f)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}

	var applies []ApplyRow
	arows, err := r.db.DB.QueryContext(ctx, `
		SELECT a.from_id, p.name, p.level, a.created_at
		FROM friend_apply a JOIN player p ON p.id = a.from_id
		WHERE a.to_id = ? AND a.status = 0 ORDER BY a.created_at DESC`, playerID)
	if err != nil {
		return nil, nil, err
	}
	defer arows.Close()
	for arows.Next() {
		var a ApplyRow
		if err := arows.Scan(&a.FromID, &a.Name, &a.Level, &a.CreatedAt); err != nil {
			return nil, nil, err
		}
		applies = append(applies, a)
	}
	return friends, applies, arows.Err()
}

// Apply 发起申请，返回业务码。
//
// 三种「看起来是新申请」的情况要分开：
//   - 对方已经申请我 → 直接成为好友（不必等对方再点一次同意）；
//   - 我已申请且待处理 → 幂等返回「已申请」，不重复插入；
//   - 我之前被拒绝 → 复用同一行回到待处理（唯一键保证一对人只有一行）。
//
// Go 知识点：整个方法体是 r.db.Tx(ctx, func(tx *sql.Tx) error {...})——
// 闭包内 return nil = 提交，return err = 回滚。业务码不用 error 传出
// （error 保留给真失败），而是通过闭包捕获的外部变量 code 回传，
// 这样「拒绝」与「出错」不会混在一个返回值里。
func (r *Repo) Apply(ctx context.Context, from, to int64) (int32, error) {
	var code = codeOK
	err := r.db.Tx(ctx, func(tx *sql.Tx) error {
		// 目标必须存在（好友列表要 JOIN player 取名，悬空 ID 会让列表丢行）。
		var exists int64
		if err := tx.QueryRowContext(ctx, `SELECT id FROM player WHERE id = ?`, to).Scan(&exists); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				code = codeTargetNotFound
				return nil
			}
			return err
		}

		var one int
		err := tx.QueryRowContext(ctx,
			`SELECT 1 FROM friend WHERE player_id = ? AND friend_id = ?`, from, to).Scan(&one)
		if err == nil {
			code = codeAlreadyFriend
			return nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}

		// 对方已经申请过我：直接建立双向关系，两条申请都置为已同意。
		var reverseStatus int32
		err = tx.QueryRowContext(ctx,
			`SELECT status FROM friend_apply WHERE from_id = ? AND to_id = ? FOR UPDATE`, to, from).Scan(&reverseStatus)
		if err == nil && reverseStatus == 0 {
			if err := r.becomeFriends(ctx, tx, from, to); err != nil {
				if errors.Is(err, errFriendFull) {
					code = codeFriendFull
					return nil
				}
				return err
			}
			code = codeOK
			return nil
		}
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}

		// 我已申请且仍在待处理：幂等。
		var myStatus int32
		err = tx.QueryRowContext(ctx,
			`SELECT status FROM friend_apply WHERE from_id = ? AND to_id = ? FOR UPDATE`, from, to).Scan(&myStatus)
		if err == nil && myStatus == 0 {
			code = codeApplyExists
			return nil
		}
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}

		// 好友上限（锁住自己的好友行，防并发穿透）。
		count, err := lockFriendCount(ctx, tx, from)
		if err != nil {
			return err
		}
		if count >= int(friendLimit()) {
			code = codeFriendFull
			return nil
		}

		// 我发出的待处理申请上限。
		var pending int
		if err := tx.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM friend_apply WHERE from_id = ? AND status = 0`, from).Scan(&pending); err != nil {
			return err
		}
		if pending >= applyLimit() {
			code = codeApplyFull
			return nil
		}

		// 唯一键冲突时复用同一行：被拒绝过的申请重新回到待处理。
		_, err = tx.ExecContext(ctx, `
			INSERT INTO friend_apply (from_id, to_id, status, created_at) VALUES (?, ?, 0, NOW())
			ON DUPLICATE KEY UPDATE status = 0, created_at = NOW()`, from, to)
		return err
	})
	return code, err
}

// Handle 处理申请：同意建立双向关系，拒绝只改状态。
//
// 状态机：0=待处理 → 1=已同意 / 2=已拒绝。status=1 再点是幂等成功，
// status=2 需要重新申请——与 onAccept 的「已领任务可重接」同思路。
func (r *Repo) Handle(ctx context.Context, to, from int64, accept bool, ttl time.Duration) (int32, error) {
	var code = codeOK
	err := r.db.Tx(ctx, func(tx *sql.Tx) error {
		var status int32
		var createdAt time.Time
		err := tx.QueryRowContext(ctx,
			`SELECT status, created_at FROM friend_apply WHERE from_id = ? AND to_id = ? FOR UPDATE`,
			from, to).Scan(&status, &createdAt)
		if errors.Is(err, sql.ErrNoRows) {
			code = codeApplyNotFound
			return nil
		}
		if err != nil {
			return err
		}

		switch status {
		case 1:
			code = codeAlreadyFriend // 已同意过：幂等
			return nil
		case 2:
			code = codeApplyNotFound // 已拒绝：需要重新申请
			return nil
		}

		if time.Since(createdAt) > ttl {
			// 过期：顺手置为已拒绝（状态收敛），下次要重新申请
			if _, err := tx.ExecContext(ctx,
				`UPDATE friend_apply SET status = 2 WHERE from_id = ? AND to_id = ?`, from, to); err != nil {
				return err
			}
			code = codeApplyExpired
			return nil
		}

		if !accept {
			_, err := tx.ExecContext(ctx,
				`UPDATE friend_apply SET status = 2 WHERE from_id = ? AND to_id = ?`, from, to)
			return err
		}

		if err := r.becomeFriends(ctx, tx, from, to); err != nil {
			if errors.Is(err, errFriendFull) {
				code = codeFriendFull
				return nil
			}
			return err
		}
		return nil
	})
	return code, err
}

// becomeFriends 双向插入并在双方都未超限时把两条申请置为已同意。
//
// errFriendFull 是「事务内信号错误」：在闭包深处返回、由外层转业务码。
// Go 知识点：用 errors.New 造一个包内哨兵值 + errors.Is 判定，
// 比返回裸 bool 多带了错误链语义，也不必为每种业务拒绝都定义一个 error。
func (r *Repo) becomeFriends(ctx context.Context, tx *sql.Tx, a, b int64) error {
	limit := int(friendLimit())
	ca, err := lockFriendCount(ctx, tx, a)
	if err != nil {
		return err
	}
	cb, err := lockFriendCount(ctx, tx, b)
	if err != nil {
		return err
	}
	if ca >= limit || cb >= limit {
		return errFriendFull
	}

	// 两行必须同时存在：IGNORE 保证重复同意不报错（幂等）。
	if _, err := tx.ExecContext(ctx,
		`INSERT IGNORE INTO friend (player_id, friend_id) VALUES (?, ?), (?, ?)`,
		a, b, b, a); err != nil {
		return err
	}
	// 同时把反向申请也置为已同意：两人几乎同时互相申请时不会留下第二条待处理。
	_, err = tx.ExecContext(ctx, `
		UPDATE friend_apply SET status = 1
		WHERE (from_id = ? AND to_id = ?) OR (from_id = ? AND to_id = ?)`, a, b, b, a)
	return err
}

// errFriendFull 事务内信号：由 Handle/Apply 转成业务码。
var errFriendFull = errors.New("friend full")

// lockFriendCount 锁住某玩家的好友行并返回数量。
//
// 用 `SELECT id ... FOR UPDATE` 而不是 COUNT(*) FOR UPDATE：
// 需要在 REPEATABLE READ 下拿到 next-key lock，防止两个并发同意同时穿透上限。
// 这是 MySQL 间隙锁的典型用法——锁住「现有行 + 行间空隙」，
// 让并发 INSERT 在唯一键冲突前就被挡下。
func lockFriendCount(ctx context.Context, tx *sql.Tx, playerID int64) (int, error) {
	rows, err := tx.QueryContext(ctx,
		`SELECT id FROM friend WHERE player_id = ? FOR UPDATE`, playerID)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return 0, err
		}
		n++
	}
	return n, rows.Err()
}

// Delete 删除双向关系；本来不是好友也返回成功（幂等）。
func (r *Repo) Delete(ctx context.Context, a, b int64) error {
	return r.db.Tx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `
			DELETE FROM friend
			WHERE (player_id = ? AND friend_id = ?) OR (player_id = ? AND friend_id = ?)`,
			a, b, b, a)
		return err
	})
}
