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

	"game_study/internal/common/errcode"
	"game_study/internal/game/module"
	"game_study/pkg/db"
	"game_study/pkg/gamedata"
	"game_study/pkg/log"
	"game_study/pkg/model"

	gamev1 "game_study/pkg/pb/game/v1"
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
//
// 嵌入 Thin：接口七件套与错误回包由基座提供；四个处理器经 module.Typed
// 拿到解包后的请求与待填的响应，不再手写 Unmarshal / reply。
type Module struct {
	*module.Thin

	repo   *Repo
	online onlineChecker
	logger *log.Logger
}

// New 创建模块；online 可为 nil（列表全部显示离线）。
func New(database *db.DB, online onlineChecker, logger *log.Logger) *Module {
	m := &Module{repo: NewRepo(database), online: online, logger: logger}
	m.Thin = module.NewThin("friend", module.ThinLogger(logger))
	module.Handle(m.Thin, m.onList)
	module.Handle(m.Thin, m.onApply)
	module.Handle(m.Thin, m.onHandle)
	module.Handle(m.Thin, m.onDelete)
	return m
}

// onList 好友 + 待处理申请列表；在线状态只作展示，不影响关系判定。
func (m *Module) onList(ctx *module.Context, _ *gamev1.FriendListReq, resp *gamev1.FriendListResp) error {
	// Go 知识点：context.WithTimeout 派生带超时的 ctx，
	// defer cancel() 保证函数返回时释放定时器资源（取消会沿 context
	// 树传播到 database/sql 的驱动层，中断慢查询）。
	ctxDB, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	friends, applies, err := m.repo.List(ctxDB, ctx.State.PlayerID())
	if err != nil {
		return fmt.Errorf("list friends: %w", err)
	}

	resp.Code = int32(errcode.OK)
	resp.Limit = friendLimit()
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
	return nil
}

// onApply 发起好友申请；自加/非法目标在模块层拦截，不发 SQL。
//
// 参数校验前置：非法的请求（PlayerId<=0、加自己）连库都不查，
// 既省一次往返，也避免把无意义查询打到 DB。
func (m *Module) onApply(ctx *module.Context, req *gamev1.FriendApplyReq, resp *gamev1.FriendApplyResp) error {
	if req.PlayerId <= 0 {
		return ctx.FailCode(resp, errcode.ErrFriendTargetNotFound)
	}
	if req.PlayerId == ctx.State.PlayerID() {
		return ctx.FailCode(resp, errcode.ErrFriendSelf)
	}

	ctxDB, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	code, err := m.repo.Apply(ctxDB, ctx.State.PlayerID(), req.PlayerId)
	if err != nil {
		return fmt.Errorf("apply friend: %w", err)
	}
	resp.Code = code
	return nil
}

// onHandle 同意/拒绝申请；过期在仓储层按 TTL 判定并顺手置为已拒绝。
func (m *Module) onHandle(ctx *module.Context, req *gamev1.FriendHandleReq, resp *gamev1.FriendHandleResp) error {
	if req.PlayerId <= 0 || req.PlayerId == ctx.State.PlayerID() {
		return ctx.FailCode(resp, errcode.ErrFriendApplyNotFound)
	}

	ctxDB, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	code, err := m.repo.Handle(ctxDB, ctx.State.PlayerID(), req.PlayerId, req.Accept, applyTTL)
	if err != nil {
		return fmt.Errorf("handle friend apply: %w", err)
	}
	resp.Code = code
	return nil
}

// onDelete 删除好友；幂等——本来不是好友也回成功（双向两行一起删）。
func (m *Module) onDelete(ctx *module.Context, req *gamev1.FriendDeleteReq, resp *gamev1.FriendDeleteResp) error {
	if req.PlayerId <= 0 {
		return ctx.FailCode(resp, errcode.ErrFriendTargetNotFound)
	}

	ctxDB, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := m.repo.Delete(ctxDB, ctx.State.PlayerID(), req.PlayerId); err != nil {
		return fmt.Errorf("delete friend: %w", err)
	}
	resp.Code = int32(errcode.OK) // 删除幂等：本来不是好友也视为成功
	return nil
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
	var code = int32(errcode.OK)
	err := r.db.Tx(ctx, func(tx *sql.Tx) error {
		// 目标必须存在（好友列表要 JOIN player 取名，悬空 ID 会让列表丢行）。
		if _, err := model.FindPlayerByPK(ctx, tx, to); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				code = int32(errcode.ErrFriendTargetNotFound)
				return nil
			}
			return err
		}

		if _, err := model.FindFriendByPlayerIdFriendId(ctx, tx, from, to); err == nil {
			code = int32(errcode.ErrFriendAlready)
			return nil
		} else if !errors.Is(err, sql.ErrNoRows) {
			return err
		}

		// 对方已经申请过我：直接建立双向关系，两条申请都置为已同意。
		reverse, err := model.LockFriendApplyByFromIdToId(ctx, tx, to, from)
		if err == nil && reverse.Status == 0 {
			if err := r.becomeFriends(ctx, tx, from, to); err != nil {
				if errors.Is(err, errFriendFull) {
					code = int32(errcode.ErrFriendFull)
					return nil
				}
				return err
			}
			code = int32(errcode.OK)
			return nil
		}
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}

		// 我已申请且仍在待处理：幂等。
		mine, err := model.LockFriendApplyByFromIdToId(ctx, tx, from, to)
		if err == nil && mine.Status == 0 {
			code = int32(errcode.ErrFriendApplyExists)
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
			code = int32(errcode.ErrFriendFull)
			return nil
		}

		// 我发出的待处理申请上限。
		pending, err := model.CountFriendApplyByFromIdStatus(ctx, tx, from, 0)
		if err != nil {
			return err
		}
		if pending >= int64(applyLimit()) {
			code = int32(errcode.ErrFriendApplyFull)
			return nil
		}

		// 唯一键冲突时复用同一行：被拒绝过的申请重新回到待处理。
		// 原来是 INSERT ... ON DUPLICATE KEY UPDATE status=0, created_at=NOW()；
		// 拆成「插入 + 1062 命中则更新」，语义等价且不依赖 MySQL 方言。
		if _, err := model.InsertFriendApply(ctx, tx, &model.FriendApply{
			FromId: from,
			ToId:   to,
			Status: 0,
		}); err != nil {
			if !db.IsDuplicate(err) {
				return err
			}
			_, err = model.UpdateFriendApplyReapplyByFromIdToId(ctx, tx, &model.FriendApply{
				FromId:    from,
				ToId:      to,
				Status:    0,
				CreatedAt: time.Now(),
			})
			return err
		}
		return nil
	})
	return code, err
}

// Handle 处理申请：同意建立双向关系，拒绝只改状态。
//
// 状态机：0=待处理 → 1=已同意 / 2=已拒绝。status=1 再点是幂等成功，
// status=2 需要重新申请——与 onAccept 的「已领任务可重接」同思路。
func (r *Repo) Handle(ctx context.Context, to, from int64, accept bool, ttl time.Duration) (int32, error) {
	var code = int32(errcode.OK)
	err := r.db.Tx(ctx, func(tx *sql.Tx) error {
		apply, err := model.LockFriendApplyByFromIdToId(ctx, tx, from, to)
		if errors.Is(err, sql.ErrNoRows) {
			code = int32(errcode.ErrFriendApplyNotFound)
			return nil
		}
		if err != nil {
			return err
		}
		status, createdAt := apply.Status, apply.CreatedAt

		switch status {
		case 1:
			code = int32(errcode.ErrFriendAlready) // 已同意过：幂等
			return nil
		case 2:
			code = int32(errcode.ErrFriendApplyNotFound) // 已拒绝：需要重新申请
			return nil
		}

		if time.Since(createdAt) > ttl {
			// 过期：顺手置为已拒绝（状态收敛），下次要重新申请
			if _, err := model.UpdateFriendApplyStatusByFromIdToId(ctx, tx, &model.FriendApply{
				FromId: from,
				ToId:   to,
				Status: 2,
			}); err != nil {
				return err
			}
			code = int32(errcode.ErrFriendApplyExpired)
			return nil
		}

		if !accept {
			_, err := model.UpdateFriendApplyStatusByFromIdToId(ctx, tx, &model.FriendApply{
				FromId: from,
				ToId:   to,
				Status: 2,
			})
			return err
		}

		if err := r.becomeFriends(ctx, tx, from, to); err != nil {
			if errors.Is(err, errFriendFull) {
				code = int32(errcode.ErrFriendFull)
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

	// 双向插入：两行必须同时存在。重复同意时 1062 即幂等命中，忽略即可
	// （原来是 INSERT IGNORE，语义一致，但不依赖 MySQL 方言）。
	for _, pair := range [][2]int64{{a, b}, {b, a}} {
		if _, err := model.InsertFriend(ctx, tx, &model.Friend{
			PlayerId: pair[0],
			FriendId: pair[1],
		}); err != nil && !db.IsDuplicate(err) {
			return err
		}
	}
	// 同时把反向申请也置为已同意：两人几乎同时互相申请时不会留下第二条待处理。
	// 原来的 OR 条件拆成两次按复合键更新，同一事务内等价。
	for _, pair := range [][2]int64{{a, b}, {b, a}} {
		if _, err := model.UpdateFriendApplyStatusByFromIdToId(ctx, tx, &model.FriendApply{
			FromId: pair[0],
			ToId:   pair[1],
			Status: 1,
		}); err != nil {
			return err
		}
	}
	return nil
}

// errFriendFull 事务内信号：由 Handle/Apply 转成业务码。
var errFriendFull = errors.New("friend full")

// lockFriendCount 锁住某玩家的好友行并返回数量。
//
// 用 `SELECT ... FOR UPDATE`（不带 LIMIT）而不是 COUNT(*) FOR UPDATE：
// 需要在 REPEATABLE READ 下拿到 next-key lock，防止两个并发同意同时穿透上限。
// 这是 MySQL 间隙锁的典型用法——锁住「现有行 + 行间空隙」，
// 让并发 INSERT 在唯一键冲突前就被挡下。
// 生成函数 LockListFriendByPlayerId 正是这条范围锁（@locklistby:player_id）。
func lockFriendCount(ctx context.Context, tx *sql.Tx, playerID int64) (int, error) {
	rows, err := model.LockListFriendByPlayerId(ctx, tx, playerID)
	if err != nil {
		return 0, err
	}
	return len(rows), nil
}

// Delete 删除双向关系；本来不是好友也返回成功（幂等）。
func (r *Repo) Delete(ctx context.Context, a, b int64) error {
	return r.db.Tx(ctx, func(tx *sql.Tx) error {
		// 原来是 `WHERE (a AND b) OR (c AND d)`；拆成两条按复合键删除，
		// 同一事务内等价，且各自幂等（删 0 行不报错）。
		for _, pair := range [][2]int64{{a, b}, {b, a}} {
			if _, err := model.DeleteFriendByPlayerIdFriendId(ctx, tx, pair[0], pair[1]); err != nil {
				return err
			}
		}
		return nil
	})
}
