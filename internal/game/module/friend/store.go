// store.go —— friend / friend_apply 两张表的访问层。
//
// 这里的事务与行锁不是过度设计：好友是双向关系，加好友要同时写两行、
// 校验双方上限，扣一次事务就会出现「A 有 B、B 没有 A」的半边关系。
package friend

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"game_study/internal/common/errcode"
	"game_study/pkg/db"
	"game_study/pkg/model"
	"game_study/pkg/sqlstore"
)

// Repo 好友仓储：所有写操作都在事务里完成，保证双边一致。
type Repo struct {
	*sqlstore.Base
}

// NewRepo 创建仓储。
func NewRepo(database *db.DB) *Repo {
	return &Repo{Base: sqlstore.New(database, "friend")}
}

// List 读取好友与待我处理的申请。
func (r *Repo) List(ctx context.Context, playerID int64) ([]FriendRow, []ApplyRow, error) {
	var friends []FriendRow
	rows, err := r.SQL().QueryContext(ctx, `
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
	arows, err := r.SQL().QueryContext(ctx, `
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
// Go 知识点：整个方法体是 r.Tx(ctx, func(tx *sql.Tx) error {...})——
// 闭包内 return nil = 提交，return err = 回滚。业务码不用 error 传出
// （error 保留给真失败），而是通过闭包捕获的外部变量 code 回传，
// 这样「拒绝」与「出错」不会混在一个返回值里。
func (r *Repo) Apply(ctx context.Context, from, to int64) (int32, error) {
	var code = int32(errcode.OK)
	err := r.Tx(ctx, func(tx *sql.Tx) error {
		// 目标必须存在（好友列表要 JOIN player 取名，悬空 ID 会让列表丢行）。
		if _, err := model.FindPlayerByPK(ctx, tx, to); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				code = int32(errcode.ErrFriendTargetNotFound)
				return nil
			}
			return err
		}

		// 黑名单双向检查：任一方拉黑都不允许建立关系。
		// 用两次单行查（走唯一键）而不是一条 OR 手写 SQL——生成器表达不了 OR，
		// 而这里两次查的代价可以忽略（点「加好友」是低频操作）。
		for _, pair := range [][2]int64{{to, from}, {from, to}} {
			if _, err := model.FindFriendBlockByPlayerIdBlockedId(ctx, tx, pair[0], pair[1]); err == nil {
				code = int32(errcode.ErrFriendBlocked)
				return nil
			} else if !errors.Is(err, sql.ErrNoRows) {
				return err
			}
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
	err := r.Tx(ctx, func(tx *sql.Tx) error {
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
	return r.Tx(ctx, func(tx *sql.Tx) error {
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

// Block 拉黑一个玩家：写黑名单 + 解除双向好友关系（同一事务）。
//
// 拉黑必须顺带解好友——否则会出现「已拉黑但仍是好友」，好友列表里还能
// 看到对方，与玩家的直觉相反，而且这类残留状态没有任何入口能清理掉。
//
// 已拉黑按成功处理（唯一键冲突忽略）：玩家重复点「拉黑」不该看到报错。
func (r *Repo) Block(ctx context.Context, playerID, targetID int64) error {
	return r.Tx(ctx, func(tx *sql.Tx) error {
		if _, err := model.InsertFriendBlock(ctx, tx, &model.FriendBlock{
			PlayerId:  playerID,
			BlockedId: targetID,
		}); err != nil && !db.IsDuplicate(err) {
			return err
		}
		// 双向删：好友关系是两行（各自一份），只删一边会留下单向好友。
		for _, pair := range [][2]int64{{playerID, targetID}, {targetID, playerID}} {
			if _, err := model.DeleteFriendByPlayerIdFriendId(ctx, tx, pair[0], pair[1]); err != nil {
				return err
			}
		}
		return nil
	})
}

// Unblock 取消拉黑（幂等：本来不在黑名单也返回成功）。
func (r *Repo) Unblock(ctx context.Context, playerID, targetID int64) error {
	_, err := model.DeleteFriendBlockByPlayerIdBlockedId(ctx, r.SQL(), playerID, targetID)
	return err
}

// Blocks 黑名单列表（带名字与等级，需要 JOIN player）。
//
// 这是本模块第二条手写的只读 JOIN（第一条是 List）——gendb 表达不了关联查询，
// 已登记进 docs/功能实现状态.md 的例外清单。
func (r *Repo) Blocks(ctx context.Context, playerID int64) ([]FriendRow, error) {
	rows, err := r.SQL().QueryContext(ctx, `
		SELECT b.blocked_id, p.name, p.level, b.created_at
		FROM friend_block b JOIN player p ON p.id = b.blocked_id
		WHERE b.player_id = ? ORDER BY b.created_at DESC`, playerID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var out []FriendRow
	for rows.Next() {
		var f FriendRow
		if err := rows.Scan(&f.PlayerID, &f.Name, &f.Level, &f.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, rows.Err()
}
