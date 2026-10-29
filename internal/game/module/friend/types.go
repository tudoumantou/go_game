// types.go —— 好友域的领域类型、错误与业务上限。
//
// 上限读配置（friend_limit.xml / friend_apply.xml），放在这里而不是 handler：
// store 的 becomeFriends 也要用它做二次校验，两边必须同一个来源。
package friend

import (
	"errors"
	"time"

	"game_study/pkg/gamedata"
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

// FriendRow 好友列表行。
type FriendRow struct {
	PlayerID  int64
	Name      string
	Level     int32
	CreatedAt time.Time
}

// ApplyRow 待处理申请行。

// ApplyRow 待处理申请行。
type ApplyRow struct {
	FromID    int64
	Name      string
	Level     int32
	CreatedAt time.Time
}

// Repo 好友仓储：所有写操作都在事务里完成，保证双边一致。

// errFriendFull 事务内信号：由 Handle/Apply 转成业务码。
var errFriendFull = errors.New("friend full")

// lockFriendCount 锁住某玩家的好友行并返回数量。
//
// 用 `SELECT ... FOR UPDATE`（不带 LIMIT）而不是 COUNT(*) FOR UPDATE：
// 需要在 REPEATABLE READ 下拿到 next-key lock，防止两个并发同意同时穿透上限。
// 这是 MySQL 间隙锁的典型用法——锁住「现有行 + 行间空隙」，
// 让并发 INSERT 在唯一键冲突前就被挡下。
// 生成函数 LockListFriendByPlayerId 正是这条范围锁（@locklistby:player_id）。
