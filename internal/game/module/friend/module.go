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
	"game_study/internal/game/module"
	"game_study/pkg/db"
	"game_study/pkg/log"
)

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
