package battle

import (
	"game_study/internal/scenemap/mgr"
)

// EnsureCommand 在 MGR goroutine 内初始化战斗运行时。
//
// 为什么要走命令而不是在装配层直接调 Ensure：Ensure 会写 State.Ext
// （可观察状态），必须在 MGR 的串行队列里做。直接调会与正在执行的
// 进图/移动命令竞争同一份 State——这正是单写者纪律要杜绝的越权。
type EnsureCommand struct {
	// Options 运行时参数（表、钩子、白名单、地图类型）。
	Options Options
}

// NewEnsureCommand 构造初始化命令。
func NewEnsureCommand(opts Options) mgr.Command { return &EnsureCommand{Options: opts} }

// Name 实现 mgr.Command。
func (c *EnsureCommand) Name() string { return "battle.ensure" }

// MayNoop 实现 noopCommand：重复初始化是幂等的，第二次不该报 ErrNoEffect。
func (c *EnsureCommand) MayNoop() bool { return true }

// Apply 实现 mgr.Command。
func (c *EnsureCommand) Apply(s *mgr.State) (any, error) {
	return Ensure(s, c.Options), nil
}

// SyncPlayerCommand 同步一个玩家侧战斗投影（属性变化/进图时投递）。
//
// 只写战斗相关字段，位置仍以场景为准：客户端上报的坐标不能成为
// 战斗结算的射程依据（否则"报一个远处的坐标"就能打到全图）。
type SyncPlayerCommand struct {
	// Sprite 玩家侧属性（UUID 必填）。
	Sprite Sprite
}

// NewSyncPlayerCommand 构造同步命令。
func NewSyncPlayerCommand(sp Sprite) mgr.Command { return &SyncPlayerCommand{Sprite: sp} }

// Name 实现 mgr.Command。
func (c *SyncPlayerCommand) Name() string { return "battle.sync_player" }

// MayNoop 实现 noopCommand：属性没变化的重复同步是合法的。
func (c *SyncPlayerCommand) MayNoop() bool { return true }

// Apply 实现 mgr.Command。
func (c *SyncPlayerCommand) Apply(s *mgr.State) (any, error) {
	rt, ok := From(s)
	if !ok {
		// 没装战斗运行时的图（纯场景/主城）：忽略同步，不自动创建，
		// 避免"一个同步包把主城变成可战斗地图"。
		return nil, nil
	}
	rt.UpsertPlayer(s, c.Sprite)
	return nil, nil
}
