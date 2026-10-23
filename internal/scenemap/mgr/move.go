package mgr

import (
	"fmt"

	"game_study/internal/scenemap"
	"game_study/internal/scenemap/grid"
)

// 本文件是位置同步（规格 6.3 SyncMap）。
//
// # 校验为什么做两次
//
// 玩家 Actor 在收到移动包时已经校验过一次（它手上有 AOI 发的阻挡快照），
// MGR 这里再做一次"二次校验"。双保险不是多余的：
//   - 玩家 Actor 的快照可能过期（机关刚关门，快照还没同步）；
//   - 更关键的是**作弊防线**：客户端可以伪造任意坐标，只要 MGR 不校验，
//     玩家就能穿墙/瞬移。信任边界必须落在权威这里。
//
// # 同格微调（规格 11.7 的核心不变量）
//
// 客户端每 100ms 左右发一次"我在这个像素"，绝大多数落在同一格。
// 这类包只允许更新像素与朝向，**不得触发任何格子级判定**
// （EntitySync / 触发器 / 寻路 / 仇恨）。一旦触发，一张图每秒会产生
// 数万次无意义的区域事件，玩法里的"进入区域"逻辑会被重复执行。
//
// 所以：只有 Index.Move 返回 moved=true 才走判定，其余路径直接返回。

// MoveReply 是位置同步的回执。
type MoveReply struct {
	UUID scenemap.UUID
	// From/To 格子变化前后（Moved=false 时二者相同）。
	From, To scenemap.GridPos
	Pixel    scenemap.PixelPos
	Dir      scenemap.Dir
	// Moved 格子是否真的变了。调用方据此决定要不要做区域逻辑。
	Moved bool
}

// syncMap 处理一次位置同步。
func (m *Mgr) syncMap(s *State, c CommandMove) (any, error) {
	old, ok := s.Index.At(c.UUID)
	if !ok {
		// 子弹/飞行物不进索引，它们由服务端驱动（客户端不上报），
		// 这里直接拒绝，避免"客户端把子弹挪到墙上"这类伪造。
		return nil, fmt.Errorf("%w: uuid=%s 不在格子索引中", scenemap.ErrNotFound, c.UUID)
	}

	cell := m.cfg.Geo.CellOf(c.Pixel.X, c.Pixel.Y)
	if !m.cfg.Size.Contains(cell) {
		// 负数像素会被 CellOf 算成负格，这里统一按越界处理。
		return nil, fmt.Errorf("%w: 像素 (%d,%d) → 格 %s 在地图外",
			scenemap.ErrOutOfRange, c.Pixel.X, c.Pixel.Y, cell)
	}

	// 单步位移上限：客户端丢包后可能一次跳几格，但跳太多就是瞬移。
	if d := grid.Chebyshev(old, cell); d > m.cfg.MaxStep {
		return nil, fmt.Errorf("%w: 单步 %d 格超过上限 %d（%s → %s）",
			scenemap.ErrOutOfRange, d, m.cfg.MaxStep, old, cell)
	}

	moved := cell != old
	if moved {
		// 静态阻挡（墙/岩浆/关闭的门）。nil 表示这张图不做静态校验。
		if m.deps.Walkable != nil && !m.deps.Walkable(cell) {
			return nil, fmt.Errorf("%w: %s 静态阻挡", scenemap.ErrBlocked, cell)
		}
		// 动态阻挡只看"怪物体积"，玩家之间允许重叠（见 Index 注释）。
		if s.Index.BlockedFor(c.UUID, cell) {
			return nil, fmt.Errorf("%w: %s 被怪物占据", scenemap.ErrBlocked, cell)
		}
	}

	changed, err := s.Index.Move(c.UUID, cell)
	if err != nil {
		return nil, err
	}
	// 理论上下游已经保证一致，Moved 与 changed 必须相同；不同说明
	// 索引与业务 map 漂移了，按索引为准（索引是站位权威）。
	moved = changed

	// 更新业务状态并记录路径。三类实体分别在各自的表里，
	// 这里用一个 switch 保证"不会有人漏更新"。
	switch {
	case s.Users[c.UUID] != nil:
		u := s.Users[c.UUID]
		u.GridPos = cell
		u.Pixel = c.Pixel
		u.Dir = c.Dir
		u.State = c.State
		u.LastSyncAt = m.now()
	case s.NPES[c.UUID] != nil:
		n := s.NPES[c.UUID]
		n.GridPos = cell
		n.Pixel = c.Pixel
		n.Dir = c.Dir
	}
	s.RecordPath(c.UUID, cell, m.now())
	s.Touch()

	// AOI 同步：**同格微调也要发**，客户端靠它做像素级插值。
	m.aoiSync(c.UUID, c.Pixel.X, c.Pixel.Y, c.Dir, c.State)

	if moved {
		// 区域机制（进入/离开某区域）与触发器 COND_2 都只在换格时触发。
		if err := m.gameplay.EntitySync(c.UUID, old, cell, s); err != nil {
			m.deps.Logger.Warn("scenemap/mgr: EntitySync 失败",
				"key", m.cfg.Key.Key(), "uuid", c.UUID, "err", err)
		}
		if fn := m.getCondTrigger(); fn != nil {
			fn(s, c.UUID, old, cell, CondMove)
		}
	}

	return MoveReply{
		UUID:  c.UUID,
		From:  old,
		To:    cell,
		Pixel: c.Pixel,
		Dir:   c.Dir,
		Moved: moved,
	}, nil
}
