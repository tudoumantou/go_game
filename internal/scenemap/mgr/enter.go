package mgr

import (
	"fmt"

	"game_study/internal/scenemap"
	"game_study/internal/scenemap/grid"
)

// 本文件是实体进/出图（规格 5.5 / 5.6）。
//
// # 按 UUID 类型三分
//
//	玩家     → 格子索引 + Users + Gameplay.PlayerEnter + 关图计时归零 + 路径缓存
//	怪物/NPC → NPES + 动态阻挡（按体积）+ 小地图点
//	宠物/召唤 → 主子登记（Pets）+ 格子索引（可重叠）
//
// 三分而不是一条统一路径：三类实体的"权威"不同（玩家属性在玩家 Actor、
// 怪物全在 MGR、宠物跟随主人），混在一起写迟早会出现"给怪物发 PlayerEnter"。

// EnterReply 是进图命令的回执。
type EnterReply struct {
	UUID    scenemap.UUID
	Kind    scenemap.EntityKind
	GridPos scenemap.GridPos
	Pixel   scenemap.PixelPos
}

// LeaveReply 是离图命令的回执。
type LeaveReply struct {
	UUID scenemap.UUID
	Kind scenemap.EntityKind
}

// entityEnter 按实体类型分派进图逻辑。
func (m *Mgr) entityEnter(s *State, c CommandEnterPlayer) (any, error) {
	kind := c.Kind
	if kind == scenemap.EntityKindNone {
		kind = scenemap.EntityKindPlayer
	}
	switch kind {
	case scenemap.EntityKindPlayer:
		return m.enterPlayer(s, c)
	case scenemap.EntityKindNPE:
		return m.enterNPE(s, c)
	case scenemap.EntityKindPet, scenemap.EntityKindSummon, scenemap.EntityKindMirror:
		return m.enterAttachment(s, c, kind)
	case scenemap.EntityKindBullet:
		return m.enterBullet(s, c)
	default:
		return nil, fmt.Errorf("%w: 未知实体类型 %d", scenemap.ErrInvalidArgument, int32(kind))
	}
}

// enterPlayer 玩家进图。
//
// 顺序（与规格 5.5 一致）：格子索引 → Users → PlayerEnter → 关图计时 → 路径。
// 只有 PlayerEnter 失败会回滚：它是唯一"可能拒绝进图"的环节（例如副本
// 已满、等级不足）。一旦进了图，后续步骤都必须在状态里留下痕迹，
// 否则会出现"索引里有、Users 里没有"的半成品。
func (m *Mgr) enterPlayer(s *State, c CommandEnterPlayer) (any, error) {
	if !c.UUID.Valid() {
		return nil, fmt.Errorf("%w: uuid 为 0", scenemap.ErrInvalidArgument)
	}
	if _, dup := s.Users[c.UUID]; dup {
		return nil, fmt.Errorf("%w: 玩家 %s 已在图内", scenemap.ErrAlreadyExists, c.UUID)
	}

	now := m.now()
	pos, pixel := m.resolvePos(c.GridPos, c.Pixel)
	if !m.cfg.Size.Contains(pos) {
		return nil, fmt.Errorf("%w: 落点 %s 不在地图内", scenemap.ErrOutOfRange, pos)
	}

	// 玩家允许与玩家重叠（出生点扎堆、同点回城）；与怪物重叠仍会被
	// BlockedFor 之外的判定拦住——但那属于"移动"路径，进图落点是服务端
	// 指定的，不做动态阻挡校验，否则副本开场就可能把人挤到图外。
	actual, err := s.Index.Enter(c.UUID, pos, EnterOption{
		Kind:         scenemap.EntityKindPlayer,
		AllowOverlap: true,
	})
	if err != nil {
		return nil, err
	}

	u := &UserState{
		UUID:       c.UUID,
		Kind:       scenemap.EntityKindPlayer,
		GridPos:    actual,
		Pixel:      pixel,
		Dir:        c.Dir,
		CharName:   c.CharName,
		EnterAt:    now,
		LastSyncAt: now,
	}
	s.Users[c.UUID] = u

	if err := m.gameplay.PlayerEnter(c.UUID, s); err != nil {
		// 唯一可回滚点：把刚写进去的全部撤掉，保持"要么完全在图内、
		// 要么完全不在"的不变式。
		delete(s.Users, c.UUID)
		s.Index.Leave(c.UUID)
		return nil, err
	}

	m.onPlayerEnter(s)
	s.RecordPath(c.UUID, actual, now)
	s.Touch()

	m.aoiEnter(AOIEntity{
		UUID:    c.UUID,
		Kind:    scenemap.EntityKindPlayer,
		GridPos: actual,
		Pixel:   pixel,
		Dir:     c.Dir,
	})

	// TODO(规格 5.5 小地图): 玩家进图要通知小地图刷新自己与队友的位置。
	// 小地图协议尚未定稿，这里不写"假实现"——接入时在此调用小地图接口。
	//
	// TODO(规格 5.5 被动 CD): 进图要恢复/开始玩家被动的冷却计时（离线期间
	// 不推进）。被动 CD 的权威在玩家 Actor，MGR 只应投递一条"已进图"事件，
	// 同样等接口定稿后再接。

	return EnterReply{UUID: c.UUID, Kind: scenemap.EntityKindPlayer, GridPos: actual, Pixel: pixel}, nil
}

// enterNPE 怪物/NPC 进图。
//
// 怪物不允许重叠（见 Index 的注释）：先找空位，找不到就 ErrBlocked。
// 落点搜索用切比雪夫环由内向外，顺序固定，可复现。
func (m *Mgr) enterNPE(s *State, c CommandEnterPlayer) (any, error) {
	if !c.UUID.Valid() {
		return nil, fmt.Errorf("%w: uuid 为 0", scenemap.ErrInvalidArgument)
	}
	if _, dup := s.NPES[c.UUID]; dup {
		return nil, fmt.Errorf("%w: 怪物 %s 已在图内", scenemap.ErrAlreadyExists, c.UUID)
	}
	pos, pixel := m.resolvePos(c.GridPos, c.Pixel)
	if !m.cfg.Size.Contains(pos) {
		return nil, fmt.Errorf("%w: 落点 %s 不在地图内", scenemap.ErrOutOfRange, pos)
	}

	radius := c.Radius
	if radius < 0 {
		radius = 0
	}
	actual, ok := m.findMonsterCell(s, pos, radius, 3)
	if !ok {
		return nil, fmt.Errorf("%w: %s 附近没有可放置的空位", scenemap.ErrBlocked, pos)
	}

	entered, err := s.Index.Enter(c.UUID, actual, EnterOption{
		Kind:         scenemap.EntityKindNPE,
		Radius:       radius,
		AllowOverlap: false,
		// 上面的 findMonsterCell 已经找好位置，这里只允许原地放置；
		// 若这中间被别的命令抢占（不可能，单写者），宁可报错也不要
		// 静默挪走——那会让刷怪器的"落点"与实际位置对不上。
		MaxSearchR: 1,
	})
	if err != nil {
		return nil, err
	}

	hp := c.HP
	if c.MaxHP > 0 && hp <= 0 {
		hp = c.MaxHP
	}
	info := &NpeAIInfo{
		UUID:           c.UUID,
		TemplateID:     c.TemplateID,
		Kind:           scenemap.EntityKindNPE,
		GridPos:        entered,
		Pixel:          pixel,
		Dir:            c.Dir,
		Radius:         radius,
		HP:             hp,
		MaxHP:          c.MaxHP,
		AI:             AIStateIdle,
		MasterUUID:     c.MasterUUID,
		BornAt:         m.now(),
		SummonExpireAt: c.SummonExpireAt,
	}
	s.NPES[c.UUID] = info
	s.Touch()

	m.aoiEnter(AOIEntity{
		UUID:       c.UUID,
		Kind:       scenemap.EntityKindNPE,
		GridPos:    entered,
		Pixel:      pixel,
		Dir:        c.Dir,
		MasterUUID: c.MasterUUID,
	})

	// TODO(规格 5.5 小地图): 怪物进图向小地图推一个红点（按阵营/类型着色）。
	// 接口未定稿，不伪造实现。

	return EnterReply{UUID: c.UUID, Kind: scenemap.EntityKindNPE, GridPos: entered, Pixel: pixel}, nil
}

// enterAttachment 宠物/召唤/镜像进图：登记主子关系 + 上索引（允许重叠）。
func (m *Mgr) enterAttachment(s *State, c CommandEnterPlayer, kind scenemap.EntityKind) (any, error) {
	if !c.UUID.Valid() {
		return nil, fmt.Errorf("%w: uuid 为 0", scenemap.ErrInvalidArgument)
	}
	if c.MasterUUID == 0 {
		return nil, fmt.Errorf("%w: %s 缺少主人", scenemap.ErrInvalidArgument, kind)
	}
	if _, ok := s.Users[c.MasterUUID]; !ok {
		// 主人不在图内却先来了宠物：这是"进图顺序错了"，必须显式报错。
		// 允许它进图会让关图计时把宠物当玩家、让 AOI 的可见性规则失效。
		return nil, fmt.Errorf("%w: 主人 %s 不在图内", scenemap.ErrNotFound, c.MasterUUID)
	}
	if _, dup := s.Pets[c.UUID]; dup {
		return nil, fmt.Errorf("%w: 附属实体 %s 已在图内", scenemap.ErrAlreadyExists, c.UUID)
	}

	pos, pixel := m.resolvePos(c.GridPos, c.Pixel)
	if !m.cfg.Size.Contains(pos) {
		return nil, fmt.Errorf("%w: 落点 %s 不在地图内", scenemap.ErrOutOfRange, pos)
	}
	actual, err := s.Index.Enter(c.UUID, pos, EnterOption{
		Kind:         kind,
		AllowOverlap: true, // 宠物站在主人身上是常态
	})
	if err != nil {
		return nil, err
	}

	s.Pets[c.UUID] = c.MasterUUID
	s.Touch()
	m.aoiEnter(AOIEntity{
		UUID:       c.UUID,
		Kind:       kind,
		GridPos:    actual,
		Pixel:      pixel,
		Dir:        c.Dir,
		MasterUUID: c.MasterUUID,
	})
	return EnterReply{UUID: c.UUID, Kind: kind, GridPos: actual, Pixel: pixel}, nil
}

// enterBullet 飞行物进图：只登记在 Bullets，不进格子索引。
//
// 子弹不占位、不阻挡：放进索引会让 blockCount 在几百发子弹时剧烈抖动，
// 还会把"怪物落点判空"搞成随机失败。AOI 用像素坐标处理它。
func (m *Mgr) enterBullet(s *State, c CommandEnterPlayer) (any, error) {
	if !c.UUID.Valid() {
		return nil, fmt.Errorf("%w: uuid 为 0", scenemap.ErrInvalidArgument)
	}
	if _, dup := s.Bullets[c.UUID]; dup {
		return nil, fmt.Errorf("%w: 子弹 %s 已存在", scenemap.ErrAlreadyExists, c.UUID)
	}
	pos, pixel := m.resolvePos(c.GridPos, c.Pixel)
	b := &Bullet{
		UUID:  c.UUID,
		Owner: c.MasterUUID,
		Pos:   pos,
		Pixel: pixel,
		Dir:   c.Dir,
	}
	s.Bullets[c.UUID] = b
	s.Touch()
	m.aoiEnter(AOIEntity{
		UUID:    c.UUID,
		Kind:    scenemap.EntityKindBullet,
		GridPos: pos,
		Pixel:   pixel,
		Dir:     c.Dir,
	})
	return EnterReply{UUID: c.UUID, Kind: scenemap.EntityKindBullet, GridPos: pos, Pixel: pixel}, nil
}

// entityLeave 实体离图。
//
// 顺序（规格 5.6）：光环离开判定（占位）→ 删索引/动态阻挡 →
// 玩家 PlayerLeave → 关图计时 → 怪物删 NPES/小地图。
//
// PlayerLeave 放在索引清理之后：玩法拿到的 s 已经是"这个人不在了"的状态，
// 此时再读 Users/NPES 不会读到脏数据。反过来（先回调再清理）会让玩法里
// 的"遍历当前玩家给他们发消息"包含一个已经走掉的人。
func (m *Mgr) entityLeave(s *State, uuid scenemap.UUID, reason scenemap.LeaveReason) (any, error) {
	now := m.now()

	if _, ok := s.Users[uuid]; ok {
		m.auraOnLeave(s, uuid)
		// 先随主人一起清掉宠物/召唤，再从索引里删主人：
		// 反过来的话 BlockedFor 会有一瞬间把宠物当成"没有主人的路人"。
		m.leaveAttachments(s, uuid)

		s.Index.Leave(uuid)
		delete(s.Users, uuid)
		delete(s.paths, uuid)

		if err := m.gameplay.PlayerLeave(uuid, reason, s); err != nil {
			// 状态已提交，不回滚：玩家已经走了，回滚等于把人"变回来"，
			// 比丢一次玩法通知更糟。只记录，交给玩法自己补偿。
			m.deps.Logger.Warn("scenemap/mgr: PlayerLeave 失败",
				"key", m.cfg.Key.Key(), "uuid", uuid, "err", err)
		}

		m.onPlayerLeave(s, reason, now)
		s.Touch()
		m.aoiLeave(uuid)
		return LeaveReply{UUID: uuid, Kind: scenemap.EntityKindPlayer}, nil
	}

	if _, ok := s.NPES[uuid]; ok {
		m.auraOnLeave(s, uuid)
		s.Index.Leave(uuid) // 体积怪的动态阻挡在这里一并回收
		delete(s.NPES, uuid)
		s.Touch()
		m.aoiLeave(uuid)
		// TODO(规格 5.6 小地图): 这里要移除小地图上的怪点。
		// 接口未定稿，不伪造实现。
		return LeaveReply{UUID: uuid, Kind: scenemap.EntityKindNPE}, nil
	}

	if _, ok := s.Pets[uuid]; ok {
		s.Index.Leave(uuid)
		delete(s.Pets, uuid)
		s.Touch()
		m.aoiLeave(uuid)
		return LeaveReply{UUID: uuid, Kind: scenemap.EntityKindPet}, nil
	}

	if _, ok := s.Bullets[uuid]; ok {
		delete(s.Bullets, uuid)
		s.Touch()
		m.aoiLeave(uuid)
		return LeaveReply{UUID: uuid, Kind: scenemap.EntityKindBullet}, nil
	}

	return nil, fmt.Errorf("%w: uuid=%s 不在图内", scenemap.ErrNotFound, uuid)
}

// leaveAttachments 清掉某个主人的全部宠物/召唤（进图顺序的逆序）。
//
// 先收集再删：直接在 range 里 delete 虽然 Go 允许，但会产生"删一半时
// 索引与 Pets 短暂不一致"的窗口，且遍历顺序随机会让 AOI 的 Leave 顺序
// 不稳定。排序后再删，行为可复现。
func (m *Mgr) leaveAttachments(s *State, master scenemap.UUID) {
	var ids []scenemap.UUID
	for id, mn := range s.Pets {
		if mn == master {
			ids = append(ids, id)
		}
	}
	if len(ids) == 0 {
		return
	}
	sortUUIDs(ids)
	for _, id := range ids {
		s.Index.Leave(id)
		delete(s.Pets, id)
		m.aoiLeave(id)
	}
}

// findMonsterCell 从 want 开始向内→外找一个"静态可走 + 无人占位"的格。
//
// grid.Ring 的顺序是固定的（上边→下边→左边→右边），所以同样的地图状态
// 一定得到同样的落点。r=0 时 Ring 返回中心格本身。
func (m *Mgr) findMonsterCell(s *State, want scenemap.GridPos, radius int32, maxR int32) (scenemap.GridPos, bool) {
	for r := int32(0); r <= maxR; r++ {
		for _, cand := range grid.Ring(want, r) {
			if !m.cfg.Size.Contains(cand) {
				continue
			}
			if m.deps.Walkable != nil && !m.deps.Walkable(cand) {
				continue
			}
			if s.Index.CanPlace(cand, radius) {
				return cand, true
			}
		}
	}
	return want, false
}

// resolvePos 统一"格坐标 / 像素坐标"的输入约定。
//
// 客户端/服务端调用方可能只给其中一个：
//   - 只给像素（最常见，位置同步）→ 用 CellOf 换算；
//   - 只给格（刷怪器、传送）→ 用格中心补像素；
//   - 两个都给 → 格为准，像素原样保留（像素可能是精确落点）。
func (m *Mgr) resolvePos(pos scenemap.GridPos, pixel scenemap.PixelPos) (scenemap.GridPos, scenemap.PixelPos) {
	zeroPixel := pixel == scenemap.PixelPos{}
	zeroGrid := pos == scenemap.GridPos{}
	switch {
	case zeroPixel:
		return pos, m.cfg.Geo.CellCenter(pos)
	case zeroGrid:
		return m.cfg.Geo.CellOf(pixel.X, pixel.Y), pixel
	default:
		return pos, pixel
	}
}

// auraOnLeave 是"光环/领域效果离开判定"的显式占位。
//
// 规格 5.6 要求离图时先做光环离开判定（离开光环范围要移除 buff）。
// 光环系统属于玩法（buff 数值在玩法侧），接口尚未定稿。**这里不写假实现**：
// 一个空的 aura 结构只会让人误以为功能已完成。接入时在此调用
// m.gameplay 的光环回调或 aura 包。
func (m *Mgr) auraOnLeave(s *State, uuid scenemap.UUID) {
	_ = s
	_ = uuid
	// TODO(规格 5.6): 光环/领域离开结算。
}

// sortUUIDs 对 UUID 切片升序排序（小工具，避免到处 import sort）。
func sortUUIDs(ids []scenemap.UUID) {
	for i := 1; i < len(ids); i++ {
		for j := i; j > 0 && ids[j] < ids[j-1]; j-- {
			ids[j], ids[j-1] = ids[j-1], ids[j]
		}
	}
}
