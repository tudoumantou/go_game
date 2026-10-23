package scenemap

import (
	"fmt"
	"strconv"
	"strings"
)

// UUID 是场景内的实体编号（玩家/怪物/宠物/召唤/子弹/镜像）。
//
// 用 int64 而不是 uint64：0 要留给"无效"，负数在调试里一眼能看出
// "这是占位/未初始化"，比 0 和 1 的差别更醒目。玩家 UUID 与全局角色 ID
// 同源，怪物 UUID 由 MGR 从负数段分配（-1 递减），两类永不冲突。
type UUID int64

// Valid 报告是否为有效 UUID。
func (u UUID) Valid() bool { return u != 0 }

// String 便于日志与 Actor 命名。
func (u UUID) String() string { return strconv.FormatInt(int64(u), 10) }

// EntityKind 实体类型。决定「进图走哪条分支」「能不能有 AI」「收不收广播」。
//
// 枚举值会写入协议与快照，只能追加不能重排。
type EntityKind int32

const (
	EntityKindNone EntityKind = 0
	// EntityKindPlayer 真人玩家。场景内只存 UUID 与只读快照，
	// 属性权威在玩家自己的 Actor。
	EntityKindPlayer EntityKind = 1
	// EntityKindNPE 怪物/NPC（Non-Player Entity）。
	// 状态完全私有于 MGR，跨 Actor 读只读快照。
	EntityKindNPE EntityKind = 2
	// EntityKindPet 宠物。跟随主人，属性同玩家。
	EntityKindPet EntityKind = 3
	// EntityKindSummon 召唤物。同宠物，但可能有存活时间。
	EntityKindSummon EntityKind = 4
	// EntityKindBullet 飞行物。只有位置与速度，没有 AI。
	EntityKindBullet EntityKind = 5
	// EntityKindMirror 前端代控镜像。只允许白名单地图移动，
	// 服务端不跑 AI（对应原实现的 CLIENT 态）。
	EntityKindMirror EntityKind = 6
)

// IsPlayerSide 报告是否属于"玩家一侧"（玩家/宠物/召唤）。
// 视野规则里非玩家实体只与玩家互相可见，因此这个判定是热点。
func (k EntityKind) IsPlayerSide() bool {
	return k == EntityKindPlayer || k == EntityKindPet || k == EntityKindSummon
}

// IsActor 报告是否需要独立 Actor。本框架只有玩家有（且不由 scenemap 管理），
// 其余实体全部活在 MGR 里——这个方法存在的意义是让调用方显式想一想，
// 而不是顺手给怪物起 goroutine。
func (k EntityKind) IsActor() bool { return k == EntityKindPlayer }

func (k EntityKind) String() string {
	switch k {
	case EntityKindPlayer:
		return "PLAYER"
	case EntityKindNPE:
		return "NPE"
	case EntityKindPet:
		return "PET"
	case EntityKindSummon:
		return "SUMMON"
	case EntityKindBullet:
		return "BULLET"
	case EntityKindMirror:
		return "MIRROR"
	default:
		return fmt.Sprintf("EntityKind(%d)", int32(k))
	}
}

// MapKey 是地图的唯一标识，三种形态收敛成这一个结构（规格 3.1）。
//
//	公共图       {MapID}                      CliKey 为空、Node 为空
//	本服房间图   {CliKey, MapID}              副本/秘境/竞技场
//	跨节点房间   {CliKey, MapID, Node}        试炼塔/组队 Boss
//	跨服房间     {CliKey, MapID, Node, Host}  再外包一层
//
// 为什么要 Key() 字符串化：Actor 注册表、日志、房间登记表都按字符串寻址，
// 三处各拼一次格式必然漂移（"map:1" vs "map:1:"），所以只允许一个实现。
type MapKey struct {
	// CliKey 房间/队伍/玩家标识；公共图为空。
	CliKey string
	// MapID 地图配置 ID。
	MapID int32
	// Node 目标节点；空表示本节点。
	Node string
	// Host 跨服主机标识；非空时 Node 是跨服语义。
	Host string
}

// Key 返回稳定的字符串键：`map:{MapID}:{CliKey}@{Node}#{Host}`。
//
// CliKey 可能包含 ':'（例如 "team:123"），所以用 '@' 分隔节点、
// 用 '#' 分隔主机——两个分隔符都不允许出现在 CliKey 里（由 Validate 保证）。
func (k MapKey) Key() string {
	var b strings.Builder
	b.WriteString("map:")
	b.WriteString(strconv.Itoa(int(k.MapID)))
	if k.CliKey != "" {
		b.WriteString(":")
		b.WriteString(k.CliKey)
	}
	if k.Node != "" {
		b.WriteString("@")
		b.WriteString(k.Node)
	}
	if k.Host != "" {
		b.WriteString("#")
		b.WriteString(k.Host)
	}
	return b.String()
}

// IsPublic 报告是否为公共图（无 CliKey）。
func (k MapKey) IsPublic() bool { return k.CliKey == "" }

// IsCrossNode 报告是否需要跨节点投递。
func (k MapKey) IsCrossNode() bool { return k.Node != "" }

// Validate 校验 Key 的三段能否安全拼接。
//
// 分隔符冲突是"看起来正常、日志里全糊在一起"的典型事故来源：
// 两个不同的 MapKey 生成同一个字符串，Actor 注册表会互相覆盖。
func (k MapKey) Validate() error {
	if k.MapID <= 0 {
		return fmt.Errorf("%w: map_id 必须为正, got %d", ErrInvalidArgument, k.MapID)
	}
	if strings.ContainsAny(k.CliKey, "@#") {
		return fmt.Errorf("%w: cli_key 不能包含 '@' 或 '#': %q", ErrInvalidArgument, k.CliKey)
	}
	if strings.ContainsAny(k.Node, "@#") {
		return fmt.Errorf("%w: node 不能包含 '@' 或 '#'", ErrInvalidArgument)
	}
	return nil
}

// GridPos 是行走格坐标（左上角原点，X 向右、Y 向下）。
//
// 行走格决定：站位、阻挡、技能射程、刷怪落点、寻路。
// 它不等于 AOI 视野格（后者 = 行走格 × aoi_grid，默认 6）。
// 两者混用会让"视野 6 倍"变成"视野 1 倍"，是这套框架最常见的 bug。
type GridPos struct {
	X int32
	Y int32
}

// Add 返回偏移后的格（不修改自身：GridPos 是值类型，误改副本很容易）。
func (p GridPos) Add(dx, dy int32) GridPos { return GridPos{X: p.X + dx, Y: p.Y + dy} }

// Valid 报告是否为非负坐标。地图原点 (0,0) 合法，负数一律非法。
func (p GridPos) Valid() bool { return p.X >= 0 && p.Y >= 0 }

func (p GridPos) String() string { return fmt.Sprintf("(%d,%d)", p.X, p.Y) }

// PixelPos 是像素级坐标（等距投影前的世界坐标）。
//
// 权威位置在 MGR 的格子索引；像素位置只用于 AOI 决定广播范围与客户端表现。
// 三者关系（规格 6.8）：MGR 格子（权威）→ AOI 像素（广播）→ 存档 → 客户端。
type PixelPos struct {
	X int32
	Y int32
}

// Dir 是 8 方向朝向。0 表示"未设置"（保留给"没有移动过"的实体）。
//
// 方向值定义：1=正右，2=右下，3=正下，4=左下，5=左，6=左上，7=上，8=右上。
// 用显式常量而不是枚举位移运算，是因为日志里看"方向 3"比"方向 0b011"直观。
type Dir int32

const (
	DirNone      Dir = 0
	DirRight     Dir = 1
	DirDownRight Dir = 2
	DirDown      Dir = 3
	DirDownLeft  Dir = 4
	DirLeft      Dir = 5
	DirUpLeft    Dir = 6
	DirUp        Dir = 7
	DirUpRight   Dir = 8
)

// LeaveReason 离图原因。决定关图计时怎么走（规格 4.4）。
type LeaveReason int32

const (
	// LeaveReasonQuit 玩家主动退图。
	LeaveReasonQuit LeaveReason = 1
	// LeaveReasonOffline 掉线。按地图参数中的 offline_time 延迟关图。
	LeaveReasonOffline LeaveReason = 2
	// LeaveReasonSwitch 切图（旧图离场）。旧图按"其他原因"重置 enter_time 保护。
	LeaveReasonSwitch LeaveReason = 3
	// LeaveReasonKick 被踢（封禁/顶号）。
	LeaveReasonKick LeaveReason = 4
	// LeaveReasonClose 关图强制离场。
	LeaveReasonClose LeaveReason = 5
)

func (r LeaveReason) String() string {
	switch r {
	case LeaveReasonQuit:
		return "QUIT"
	case LeaveReasonOffline:
		return "OFFLINE"
	case LeaveReasonSwitch:
		return "SWITCH"
	case LeaveReasonKick:
		return "KICK"
	case LeaveReasonClose:
		return "CLOSE"
	default:
		return fmt.Sprintf("LeaveReason(%d)", int32(r))
	}
}

// Route 是分线号。0 表示"由动态平衡器决定"（动态多线公共图）。
//
// 不要把 Route 当场景编号：一个 MapKey + Route 才唯一确定一个 MGR 实例。
// 主城 1 线、2 线是两个独立的 MGR。
type Route int32
