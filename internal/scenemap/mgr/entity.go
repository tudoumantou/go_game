package mgr

import "game_study/internal/scenemap"

// 本文件定义"活在 MGR 里的实体"的数据结构，对应规格 3.3 的 NPES/Bullets/
// Firewells 以及第 6 章的 AI 状态。
//
// # 为什么这些类型放在 mgr 而不是 entity 包（重要约定）
//
// 依赖方向必须是单向的：
//
//	mgr      → 只依赖 scenemap（根包）与 grid（纯函数）
//	entity   → 可以依赖 mgr（读类型/做决策），mgr 绝不反向依赖 entity
//
// 如果 NpeAIInfo 定义在 entity 包，mgr 的 State 就必须 import entity，
// 而 entity 的 AI 决策函数又要 import mgr 的 State 才能读写——立刻形成
// 循环依赖。所以"数据"留在唯一写它的 Actor（mgr）里，"算法"以后放到
// entity：entity 用类型别名（type NpeAIInfo = mgr.NpeAIInfo）复用，
// 或者整体迁到 scenemap/model 这种双方都依赖的叶子包。
//
// 这条注释是给下一位改代码的人看的约定：不要为了"看起来分层"把数据
// 搬去 entity，那会把依赖图拧成环。

// AIState 是怪物 AI 状态机的状态。
//
// 与 entity 包将来实现的决策逻辑解耦：本包只存储、不解释。
// 枚举值会写入快照与协议，只能追加。
type AIState int32

const (
	// AIStateIdle 待机（原地，等待仇恨或巡逻点）。
	AIStateIdle AIState = 0
	// AIStatePatrol 巡逻。
	AIStatePatrol AIState = 1
	// AIStateChase 追击。
	AIStateChase AIState = 2
	// AIStateAttack 攻击（进入技能循环）。
	AIStateAttack AIState = 3
	// AIStateReturn 脱战回归（回血、回出生点）。
	AIStateReturn AIState = 4
	// AIStateDead 死亡，等待尸体消失/掉落结算。
	AIStateDead AIState = 5
	// AIStateStopped 时停（被控制，AI 不推进）。
	AIStateStopped AIState = 6
)

func (a AIState) String() string {
	switch a {
	case AIStateIdle:
		return "IDLE"
	case AIStatePatrol:
		return "PATROL"
	case AIStateChase:
		return "CHASE"
	case AIStateAttack:
		return "ATTACK"
	case AIStateReturn:
		return "RETURN"
	case AIStateDead:
		return "DEAD"
	case AIStateStopped:
		return "STOPPED"
	default:
		return "AIState(" + itoa(int64(a)) + ")"
	}
}

// HateEntry 是一条仇恨记录。
//
// 用切片而不是 map：怪物目标数是个位数，切片遍历的常数远小于 map，
// 且顺序可复现（map 遍历随机，会让"同仇恨值谁先被选中"在两次运行间
// 不同，线上无法复现 bug）。
type HateEntry struct {
	// UUID 仇恨来源（玩家/宠物/召唤）。
	UUID scenemap.UUID
	// Hate 仇恨值。
	Hate int64
	// UpdatedAt 最近一次加仇时间（unix 毫秒），用于仇恨衰减。
	UpdatedAt int64
}

// SkillEntry 是怪物技能槽的运行时状态。
type SkillEntry struct {
	SkillID int32
	Level   int32
	// CDUntil 冷却结束时间（unix 毫秒）。0 表示可用。
	CDUntil int64
	// NextAt 下一次允许释放的时间（unix 毫秒），用于施法前摇/后摇。
	NextAt int64
}

// NpeAIInfo 是一只怪物/NPC 的全部私有状态。
//
// 它只存在 MGR 的 map 里，没有独立 goroutine（一怪一 goroutine 会在
// 几百怪时产生 mailbox 风暴，见包 doc）。跨 Actor 想读只能读 MGR 发布
// 的只读快照。
type NpeAIInfo struct {
	UUID       scenemap.UUID
	TemplateID int32
	Kind       scenemap.EntityKind
	GridPos    scenemap.GridPos
	Pixel      scenemap.PixelPos
	Dir        scenemap.Dir
	// Radius 体积半径：0=占 1 格，r>0 占 (2r+1)² 格并写入动态阻挡。
	Radius int32

	HP    int64
	MaxHP int64
	// Speed 移动速度（格/秒 ×1000，避免浮点）。
	Speed int32

	// AI 当前状态机状态。
	AI AIState
	// Target 当前目标；scenemap.UUID(0) 表示无目标。
	Target scenemap.UUID
	// Hate 仇恨列表（顺序即优先级，由 AI 决策维护）。
	Hate []HateEntry
	// Skills 技能槽。
	Skills []SkillEntry

	// MasterUUID 召唤物的主人（怪物为 0）。
	MasterUUID scenemap.UUID
	// BornAt 出生时间（unix 毫秒）。
	BornAt int64
	// DeadAt 死亡时间（unix 毫秒），未死亡为 0。
	DeadAt int64
	// SummonExpireAt 召唤物到期时间（unix 毫秒）；0 表示永久。
	SummonExpireAt int64
	// NextThinkAt 下次 AI 决策时间（unix 毫秒）。
	NextThinkAt int64

	// Buffs buff 列表的占位（规格 7.2 的 buff 周期结算）。
	// 第一版只保存 ID，不解释数值——玩法在 entity 里实现。
	Buffs []int32
}

// Alive 报告是否存活（未死亡）。
func (n *NpeAIInfo) Alive() bool { return n != nil && n.DeadAt == 0 && n.AI != AIStateDead }

// UserState 是"玩家在场景内的只读投影"。
//
// 属性权威在玩家自己的 Actor；MGR 只存站位与 AOI 需要的少量字段。
// 千万不要在这里加 HP/背包这类数据——那会变成第二份权威，改一份漏一份。
type UserState struct {
	UUID    scenemap.UUID
	Kind    scenemap.EntityKind
	GridPos scenemap.GridPos
	Pixel   scenemap.PixelPos
	Dir     scenemap.Dir
	// State 客户端上报的动画/状态位（站立/跑动/施法），原样回广播。
	State int32
	// CharName 角色名，仅用于日志（不进协议）。
	CharName string
	// EnterAt 进图时间（unix 毫秒）。
	EnterAt int64
	// LastSyncAt 最近一次同步位置的时间（unix 毫秒），用于超时踢人/纠偏。
	LastSyncAt int64
	// MasterUUID 宠物/召唤的主人。
	MasterUUID scenemap.UUID
}

// Bullet 是飞行物的最小状态。
//
// 子弹不进格子索引：它不占位、不阻挡、生命周期极短（几十到几百毫秒），
// 放进索引只会让 blockCount 频繁抖动。AOI 广播与碰撞判定用像素坐标。
type Bullet struct {
	UUID     scenemap.UUID
	Owner    scenemap.UUID
	SkillID  int32
	Pos      scenemap.GridPos
	Pixel    scenemap.PixelPos
	Dir      scenemap.Dir
	Speed    int32
	ExpireAt int64
}

// Firewell 是持续伤害区域（火墙/毒池）。
//
// 同样不进格子索引：它是"区域效果"，不阻挡移动，只按 tick 结算。
type Firewell struct {
	UUID     scenemap.UUID
	Owner    scenemap.UUID
	GridPos  scenemap.GridPos
	Radius   int32
	Damage   int64
	ExpireAt int64
	// NextTickAt 下次结算时间（unix 毫秒）。
	NextTickAt int64
}

// Spawner 是一个刷怪器的运行时状态（规格 7.2）。
//
// 刷怪器是"遗留接口"：MGR 只负责按 tick 调用 RefreshFunc，
// 具体的刷怪规则由玩法通过 RefreshFunc 注入。这里保留数据结构，
// 让"刷怪进度"成为 State 的一部分（可快照、可回放）。
type Spawner struct {
	ID         int32
	TemplateID int32
	Center     scenemap.GridPos
	MinR       int32
	MaxR       int32
	MaxAlive   int32
	Alive      int32
	// IntervalMS 刷新间隔（毫秒）。
	IntervalMS int64
	// NextAt 下次刷新时间（unix 毫秒）。
	NextAt  int64
	Enabled bool
}

// RefreshData 是刷怪器的汇总运行时状态。
//
// 放在 State 而不是 Mgr 的私有字段：它是"玩法可见的场景状态"，
// MgrInit/Loop 可能读写它，刷新逻辑要能随 State 一起被测试构造。
type RefreshData struct {
	// Spawners 按刷怪器 ID 索引。
	Spawners map[int32]*Spawner
	// RandSeed 确定性随机种子：同一 seed 回放同一批落点，
	// 线上"这只怪为什么刷在墙里"只能靠回放查。
	RandSeed uint64
}

// NewRefreshData 创建空的刷怪器状态。
func NewRefreshData() *RefreshData {
	return &RefreshData{Spawners: make(map[int32]*Spawner)}
}

// itoa 是本文件的小工具：避免为了一个 AIState.String 引入 strconv 的
// 全部依赖语义（这里只是调试输出）。
func itoa(v int64) string {
	if v == 0 {
		return "0"
	}
	neg := v < 0
	if neg {
		v = -v
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
