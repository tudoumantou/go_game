package battle

import (
	"game_study/internal/scenemap"
)

// Sprite 是参与战斗结算的实体属性（规格 §3.8 的 #sprite{}）。
//
// # 与场景权威的关系
//
//   - NPC：HP 与 npc 表里的 NpeAIInfo **双写**——battle 结算后立即回写
//     NpeAIInfo.HP，保证"场景看到的血量 = 战斗看到的血量"；
//   - 玩家/宠物/召唤：属性权威在玩家 Actor，本结构是**只读投影 + 结算缓存**；
//     伤害结果通过 SkillResultEvent 回给主人，由玩家 Actor 写自己的状态。
//
// 所有系数用万分比（10000 = 1.0），与 config.go 的约定一致。
type Sprite struct {
	// UUID 实体编号。
	UUID scenemap.UUID
	// Kind 实体类型（玩家/怪/宠物/召唤/镜像）。
	Kind scenemap.EntityKind
	// Owner 主人（宠物/召唤/镜像）；玩家为自己，怪物为 0。
	Owner scenemap.UUID
	// CfgID 配置 ID（怪物为 cfg_npc，玩家为职业/角色模板）。
	CfgID int32
	// Camp 阵营。选敌与 PK 模式都读它。
	Camp int32

	// ---- 生命 ----
	// HP 当前生命（NPC 回写 NpeAIInfo）。
	HP int64
	// MaxHP 最大生命。
	MaxHP int64
	// Shield 护盾值（受击优先扣，扣完为 0）。
	Shield int64
	// Spirit 灵力：1 点抵 2 点伤害（规格 r/t_real_harm）。
	Spirit int64

	// ---- 攻防 ----
	Atk int64
	Def int64
	// RealAtk/RealDef 公式末尾的固定加减（穿防后的真实值）。
	RealAtk int64
	RealDef int64
	// IgnoreDef 无视防御比例（万分比）。
	IgnoreDef int64

	// ---- 命中 ----
	Hit       int64
	IgMonHit  int64
	Dod       int64
	IgnoreDod int64
	// Crit 暴击率（万分比）；ReCrit 抗暴。
	Crit   int64
	ReCrit int64
	// CritDmgInc/Dec 暴伤增减（万分比，10000 = +1.0 倍）。
	CritDmgInc int64
	CritDmgDec int64
	// LuckyHit 会心率；ReLuckyHit 抗会心。
	LuckyHit    int64
	ReLuckyHit  int64
	LuckyHitInc int64
	LuckyHitDec int64

	// ---- 格挡 ----
	Bok       int64
	IgnoreBok int64
	BokDmgInc int64
	BokDmgDec int64

	// ---- 增伤 ----
	// AddDmgCoe / FinalAddDmgCoe 增伤系数（万分比，后者下限 0.1）。
	AddDmgCoe      int64
	FinalAddDmgCoe int64
	// Discriminate 歧视（对特定类型的增伤，万分比）。
	Discriminate int64
	// AddPetDmg / DecPetDmg 宠物/召唤增伤与减伤。
	AddPetDmg int64
	DecPetDmg int64
	// MapTypeDmg 地图类型增伤（万分比）。
	MapTypeDmg int64
	// PkPress PK 压制（万分比，减少来自玩家的伤害）。
	PkPress int64
	// ElementCoe 元素克制系数（万分比）。
	ElementCoe int64

	// ---- 反伤/吸血 ----
	// ThornsRate 反伤概率（万分比）；ReThornsRate 抗反伤。
	ThornsRate   int64
	ReThornsRate int64
	// ThornsCoe 反伤系数（万分比，乘攻击者攻击力）。
	ThornsCoe int64

	// ---- 节奏 ----
	// SpeedAtk 攻速系数（万分比，10000 = 1.0；越大前摇越短）。
	SpeedAtk int64

	// ---- 状态 ----
	// Buffs 当前 Buff（顺序即施加顺序，便于复现）。
	Buffs []*Buff
	// Fight 是否处于战斗状态（出手/受击置位，超时脱战）。
	Fight bool
	// Pause 战斗暂停（神像/引导等），暂停时不结算也不受击。
	Pause bool
	// Dead 是否已死亡（与 HP<=0 同步维护）。
	Dead bool
	// LastFightAt 最近一次进战时间（unix 毫秒）。
	LastFightAt int64

	// ---- 位置 ----
	// Pos 当前格（与 MGR 索引同步；只读投影）。
	Pos scenemap.GridPos
	// Dir 当前朝向。
	Dir scenemap.Dir
}

// Alive 报告是否存活且未进入死亡态。
func (s *Sprite) Alive() bool { return s != nil && !s.Dead && s.HP > 0 }

// IsNpc 报告是否为怪物（死亡链只处理 NPC）。
func (s *Sprite) IsNpc() bool { return s != nil && s.Kind == scenemap.EntityKindNPE }

// EffAtk 返回含 Buff 加成后的攻击力。
//
// 只累加 BuffAttr 类且 AttrField=="atk" 的层数；其他来源（装备/光环）
// 由玩家 Actor 在同步属性时算好，battle 不重复计算——两份计算必然漂移。
func (s *Sprite) EffAtk() int64 {
	v := s.Atk
	for _, b := range s.Buffs {
		if b != nil && b.Kind == BuffAttr && b.AttrField == "atk" {
			v += b.Value * int64(maxInt32(b.Stacks, 1))
		}
	}
	if v < 0 {
		v = 0
	}
	return v
}

// EffDef 返回含 Buff 加成后的防御力。
func (s *Sprite) EffDef() int64 {
	v := s.Def
	for _, b := range s.Buffs {
		if b != nil && b.Kind == BuffAttr && b.AttrField == "def" {
			v += b.Value * int64(maxInt32(b.Stacks, 1))
		}
	}
	if v < 0 {
		v = 0
	}
	return v
}

// HasBuffKind 报告是否持有某类未过期的 Buff。
//
// 热路径（每次出手都要问"禁攻了吗"），Buffs 通常个位数，线性扫描比
// 维护一张 kind→count 的辅助表更不容易出错。now 显式传入而不是读
// 全局时钟：一次结算内所有判断必须用同一个时间点，否则同一条链路里
// "出手时没禁攻、结算时禁攻了"会给出互相矛盾的结论。
func (s *Sprite) HasBuffKind(kind BuffKind, now int64) bool {
	if s == nil {
		return false
	}
	for _, b := range s.Buffs {
		if b != nil && b.Kind == kind && !b.Expired(now) {
			return true
		}
	}
	return false
}

// BuffStacks 返回某个 Buff 的层数（不存在为 0）。
func (s *Sprite) BuffStacks(id int32) int32 {
	if s == nil {
		return 0
	}
	for _, b := range s.Buffs {
		if b != nil && b.ID == id {
			return b.Stacks
		}
	}
	return 0
}

// Buff 是一条已施加的 Buff 实例。
//
// 字段在施加时从 BuffConfig **快照**下来：配置热更不应该改变已经挂在
// 玩家身上的 Dot 数值（否则"更新配置导致全服掉血"这类事故无法解释）。
type Buff struct {
	// ID 配置 ID。
	ID int32
	// Kind 效果类别（配置快照）。
	Kind BuffKind
	// AttrField Attr 类加成的字段（配置快照）。
	AttrField string
	// Value 每跳/总量数值（配置快照）。
	Value int64
	// Stacks 层数。
	Stacks int32
	// From 施加者。
	From scenemap.UUID
	// SkillID 来源技能（伤害榜/日志用）。
	SkillID int32
	// AppliedAt 施加时间（unix 毫秒）。
	AppliedAt int64
	// ExpireAt 到期时间（unix 毫秒）。
	ExpireAt int64
	// NextTickAt 下次周期结算时间（unix 毫秒）。
	NextTickAt int64
	// TickMS 周期（配置快照）。
	TickMS int64
	// MaxStacks 最大层数（配置快照）。
	MaxStacks int32
	// Remain 护盾剩余可吸收量（仅 BuffShield 使用）。
	//
	// 护盾的权威存在这里而不是 Sprite.Shield：Sprite.Shield 只是汇总缓存，
	// 受击时按条扣减 Remain 才不会出现"盾破了但到期又冒出来"。
	Remain int64
}

// Expired 报告 Buff 是否已到期。
func (b *Buff) Expired(now int64) bool {
	return b != nil && b.ExpireAt > 0 && now >= b.ExpireAt
}

// maxInt32 返回较大值（本包内部小工具，避免引入 math）。
func maxInt32(a, b int32) int32 {
	if a > b {
		return a
	}
	return b
}

// clamp 把 v 限制在 [lo, hi]。
func clamp(v, lo, hi int64) int64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}
