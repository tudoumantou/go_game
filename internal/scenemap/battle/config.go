package battle

import (
	"fmt"

	"game_study/internal/scenemap"
)

// 系数统一用"万分比"（10000 = 1.0）。
//
// 为什么不用 float64：伤害链路要可复现、可回放，浮点在跨平台/编译器
// 优化下可能出现最后一位差异，而"同一场战斗两次跑出不同伤害"是最难
// 排查的一类线上问题。整数乘法 + 显式截断把舍入点固定在每一步。
const (
	// PermilleOne 是 1.0 的定点表示。
	PermilleOne int64 = 10000
)

// HarmType 是技能伤害类型（规格 §10.6.3 的 skill_harm_type）。
type HarmType int32

const (
	// HarmTypeNormal 普通伤害：走完整公式链。
	HarmTypeNormal HarmType = 0
	// HarmTypeNotDamage 纯效果技：只加 Buff，不结算伤害。
	HarmTypeNotDamage HarmType = 1
	// HarmTypeOnlyAOE 只结算范围伤（不判闪避/暴击）。
	HarmTypeOnlyAOE HarmType = 2
	// HarmTypeSuckBlood 只吸血（不结算伤害）。
	HarmTypeSuckBlood HarmType = 3
)

// TargetType 是选敌类型（规格 §10.4 的 ttype）。
type TargetType int32

const (
	// TargetEnemy 敌方。
	TargetEnemy TargetType = 0
	// TargetAlly 友方（含自己）。
	TargetAlly TargetType = 1
	// TargetSelf 只对自己。
	TargetSelf TargetType = 2
	// TargetAll 不分阵营。
	TargetAll TargetType = 3
)

// AreaKind 是范围形状。
type AreaKind int32

const (
	// AreaSingle 单体。
	AreaSingle AreaKind = 0
	// AreaCircle 圆形（切比雪夫半径）。
	AreaCircle AreaKind = 1
	// AreaSector 扇形（朝向 ± halfAngle）。
	AreaSector AreaKind = 2
	// AreaRect 矩形（以目标格为中心的方形，示意"直线穿透"）。
	AreaRect AreaKind = 3
)

// EffectKind 是命中后的额外实体。
type EffectKind int32

const (
	// EffectNone 无。
	EffectNone EffectKind = 0
	// EffectBullet 生成飞行物，到达后结算子技能。
	EffectBullet EffectKind = 1
	// EffectFirewell 生成地面持续伤害区域。
	EffectFirewell EffectKind = 2
)

// SkillConfig 是一条技能配置（字段与 §10.6 的公式参数一一对应）。
//
// 生产环境应从 XML/DB 表生成；这里用内存表，是为了让"改一个系数"
// 不需要起数据库，测试也能直接构造。
type SkillConfig struct {
	// ID 技能 ID（唯一、稳定）。
	ID int32
	// Name 名称（日志用）。
	Name string

	// ---- 伤害 ----
	// HarmType 伤害类型（普通/纯效果/只 AOE/只吸血）。
	HarmType HarmType
	// Coeff 技能系数（万分比，规格 skill_coe）。
	Coeff int64
	// Harm 固定加伤（规格 skill_harm）。
	Harm int64
	// PercentDmg 目标当前生命的百分比伤害（万分比，规格 percent_dmg）。
	PercentDmg int64
	// SeckillPermille 对小怪的斩杀概率（万分比）。
	SeckillPermille int64

	// ---- 射程与目标 ----
	// Range 射程（格，切比雪夫）。
	Range int32
	// TargetNum 最多命中人数（0 视为 1；范围技按距离截断）。
	TargetNum int32
	// TargetType 选敌类型。
	TargetType TargetType
	// Area 范围形状；AreaSingle 表示单体。
	Area AreaKind
	// AreaRadius 圆形/矩形半径（格）。
	AreaRadius int32
	// SectorDeg 扇形半角（度）。
	SectorDeg int32

	// ---- 节奏 ----
	// PrefixMS 基础前摇（毫秒，规格 base_prefix）。
	PrefixMS int64
	// Parts 伤害段数（规格 numbers_damage_part，0/1 视为 1 段）。
	Parts int32
	// PartIntervalMS 段间隔（毫秒，实际按攻速缩放）。
	PartIntervalMS int64
	// RemoteDelayPerGridMS 远程弹道每格追加延迟（规格 effect_args ms）。
	RemoteDelayPerGridMS int64
	// CooldownMS 技能冷却（毫秒）。
	CooldownMS int64

	// ---- 效果 ----
	// LaunchBuffs 前摇阶段加给目标的 Buff。
	LaunchBuffs []int32
	// HitBuffs 命中阶段加给目标的 Buff。
	HitBuffs []int32
	// SelfBuffs 加给自己的 Buff。
	SelfBuffs []int32
	// TimeStopMS 命中后附加的时停（毫秒，0 = 不加）。
	TimeStopMS int64
	// Effect 命中后的额外实体。
	Effect EffectKind
	// BulletSpeed 子弹速度（格/秒；<=0 用默认）。
	BulletSpeed int32
	// SubSkillID 子弹命中/火墙 tick 时结算的子技能。
	SubSkillID int32
	// FirewellRadius / FirewellTickMS / FirewellDamage 火墙参数。
	FirewellRadius int32
	FirewellTickMS int64
	FirewellDamage int64
	// ReboundPermille 反弹概率（万分比，0 = 不反弹）。
	ReboundPermille int64
	// SuckPermille 吸血比例（万分比，规格 SUCK_BLOOD）。
	SuckPermille int64
	// PassiveIDs 释放时触发的被动 ID 列表。
	PassiveIDs []int32
	// MoreActiveSkillID 连击技（MORE_ACTV，0 = 无）。
	MoreActiveSkillID int32
}

// BuffKind 是 Buff 的效果类别。
type BuffKind int32

const (
	// BuffAttr 属性类（当前只支持 Atk/Def 加成，按 Value 折算）。
	BuffAttr BuffKind = 0
	// BuffDot 持续掉血（每 TickMS 扣 Value）。
	BuffDot BuffKind = 1
	// BuffHot 持续回血。
	BuffHot BuffKind = 2
	// BuffStun 时停/眩晕（禁止施法，AI 暂停由装配层处理）。
	BuffStun BuffKind = 3
	// BuffBanAttack 禁攻（禁止一切技能）。
	BuffBanAttack BuffKind = 4
	// BuffSuper 无敌（伤害直接按 SUPER 结算为 0）。
	BuffSuper BuffKind = 5
	// BuffUndie 免死（致死伤害压到 1 点）。
	BuffUndie BuffKind = 6
	// BuffShield 护盾（Value 为可吸收量，随伤害扣减）。
	BuffShield BuffKind = 7
	// BuffSlow 减速（移速由装配层读取，battle 只存状态）。
	BuffSlow BuffKind = 8
)

// BuffConfig 是一条 Buff 配置。
type BuffConfig struct {
	// ID Buff ID。
	ID int32
	// Name 名称（日志用）。
	Name string
	// Kind 效果类别。
	Kind BuffKind
	// DurationMS 持续时间（毫秒）。
	DurationMS int64
	// TickMS 周期结算间隔（Dot/Hot 用；<=0 表示不周期结算）。
	TickMS int64
	// Value 数值（Dot/Hot 的每跳伤害、Shield 的吸收量、Attr 的加成值）。
	Value int64
	// AttrField Attr 类加成的字段（"atk"/"def"）。
	AttrField string
	// MaxStacks 最大叠层（0/1 = 不叠加，刷新时长）。
	MaxStacks int32
	// Replace 同名 Buff 是否顶替（true 顶替，false 追加层数）。
	Replace bool
}

// NpcConfig 是怪物/NPC 的战斗属性模板（规格 cfg_npc 的简化）。
//
// 只放"参与结算"的字段；血量仍以 mgr.NpeAIInfo 为准（场景权威），
// 这里只补充攻击/防御/命中这类战斗属性。
type NpcConfig struct {
	CfgID int32
	Name  string
	// MaxHP 模板血量（刷怪时若 NpeAIInfo 未给血量，用它初始化）。
	MaxHP int64
	Atk   int64
	Def   int64
	Hit   int64
	Dod   int64
	Crit  int64
	// Camp 阵营（0 = 中立怪）。
	Camp int32
	// Skills 主动技能（按优先级；AI 目前只读第一个）。
	Skills []int32
}

// Table 是战斗配置表的集合。
//
// 查不到技能/ Buff 时显式报错（规格 §13.13 的"不默认兜底"）：
// 默认成普攻会让"技能配错"表现为"伤害不对"，比开图失败难查得多。
type Table struct {
	// Skills 技能表。
	Skills map[int32]*SkillConfig
	// Buffs Buff 表。
	Buffs map[int32]*BuffConfig
	// Npcs 怪物模板表。
	Npcs map[int32]*NpcConfig
	// Player 玩家默认战斗属性（玩家 Actor 未同步属性时使用）。
	Player PlayerTemplate
}

// PlayerTemplate 是玩家未同步属性时的兜底模板。
type PlayerTemplate struct {
	MaxHP int64
	Atk   int64
	Def   int64
	Hit   int64
	Dod   int64
	Crit  int64
	Camp  int32
}

// Skill 按 ID 查技能。
func (t *Table) Skill(id int32) (*SkillConfig, error) {
	if t == nil || t.Skills == nil {
		return nil, fmt.Errorf("%w: 技能表为空", scenemap.ErrNotFound)
	}
	cfg, ok := t.Skills[id]
	if !ok || cfg == nil {
		return nil, fmt.Errorf("%w: 技能 %d", scenemap.ErrNotFound, id)
	}
	return cfg, nil
}

// Buff 按 ID 查 Buff。
func (t *Table) Buff(id int32) (*BuffConfig, error) {
	if t == nil || t.Buffs == nil {
		return nil, fmt.Errorf("%w: Buff 表为空", scenemap.ErrNotFound)
	}
	cfg, ok := t.Buffs[id]
	if !ok || cfg == nil {
		return nil, fmt.Errorf("%w: Buff %d", scenemap.ErrNotFound, id)
	}
	return cfg, nil
}

// Npc 按 ID 查怪物模板。
func (t *Table) Npc(id int32) (*NpcConfig, error) {
	if t == nil || t.Npcs == nil {
		return nil, fmt.Errorf("%w: 怪物表为空", scenemap.ErrNotFound)
	}
	cfg, ok := t.Npcs[id]
	if !ok || cfg == nil {
		return nil, fmt.Errorf("%w: 怪物 %d", scenemap.ErrNotFound, id)
	}
	return cfg, nil
}

// NewTable 构造空表。
func NewTable() *Table {
	return &Table{
		Skills: make(map[int32]*SkillConfig),
		Buffs:  make(map[int32]*BuffConfig),
		Npcs:   make(map[int32]*NpcConfig),
		Player: PlayerTemplate{MaxHP: 1000, Atk: 100, Def: 20, Hit: PermilleOne, Camp: 1},
	}
}

// AddSkill 注册技能（重复 ID 直接 panic：配置错误要在启动期暴露）。
func (t *Table) AddSkill(cfg SkillConfig) *Table {
	if _, dup := t.Skills[cfg.ID]; dup {
		panic(fmt.Sprintf("battle: duplicate skill %d", cfg.ID))
	}
	c := cfg
	t.Skills[cfg.ID] = &c
	return t
}

// AddBuff 注册 Buff。
func (t *Table) AddBuff(cfg BuffConfig) *Table {
	if _, dup := t.Buffs[cfg.ID]; dup {
		panic(fmt.Sprintf("battle: duplicate buff %d", cfg.ID))
	}
	c := cfg
	t.Buffs[cfg.ID] = &c
	return t
}

// AddNpc 注册怪物模板。
func (t *Table) AddNpc(cfg NpcConfig) *Table {
	if _, dup := t.Npcs[cfg.CfgID]; dup {
		panic(fmt.Sprintf("battle: duplicate npc %d", cfg.CfgID))
	}
	c := cfg
	t.Npcs[cfg.CfgID] = &c
	return t
}

// DefaultTable 返回演示配置：普攻、群体火、火球（子弹）、灼烧、时停。
//
// 数值刻意做成"一眼能算出来"：普攻对 5 点防御的怪打出固定伤害，
// 公式单测直接拿这组数断言（见 formula_test.go）。
func DefaultTable() *Table {
	t := NewTable()
	t.AddBuff(BuffConfig{
		ID: 3001, Name: "灼烧", Kind: BuffDot,
		DurationMS: 3000, TickMS: 500, Value: 10, MaxStacks: 3,
	})
	t.AddBuff(BuffConfig{
		ID: 3002, Name: "时停", Kind: BuffStun,
		DurationMS: 1500, Replace: true,
	})
	t.AddBuff(BuffConfig{
		ID: 3003, Name: "禁攻诅咒", Kind: BuffBanAttack,
		DurationMS: 2000, Replace: true,
	})
	t.AddBuff(BuffConfig{
		ID: 3004, Name: "护盾", Kind: BuffShield,
		DurationMS: 5000, Value: 50, Replace: true,
	})
	t.AddBuff(BuffConfig{
		ID: 3005, Name: "无敌", Kind: BuffSuper,
		DurationMS: 2000, Replace: true,
	})
	t.AddBuff(BuffConfig{
		ID: 3006, Name: "免死", Kind: BuffUndie,
		DurationMS: 3000, Replace: true,
	})
	t.AddSkill(SkillConfig{
		ID: 2001, Name: "普攻", HarmType: HarmTypeNormal,
		Coeff: PermilleOne, Harm: 10,
		Range: 1, TargetNum: 1, TargetType: TargetEnemy, Area: AreaSingle,
		PrefixMS: 300, CooldownMS: 800,
	})
	t.AddSkill(SkillConfig{
		ID: 2002, Name: "烈焰风暴", HarmType: HarmTypeNormal,
		Coeff: 8000, Harm: 20,
		Range: 5, TargetNum: 6, TargetType: TargetEnemy,
		Area: AreaCircle, AreaRadius: 2,
		PrefixMS: 500, CooldownMS: 3000,
		HitBuffs: []int32{3001},
	})
	t.AddSkill(SkillConfig{
		ID: 2003, Name: "火球", HarmType: HarmTypeNormal,
		Coeff: 9000, Harm: 5,
		Range: 8, TargetNum: 1, TargetType: TargetEnemy, Area: AreaSingle,
		PrefixMS: 400, CooldownMS: 2000,
		RemoteDelayPerGridMS: 60,
		Effect:               EffectBullet, BulletSpeed: 8, SubSkillID: 2004,
	})
	t.AddSkill(SkillConfig{
		ID: 2004, Name: "火球爆炸", HarmType: HarmTypeNormal,
		Coeff: 6000, Harm: 0,
		Range: 1, TargetNum: 4, TargetType: TargetEnemy,
		Area: AreaCircle, AreaRadius: 1,
		PrefixMS: 0, CooldownMS: 0,
	})
	t.AddSkill(SkillConfig{
		ID: 2005, Name: "战吼", HarmType: HarmTypeNotDamage,
		Range: 3, TargetNum: 3, TargetType: TargetAlly,
		Area: AreaCircle, AreaRadius: 3,
		PrefixMS: 200, CooldownMS: 5000,
		SelfBuffs: []int32{3004},
	})
	t.AddNpc(NpcConfig{
		CfgID: 9001, Name: "训练木桩", MaxHP: 100, Atk: 10, Def: 5, Hit: PermilleOne, Camp: 2,
	})
	t.AddNpc(NpcConfig{
		CfgID: 9002, Name: "精英怪", MaxHP: 500, Atk: 30, Def: 10, Hit: PermilleOne, Camp: 2,
	})
	return t
}
