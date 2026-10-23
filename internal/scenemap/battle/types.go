package battle

import (
	"fmt"

	"game_study/internal/scenemap"
)

// HarmTag 是伤害标签（规格 §3.8 的 HarmTag）。
//
// 枚举值会写进协议飘字，只能追加不能重排。
type HarmTag int32

const (
	// HarmNormal 普通伤害。
	HarmNormal HarmTag = 0
	// HarmMiss 闪避（伤害 0，但仍算一次命中判定）。
	HarmMiss HarmTag = 1
	// HarmCrit 暴击。
	HarmCrit HarmTag = 2
	// HarmSuck 吸血（挂在攻击方身上的回血数值）。
	HarmSuck HarmTag = 3
	// HarmThorns 反伤。
	HarmThorns HarmTag = 4
	// HarmAbsorb 护盾/灵力吸收（吸收量，不扣血）。
	HarmAbsorb HarmTag = 5
	// HarmUndie 免死触发（本应致死，被压到 1 点）。
	HarmUndie HarmTag = 6
	// HarmSuper 无敌（完全不结算）。
	HarmSuper HarmTag = 7
	// HarmUnctrl 免控。
	HarmUnctrl HarmTag = 8
	// HarmAOE 范围伤（与普通/暴击并存，作为追加标记）。
	HarmAOE HarmTag = 9
	// HarmRecovery 技能回血。
	HarmRecovery HarmTag = 10
	// HarmBok 格挡。
	HarmBok HarmTag = 11
	// HarmSeckill 斩杀（对小怪按剩余血量直接结算）。
	HarmSeckill HarmTag = 12
)

func (t HarmTag) String() string {
	switch t {
	case HarmNormal:
		return "NORMAL"
	case HarmMiss:
		return "MISS"
	case HarmCrit:
		return "CRIT"
	case HarmSuck:
		return "SUCK"
	case HarmThorns:
		return "THORNS"
	case HarmAbsorb:
		return "ABSORB"
	case HarmUndie:
		return "UNDIE"
	case HarmSuper:
		return "SUPER"
	case HarmUnctrl:
		return "UNCTRL"
	case HarmAOE:
		return "AOE"
	case HarmRecovery:
		return "RECOVERY"
	case HarmBok:
		return "BOK"
	case HarmSeckill:
		return "SECKILL"
	default:
		return fmt.Sprintf("HarmTag(%d)", int32(t))
	}
}

// HarmEntry 是一条飘字。
//
// Val 的单位是"点"（已取整），不是千分比——客户端直接显示。
type HarmEntry struct {
	Tag HarmTag
	Val int64
}

// 施法动作（规格 §3.8 的 Action）。
const (
	// ActionLaunch 前摇：只处理释放 Buff / 被动 / 时停，不产生伤害与飘字。
	ActionLaunch int32 = 1
	// ActionCast 命中：选目标 → 伤害公式 → 飘字 + 结果事件。
	ActionCast int32 = 2
)

// SkillContext 是一次出手的完整上下文（规格 §3.8 的 #skill_acc{}）。
//
// 整条链路只带这一个结构：LAUNCH 与 CAST 都用它，伤害公式按 Action
// 走不同分支；结果（harms/deads/触发技能）也写回它，调用方据此发包。
type SkillContext struct {
	// SkillID 技能配置 ID。
	SkillID int32
	// Caster 攻击者 UUID（规格 a_uuid）。
	Caster scenemap.UUID
	// Target 当前目标 UUID（规格 t_uuid）。
	Target scenemap.UUID
	// PrimaryTarget 主目标（多目标时用于弹道/朝向基准，规格 prim_t_uuid）。
	PrimaryTarget scenemap.UUID
	// TargetPos 客户端给的目标格（规格 cli_t_pos），用于算弹道距离。
	TargetPos *scenemap.GridPos
	// SpecPos 指定落点（规格 spec_pos）。
	SpecPos *scenemap.GridPos
	// Dir 出手朝向（0 = 沿用施法者当前朝向）。
	Dir scenemap.Dir
	// Action LAUNCH / CAST。
	Action int32
	// DelayExec 本段延迟（毫秒，已按倍速缩放）。
	DelayExec int64
	// HarmIndex 多段伤害的第几段（0 起）。
	HarmIndex int32
	// Harms 本段飘字列表。
	Harms []HarmEntry
	// RealHarm 实际扣血（已被护盾/灵力/免死截断后的值）。
	RealHarm int64
	// SuckBlood 本段吸血合计（多目标合并后发一次）。
	SuckBlood int64
	// AddBuffs / DelBuffs 目标 → Buff ID 列表。
	AddBuffs map[scenemap.UUID][]int32
	DelBuffs map[scenemap.UUID][]int32
	// Deads 本轮死亡的实体（去重、按结算顺序）。
	Deads []scenemap.UUID
	// TriggerSkills 本轮触发的被动（技能 ID 序列）。
	TriggerSkills [][]int32
	// ErrorCode 0 成功；非 0 见 ErrCode* 常量。
	ErrorCode int32
	// DebugInfo 公式步骤追踪（规格 §16.3 要求可追踪）。
	DebugInfo string
}

// Cast 是一次"客户端报技能"的输入（规格 §12.1 的 16000）。
//
// 只带不可变参数与 UUID：真正的属性从 MGR 的 Sprite 里读，
// 客户端无法借协议伪造攻击力。
type Cast struct {
	// Caster 攻击者 UUID（必须是玩家或其宠物/召唤/镜像）。
	Caster scenemap.UUID
	// SkillID 技能配置 ID。
	SkillID int32
	// Target 目标 UUID；0 表示"没点目标"（由服务端动态选敌）。
	Target scenemap.UUID
	// TargetPos 客户端上报的目标格（算弹道距离，可空）。
	TargetPos *scenemap.GridPos
	// Dir 出手朝向；0 表示沿用攻击者当前朝向。
	Dir scenemap.Dir
}

// CastResult 是 CastCommand 的回执。
//
// 同步回执只给"受理结果"，伤害结果走广播与事件两条回路——
// 调用方不要拿它当结算结果使用（规格 §10.7）。
type CastResult struct {
	// SkillID 受理的技能。
	SkillID int32
	// Accepted 是否受理（false 时 Code 非 0）。
	Accepted bool
	// Code 错误码（0 成功）。
	Code int32
	// PrefixMS 本次前摇（毫秒，已缩放），客户端可据此播动作。
	PrefixMS int64
	// Target 选中的主目标（可能为 0：纯 Buff 技）。
	Target scenemap.UUID
}

// 战斗错误码。0 成功；其余只用于回执与日志，不直接当协议号。
const (
	ErrCodeOK int32 = 0
	// ErrCodeNoBattle 主城/安全区：不结算技能（规格 §13.9）。
	ErrCodeNoBattle int32 = 1001
	// ErrCodeNoSprite 施法者不存在（已离图/被删）。
	ErrCodeNoSprite int32 = 1002
	// ErrCodeDead 施法者已死亡。
	ErrCodeDead int32 = 1003
	// ErrCodeBanAttack 禁攻（Buff 或战斗暂停）。
	ErrCodeBanAttack int32 = 1004
	// ErrCodeCooldown 技能冷却中。
	ErrCodeCooldown int32 = 1005
	// ErrCodeMissingSkill 技能配置不存在（显式报错，不默认兜底）。
	ErrCodeMissingSkill int32 = 1006
	// ErrCodeBadTarget 目标非法（死亡/阵营不符/超出射程）。
	ErrCodeBadTarget int32 = 1007
	// ErrCodeMirrorForbidden 镜像在本图不允许施法（规格 §13.15 白名单）。
	ErrCodeMirrorForbidden int32 = 1008
	// ErrCodeNoTarget 范围技没有命中任何目标（不算错误，仅回执）。
	ErrCodeNoTarget int32 = 1009
)

// TargetResult 是"结果事件"里单个目标的结算摘要。
//
// 玩家 Actor 只处理 owner==自己 的目标；宠物/召唤按 Owner 归属主人。
type TargetResult struct {
	// UUID 目标实体。
	UUID scenemap.UUID
	// HP/MaxHP 结算后的值（供客户端校准）。
	HP    int64
	MaxHP int64
	// Delta 本次扣血（正数=掉血）。
	Delta int64
	// Dead 本次是否死亡。
	Dead bool
	// Harms 本目标的飘字。
	Harms []HarmEntry
	// BuffsAdded / BuffsRemoved 本目标的 Buff 变化。
	BuffsAdded   []int32
	BuffsRemoved []int32
}

// SkillResultEvent 是 MGR 回给"目标主人"的结果事件（规格 EVT_BATTLE_SKILL_RESULT）。
//
// 语义：**玩家 Actor 在自己的事务里回写这些值**。MGR 侧的 Sprite 只是
// 结算用的投影，不是玩家属性的第二权威。
type SkillResultEvent struct {
	SkillID int32
	// Caster 攻击者；Targets 里可能包含攻击者自己（自伤/回血）。
	Caster scenemap.UUID
	// Targets 按结算顺序排列。
	Targets []TargetResult
	// SuckBlood 攻击方吸血合计（合并发一次）。
	SuckBlood int64
	// TriggerSkills 被动触发（技能 ID 列表）。
	TriggerSkills [][]int32
}

// MonDeadEvent 是"怪物死亡"事件（规格 EVT_BATTLE_MON_DEAD）。
//
// 只发给击杀者的主人（宠物击杀 → 主人拿掉落），由玩家 Actor 走掉落主流程。
type MonDeadEvent struct {
	// Monster 死亡怪物 UUID。
	Monster scenemap.UUID
	// NpcID 怪物配置 ID。
	NpcID int32
	// Killer 最后一击的实体（可能是宠物/召唤）。
	Killer scenemap.UUID
	// X/Y 死亡格。
	X, Y int32
}

// PlayerDeadEvent 是"玩家侧实体死亡"事件。
//
// MGR 不处理玩家复活（规格 §10.8）：这里只把事实告诉主人，
// 复活点选择、掉装备、结算界面全部在玩家 Actor 侧。
type PlayerDeadEvent struct {
	// UUID 死亡的实体（玩家本人或宠物/召唤）。
	UUID scenemap.UUID
	// Killer 击杀者（0 表示持续伤害/环境致死）。
	Killer scenemap.UUID
}

// HarmBroadcast 是飘字广播（规格 16000 S2C）。
//
// 走 AOI 视野广播而不是全图：表现回路只需要"看得见的人"收到。
type HarmBroadcast struct {
	SkillID int32
	Caster  scenemap.UUID
	Target  scenemap.UUID
	// HP/MaxHP 结算后目标血量（客户端血条）。
	HP    int64
	MaxHP int64
	// Harms 飘字。
	Harms []HarmEntry
	// Crit/Bok/Aoe 便于客户端快速分支（与 Harms 冗余但省解析）。
	Crit bool
	Bok  bool
	Aoe  bool
	// DebugInfo 公式追踪（客户端调试开关打开时显示）。
	DebugInfo string
}

// BuffSync 是 Buff 变化的视野广播（规格 16005 的简化）。
type BuffSync struct {
	UUID  scenemap.UUID
	Added []int32
	// Removed 被移除的 Buff。
	Removed []int32
}

// CastStart 是"开始施法"的表现广播（可选，客户端据此播前摇动作）。
type CastStart struct {
	Caster     scenemap.UUID
	SkillID    int32
	Target     scenemap.UUID
	TargetPos  *scenemap.GridPos
	PrefixMS   int64
	ActionSeen int32
}
