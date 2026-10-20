package domain

import "fmt"

// ObjectKind 对象种类。枚举值会写入协议与快照，一经发布不可重排：
// 新增种类只能往后追加。LINE 是投影不是业务对象，不在此列。
type ObjectKind int32

const (
	// ObjectKindUnspecified 零值非法，用于把“未设置”和 CITY 区分开。
	ObjectKindUnspecified ObjectKind = 0
	// ObjectKindCity 城池/主堡。
	ObjectKindCity ObjectKind = 1
	// ObjectKindTroop 部队。
	ObjectKindTroop ObjectKind = 2
	// ObjectKindResource 资源点。
	ObjectKindResource ObjectKind = 3
)

// Valid 报告种类是否为已知业务种类。
func (k ObjectKind) Valid() bool {
	return k == ObjectKindCity || k == ObjectKindTroop || k == ObjectKindResource
}

func (k ObjectKind) String() string {
	switch k {
	case ObjectKindCity:
		return "CITY"
	case ObjectKindTroop:
		return "TROOP"
	case ObjectKindResource:
		return "RESOURCE"
	default:
		return fmt.Sprintf("ObjectKind(%d)", int32(k))
	}
}

// ObjectHeader 是所有地图对象的公共头。文档第 6 章：
//
//   - WorldID/ID 组成跨世界唯一键；
//   - OwnerScene 是“当前唯一写入者”，与按位置推导的 PhysicalScene
//     是两个概念（驻防/战斗时可临时不一致），混用会导致写权限歧义；
//   - Version 是对象级版本号，用于乐观并发（ExpectedObjectVersion）；
//   - SchemaVersion 决定 payload 如何解释，与业务 Version 互相独立：
//     加字段要升 schema，改数值只升 version。
type ObjectHeader struct {
	WorldID       WorldID
	ID            ObjectID
	Kind          ObjectKind
	OwnerScene    SceneID
	Version       uint64
	SchemaVersion uint32
	Deleted       bool
}

// Key 返回跨世界唯一对象键（日志与 map 索引用）。
func (h ObjectHeader) Key() ObjectKey {
	return ObjectKey{WorldID: h.WorldID, ID: h.ID}
}

// ObjectKey 是对象的轻量标识，不含版本，适合做 map 键。
type ObjectKey struct {
	WorldID WorldID
	ID      ObjectID
}

func (k ObjectKey) String() string {
	return fmt.Sprintf("%d/%d", k.WorldID, k.ID)
}

// TargetRef 描述一次行动指向的目标。文档第 9.1 章：
// 客户端只允许传 target_id 与 intent，不允许指定坐标、速度、奖励。
// 坐标全部由服务端按权威状态推导，防止客户端伪造落点。
type TargetRef struct {
	TargetID ObjectID
	Intent   MarchIntent
}

// MarchIntent 出征意图。第一版只区分采集与攻击，后续扩展
// （驻防/侦察）向后追加枚举值，不重排。
type MarchIntent int32

const (
	// MarchIntentUnspecified 零值：必须显式拒绝，避免客户端漏传时
	// 被默认成“采集”这种有副作用的行为。
	MarchIntentUnspecified MarchIntent = 0
	// MarchIntentCollect 采集资源点。
	MarchIntentCollect MarchIntent = 1
	// MarchIntentAttack 攻击敌对目标（部队/城池）。
	MarchIntentAttack MarchIntent = 2
)

func (i MarchIntent) Valid() bool {
	return i == MarchIntentCollect || i == MarchIntentAttack
}
