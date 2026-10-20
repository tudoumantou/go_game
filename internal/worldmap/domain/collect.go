package domain

import (
	"fmt"
	"sort"
)

// collect.go 承载到期事件的优先级与确定性键构造（文档
// 9.2/7.1/10.5）。文件名叫 collect 是历史落点（18.1 目录
// 把采集与到期事件放在一起），实际职责是“到期调度”：
// SceneLoop 的定时堆不单独持久化，重启时由 Move/Collect/
// Battle/Refresh 的 deadline 重建（6.2），重建后必须按
// 与重启前完全一致的顺序补处理——本文件提供顺序的
// 定义（DueKind 优先级 + DueLess 比较器）和事件对应的
// 内部命令幂等键。
//
// 纯函数、无状态、无包级变量：同毫秒事件的顺序只取决于
// 事件自身字段，不取决于 map 遍历顺序或扫描时序。

// DueKind 到期事件类型及优先级（文档 9.2）。
//
// 用 int32 显式赋值而不用 iota：这些数值是协议级的排序
// 语义，会写进恢复逻辑与测试断言。iota 的默认顺序依赖
// 声明位置，将来在中间插入一个新类型就会让后面所有值
// 静默错位——已持久化/已联调的优先级全部漂移。显式
// 赋值让“插入新值必须显式选号”成为编译期可见的动作。
//
// 为什么同毫秒要按 kind 排序：同一毫秒可能同时到期
// “战斗超时”和“采集结束”，处理顺序不同结果不同
// （先超时则采集不再结算）。固定优先级保证任何进程、
// 任何重启次数下顺序一致。
type DueKind int32

const (
	// DueKindUnspecified 零值非法。
	DueKindUnspecified DueKind = 0
	// DueKindBattleTimeout 战斗超时裁决：PENDING→TIMED_OUT，
	// 按“攻击无效、防守保留、无战损”处理（11.3）。
	DueKindBattleTimeout DueKind = 10
	// DueKindCollectEnd 采集自然结束：按 planned 结算，
	// 余量=0 标记删除或按保留率标 pending_destroy（10.3）。
	DueKindCollectEnd DueKind = 20
	// DueKindMoveBoundary 行军穿越场景边界：计算穿越时刻
	// 与位置，触发所有权移交（8.1）。
	DueKindMoveBoundary DueKind = 30
	// DueKindMoveArrive 行军到达目标：重新校验目标并
	// 分流到采集/战斗/回城（9.3）。
	DueKindMoveArrive DueKind = 40
	// DueKindRefresh 富饶块刷新到点：按 (block,generation,
	// slot) 稳定键补点，重试不重复创建（10.5）。
	DueKindRefresh DueKind = 50
)

// Valid 报告是否为已知事件类型。
func (k DueKind) Valid() bool {
	switch k {
	case DueKindBattleTimeout, DueKindCollectEnd, DueKindMoveBoundary,
		DueKindMoveArrive, DueKindRefresh:
		return true
	default:
		return false
	}
}

// String 返回日志用名称。
func (k DueKind) String() string {
	switch k {
	case DueKindBattleTimeout:
		return "BATTLE_TIMEOUT"
	case DueKindCollectEnd:
		return "COLLECT_END"
	case DueKindMoveBoundary:
		return "MOVE_BOUNDARY"
	case DueKindMoveArrive:
		return "MOVE_ARRIVE"
	case DueKindRefresh:
		return "REFRESH"
	default:
		return fmt.Sprintf("DueKind(%d)", int32(k))
	}
}

// DueEvent 是一条到期事件（定时堆元素，文档 9.2）。
// 字段按“定位一件事”的最小集合设计：due_at_ms + kind +
// object_id 是排序三元组（DueLess），其余是各 kind 的
// 关联键——只有对应 kind 会填写，未用到的保持零值。
type DueEvent struct {
	// WorldID 事件归属世界。不参与 DueLess 排序（文档 9.2
	// 的三元组固定为 due_at/kind/object_id，SceneLoop 按
	// 世界隔离，堆内不会跨世界比较），但刷新键与日志需要
	// 它（17.3：日志必须带 world）。
	WorldID WorldID
	// DueAtMS 到期时刻（毫秒）。补处理按它排序（16.1）。
	DueAtMS int64
	// Kind 事件类型，同毫秒时的第二排序键（优先级）。
	Kind DueKind
	// ObjectID 关联对象（部队/资源点），同毫秒同 kind 时
	// 的第三排序键。刷新事件没有单一对象，填 0，
	// 由 RichBlockID/RefreshGeneration/RefreshSlot 定位。
	ObjectID ObjectID
	// MoveID 移动类事件关联的移动段。旧 MoveID 的到达
	// 任务触发时直接判定过期（9.2 改速语义）。
	MoveID string
	// BattleID 战斗超时关联的战斗编号。
	BattleID string
	// SessionID 采集结束关联的会话编号。
	SessionID string
	// RichBlockID/RefreshGeneration/RefreshSlot 刷新事件
	// 定位三元组（10.5：同一 (block,generation,slot) 使用
	// 稳定刷新键，重试不能重复创建）。
	RichBlockID       int64
	RefreshGeneration int64
	RefreshSlot       int64
}

// DueLess 是到期事件的固定排序比较器（文档 9.2/19.1）：
// (due_at_ms, kind, object_id) 字典序。
//
// 为什么必须是固定比较器而不是“谁先入堆谁先处理”：
// 重启后定时堆从持久 deadline 重建，插入顺序来自扫描
// 顺序（SQL 行序、map 遍历），同一份数据两次重建可能
// 插入顺序不同。若堆序依赖插入顺序，同毫秒事件的处理
// 顺序就不可重复，玩家会观察到“重启前后奖励结算不同”。
// 字典序只依赖事件字段，任何重建路径都得到同一序列。
//
// 三者全等时返回 false（视为等价元素）：比较器是弱序，
// 配合 SortDueEvents 的稳定排序保留重建时的确定插入
// 顺序，进一步缩小不确定性面。
func DueLess(a, b DueEvent) bool {
	if a.DueAtMS != b.DueAtMS {
		return a.DueAtMS < b.DueAtMS
	}
	if a.Kind != b.Kind {
		return a.Kind < b.Kind
	}
	return a.ObjectID < b.ObjectID
}

// SortDueEvents 按 DueLess 稳定排序（文档 16.1：依 due_at
// 顺序补处理）。用 sort.SliceStable 而不是 sort.Slice：
// 比较器等价的事件保持入栈顺序，不引入额外抖动。
// Slice 的不确定排序对“全序键”无差别，但 DueLess 是
// 弱序（等价事件存在），稳定排序让结果完全可复现。
func SortDueEvents(evs []DueEvent) {
	sort.SliceStable(evs, func(i, j int) bool { return DueLess(evs[i], evs[j]) })
}

// 到期事件的内部命令键构造函数。这些键是“处理过一次
// 就不再产生第二次副作用”的幂等依据：重启重扫会重复
// 投递同一事件，SceneLoop 用键识别已处理项。
//
// 注入性说明：键中的数字字段都是十进制无符号整数（不含
// 分隔符），字符串后缀排在最后，因此不同参数组合不会
// 生成同一个键——例如 ArriveKey(1, "2:x") = "arrive:1:2:x"
// 与 ArriveKey(12, "x") = "arrive:12:x" 不同， troop 的
// 十进制表示把第一个冒号后的数字段边界唯一确定下来。

// ArriveKey 构造移动到达命令键（文档 9.2）：
//
//	arrive:{troop}:{move_id}
//
// move_id 在键里而不是只在值里：改速会生成新 MoveID，
// 旧到达任务触发时按旧键直接判定过期（9.2），不需要
// 额外比对当前 Move 指针。
func ArriveKey(troop ObjectID, moveID string) string {
	return fmt.Sprintf("arrive:%d:%s", troop, moveID)
}

// CollectEndKey 构造采集结束命令键（文档 10.3）：
//
//	collect-end:{session}
//
// 以会话为键而不是以资源点为键：同一资源点会有连续多个
// 会话（采完旧的开新的），按资源点去重会串味。
func CollectEndKey(session string) string {
	return "collect-end:" + session
}

// BattleResultKey 构造战斗结果命令键（文档 11.3）：
//
//	battle-result:{battle_id}
//
// 超时命令与结果命令共用 battle 维度：谁先提交谁生效，
// 迟到者通过本键识别“这个 battle 已有终态”（INV-06）。
func BattleResultKey(battleID string) string {
	return "battle-result:" + battleID
}

// RefreshKey 构造刷新计划键（文档 10.5）：
//
//	refresh:{world}:{block}:{generation}:{slot}
//
// (block,generation,slot) 唯一确定一个补点槽位：批处理
// 中断后重启从未完成 slot 继续，键相同即已创建，重试
// 不重复抽奖、不重复创建。
func RefreshKey(world WorldID, block, generation, slot int64) string {
	return fmt.Sprintf("refresh:%d:%d:%d:%d", world, block, generation, slot)
}

// CommandKey 返回事件对应的内部命令键；MOVE_BOUNDARY
// 返回空字符串——边界事件的幂等由移交单号
// （TransferPlan.TransferID，见 transfer.go）承担，
// 不重复造一套移动键。未知 kind 同样返回空，
// 调用方应把空键视为“无幂等键，按状态机推进”，
// 而不是拿空字符串当合法键去重（那会把所有无键事件
// 误判为同一个）。
func (e DueEvent) CommandKey() string {
	switch e.Kind {
	case DueKindBattleTimeout:
		return BattleResultKey(e.BattleID)
	case DueKindCollectEnd:
		return CollectEndKey(e.SessionID)
	case DueKindMoveArrive:
		return ArriveKey(e.ObjectID, e.MoveID)
	case DueKindRefresh:
		return RefreshKey(e.WorldID, e.RichBlockID, e.RefreshGeneration, e.RefreshSlot)
	default:
		return ""
	}
}
