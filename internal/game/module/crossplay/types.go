// Package crossplay 跨服玩法的游戏服侧：报名、分组镜像、积分同步与跨服扣分。
//
// 数据归属（决定了整个模块的形态）：
//   - 积分是**玩法数据，不是玩家存档**。玩家离线也能被打、被改分，所以它
//     不进 Ext 槽位、不经过 Actor、不走 flusher——模块自己管表、自己管内存。
//   - 谁的分谁来改：任何服只能写自己玩家的分。别的服要扣分只能发差值过来
//     （见 callback.go 的 OnScoreDelta）。
//   - 权威在中心服：分组是它算的；本服只在内存里维护一份镜像，供界面读取。
//
// 读取路径分三层，正常情况只走第一层：
//
//	① 本模块内存（99.9%）
//	② 本服库 cross_player / cross_group（重启后、推送延迟）
//	③ 异常：向中心服单查一个玩家（本版未实现，靠 10 分钟全量校准覆盖）
//
// 与 center/crossplay 的分工：那边算分组、合并积分、按服裁剪下发；这边
// 只管「本服视角」——存自己的分、展示别人的分、把自己变了的报上去。
package crossplay

import (
	"time"
)

// Phase 玩法阶段。
//
// 数值与客户端协议 CrossPlayInfoResp.phase 一一对应，不要重排。
type Phase int32

// 玩法常量。与 center/crossplay 的同名常量必须一致——两边算的是同一场活动，
// 初始分或下限一旦分叉，玩家在界面看到的分与结算用的分就不是一回事。
const (
	// InitialScore 活动初始积分。
	InitialScore int64 = 1000
	// ScoreFloor 分数下限：扣到 600 后不再扣，但仍可被攻击、攻击方仍加分。
	//
	// 刷分口子（600 分玩家变成无限靶子）已确认不堵：道具消耗是唯一成本约束。
	// 保留 max 语义不是为了防刷，而是为了不出现负分。
	ScoreFloor int64 = 600
	// GroupSize 每组人数（仅用于界面展示预期，实际分组在中心服算）。
	GroupSize = 100
)

const (
	// PhaseNone 未开放（活动配置缺失，或此刻不在任何一期）。
	PhaseNone Phase = 0
	// PhaseEnroll 报名期：周一 00:00 → 周二 00:00。
	PhaseEnroll Phase = 1
	// PhaseGrouping 分组期：周二 00:00 → 06:00。中心服算分组，游戏服只读。
	PhaseGrouping Phase = 2
	// PhaseActive 活动期：周二 06:00 → 周日 22:00。可攻击、可改分。
	PhaseActive Phase = 3
	// PhaseSettle 结束期：周日 22:00 → 周一 00:00。停分、结算、归档。
	PhaseSettle Phase = 4
)

// String 便于日志与测试断言。
func (p Phase) String() string {
	switch p {
	case PhaseEnroll:
		return "enroll"
	case PhaseGrouping:
		return "grouping"
	case PhaseActive:
		return "active"
	case PhaseSettle:
		return "settle"
	default:
		return "none"
	}
}

// Config 阶段偏移，单位小时，全部相对「本周起点（周一 00:00）」。
//
// 用偏移而不是绝对时刻：期次由 activity.Week 算，配置只描述「这一周里
// 第几小时切哪个阶段」。这样跨年、跨月都不需要额外配置，也不会因为
// 某期活动顺延而改表。
type Config struct {
	// EnrollHours 报名期长度（默认 24h：周一 00:00 → 周二 00:00）。
	EnrollHours int64
	// GroupHours 分组期长度（默认 6h：周二 00:00 → 06:00）。
	//
	// 6 小时不是用来算的——10 万人分组只要毫秒级——而是留给「最后一轮
	// 补报」和「30 个服确认落库」的等待窗口。
	GroupHours int64
	// SettleHours 结束期长度（默认 2h：周日 22:00 → 周一 00:00）。
	SettleHours int64
}

// DefaultConfig 默认阶段配置，与设计文档一致（合计 168h 正好一周）。
func DefaultConfig() Config {
	return Config{
		EnrollHours: 24,
		GroupHours:  6,
		SettleHours: 2,
	}
}

// normalized 补齐零值，保证四个阶段合计恰好一周。
//
// 配错（比如三个都填 0）时退回默认值而不是算出零长度的阶段：
// 零长度阶段会让「报名期」永远不成立，玩家点了报名得到「非报名期」，
// 而这种错在日志里看起来完全正常。
func (c Config) normalized() Config {
	d := DefaultConfig()
	if c.EnrollHours <= 0 {
		c.EnrollHours = d.EnrollHours
	}
	if c.GroupHours <= 0 {
		c.GroupHours = d.GroupHours
	}
	if c.SettleHours <= 0 {
		c.SettleHours = d.SettleHours
	}
	return c
}

// KV 一条玩家基本信息（等级 / 战力展示 / 外观 …）。
//
// 用 KV 列表而不是固定结构体：展示项随版本增（时装、坐骑、称号、公会），
// 固定字段每加一项就要改三处（协议、表、多个展示点）；KV 只有上报端要改，
// 展示层认不出的 k 直接忽略，存量数据不受影响。
type KV struct {
	K string `json:"k"`
	V string `json:"v"`
}

// Entry 一个玩家在玩法里的视图。
//
// 本服玩家的 Entry 由 players 与 groups 共享同一个指针（见 Registry.AddScore）：
// 改分只写一次，界面排行和上报数据自动一致。别服玩家的 Entry 只存在于
// groups 里，来自中心服下发——本服不写它，也无权写。
type Entry struct {
	PlayerID int64
	GroupID  int32
	ServerID int32
	// Score 当前积分，初始 1000，下限 600。
	Score int64
	// Power 报名时锁定的战力快照。分组依据，活动期内不更新。
	Power int64
	// UpdatedAt 本服玩家：最后一次本地改分的时刻；别服玩家：下发报文里的值。
	//
	// 只用于展示与排查，不参与任何决策——跨机器时钟不可信，拿它做「谁更新」
	// 的比较会在时钟回拨时判错。
	UpdatedAt int64
	// Info 只对本服玩家有意义（别服玩家的展示信息中心服不下发）。
	Info []KV
}

// GroupView 一组的内存镜像。
//
// entries 里同时有本服和别服玩家：界面要显示组内排行，就必须知道别人的分。
// 本服玩家的 entry 与 Registry.players 指向同一个对象。
type GroupView struct {
	ID      int32
	entries map[int64]*Entry
	// dirty 本组自上次落库后是否有变化（cross_group 是恢复副本，懒落库）。
	dirty bool
}

// Snapshot 组内成员快照（按积分降序的稳定拷贝，调用方无需加锁）。
func (g *GroupView) Snapshot() []Entry {
	if g == nil || len(g.entries) == 0 {
		return nil
	}
	out := make([]Entry, 0, len(g.entries))
	for _, e := range g.entries {
		out = append(out, *e)
	}
	sortByScore(out)
	return out
}

// Rank 返回某玩家在组内的名次（1 起），0 表示不在组内。
//
// 每算一次就排一遍：一组 100 人，排序在微秒级，而维护一份实时排名索引
// 要在每次改分时重排 + 加锁，反而更贵也更容易错。界面一次只查一个人。
func (g *GroupView) Rank(playerID int64) int32 {
	if g == nil {
		return 0
	}
	e, ok := g.entries[playerID]
	if !ok {
		return 0
	}
	// 同分按 player_id 升序，与中心服分组算法的排序口径一致：
	// 否则「同分不同名次」在两边算出来不一样，界面与结算会对不上。
	rank := int32(1)
	for _, other := range g.entries {
		if other.PlayerID == playerID {
			continue
		}
		if other.Score > e.Score || (other.Score == e.Score && other.PlayerID < e.PlayerID) {
			rank++
		}
	}
	return rank
}

// Size 组内人数。
func (g *GroupView) Size() int {
	if g == nil {
		return 0
	}
	return len(g.entries)
}

// sortByScore 积分降序、同分按 player_id 升序。
//
// 用插入排序：n=100 时比 sort.Slice 快且不分配（组内人数固定为 100，
// 走大数组的通用排序反而要传闭包、做反射式的比较）。
func sortByScore(list []Entry) {
	for i := 1; i < len(list); i++ {
		cur := list[i]
		j := i - 1
		for j >= 0 && less(cur, list[j]) {
			list[j+1] = list[j]
			j--
		}
		list[j+1] = cur
	}
}

// less a 排在 b 前面。
func less(a, b Entry) bool {
	if a.Score != b.Score {
		return a.Score > b.Score
	}
	return a.PlayerID < b.PlayerID
}

// clampScore 应用分数下限。
//
// 扣到 600 后不再扣，但「可以被攻击、攻击方仍加分」是产品已确认的口子
// （道具消耗是唯一成本约束，不做同目标限次）。这里只保证不出现负分。
func clampScore(score int64) int64 {
	if score < ScoreFloor {
		return ScoreFloor
	}
	return score
}

// nowUnix 当前时刻（抽成方法便于测试注入）。
func (m *Module) nowUnix() int64 {
	if m.clock != nil {
		return m.clock().Unix()
	}
	return time.Now().Unix()
}
