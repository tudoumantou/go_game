package domain

import (
	"github.com/stretchr/testify/require"
	"testing"
)

// TestDueKindValues 验证优先级常量与文档 9.2 固定表一致。
// 这些值参与排序语义，被改动必须在此处显式失败——
// 防止有人把显式赋值改回 iota 后静默错位。
func TestDueKindValues(t *testing.T) {
	tests := []struct {
		kind DueKind
		want int32
	}{
		{DueKindUnspecified, 0},
		{DueKindBattleTimeout, 10},
		{DueKindCollectEnd, 20},
		{DueKindMoveBoundary, 30},
		{DueKindMoveArrive, 40},
		{DueKindRefresh, 50},
	}
	for _, tc := range tests {
		require.EqualValues(t, tc.want, int32(tc.kind))
		if tc.kind == DueKindUnspecified {
			continue
		}
		if !tc.kind.Valid() {
			t.Errorf("%s 应合法", tc.kind)
		}
	}
	if DueKind(99).Valid() {
		t.Error("DueKind(99) 应非法")
	}
}

// TestDueLess 表驱动验证 (due_at_ms, kind, object_id)
// 字典序比较器。
func TestDueLess(t *testing.T) {
	tests := []struct {
		name string
		a, b DueEvent
		want bool
	}{
		{
			name: "due_at 优先于 kind",
			a:    DueEvent{DueAtMS: 100, Kind: DueKindRefresh},
			b:    DueEvent{DueAtMS: 200, Kind: DueKindBattleTimeout},
			want: true,
		},
		{
			name: "同毫秒按 kind 优先级",
			a:    DueEvent{DueAtMS: 100, Kind: DueKindBattleTimeout},
			b:    DueEvent{DueAtMS: 100, Kind: DueKindCollectEnd},
			want: true,
		},
		{
			name: "同毫秒同 kind 按 object_id",
			a:    DueEvent{DueAtMS: 100, Kind: DueKindBattleTimeout, ObjectID: 5},
			b:    DueEvent{DueAtMS: 100, Kind: DueKindBattleTimeout, ObjectID: 9},
			want: true,
		},
		{
			name: "反向为假",
			a:    DueEvent{DueAtMS: 100, Kind: DueKindCollectEnd},
			b:    DueEvent{DueAtMS: 100, Kind: DueKindBattleTimeout},
			want: false,
		},
		{
			name: "全等视为等价（弱序）",
			a:    DueEvent{DueAtMS: 100, Kind: DueKindMoveArrive, ObjectID: 7},
			b:    DueEvent{DueAtMS: 100, Kind: DueKindMoveArrive, ObjectID: 7},
			want: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := DueLess(tc.a, tc.b); got != tc.want {
				t.Fatalf("DueLess(%+v, %+v) = %v, want %v", tc.a, tc.b, got, tc.want)
			}
			// 反对称性：a<b 与 b<a 不能同时为真。
			if DueLess(tc.a, tc.b) && DueLess(tc.b, tc.a) {
				t.Fatal("比较器不满足反对称性")
			}
		})
	}
}

// TestSortDueEventsRepeatable 验证排序稳定性：同一份事件
// 的不同初始排列，排序后必须得到完全相同的序列
// （文档 19.1“重启前后顺序可重复”）。
func TestSortDueEventsRepeatable(t *testing.T) {
	// 覆盖全部优先级与同毫秒同 kind 的并列。
	events := []DueEvent{
		{WorldID: 1, DueAtMS: 1000, Kind: DueKindMoveArrive, ObjectID: 7, MoveID: "mv-7"},
		{WorldID: 1, DueAtMS: 1000, Kind: DueKindBattleTimeout, ObjectID: 9, BattleID: "bt-9"},
		{WorldID: 1, DueAtMS: 1000, Kind: DueKindBattleTimeout, ObjectID: 3, BattleID: "bt-3"},
		{WorldID: 1, DueAtMS: 500, Kind: DueKindRefresh, ObjectID: 0, RichBlockID: 100, RefreshGeneration: 2, RefreshSlot: 1},
		{WorldID: 1, DueAtMS: 1000, Kind: DueKindCollectEnd, ObjectID: 5, SessionID: "cs-5"},
		{WorldID: 1, DueAtMS: 2000, Kind: DueKindMoveBoundary, ObjectID: 11, MoveID: "mv-11"},
	}
	// 期望顺序：ms=500 的 refresh 最先；随后 ms=1000 按
	// kind 优先级 battle(obj3) < battle(obj9) < collect < arrive；
	// 最后 ms=2000 的 boundary。
	want := []DueEvent{events[3], events[2], events[1], events[4], events[0], events[5]}

	permutations := [][]int{
		{0, 1, 2, 3, 4, 5}, // 原始顺序
		{5, 4, 3, 2, 1, 0}, // 完全反转
		{2, 0, 5, 1, 4, 3}, // 打乱
		{3, 1, 5, 0, 4, 2}, // 再次打乱
	}
	for _, perm := range permutations {
		shuffled := make([]DueEvent, len(events))
		for j, idx := range perm {
			shuffled[j] = events[idx]
		}
		SortDueEvents(shuffled)
		require.EqualValues(t, len(want), len(shuffled))
		for j := range want {
			require.EqualValues(t, want[j], shuffled[j])
		}
	}
}

// TestEventKeys 验证内部命令确定性键的格式（文档
// 9.2/10.5/11.3）。
func TestEventKeys(t *testing.T) {
	tests := []struct {
		name string
		got  string
		want string
	}{
		{"ArriveKey", ArriveKey(4001, "mv-9"), "arrive:4001:mv-9"},
		{"ArriveKey move 含冒号不歧义", ArriveKey(12, "x"), "arrive:12:x"},
		{"CollectEndKey", CollectEndKey("cs-7"), "collect-end:cs-7"},
		{"BattleResultKey", BattleResultKey("bt-3"), "battle-result:bt-3"},
		{"RefreshKey", RefreshKey(1, 100, 5, 2), "refresh:1:100:5:2"},
	}
	for _, tc := range tests {
		require.EqualValues(t, tc.want, tc.got)
	}
	// 键的唯一性：不同 (troop, move) 不碰撞。
	if ArriveKey(1, "2:x") == ArriveKey(12, "x") {
		t.Fatal("ArriveKey 存在键碰撞")
	}
}

// TestDueEventCommandKey 验证事件到命令键的分发，
// 包括 MOVE_BOUNDARY 走移交单号（返回空）的约定。
func TestDueEventCommandKey(t *testing.T) {
	tests := []struct {
		name string
		ev   DueEvent
		want string
	}{
		{
			name: "战斗超时",
			ev:   DueEvent{Kind: DueKindBattleTimeout, BattleID: "bt-3"},
			want: "battle-result:bt-3",
		},
		{
			name: "采集结束",
			ev:   DueEvent{Kind: DueKindCollectEnd, SessionID: "cs-5"},
			want: "collect-end:cs-5",
		},
		{
			name: "到达",
			ev:   DueEvent{Kind: DueKindMoveArrive, ObjectID: 4001, MoveID: "mv-9"},
			want: "arrive:4001:mv-9",
		},
		{
			name: "边界（幂等由 transfer_id 承担）",
			ev:   DueEvent{Kind: DueKindMoveBoundary, ObjectID: 4001, MoveID: "mv-9"},
			want: "",
		},
		{
			name: "刷新",
			ev:   DueEvent{Kind: DueKindRefresh, WorldID: 1, RichBlockID: 100, RefreshGeneration: 5, RefreshSlot: 2},
			want: "refresh:1:100:5:2",
		},
		{
			name: "未知类型无键",
			ev:   DueEvent{Kind: DueKind(99)},
			want: "",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.ev.CommandKey(); got != tc.want {
				t.Fatalf("CommandKey() = %q, want %q", got, tc.want)
			}
		})
	}
}
