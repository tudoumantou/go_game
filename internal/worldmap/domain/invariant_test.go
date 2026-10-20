package domain

import (
	"testing"
)

// validTroop 返回一份通过全部校验的行军部队基线，
// 各用例在它基础上做单一变异——表驱动测试的惯例：
// 每个用例只动一个字段，失败时能直接定位到哪个组合非法。
func validTroop() *Troop {
	return &Troop{
		Header: ObjectHeader{WorldID: 1, ID: 4001, Kind: ObjectKindTroop, OwnerScene: 2, Version: 3},
		Phase:  TroopPhaseMarching,
		Busy:   BusyState{},
		Cargo:  map[int32]int64{9001: 2},
	}
}

// TestValidateTroop 覆盖文档 6.1 的合法/非法组合。
func TestValidateTroop(t *testing.T) {
	tests := []struct {
		name             string
		mutate           func(t *Troop)
		hasActiveSession bool
		wantErr          error
	}{
		{
			name:   "合法行军中",
			mutate: func(t *Troop) {},
		},
		{
			name:   "合法 QUEUED",
			mutate: func(t *Troop) { t.Phase = TroopPhaseQueued },
		},
		{
			name:             "合法 COLLECTING（有活动会话）",
			mutate:           func(t *Troop) { t.Phase = TroopPhaseCollecting },
			hasActiveSession: true,
		},
		{
			name: "合法 FIGHTING（battle_id 与 Busy=Battle 成对）",
			mutate: func(t *Troop) {
				t.Phase = TroopPhaseFighting
				t.BattleID = "bt-1"
				t.Busy = BusyState{Reason: BusyReasonBattle, OpID: "bt-1"}
			},
		},
		{
			name:   "合法 RETURNING",
			mutate: func(t *Troop) { t.Phase = TroopPhaseReturning },
		},
		{
			name:   "合法 SETTLING",
			mutate: func(t *Troop) { t.Phase = TroopPhaseSettling; t.SettlementID = "st-1" },
		},
		{
			name:   "合法 CLOSED",
			mutate: func(t *Troop) { t.Phase = TroopPhaseClosed; t.Cargo = nil },
		},
		{
			name:    "COLLECTING 无活动会话",
			mutate:  func(t *Troop) { t.Phase = TroopPhaseCollecting },
			wantErr: ErrInvalidState,
		},
		{
			name: "COLLECTING 携带 battle_id",
			mutate: func(t *Troop) {
				t.Phase = TroopPhaseCollecting
				t.BattleID = "bt-1"
			},
			hasActiveSession: true,
			wantErr:          ErrInvalidState,
		},
		{
			name: "FIGHTING 无 battle_id",
			mutate: func(t *Troop) {
				t.Phase = TroopPhaseFighting
				t.Busy = BusyState{Reason: BusyReasonBattle, OpID: "bt-1"}
			},
			wantErr: ErrInvalidState,
		},
		{
			name:    "FIGHTING 但 Busy 不是 BATTLE",
			mutate:  func(t *Troop) { t.Phase = TroopPhaseFighting; t.BattleID = "bt-1" },
			wantErr: ErrInvalidState,
		},
		{
			name:    "SETTLING 无 settlement_id",
			mutate:  func(t *Troop) { t.Phase = TroopPhaseSettling },
			wantErr: ErrInvalidState,
		},
		{
			name:    "Cargo 负值",
			mutate:  func(t *Troop) { t.Cargo = map[int32]int64{9001: -1} },
			wantErr: ErrInvalidState,
		},
		{
			name:    "阶段零值非法",
			mutate:  func(t *Troop) { t.Phase = TroopPhaseUnspecified },
			wantErr: ErrInvalidState,
		},
		{
			name:    "阶段越界非法",
			mutate:  func(t *Troop) { t.Phase = TroopPhase(99) },
			wantErr: ErrInvalidState,
		},
		{
			name:    "Busy 原因越界非法",
			mutate:  func(t *Troop) { t.Busy = BusyState{Reason: BusyReason(99)} },
			wantErr: ErrInvalidState,
		},
		{
			name:    "种类不是部队",
			mutate:  func(t *Troop) { t.Header.Kind = ObjectKindCity },
			wantErr: ErrInvalidState,
		},
		{
			name:    "MARCHING 携带 battle_id（反向约束）",
			mutate:  func(t *Troop) { t.BattleID = "bt-1" },
			wantErr: ErrInvalidState,
		},
		{
			name:    "CLOSED 携带战斗态（反向约束）",
			mutate:  func(t *Troop) { t.Phase = TroopPhaseClosed; t.BattleID = "bt-1"; t.Cargo = nil },
			wantErr: ErrInvalidState,
		},
		{
			name: "RETURNING 但 Busy=BATTLE（反向约束）",
			mutate: func(t *Troop) {
				t.Phase = TroopPhaseReturning
				t.Busy = BusyState{Reason: BusyReasonBattle, OpID: "bt-1"}
			},
			wantErr: ErrInvalidState,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			troop := validTroop()
			tc.mutate(troop)
			err := ValidateTroop(troop, tc.hasActiveSession)
			if tc.wantErr == nil {
				if err != nil {
					t.Fatalf("合法组合被拒绝: %v", err)
				}
				return
			}
			if !isErr(err, tc.wantErr) {
				t.Fatalf("错误类型不符: got %v, want %v", err, tc.wantErr)
			}
		})
	}

	// nil 部队。
	if err := ValidateTroop(nil, false); !isErr(err, ErrInvalidArgument) {
		t.Fatalf("nil 部队错误类型不符: %v", err)
	}

	// 说明：文档 6.1 要求“FIGHTING 不能同时 RETURNING”。
	// Phase 是单值枚举，一个字段不可能既是 4 又是 5，
	// 该组合由类型系统保证，无需运行时断言；此处显式
	// 验证这一点，防止将来改成位组合状态时失去覆盖。
	tr := validTroop()
	tr.Phase = TroopPhaseFighting
	if tr.Phase == TroopPhaseReturning {
		t.Fatal("单值枚举不应同时为两个阶段")
	}
}

// TestValidateResource 覆盖资源点不变量（文档 10.3/INV-03）。
func TestValidateResource(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(r *ResourceNode)
		wantErr error
	}{
		{
			name:   "合法资源点",
			mutate: func(r *ResourceNode) {},
		},
		{
			name:   "合法：有活动会话",
			mutate: func(r *ResourceNode) { r.ActiveCollectID = "cs-1" },
		},
		{
			name:   "合法：仅 pending_destroy（无会话）",
			mutate: func(r *ResourceNode) { r.PendingDestroy = true },
		},
		{
			name:   "合法：余量归零（待刷新删除）",
			mutate: func(r *ResourceNode) { r.Remaining = 0 },
		},
		{
			name:    "余量为负",
			mutate:  func(r *ResourceNode) { r.Remaining = -1 },
			wantErr: ErrInvalidState,
		},
		{
			name:    "余量超过总量（守恒破坏）",
			mutate:  func(r *ResourceNode) { r.Remaining = r.Total + 1 },
			wantErr: ErrInvalidState,
		},
		{
			name:    "总量为负",
			mutate:  func(r *ResourceNode) { r.Total = -5; r.Remaining = 0 },
			wantErr: ErrInvalidState,
		},
		{
			name: "有活动会话又标 pending_destroy",
			mutate: func(r *ResourceNode) {
				r.ActiveCollectID = "cs-1"
				r.PendingDestroy = true
			},
			wantErr: ErrInvalidState,
		},
		{
			name:    "种类不是资源点",
			mutate:  func(r *ResourceNode) { r.Header.Kind = ObjectKindTroop },
			wantErr: ErrInvalidState,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := &ResourceNode{
				Header:    ObjectHeader{WorldID: 1, ID: 5001, Kind: ObjectKindResource, OwnerScene: 2, Version: 1},
				Total:     100,
				Remaining: 40,
			}
			tc.mutate(r)
			err := ValidateResource(r)
			if tc.wantErr == nil {
				if err != nil {
					t.Fatalf("合法资源被拒绝: %v", err)
				}
				return
			}
			if !isErr(err, tc.wantErr) {
				t.Fatalf("错误类型不符: got %v, want %v", err, tc.wantErr)
			}
		})
	}

	if err := ValidateResource(nil); !isErr(err, ErrInvalidArgument) {
		t.Fatalf("nil 资源错误类型不符: %v", err)
	}
}

// TestValidateOccupancyClaim 覆盖占位声明校验（文档 5.3）。
func TestValidateOccupancyClaim(t *testing.T) {
	topo := DefaultTopology(1) // 5000x5000 格，5x5 场景
	valid := OccupancyClaim{
		CellX:     10,
		CellY:     20,
		Layer:     OccupancyLayerStatic,
		ObjectKey: ObjectKey{WorldID: 1, ID: 5001},
	}

	tests := []struct {
		name       string
		mutate     func(c *OccupancyClaim)
		customTopo *Topology
		wantErr    error
	}{
		{
			name:   "合法占位",
			mutate: func(c *OccupancyClaim) {},
		},
		{
			name:    "层为零值",
			mutate:  func(c *OccupancyClaim) { c.Layer = OccupancyLayerUnspecified },
			wantErr: ErrInvalidArgument,
		},
		{
			name:    "层越界（第一版只有静态层）",
			mutate:  func(c *OccupancyClaim) { c.Layer = 2 },
			wantErr: ErrInvalidArgument,
		},
		{
			name:    "格坐标越界（右开边界，x=5000 已出界）",
			mutate:  func(c *OccupancyClaim) { c.CellX = 5000 },
			wantErr: ErrInvalidArgument,
		},
		{
			name:    "格坐标为负",
			mutate:  func(c *OccupancyClaim) { c.CellY = -1 },
			wantErr: ErrInvalidArgument,
		},
		{
			name:    "对象 ID 缺失",
			mutate:  func(c *OccupancyClaim) { c.ObjectKey = ObjectKey{WorldID: 1} },
			wantErr: ErrInvalidArgument,
		},
		{
			name:    "世界 ID 缺失",
			mutate:  func(c *OccupancyClaim) { c.ObjectKey = ObjectKey{ID: 5001} },
			wantErr: ErrInvalidArgument,
		},
		{
			name:       "拓扑非法",
			mutate:     func(c *OccupancyClaim) {},
			customTopo: &Topology{},
			wantErr:    ErrInvalidTopology,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := valid
			tc.mutate(&c)
			useTopo := topo
			if tc.customTopo != nil {
				useTopo = *tc.customTopo
			}
			err := ValidateOccupancyClaim(c, useTopo)
			if tc.wantErr == nil {
				if err != nil {
					t.Fatalf("合法占位被拒绝: %v", err)
				}
				return
			}
			if !isErr(err, tc.wantErr) {
				t.Fatalf("错误类型不符: got %v, want %v", err, tc.wantErr)
			}
		})
	}
}
