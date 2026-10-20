package domain

import (
	"testing"
)

// testTransferTroop 构造一支可移交的行军部队。
func testTransferTroop() *Troop {
	return &Troop{
		Header: ObjectHeader{WorldID: 1, ID: 4001, Kind: ObjectKindTroop, OwnerScene: 2, Version: 5},
		Phase:  TroopPhaseMarching,
		Move:   &MoveSegment{MoveID: "mv-1", From: Pos{X: 0, Y: 0}, To: Pos{X: 1000, Y: 0}, StartAtMS: 0, EndAtMS: 1000},
	}
}

// TestValidateTransferOK 验证合法移交：行军中、源目不同、
// 无在途记录。
func TestValidateTransferOK(t *testing.T) {
	topo := DefaultTopology(1)
	plan := TransferPlan{
		TransferID:             "tf-1",
		Object:                 ObjectKey{WorldID: 1, ID: 4001},
		SourceScene:            2,
		DestScene:              3,
		BeforeVersion:          5,
		ExpectedSourceRevision: 10,
		ExpectedDestRevision:   20,
	}
	if err := ValidateTransfer(testTransferTroop(), plan, topo); err != nil {
		t.Fatalf("合法移交被拒绝: %v", err)
	}
	// 已终态的在途记录不影响新移交（ABORTED/RELEASED 都算完结）。
	old := &TransferRecord{TransferID: "tf-0", Object: plan.Object, Status: TransferStatusReleased}
	if err := ValidateTransfer(testTransferTroop(), plan, topo, old); err != nil {
		t.Fatalf("终态在途记录不应阻止新移交: %v", err)
	}
	// 其他对象的在途记录同样不影响。
	other := &TransferRecord{TransferID: "tf-x", Object: ObjectKey{WorldID: 1, ID: 9999}, Status: TransferStatusPreparing}
	if err := ValidateTransfer(testTransferTroop(), plan, topo, other); err != nil {
		t.Fatalf("其他对象的在途记录不应阻止本移交: %v", err)
	}
}

// TestValidateTransferRejects 表驱动覆盖各类拒绝路径。
func TestValidateTransferRejects(t *testing.T) {
	topo := DefaultTopology(1)
	basePlan := TransferPlan{
		TransferID:  "tf-1",
		Object:      ObjectKey{WorldID: 1, ID: 4001},
		SourceScene: 2,
		DestScene:   3,
	}

	tests := []struct {
		name     string
		troop    *Troop
		plan     TransferPlan
		topo     Topology
		inflight []*TransferRecord
		wantErr  error
	}{
		{
			name: "FIGHTING 不能另起移交",
			troop: func() *Troop {
				tr := testTransferTroop()
				tr.Phase = TroopPhaseFighting
				tr.BattleID = "bt-1"
				tr.Busy = BusyState{Reason: BusyReasonBattle, OpID: "bt-1"}
				return tr
			}(),
			plan:    basePlan,
			topo:    topo,
			wantErr: ErrTroopBusy,
		},
		{
			name: "Busy=TRANSFER 不能另起移交",
			troop: func() *Troop {
				tr := testTransferTroop()
				tr.Busy = BusyState{Reason: BusyReasonTransfer, OpID: "tf-0"}
				return tr
			}(),
			plan:    basePlan,
			topo:    topo,
			wantErr: ErrTroopBusy,
		},
		{
			name: "Busy=DEFEND_TRANSITION 不能另起移交",
			troop: func() *Troop {
				tr := testTransferTroop()
				tr.Busy = BusyState{Reason: BusyReasonDefendTransition, OpID: "tf-0"}
				return tr
			}(),
			plan:    basePlan,
			topo:    topo,
			wantErr: ErrTroopBusy,
		},
		{
			name: "Busy=BATTLE 不能另起移交",
			troop: func() *Troop {
				tr := testTransferTroop()
				tr.Busy = BusyState{Reason: BusyReasonBattle, OpID: "bt-9"}
				return tr
			}(),
			plan:    basePlan,
			topo:    topo,
			wantErr: ErrTroopBusy,
		},
		{
			name:     "已有未终态在途移交（一次只移交一支部队）",
			troop:    testTransferTroop(),
			plan:     basePlan,
			topo:     topo,
			inflight: []*TransferRecord{{TransferID: "tf-0", Object: basePlan.Object, Status: TransferStatusPreparing}},
			wantErr:  ErrTroopBusy,
		},
		{
			name:     "在途移交处于 PREPARED 同样拒绝",
			troop:    testTransferTroop(),
			plan:     basePlan,
			topo:     topo,
			inflight: []*TransferRecord{{TransferID: "tf-0", Object: basePlan.Object, Status: TransferStatusPrepared}},
			wantErr:  ErrTroopBusy,
		},
		{
			name:    "源目场景相同",
			troop:   testTransferTroop(),
			plan:    TransferPlan{TransferID: "tf-1", Object: basePlan.Object, SourceScene: 2, DestScene: 2},
			topo:    topo,
			wantErr: ErrInvalidArgument,
		},
		{
			name:    "目标场景为 0",
			troop:   testTransferTroop(),
			plan:    TransferPlan{TransferID: "tf-1", Object: basePlan.Object, SourceScene: 2, DestScene: 0},
			topo:    topo,
			wantErr: ErrInvalidArgument,
		},
		{
			name:    "目标场景越界（5x5 世界最大 25）",
			troop:   testTransferTroop(),
			plan:    TransferPlan{TransferID: "tf-1", Object: basePlan.Object, SourceScene: 2, DestScene: 26},
			topo:    topo,
			wantErr: ErrInvalidArgument,
		},
		{
			name:    "transfer_id 为空",
			troop:   testTransferTroop(),
			plan:    TransferPlan{Object: basePlan.Object, SourceScene: 2, DestScene: 3},
			topo:    topo,
			wantErr: ErrInvalidArgument,
		},
		{
			name:    "计划对象标识缺失",
			troop:   testTransferTroop(),
			plan:    TransferPlan{TransferID: "tf-1", SourceScene: 2, DestScene: 3},
			topo:    topo,
			wantErr: ErrInvalidArgument,
		},
		{
			name:    "部队与计划对象不符",
			troop:   testTransferTroop(),
			plan:    TransferPlan{TransferID: "tf-1", Object: ObjectKey{WorldID: 1, ID: 8888}, SourceScene: 2, DestScene: 3},
			topo:    topo,
			wantErr: ErrInvalidArgument,
		},
		{
			name:    "部队不存在",
			troop:   nil,
			plan:    basePlan,
			topo:    topo,
			wantErr: ErrObjectNotFound,
		},
		{
			name:    "非部队对象（第一版城池/资源点不移交）",
			troop:   func() *Troop { tr := testTransferTroop(); tr.Header.Kind = ObjectKindCity; return tr }(),
			plan:    basePlan,
			topo:    topo,
			wantErr: ErrInvalidState,
		},
		{
			name:    "拓扑非法",
			troop:   testTransferTroop(),
			plan:    basePlan,
			topo:    Topology{},
			wantErr: ErrInvalidTopology,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateTransfer(tc.troop, tc.plan, tc.topo, tc.inflight...)
			if !isErr(err, tc.wantErr) {
				t.Fatalf("错误类型不符: got %v, want %v", err, tc.wantErr)
			}
		})
	}
}

// TestTransferStatusMachine 验证状态机的合法值与终态判定。
func TestTransferStatusMachine(t *testing.T) {
	tests := []struct {
		status    TransferStatus
		wantValid bool
		wantTerm  bool
	}{
		{TransferStatusUnspecified, false, false},
		{TransferStatusPreparing, true, false},
		{TransferStatusPrepared, true, false},
		{TransferStatusCommitted, true, false},
		{TransferStatusActivated, true, false},
		{TransferStatusReleased, true, true},
		{TransferStatusAborted, true, true},
		{TransferStatus(99), false, false},
	}
	for _, tc := range tests {
		if got := tc.status.Valid(); got != tc.wantValid {
			t.Errorf("%s Valid() = %v, want %v", tc.status, got, tc.wantValid)
		}
		if got := tc.status.Terminal(); got != tc.wantTerm {
			t.Errorf("%s Terminal() = %v, want %v", tc.status, got, tc.wantTerm)
		}
	}
	// String 不出现 "TransferStatus(" 前缀（已知值必须可读）。
	if s := TransferStatusPrepared.String(); s != "PREPARED" {
		t.Fatalf("String() = %q, want PREPARED", s)
	}
}
