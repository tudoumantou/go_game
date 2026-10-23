package domain

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/require"
	"reflect"
	"testing"
)

// testBattleInput 构造一份确定性战斗输入。seed 可变，
// 其余固定——测试“同输入同结果”时用同一 seed，
// 测试“不同 seed 不同结果”时只改 seed。
func testBattleInput(seed int64) BattleInput {
	return BattleInput{
		BattleID:    "bt-1001",
		AttackerID:  2001,
		DefenderID:  3001,
		Attacker:    CombatSnapshot{ConfigID: 11, Level: 1, Attack: 120, Defense: 40, HP: 1000, Speed: 10, RuleVersion: "battle-rule-v1"},
		Defender:    CombatSnapshot{ConfigID: 21, Level: 1, Attack: 60, Defense: 90, HP: 1200, Speed: 8, RuleVersion: "battle-rule-v1"},
		InputHash:   PayloadHashOf([]byte("bt-1001|2001|3001|battle-rule-v1")),
		RuleVersion: "battle-rule-v1",
		RandomSeed:  seed,
		ResolveAtMS: 1_700_000_000_000,
		DeadlineMS:  1_700_000_015_000,
	}
}

// testBattleTroops 构造一对处于 FIGHTING 的攻守部队，
// 模拟“双方已共置到目标 OwnerScene 并开战”的状态。
func testBattleTroops() (attacker, defender *Troop) {
	busy := BusyState{Reason: BusyReasonBattle, OpID: "bt-1001", SinceMS: 1_700_000_000_000, DeadlineMS: 1_700_000_015_000}
	attacker = &Troop{
		Header:   ObjectHeader{WorldID: 1, ID: 2001, Kind: ObjectKindTroop, OwnerScene: 3, Version: 7},
		PlayerID: 100,
		Phase:    TroopPhaseFighting,
		Busy:     busy,
		BattleID: "bt-1001",
		Cargo:    map[int32]int64{9001: 5},
	}
	defender = &Troop{
		Header:   ObjectHeader{WorldID: 1, ID: 3001, Kind: ObjectKindTroop, OwnerScene: 3, Version: 9},
		PlayerID: 200,
		Phase:    TroopPhaseFighting,
		Busy:     busy,
		BattleID: "bt-1001",
		Cargo:    map[int32]int64{9002: 3},
	}
	return attacker, defender
}

// TestFakeResolverDeterministic 验证确定性：同一输入重复
// Resolve 必须得到逐字节相同的结果（11.2）。
func TestFakeResolverDeterministic(t *testing.T) {
	r := &FakeResolver{}
	in := testBattleInput(42)
	first, err := r.Resolve(context.Background(), in)
	require.NoError(t, err, "首次 Resolve 失败")
	for i := 0; i < 5; i++ {
		got, err := r.Resolve(context.Background(), in)
		require.NoError(t, err)
		require.False(t, !reflect.DeepEqual(first, got), fmt.Sprintf("第 %d 次结果与首次不一致:\nfirst=%+v\ngot=%+v", i, first, got))
	}
	// 胜者必须是双方之一，且与 AttackerWon 自洽。
	require.False(t, first.WinnerID != in.AttackerID && first.WinnerID != in.DefenderID, fmt.Sprintf("胜者 %d 不是攻守任何一方", first.WinnerID))
	wantWon := first.WinnerID == in.AttackerID
	require.EqualValues(t, wantWon, first.AttackerWon)
	require.EqualValues(t, in.ResolveAtMS, first.AppliedAtMS)
}

// TestFakeResolverSeedVariation 验证不同随机种子会产生
// 不同结果（否则“可回放”退化成“永远同一个结果”，
// 战斗将失去随机性）。
//
// 用对称属性输入：双方快照完全相同，力量基数相等，
// 胜负完全由 seed 派生的随机流决定。这不是巧合而是
// 测试设计——属性差距会“锁死”结果（攻击方 400 对
// 330 时 95% 的种子都攻方胜），对称输入把比较器
// 的敏感性隔离出来单独验证。
func TestFakeResolverSeedVariation(t *testing.T) {
	r := &FakeResolver{}
	symmetric := testBattleInput(42)
	symmetric.Attacker = symmetric.Defender
	type outcome struct {
		winner      ObjectID
		attackerWon bool
	}
	seen := map[outcome]bool{}
	for _, seed := range []int64{1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610, 987, 1597} {
		in := symmetric
		in.RandomSeed = seed
		got, err := r.Resolve(context.Background(), in)
		require.NoError(t, err)
		seen[outcome{got.WinnerID, got.AttackerWon}] = true
	}
	require.False(t, len(seen) < 2, fmt.Sprintf("16 个种子只产生 %d 种结果，随机性不足", len(seen)))

	// 相同 seed、不同 input_hash 同样应产生不同结果：
	// hash 参与随机流派生，改参数必须改结果。
	hashSeen := map[outcome]bool{}
	for i := 0; i < 16; i++ {
		in := symmetric
		in.InputHash = PayloadHashOf([]byte{byte(i)})
		got, err := r.Resolve(context.Background(), in)
		require.NoError(t, err)
		hashSeen[outcome{got.WinnerID, got.AttackerWon}] = true
	}
	require.False(t, len(hashSeen) < 2, fmt.Sprintf("16 个 hash 变体只产生 %d 种结果，派生不敏感", len(hashSeen)))
}

// TestFakeResolverRejects 验证输入校验与 ctx 取消路径。
func TestFakeResolverRejects(t *testing.T) {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()

	tests := []struct {
		name     string
		resolver *FakeResolver
		ctx      context.Context
		mutate   func(in *BattleInput)
		wantErr  error
	}{
		{
			name:     "空 battle_id",
			resolver: &FakeResolver{},
			ctx:      context.Background(),
			mutate:   func(in *BattleInput) { in.BattleID = "" },
			wantErr:  ErrInvalidArgument,
		},
		{
			name:     "ctx 已取消",
			resolver: &FakeResolver{},
			ctx:      canceled,
			wantErr:  context.Canceled,
		},
		{
			name:     "规则版本不符",
			resolver: &FakeResolver{RuleVersion: "battle-rule-v2"},
			ctx:      context.Background(),
			wantErr:  ErrInvalidArgument,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			in := testBattleInput(42)
			if tc.mutate != nil {
				tc.mutate(&in)
			}
			_, err := tc.resolver.Resolve(tc.ctx, in)
			require.Error(t, err)
			// errors.Is 穿透 %w 包装链；context.Canceled 经
			// fmt.Errorf %w 包装后同样可判定。
			require.False(t, !isErr(err, tc.wantErr), fmt.Sprintf("错误类型不符: got %v, want %v", err, tc.wantErr))
		})
	}
}

// isErr 是 errors.Is 的薄封装，避免每个测试文件都 import errors。
func isErr(err, target error) bool {
	for err != nil {
		if err == target {
			return true
		}
		u, ok := err.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}

// TestApplyBattleResultPending 验证 PENDING → RESOLVED 的
// 正常应用：记录 CAS、双方解除战斗态、败者保留 Cargo。
func TestApplyBattleResultPending(t *testing.T) {
	r := &FakeResolver{}
	in := testBattleInput(42)
	res, err := r.Resolve(context.Background(), in)
	require.NoError(t, err, "Resolve 失败")
	attacker, defender := testBattleTroops()
	rec := &BattleRecord{
		BattleID:    in.BattleID,
		WorldID:     1,
		TargetID:    3001,
		AttackerID:  in.AttackerID,
		DefenderID:  in.DefenderID,
		InputHash:   in.InputHash,
		RuleVersion: in.RuleVersion,
		RandomSeed:  in.RandomSeed,
		Status:      BattleStatusPending,
		DeadlineMS:  in.DeadlineMS,
	}

	got, err := ApplyBattleResult(rec, attacker, defender, res, "op-7001")
	require.NoError(t, err, "ApplyBattleResult 失败")
	require.False(t, !reflect.DeepEqual(got, res), fmt.Sprintf("返回结果与传入不一致:\ngot=%+v\nwant=%+v", got, res))
	require.EqualValues(t, BattleStatusResolved, rec.Status)
	require.False(t, rec.Result == nil || !reflect.DeepEqual(*rec.Result, res), fmt.Sprintf("记录结果未正确写入: %+v", rec.Result))
	require.EqualValues(t, "op-7001", rec.AppliedOperationID)
	// 记录里的结果必须是拷贝：外部改 res 不能污染已生效事实。
	res.WinnerID = 9999
	require.NotEqualValues(t, 9999, rec.Result.WinnerID, "记录结果与传入结果共享指针")

	for _, tt := range []struct {
		name  string
		troop *Troop
	}{{"攻方", attacker}, {"守方", defender}} {
		require.EqualValues(t, TroopPhaseReturning, tt.troop.Phase)
		require.EqualValues(t, "", tt.troop.BattleID)
		require.False(t, !tt.troop.Busy.Idle(), fmt.Sprintf("%s Busy 未解除: %+v", tt.name, tt.troop.Busy))
	}
	// 败者保留 Cargo 回城（9.5：先保留货物及交接单，
	// 不能先把 Cargo 清零再尝试发奖）。
	require.EqualValues(t, 5, attacker.Cargo[9001], fmt.Sprintf("Cargo 被篡改: attacker=%v defender=%v", attacker.Cargo, defender.Cargo))
	require.EqualValues(t, 3, defender.Cargo[9002])
}

// TestApplyBattleResultLateDoesNotOverride 验证 INV-06：
// 已终态战斗上应用迟到结果，返回首次结果且不改动部队。
func TestApplyBattleResultLateDoesNotOverride(t *testing.T) {
	tests := []struct {
		name   string
		status BattleStatus
		result *BattleResult
		// wantWinner 为返回结果中的胜者。
		wantWinner ObjectID
	}{
		{
			name:       "迟到 Resolver 结果不覆盖已 RESOLVED",
			status:     BattleStatusResolved,
			result:     &BattleResult{WinnerID: 3001, LoserID: 2001, AppliedAtMS: 1_700_000_001_000, AttackerWon: false},
			wantWinner: 3001,
		},
		{
			name:       "迟到结果不覆盖 TIMED_OUT（无 Resolver 结果，合成防守胜）",
			status:     BattleStatusTimedOut,
			result:     nil,
			wantWinner: 3001,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			attacker, defender := testBattleTroops()
			rec := &BattleRecord{
				BattleID:   "bt-1001",
				Status:     tc.status,
				Result:     tc.result,
				DeadlineMS: 1_700_000_015_000,
			}
			late := BattleResult{WinnerID: 2001, LoserID: 3001, AppliedAtMS: 1_700_000_020_000, AttackerWon: true}

			got, err := ApplyBattleResult(rec, attacker, defender, late, "op-late")
			require.NoError(t, err, "ApplyBattleResult 失败")
			require.EqualValues(t, tc.wantWinner, got.WinnerID)
			require.False(t, got.AttackerWon, "迟到结果不应让攻击方获胜")
			// 部队状态必须保持首次生效时的样子。
			for _, tt := range []struct {
				name  string
				troop *Troop
			}{{"攻方", attacker}, {"守方", defender}} {
				require.EqualValues(t, TroopPhaseFighting, tt.troop.Phase)
				require.EqualValues(t, "bt-1001", tt.troop.BattleID)
				require.EqualValues(t, BusyReasonBattle, tt.troop.Busy.Reason)
			}
			// 记录本身也不得被迟到结果改写。
			require.EqualValues(t, tc.status, rec.Status)
			require.EqualValues(t, "", rec.AppliedOperationID)
		})
	}
}

// TestApplyBattleResultIdempotent 验证同一结果重复应用：
// 第二次是“迟到”，返回首次结果，不产生第二次副作用。
func TestApplyBattleResultIdempotent(t *testing.T) {
	res := BattleResult{WinnerID: 2001, LoserID: 3001, AppliedAtMS: 1_700_000_001_000, AttackerWon: true}
	attacker, defender := testBattleTroops()
	rec := &BattleRecord{BattleID: "bt-1001", Status: BattleStatusPending, DeadlineMS: 1_700_000_015_000}

	first, err := ApplyBattleResult(rec, attacker, defender, res, "op-1")
	require.NoError(t, err, "首次应用失败")
	second, err := ApplyBattleResult(rec, attacker, defender, res, "op-2")
	require.NoError(t, err, "二次应用失败")
	require.False(t, !reflect.DeepEqual(first, second), fmt.Sprintf("两次返回不一致:\nfirst=%+v\nsecond=%+v", first, second))
	require.EqualValues(t, "op-1", rec.AppliedOperationID)
}

// TestApplyBattleResultRejects 验证非法输入：伪造胜者、
// 自相矛盾的败者、损坏的记录状态。
func TestApplyBattleResultRejects(t *testing.T) {
	tests := []struct {
		name    string
		mutRec  func(rec *BattleRecord)
		res     BattleResult
		wantErr error
	}{
		{
			name:    "胜者是第三方",
			res:     BattleResult{WinnerID: 9999, LoserID: 3001, AppliedAtMS: 1},
			wantErr: ErrInvalidArgument,
		},
		{
			name:    "败者与胜者矛盾",
			res:     BattleResult{WinnerID: 2001, LoserID: 2001, AppliedAtMS: 1},
			wantErr: ErrInvalidArgument,
		},
		{
			name:    "记录状态非法",
			mutRec:  func(rec *BattleRecord) { rec.Status = BattleStatus(99) },
			res:     BattleResult{WinnerID: 2001, LoserID: 3001, AppliedAtMS: 1},
			wantErr: ErrInvalidState,
		},
		{
			name:    "RESOLVED 但缺少结果",
			mutRec:  func(rec *BattleRecord) { rec.Status = BattleStatusResolved },
			res:     BattleResult{WinnerID: 2001, LoserID: 3001, AppliedAtMS: 1},
			wantErr: ErrInvalidState,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			attacker, defender := testBattleTroops()
			rec := &BattleRecord{BattleID: "bt-1001", Status: BattleStatusPending, DeadlineMS: 1}
			if tc.mutRec != nil {
				tc.mutRec(rec)
			}
			_, err := ApplyBattleResult(rec, attacker, defender, tc.res, "op-x")
			require.False(t, !isErr(err, tc.wantErr), fmt.Sprintf("错误类型不符: got %v, want %v", err, tc.wantErr))
		})
	}

	// nil 入参。
	_, err := ApplyBattleResult(nil, nil, nil, BattleResult{}, "op")
	require.False(t, !isErr(err, ErrInvalidArgument), fmt.Sprintf("nil 记录错误类型不符: %v", err))
	_, defender := testBattleTroops()
	rec := &BattleRecord{BattleID: "bt-1001", Status: BattleStatusPending}
	_, err = ApplyBattleResult(rec, nil, defender, BattleResult{WinnerID: 2001}, "op")
	require.False(t, !isErr(err, ErrInvalidArgument), fmt.Sprintf("nil 攻方错误类型不符: %v", err))
}
