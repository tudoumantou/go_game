package domain

// battle_rule_test.go 覆盖规则化裁决（文档 11.2/11.3）的三条硬约束：
//  1. 相同输入必须逐字节相同（可回放/可对账）；
//  2. 不同 seed 必须能产生不同结果（战斗不能退化成固定剧本）；
//  3. 战力相等时守方胜、胜方损失按 floor 且至少 1 点。

import (
	"context"
	"encoding/json"
	"github.com/stretchr/testify/require"
	"reflect"
	"testing"
)

// ruleTestInput 构造一份只依赖攻击属性的输入，便于精确控制胜负：
// 权重只给攻击，战力就等于 Attack，扰动范围也可以用纸笔算出来。
func ruleTestInput(seed int64, attackerAttack, defenderAttack, winnerHP int64) BattleInput {
	in := testBattleInput(seed)
	in.RuleVersion = "m1"
	in.Attacker = CombatSnapshot{Attack: attackerAttack, Defense: 0, HP: winnerHP, RuleVersion: "m1"}
	in.Defender = CombatSnapshot{Attack: defenderAttack, Defense: 0, HP: 100, RuleVersion: "m1"}
	return in
}

func testCombatRule() CombatRule {
	return CombatRule{
		AttackWeight:        1,
		DefenseWeight:       0,
		HpWeight:            0,
		DamageRatioPermille: 100,
		MinDamage:           1,
		MaxRounds:           1,
		RuleVersion:         "m1",
	}
}

// TestDeterministicResolverSameInput100Times 验证"相同输入 100 次
// 结果完全一致"，比较包含 DetailJSON 在内的整个结构体。
func TestDeterministicResolverSameInput100Times(t *testing.T) {
	r := &DeterministicResolver{Rule: testCombatRule()}
	in := ruleTestInput(42, 120, 100, 1000)

	first, err := r.Resolve(context.Background(), in)
	require.NoError(t, err, "首次裁决失败")
	if first.WinnerID != in.AttackerID && first.WinnerID != in.DefenderID {
		t.Fatalf("胜者 %d 不是攻守任何一方", first.WinnerID)
	}
	require.EqualValues(t, in.ResolveAtMS, first.AppliedAtMS)
	require.EqualValues(t, (first.WinnerID == in.AttackerID), first.AttackerWon)
	for i := 0; i < 100; i++ {
		got, err := r.Resolve(context.Background(), in)
		require.NoError(t, err)
		// reflect.DeepEqual 会把 DetailJSON 的 []byte 逐字节比较：
		// 这正是"可对账"需要的强度，时间戳/浮点/遍历顺序任何一处
		// 不确定都会让它失败。
		if !reflect.DeepEqual(first, got) {
			t.Fatalf("第 %d 次结果与首次不一致:\nfirst=%+v\ngot=%+v", i, first, got)
		}
	}
}

// TestDeterministicResolverSeedVariation 验证不同 seed 至少出现两种
// 结果。用完全对称的属性：胜负完全由 seed 派生的扰动决定，
// 若实现里误用了固定常量或全局随机源，这里会退化成单一结果。
func TestDeterministicResolverSeedVariation(t *testing.T) {
	r := &DeterministicResolver{Rule: testCombatRule()}
	seen := map[ObjectID]bool{}
	for seed := int64(0); seed < 64; seed++ {
		in := ruleTestInput(seed, 100, 100, 1000)
		got, err := r.Resolve(context.Background(), in)
		require.NoError(t, err)
		seen[got.WinnerID] = true
	}
	if len(seen) < 2 {
		t.Fatalf("64 个种子只产生 %d 种结果，随机性不足", len(seen))
	}
}

// TestCombatRuleTieGoesToDefender 验证"战力相等时守方胜"。
// 这条规则必须能单独测：统计意义上的"守方胜率更高"无法证明
// 平局分支本身是对的。
func TestCombatRuleTieGoesToDefender(t *testing.T) {
	rule := testCombatRule()
	if rule.AttackerWins(100, 100) {
		t.Fatal("战力相等时不应判攻击方胜（防守优势）")
	}
	if !rule.AttackerWins(101, 100) {
		t.Fatal("攻击方战力更高时必须判攻击方胜")
	}
	if rule.AttackerWins(99, 100) {
		t.Fatal("攻击方战力更低时必须判守方胜")
	}
}

// TestDeterministicResolverDefenderAdvantageNearEqual 验证"战力相近时
// 守方胜率更高"（统计 200 次）。
//
// 攻击方战力 97、守方 100（差 3%，属于"相近"）。若没有防守优势，
// 攻方应该拿到接近 45%~50% 的胜率；本用例断言守方胜场严格更多。
// 种子是固定序列，因此这是一个**确定性**的统计断言，不会因 CI
// 机器不同而抖动。
func TestDeterministicResolverDefenderAdvantageNearEqual(t *testing.T) {
	r := &DeterministicResolver{Rule: testCombatRule()}
	attackerWins, defenderWins := 0, 0
	for seed := int64(0); seed < 200; seed++ {
		in := ruleTestInput(seed, 97, 100, 1000)
		got, err := r.Resolve(context.Background(), in)
		require.NoError(t, err)
		if got.AttackerWon {
			attackerWins++
		} else {
			defenderWins++
		}
	}
	if defenderWins <= attackerWins {
		t.Fatalf("战力相近时守方应更常获胜: attacker=%d defender=%d", attackerWins, defenderWins)
	}
	if defenderWins < 100 {
		t.Fatalf("守方胜率 %d/200 明显异常（预期过半）", defenderWins)
	}
}

// combatDetail 是 DetailJSON 的测试侧解码结构。
type combatDetail struct {
	RuleVersion   string `json:"rule_version"`
	AttackerPower int64  `json:"attacker_power"`
	DefenderPower int64  `json:"defender_power"`
	AttackerWon   bool   `json:"attacker_won"`
	WinnerDamage  int64  `json:"winner_damage"`
	WinnerHP      int64  `json:"winner_hp"`
	LoserHP       int64  `json:"loser_hp"`
	WinnerID      uint64 `json:"winner_id"`
	LoserID       uint64 `json:"loser_id"`
	MaxRounds     int32  `json:"max_rounds"`
	Rounds        int32  `json:"rounds"`
	AttackWeight  int64  `json:"attack_weight"`
	DefenseWeight int64  `json:"defense_weight"`
	HpWeight      int64  `json:"hp_weight"`
}

func decodeDetail(t *testing.T, res BattleResult) combatDetail {
	t.Helper()
	var d combatDetail
	if err := json.Unmarshal(res.DetailJSON, &d); err != nil {
		t.Fatalf("DetailJSON 解析失败: %v (%s)", err, res.DetailJSON)
	}
	return d
}

// TestCombatRuleWinnerDamage 验证损失计算：floor、至少 1、比例 0 无战损、
// MinDamage/MaxDamage 上下限。
func TestCombatRuleWinnerDamage(t *testing.T) {
	cases := []struct {
		name string
		rule CombatRule
		hp   int64
		want int64
	}{
		{
			name: "floor: 999*125‰ = 124.875 → 124",
			rule: CombatRule{AttackWeight: 1, DamageRatioPermille: 125, MaxRounds: 1},
			hp:   999,
			want: 124,
		},
		{
			name: "比例极小也至少 1 点",
			rule: CombatRule{AttackWeight: 1, DamageRatioPermille: 1, MaxRounds: 1},
			hp:   100,
			want: 1,
		},
		{
			name: "比例 0 表示无战损",
			rule: CombatRule{AttackWeight: 1, DamageRatioPermille: 0, MinDamage: 5, MaxRounds: 1},
			hp:   1000,
			want: 0,
		},
		{
			name: "MinDamage 抬高下限",
			rule: CombatRule{AttackWeight: 1, DamageRatioPermille: 1, MinDamage: 5, MaxRounds: 1},
			hp:   100,
			want: 5,
		},
		{
			name: "MaxDamage 封顶",
			rule: CombatRule{AttackWeight: 1, DamageRatioPermille: 500, MinDamage: 1, MaxDamage: 30, MaxRounds: 1},
			hp:   1000,
			want: 30,
		},
		{
			name: "损失不超过持有生命",
			rule: CombatRule{AttackWeight: 1, DamageRatioPermille: 1000, MaxRounds: 1},
			hp:   7,
			want: 7,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.rule.WinnerDamage(tc.hp); got != tc.want {
				t.Fatalf("WinnerDamage(%d)=%d, want %d", tc.hp, got, tc.want)
			}
		})
	}
}

// TestDeterministicResolverDetailDamage 验证裁决结果里的损失字段与
// 规则一致（真正的应用层不做兵力扣减，10.4，损失只进审计）。
func TestDeterministicResolverDetailDamage(t *testing.T) {
	rule := testCombatRule()
	rule.DamageRatioPermille = 125
	r := &DeterministicResolver{Rule: rule}
	in := ruleTestInput(7, 1000, 1, 999) // 攻击方必胜

	res, err := r.Resolve(context.Background(), in)
	require.NoError(t, err, "裁决失败")
	if !res.AttackerWon {
		t.Fatalf("攻击方战力 1000 对 1，应获胜: %s", res.DetailJSON)
	}
	d := decodeDetail(t, res)
	require.EqualValues(t, 124, d.WinnerDamage)
	if d.WinnerHP != 999 || d.LoserHP != 100 {
		t.Fatalf("DetailJSON 中双方生命不符: %+v", d)
	}
	if d.RuleVersion != "m1" || d.Rounds != 1 || d.MaxRounds != 1 {
		t.Fatalf("DetailJSON 缺少规则版本/回合信息: %+v", d)
	}
	if d.WinnerID != uint64(in.AttackerID) || d.LoserID != uint64(in.DefenderID) {
		t.Fatalf("DetailJSON 胜负身份不符: %+v", d)
	}
}

// TestCombatRuleValidate 验证规则自洽校验。
func TestCombatRuleValidate(t *testing.T) {
	ok := testCombatRule()
	if err := ok.Validate(); err != nil {
		t.Fatalf("默认测试规则应合法: %v", err)
	}
	if err := DefaultCombatRule().Validate(); err != nil {
		t.Fatalf("默认规则应合法: %v", err)
	}
	cases := []struct {
		name string
		rule CombatRule
	}{
		{name: "权重全 0", rule: CombatRule{DamageRatioPermille: 100, MaxRounds: 1}},
		{name: "负权重", rule: CombatRule{AttackWeight: -1, MaxRounds: 1}},
		{name: "损失比例超 1000", rule: CombatRule{AttackWeight: 1, DamageRatioPermille: 1001, MaxRounds: 1}},
		{name: "负最小损失", rule: CombatRule{AttackWeight: 1, MinDamage: -1, MaxRounds: 1}},
		{name: "上下限矛盾", rule: CombatRule{AttackWeight: 1, MinDamage: 10, MaxDamage: 5, MaxRounds: 1}},
		{name: "回合上限为 0", rule: CombatRule{AttackWeight: 1}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.rule.Validate(); err == nil {
				t.Fatalf("非法规则应被拒绝: %+v", tc.rule)
			} else if !isErr(err, ErrInvalidArgument) {
				t.Fatalf("错误类型不符: %v", err)
			}
		})
	}
}

// TestDeterministicResolverRejects 验证入参校验与 ctx 取消。
func TestDeterministicResolverRejects(t *testing.T) {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()

	rule := testCombatRule()
	r := &DeterministicResolver{Rule: rule}

	t.Run("空 battle_id", func(t *testing.T) {
		in := ruleTestInput(1, 10, 10, 100)
		in.BattleID = ""
		if _, err := r.Resolve(context.Background(), in); !isErr(err, ErrInvalidArgument) {
			t.Fatalf("err=%v", err)
		}
	})
	t.Run("ctx 已取消", func(t *testing.T) {
		if _, err := r.Resolve(canceled, ruleTestInput(1, 10, 10, 100)); !isErr(err, context.Canceled) {
			t.Fatalf("err=%v", err)
		}
	})
	t.Run("规则版本不符", func(t *testing.T) {
		in := ruleTestInput(1, 10, 10, 100)
		in.RuleVersion = "m2"
		if _, err := r.Resolve(context.Background(), in); !isErr(err, ErrInvalidArgument) {
			t.Fatalf("err=%v", err)
		}
	})
	t.Run("规则非法", func(t *testing.T) {
		bad := &DeterministicResolver{Rule: CombatRule{MaxRounds: 1}}
		if _, err := bad.Resolve(context.Background(), ruleTestInput(1, 10, 10, 100)); !isErr(err, ErrInvalidArgument) {
			t.Fatalf("err=%v", err)
		}
	})
}

// TestDeterministicResolverOverflowSafe 验证脏属性（极大值）不会因
// 乘法溢出把强弱关系翻过来。
func TestDeterministicResolverOverflowSafe(t *testing.T) {
	rule := CombatRule{AttackWeight: 1 << 40, MaxRounds: 1}
	r := &DeterministicResolver{Rule: rule}
	in := ruleTestInput(3, 1<<40, 1, 100)
	got, err := r.Resolve(context.Background(), in)
	require.NoError(t, err, "裁决失败")
	if !got.AttackerWon {
		t.Fatalf("饱和后攻击方仍应获胜: %s", got.DetailJSON)
	}
}
