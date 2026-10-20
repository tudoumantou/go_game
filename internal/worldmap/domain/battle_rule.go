package domain

// battle_rule.go 提供可上线的规则化裁决实现（文档 11.2/11.3）。
//
// 与 battle.go 里 FakeResolver 的关系：
//   - FakeResolver 是"让流程跑通"的玩具（线性加权重加一次扰动），
//     只在测试/开发使用；
//   - DeterministicResolver 是规则化版本：权重、损失比例、伤害上下限
//     全部来自 CombatRule（配置注入），仍然保持**完全确定性**——
//     同一 input_hash + random_seed 必得同一结果。
//
// 为什么"确定性"是硬约束而不是优化项（文档 11.2）：
//  1. 恢复后 PENDING 战斗要用同一输入重新 Resolve（11.3），若裁决
//     内部读时钟或用全局随机源，重放结果就会与首次不同，玩家会看到
//     "重启前后胜负反转"；
//  2. 对账时数据库只存 input_hash 与 result，复核方必须能用同一份
//     seed 复算出逐字节相同的 DetailJSON；
//  3. Go 的 math/rand 全局源从 1.20 起自动随机播种，且其序列属于
//     实现细节（跨版本不保证），拿它做 seed 等于放弃可回放性。
//     因此这里继续用 splitmix64（纯函数推进，无隐藏状态）。
//
// 一个容易踩的坑：Go 的 map 遍历顺序是随机的，任何"按遍历顺序累加
// 或比较"的裁决都会不确定。本文件只对结构体字段做固定顺序的整数
// 运算，不遍历任何 map，因此结果与调度顺序无关。

import (
	"context"
	"fmt"
)

// CombatRule 是战斗裁决规则（服务端配置，文档 11.1/16.5）。
//
// 放在 domain 而不是 application：它是**裁决算法**的一部分，而
// application 只负责"什么时候裁决、裁决结果怎么落库"。规则值本身
// 由配置注入，domain 不知道这一版运营参数是多少——这正是
// FakeResolver 与 DeterministicResolver 能共存的原因。
type CombatRule struct {
	// AttackWeight/DefenseWeight/HpWeight 战力权重。
	// 战力 = attack*AttackWeight + defense*DefenseWeight + hp*HpWeight。
	// 三者允许为 0（例如只按攻击算），但总和必须为正——全 0 会让
	// 所有部队战力相同，胜负完全由扰动决定，属于配置错误。
	AttackWeight  int64
	DefenseWeight int64
	HpWeight      int64

	// DamageRatioPermille 胜方损失比例（千分比，0..1000）。
	// 100 表示胜方损失 10% 生命，0 表示无战损（第一版 10.4：
	// 不发生兵力扣减时用 0）。
	DamageRatioPermille int64

	// MinDamage 单场战斗的最小损失（绝对值）。DamageRatioPermille>0
	// 时至少造成 1 点（否则低血量部队的 floor 结果恒为 0，胜负没有
	// 任何代价）；MinDamage 可以把下限抬得更高。
	MinDamage int64

	// MaxDamage 单场战斗的最大损失（绝对值），0 表示不设上限。
	MaxDamage int64

	// MaxRounds 回合上限。第一版裁决是单轮比较，本字段只参与校验
	// 与 DetailJSON 记录（保留给将来多回合演算），不影响胜负。
	MaxRounds int32

	// RuleVersion 规则版本。非空时要求输入规则版本一致（16.5：
	// 规则热更新不重算历史战斗，旧 Resolver 拒绝新版本的旧单）。
	// 零值不校验，方便测试直接构造规则。
	RuleVersion string
}

// Validate 校验规则自洽。启动时与每次裁决前各校验一次：
// 启动校验是为了"配置错了不要上线"，裁决前校验是为了防止
// 调用方绕过构造器塞进零值规则（除零/负权重会直接污染结果）。
func (r CombatRule) Validate() error {
	if r.AttackWeight < 0 || r.DefenseWeight < 0 || r.HpWeight < 0 {
		return fmt.Errorf("%w: 战力权重不能为负 (attack=%d defense=%d hp=%d)",
			ErrInvalidArgument, r.AttackWeight, r.DefenseWeight, r.HpWeight)
	}
	if r.AttackWeight == 0 && r.DefenseWeight == 0 && r.HpWeight == 0 {
		return fmt.Errorf("%w: 战力权重不能全为 0", ErrInvalidArgument)
	}
	if r.DamageRatioPermille < 0 || r.DamageRatioPermille > 1000 {
		return fmt.Errorf("%w: 损失比例必须在 [0,1000] 千分比内, got %d",
			ErrInvalidArgument, r.DamageRatioPermille)
	}
	if r.MinDamage < 0 {
		return fmt.Errorf("%w: 最小损失不能为负, got %d", ErrInvalidArgument, r.MinDamage)
	}
	if r.MaxDamage < 0 {
		return fmt.Errorf("%w: 最大损失不能为负, got %d", ErrInvalidArgument, r.MaxDamage)
	}
	// MaxDamage=0 是"不设上限"的哨兵，其余情况必须 >= MinDamage，
	// 否则上下限自相矛盾，损失取值无解。
	if r.MaxDamage > 0 && r.MaxDamage < r.MinDamage {
		return fmt.Errorf("%w: 最大损失 %d 小于最小损失 %d",
			ErrInvalidArgument, r.MaxDamage, r.MinDamage)
	}
	if r.MaxRounds < 1 {
		return fmt.Errorf("%w: 回合上限必须至少为 1, got %d", ErrInvalidArgument, r.MaxRounds)
	}
	return nil
}

// DefaultCombatRule 返回第一版默认规则（文档 11.1 的免费 MVP）。
//
// 攻击权重高于防御：进攻方需要主动优势才能赢，否则防守方凭平局
// 判定（见 AttackerWins）就能无限拖延。HP 权重较低，只用于
// 体现"血厚"的边际价值。
func DefaultCombatRule() CombatRule {
	return CombatRule{
		AttackWeight:        3,
		DefenseWeight:       2,
		HpWeight:            1,
		DamageRatioPermille: 100, // 胜方损失 10% 生命
		MinDamage:           1,
		MaxDamage:           0,
		MaxRounds:           1,
		RuleVersion:         "m1",
	}
}

// jitterPermille 是战力扰动幅度（千分比）：±10%。
// 写成常量而不是字段，是因为"扰动幅度"属于协议级数值平衡，
// 变更它等同于换规则版本，应通过 RuleVersion 显式升版。
const jitterPermille int64 = 100

// DeterministicResolver 是规则化、完全确定性的战斗裁决器。
//
// 并发安全：结构体只读（Rule 在构造后不再修改），Resolve 不写任何
// 字段，也没有包级可变状态，因此可以被工作池的多个 goroutine 同时
// 调用。反过来，任何"缓存上一次结果"的想法都会破坏单测的
// 100 次重复一致性与恢复重算。
type DeterministicResolver struct {
	Rule CombatRule
}

// 编译期断言：必须满足 BattleResolver 契约。
var _ BattleResolver = (*DeterministicResolver)(nil)

// Resolve 按规则裁决一场战斗。
//
// 步骤（固定顺序，任何一步的输入都只来自 BattleInput 与 Rule）：
//  1. 校验 ctx/BattleID/规则/规则版本；
//  2. 计算双方基础战力；
//  3. 用 seed 派生的 splitmix64 流扰动双方战力（±10%）；
//  4. 战力高者胜，**相等时守方胜**；
//  5. 胜方按 DamageRatioPermille 损失 HP（floor，至少 1）。
//
// 注意第 4 步：比较用严格大于。平局判防守胜与 11.3 超时语义
// （攻击无效、防守保留）同向，也避免同战力双方互相刷战报。
func (r *DeterministicResolver) Resolve(ctx context.Context, in BattleInput) (BattleResult, error) {
	// 工作池执行时 ctx 可能已被取消（场景关闭/超时），及时返回，
	// 不让一个已作废的裁决占用工作协程。
	if err := ctx.Err(); err != nil {
		return BattleResult{}, fmt.Errorf("战斗 %s 裁决取消: %w", in.BattleID, err)
	}
	if in.BattleID == "" {
		return BattleResult{}, fmt.Errorf("%w: battle_id 不能为空", ErrInvalidArgument)
	}
	if err := r.Rule.Validate(); err != nil {
		return BattleResult{}, fmt.Errorf("战斗 %s 规则非法: %w", in.BattleID, err)
	}
	if r.Rule.RuleVersion != "" && in.RuleVersion != r.Rule.RuleVersion {
		return BattleResult{}, fmt.Errorf("%w: 战斗 %s 规则版本 %q 与 Resolver %q 不符",
			ErrInvalidArgument, in.BattleID, in.RuleVersion, r.Rule.RuleVersion)
	}

	// 随机流初值同时吸收 random_seed 与 input_hash：改参数后 hash
	// 变化，同一 seed 也会分叉，避免"参数不同但随机序列相同"导致
	// 的伪相关（例如同秒内两次不同目标的战斗总是同胜负）。
	rng := newFakeRNG(in)
	attackerPower := jitter(r.power(in.Attacker), rng.next())
	defenderPower := jitter(r.power(in.Defender), rng.next())

	attackerWon := r.Rule.AttackerWins(attackerPower, defenderPower)
	winner, loser := in.DefenderID, in.AttackerID
	winnerStats, loserStats := in.Defender, in.Attacker
	if attackerWon {
		winner, loser = in.AttackerID, in.DefenderID
		winnerStats, loserStats = in.Attacker, in.Defender
	}
	damage := r.Rule.WinnerDamage(winnerStats.HP)

	// DetailJSON 只包含确定性派生量，绝不含时间戳：它要参与
	// "相同输入→相同结果"的比较，混入时钟就永远比不等。
	// 用固定格式串而不是 map+json.Marshal：字段顺序确定、无反射开销。
	detail := fmt.Sprintf(
		`{"rule_version":%q,"attack_weight":%d,"defense_weight":%d,"hp_weight":%d,`+
			`"attacker_power":%d,"defender_power":%d,"attacker_won":%t,`+
			`"winner_id":%d,"loser_id":%d,"winner_damage":%d,`+
			`"winner_hp":%d,"loser_hp":%d,"max_rounds":%d,"rounds":1}`,
		r.Rule.RuleVersion, r.Rule.AttackWeight, r.Rule.DefenseWeight, r.Rule.HpWeight,
		attackerPower, defenderPower, attackerWon,
		uint64(winner), uint64(loser), damage,
		winnerStats.HP, loserStats.HP, r.Rule.MaxRounds)

	return BattleResult{
		WinnerID:    winner,
		LoserID:     loser,
		AppliedAtMS: in.ResolveAtMS,
		AttackerWon: attackerWon,
		DetailJSON:  []byte(detail),
	}, nil
}

// power 计算基础战力：攻击/防御/生命三项线性加权。
//
// 溢出处理：三项都用饱和乘法/饱和加法（见 mulSat/addSat）。属性来自
// 配置或玩家快照，脏数据可能大到让乘法回绕；一旦回绕成负数，
// "强的一方"会直接输掉比较，这类 bug 在线上极难复现。
func (r *DeterministicResolver) power(s CombatSnapshot) int64 {
	p := mulWeighted(s.Attack, r.Rule.AttackWeight)
	p = addSat(p, mulWeighted(s.Defense, r.Rule.DefenseWeight))
	return addSat(p, mulWeighted(s.HP, r.Rule.HpWeight))
}

// maxInt64 是饱和运算的上界。写成包级常量而不是每处重复
// ^uint64(0)>>1，是为了让"溢出后取什么值"在代码里只有一个答案。
const maxInt64 int64 = int64(^uint64(0) >> 1)

// addSat 是饱和加法：各项已饱和到 MaxInt64，直接相加会回绕成
// 负数。饱和后"极大的一方"仍然极大，比较结果依然正确。
func addSat(a, b int64) int64 {
	if a > maxInt64-b {
		return maxInt64
	}
	return a + b
}

// mulSat 是饱和乘法。
//
// 为什么不直接 a*w：属性来自配置/玩家快照，脏数据（负数或天文数字）
// 在乘法上会回绕成负战力，让"强的一方"直接输。饱和是最保守的选择：
// 极大值仍然极大，不会翻符号。
func mulSat(a, w int64) int64 {
	if a <= 0 || w <= 0 {
		return 0
	}
	if a > maxInt64/w {
		return maxInt64
	}
	return a * w
}

// mulWeighted 计算属性乘权重。语义与 mulSat 相同，单独命名是为了
// 在 power 里表达"这是权重项"而不是任意乘法。
func mulWeighted(a, w int64) int64 { return mulSat(a, w) }

// jitter 给战力施加 -10%..+10% 的确定性扰动。
//
// rand64 取自 splitmix64 流，factor ∈ [900,1100]。乘以 factor/1000 时
// 先拆商与余数，避免 power*1100 在战力接近 2^63/1100 时溢出：
//
//	power/1000*factor + power%1000*factor/1000
//
// 商项用饱和乘法（power 已被 power() 饱和过，可能正好是 MaxInt64），
// 余数项最大 999*1100/1000 远小于上限。整数运算保证同 seed 同结果，
// 不存在浮点在不同架构上舍入不一致的风险。
func jitter(power int64, rand64 uint64) int64 {
	if power <= 0 {
		return 0
	}
	factor := 1000 - jitterPermille + int64(rand64%uint64(2*jitterPermille+1)) // [900,1100]
	return addSat(mulSat(power/1000, factor), (power%1000)*factor/1000)
}

// AttackerWins 判断攻击方是否获胜：**严格大于**才赢。
//
// 相等判守方胜（防守优势）：若改成 >=，同战力双方各执一词，
// 且防守方没有任何主场收益，"互相刷战报"的成本为零。
func (r CombatRule) AttackerWins(attackerPower, defenderPower int64) bool {
	return attackerPower > defenderPower
}

// WinnerDamage 计算胜方损失 HP（文档 11.3 的"胜方按比例损失"）。
//
// 规则：
//   - 比例为 0 → 0（第一版 10.4 不发生兵力扣减时使用）；
//   - 否则 damage = floor(hp * ratio / 1000)；
//   - 再抬到 min(至少 1 点 / MinDamage)；
//   - 最后压到 MaxDamage（0 表示不设上限）与 hp 本身（损失不能
//     超过持有量，否则会出现负生命）。
//
// 为什么用 floor：与采集结算（10.2 也按 floor）保持一致。
// 若这里 ceil、那里 floor，同一场战斗的"伤害"与"收益"在不同路径上
// 会对不上账。
func (r CombatRule) WinnerDamage(hp int64) int64 {
	if r.DamageRatioPermille <= 0 || hp <= 0 {
		return 0
	}
	d := hp/1000*r.DamageRatioPermille + (hp%1000)*r.DamageRatioPermille/1000
	if d < r.MinDamage {
		d = r.MinDamage
	}
	// 比例为正时至少 1 点：低血量部队 floor 结果为 0 会让胜负
	// 完全没有代价，等于变相鼓励用 1 血部队消耗对手。
	if d < 1 {
		d = 1
	}
	if r.MaxDamage > 0 && d > r.MaxDamage {
		d = r.MaxDamage
	}
	if d > hp {
		d = hp
	}
	return d
}
