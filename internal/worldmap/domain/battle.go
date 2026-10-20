package domain

import (
	"context"
	"fmt"
)

// battle.go 承载战斗裁决的领域契约（文档第 11 章）：
// 不可变输入 BattleInput、可替换的 BattleResolver 接口、
// 确定性测试替身 FakeResolver，以及把结果应用到攻守双方
// Troop 的 ApplyBattleResult。
//
// 三个核心设计决策：
//
//  1. Resolver 是接口而不是具体类型。生产版本接入服务端权威规则
//     （11.2），测试与开发跑通流程时注入 FakeResolver。domain
//     只依赖接口——这是依赖倒置：高层模块（SceneLoop 应用层）
//     不依赖低层细节（具体裁决算法），两者都依赖抽象。
//     接口只有一个方法，调用方可以用任意满足签名的类型替换，
//     包括未来拆成“规则引擎 + 随机源”的组合。
//
//  2. 相同输入必须返回相同结果（11.2）。这不是靠约定，而是靠
//     数据结构保证：输入自带 input_hash 与 random_seed，
//     Resolver 内部不读时钟、不用 math/rand 全局源。恢复后
//     PENDING 战斗按 11.3 用同输入重新 Resolve，只有确定性
//     实现才能让重放结果与首次一致。
//
//  3. 迟到结果不可覆盖终态（INV-06）。ApplyBattleResult 以
//     战斗记录的状态做 CAS：已终态直接返回首次结果，
//     部队状态不做任何改动——迟到回包不能反转已生效裁决。

// BattleInput 是战斗裁决的不可变输入（文档 11.1/11.2）。
// 所有字段在构造时一次确定；Resolve 内部不得再获取时间或
// 随机数，否则“重放得到相同结果”就只剩下口头约定。
type BattleInput struct {
	// BattleID 不可变战斗编号（map_battle 主键，服务端 UUID）。
	BattleID string
	// AttackerID/DefenderID 双方对象编号。BattleResult 用
	// ObjectID 表述胜负，因此输入必须携带身份——只带属性
	// 快照无法生成可应用的结果。文档 11.1 的 BattleRecord
	// 同样把 id 与快照分列，不混在一个结构里。
	AttackerID ObjectID
	DefenderID ObjectID
	// Attacker/Defender 双方服务端战斗属性快照（11.1）。
	// 免费 MVP 属性由服务端固定配置生成；真实玩家属性在
	// 出征时从 Game 可信快照取得，绝不信任客户端上报。
	Attacker CombatSnapshot
	Defender CombatSnapshot
	// InputHash 输入摘要。它既是“相同输入→相同结果”的
	// 可判定依据，也用于“同键不同参”检测：11.3 规定
	// input_hash 不符的结果要隔离告警而不是应用。
	// [32]byte 是数组不是切片：数组可用 == 直接比较，
	// 不需要 bytes.Equal，也天然可作 map 键。
	InputHash [32]byte
	// RuleVersion 规则版本。规则热更新不重算历史战斗
	// （16.5），Resolver 可据此拒绝版本不符的旧单。
	RuleVersion string
	// RandomSeed 随机种子，持久化于 BattleRecord.random_seed，
	// 保证战斗可回放、可对账。
	RandomSeed int64
	// ResolveAtMS 裁决时刻。由调用方在规划时一次获取并入
	// 输入（9.2：命令内部的 now 一次获取，重试不重新计算），
	// Resolver 不自己读时钟——否则重放时 now 不同，
	// 连 AppliedAtMS 都会偏离首次结果。
	ResolveAtMS int64
	// DeadlineMS 战斗截止时刻；超时走确定的 TIMED_OUT
	// 命令（11.3，默认 15s，见 17.1 battle_timeout）。
	DeadlineMS int64
}

// BattleResolver 是战斗裁决接口（文档 11.2）。
//
// 契约：
//   - 相同 BattleInput 必须返回相同 BattleResult；
//   - 在工作池执行，结果回目标 SceneLoop，调用方不得在
//     SceneLoop 内阻塞等 RPC；
//   - 外部实现需认证响应来源；客户端上报的 BattleResult
//     不能操作本系统的 map_battle。
type BattleResolver interface {
	Resolve(ctx context.Context, in BattleInput) (BattleResult, error)
}

// FakeResolver 是确定性战斗裁决替身。
//
// 【仅测试/开发，不可用于生产 PVP】
// 它不实现任何真实战斗规则，只用双方属性的线性加权加一次
// 确定性扰动比较大小，用来在开发阶段跑通“出征→共置→
// 战斗→结果应用”全流程。生产版本必须接入服务端权威规则、
// 规则版本与可回放随机种子（11.2）；把假模型接到线上
// 等于把 PVP 胜负交给一个玩具。
//
// 确定性保证：所有“随机”来自 splitmix64 流，初值由
// random_seed 与 input_hash 派生；不读时钟、不用 math/rand。
// math/rand 的全局源从 Go 1.20 起自动随机播种，且其序列
// 无法从持久化种子重建——用它就没有“可回放”可言。
type FakeResolver struct {
	// RuleVersion 非空时要求输入规则版本一致，模拟规则热
	// 更新后旧 Resolver 拒绝旧单（16.5：热更新不重算历史
	// 战斗）。零值不检查，方便测试直接 &FakeResolver{}。
	RuleVersion string
}

// 编译期断言：FakeResolver 必须满足 BattleResolver。
// 接口实现错了在编译期就炸，而不是等到第一次战斗。
var _ BattleResolver = (*FakeResolver)(nil)

// Resolve 实现 BattleResolver。对相同输入返回逐字节相同的结果
// （含 DetailJSON），测试可直接 reflect.DeepEqual 比较。
func (r *FakeResolver) Resolve(ctx context.Context, in BattleInput) (BattleResult, error) {
	// Resolver 在工作池执行（11.2），ctx 取消要及时返回，
	// 不在已取消的上下文上继续演算——取消继续算不仅浪费，
	// 还可能让迟到结果挤占正常结果的应用顺序。
	if err := ctx.Err(); err != nil {
		return BattleResult{}, fmt.Errorf("战斗 %s 裁决取消: %w", in.BattleID, err)
	}
	if in.BattleID == "" {
		return BattleResult{}, fmt.Errorf("%w: battle_id 不能为空", ErrInvalidArgument)
	}
	if r.RuleVersion != "" && in.RuleVersion != r.RuleVersion {
		return BattleResult{}, fmt.Errorf("%w: 战斗 %s 规则版本 %q 与 Resolver %q 不符",
			ErrInvalidArgument, in.BattleID, in.RuleVersion, r.RuleVersion)
	}

	rng := newFakeRNG(in)
	// 假力量模型：攻击权重三倍于防御（攻击方需要严格优势），
	// 再加 0..100 的确定性扰动。平局判防守胜，与 11.3
	// 超时“攻击无效、防守保留”同向，避免平局语义二义。
	attackerPower := in.Attacker.Attack*3 + in.Attacker.Defense + int64(rng.next()%101)
	defenderPower := in.Defender.Defense*3 + in.Defender.Attack + int64(rng.next()%101)
	attackerWon := attackerPower > defenderPower

	winner, loser := in.DefenderID, in.AttackerID
	if attackerWon {
		winner, loser = in.AttackerID, in.DefenderID
	}
	// DetailJSON 只含确定性派生量，不含时间戳——它要参与
	// “相同输入相同结果”的比较，混入时钟就永远比不等。
	detail := fmt.Sprintf(`{"fake":true,"attacker_power":%d,"defender_power":%d,"attacker_won":%t}`,
		attackerPower, defenderPower, attackerWon)
	return BattleResult{
		WinnerID:    winner,
		LoserID:     loser,
		AppliedAtMS: in.ResolveAtMS,
		AttackerWon: attackerWon,
		DetailJSON:  []byte(detail),
	}, nil
}

// fakeRNG 是 splitmix64 伪随机流（SplitMix64，public domain）。
// 选它的理由：无隐藏状态、纯函数式推进、64 位周期，
// 相同种子必然产生相同序列——这正是“可回放”需要的性质。
// math/rand 的全局源带互斥锁且种子不可重建，xorshift 的
// 低比特线性结构在取模小范围时偏差明显，都不适合。
type fakeRNG uint64

// splitmix64 单步混合函数，常量取自 SplitMix64 原始实现。
func splitmix64(z uint64) uint64 {
	z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
	z = (z ^ (z >> 27)) * 0x94D049BB133111EB
	return z ^ (z >> 31)
}

// newFakeRNG 从战斗输入派生随机流初值：先混 random_seed，
// 再逐字节吸收 input_hash。同一次战斗的重放携带相同
// seed 与 hash，流初值一致；改参数后 hash 变化，流自然
// 分叉——不需要额外的“战斗序号”就能区分不同战斗。
func newFakeRNG(in BattleInput) fakeRNG {
	// int64→uint64 转换按二进制补码语义，是确定性的；
	// 负种子与正种子落在流的不同区段，不会互相污染。
	z := splitmix64(uint64(in.RandomSeed) + 0x9E3779B97F4A7C15)
	for _, b := range in.InputHash {
		z = splitmix64(z + uint64(b))
	}
	return fakeRNG(z)
}

// next 推进并取下一个 64 位随机数。
func (r *fakeRNG) next() uint64 {
	*r = fakeRNG(splitmix64(uint64(*r) + 0x9E3779B97F4A7C15))
	return uint64(*r)
}

// ApplyBattleResult 把一次战斗结果应用到攻守双方 Troop 与
// 战斗记录（文档 11.3/10.4）。
//
// 幂等与迟到处理（INV-06）：
//   - rec 已终态（RESOLVED/TIMED_OUT）时不改动任何部队状态，
//     返回记录中的首次结果。超时终态不写 Resolver 结果，
//     此时按 11.3“攻击无效、防守保留、无战损”合成防守胜
//     返回——迟到结果不可覆盖超时裁决；
//   - rec 为 PENDING 时执行 PENDING→RESOLVED 的比较更新，
//     写入结果与 AppliedOperationID，保证同一结果只应用
//     一次（11.1：applied_operation_id 的用途）。
//
// 部队侧规则（第一版，10.4：不允许掠夺 Cargo、不发生兵力
// 扣减）：
//   - 双方解除战斗态：BattleID 清空，Busy=Battle 解除。
//     业务锁的解除必须来自持久事实的完成（troop.go：不因
//     超时一律清零），这里的事实就是战斗终态本身；
//   - 败者保留 Cargo 回城：Phase→RETURNING 且不清 Cargo
//     （9.5：先保留货物及交接单，不能先把 Cargo 清零再
//     尝试发奖），回城路线由后续规划按 Cargo 容量计算；
//   - 胜者同样解除战斗态转 RETURNING。第一版没有掠夺与
//     驻防，双方都脱离接触；胜者对资源点的继续采集由
//     application 层按 10.4 在同一事务新建 CollectSession
//     后置 COLLECTING——本函数不创建会话（domain 不依赖
//     会话创建规则），也不假设目标种类；
//   - Deferred 不在此处清除：FIGHTING 期间保存的延迟召回
//     由 application 在战斗终态后执行（9.4），提前清掉
//     会丢失玩家请求。
//
// 返回实际生效的结果：可能是记录中的首次结果而非传入的
// 迟到结果，调用方应把它写回执而不是写入参。
func ApplyBattleResult(rec *BattleRecord, attacker, defender *Troop, res BattleResult, operationID string) (BattleResult, error) {
	if rec == nil {
		return BattleResult{}, fmt.Errorf("%w: 战斗记录不能为空", ErrInvalidArgument)
	}
	if attacker == nil || defender == nil {
		return BattleResult{}, fmt.Errorf("%w: 攻守双方部队快照不能为空", ErrInvalidArgument)
	}
	if !rec.Status.Valid() {
		return BattleResult{}, fmt.Errorf("%w: 战斗 %s 状态非法: %d", ErrInvalidState, rec.BattleID, int32(rec.Status))
	}
	// 结果必须指向战斗中的一方，否则无法判定谁回城、谁留下。
	// 伪造的第三方胜者直接按参数非法拒绝，不进入状态机。
	if res.WinnerID != attacker.Header.ID && res.WinnerID != defender.Header.ID {
		return BattleResult{}, fmt.Errorf("%w: 战斗 %s 结果胜者 %d 不是攻守任何一方(%d/%d)",
			ErrInvalidArgument, rec.BattleID, res.WinnerID, attacker.Header.ID, defender.Header.ID)
	}
	other := attacker.Header.ID
	if res.WinnerID == attacker.Header.ID {
		other = defender.Header.ID
	}
	// LoserID 为 0 表示结果未填写败者（允许，按胜者反推）；
	// 填了就必须是另一方，自相矛盾的结果不接受。
	if res.LoserID != 0 && res.LoserID != other {
		return BattleResult{}, fmt.Errorf("%w: 战斗 %s 结果败者 %d 与胜者 %d 不匹配",
			ErrInvalidArgument, rec.BattleID, res.LoserID, res.WinnerID)
	}

	// INV-06：已终态战斗不再应用任何结果，部队状态保持
	// 首次生效时的样子，调用方拿到的是首次结果。
	if rec.Status.Terminal() {
		if rec.Result != nil {
			return *rec.Result, nil
		}
		if rec.Status == BattleStatusTimedOut {
			// 超时裁决没有 Resolver 结果，按“攻击无效、
			// 防守保留、无战损”合成确定结果。身份取自
			// 传入快照而非记录字段：快照是调用方手中的
			// 事实，记录可能在旧 epoch 缺少序列化字段。
			return BattleResult{
				WinnerID:    defender.Header.ID,
				LoserID:     attacker.Header.ID,
				AppliedAtMS: rec.DeadlineMS,
				AttackerWon: false,
			}, nil
		}
		// RESOLVED 却没有结果是数据损坏：终态战斗必须能
		// 回报首次结果，宁可报错也不编一个。
		return BattleResult{}, fmt.Errorf("%w: 战斗 %s 已 RESOLVED 但缺少结果", ErrInvalidState, rec.BattleID)
	}

	// PENDING → RESOLVED。拷贝一份再挂到记录上，避免调用方
	// 后续修改同一个 res 变量时通过指针污染已生效事实。
	applied := res
	rec.Status = BattleStatusResolved
	rec.Result = &applied
	rec.AppliedOperationID = operationID

	clearBattleState(attacker)
	clearBattleState(defender)
	// 双方均转回城（见函数注释的第一版规则）。
	attacker.Phase = TroopPhaseReturning
	defender.Phase = TroopPhaseReturning
	return applied, nil
}

// clearBattleState 解除单支部队的战斗态：BattleID 清空；
// Busy=Battle 解除为空闲。只解除战斗原因对应的锁，
// 不碰其他 Busy 原因——清错锁会让进行中的移交/防守迁入
// 失去保护。
func clearBattleState(t *Troop) {
	t.BattleID = ""
	if t.Busy.Reason == BusyReasonBattle {
		t.Busy = BusyState{}
	}
}
