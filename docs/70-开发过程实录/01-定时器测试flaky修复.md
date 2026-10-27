# 70-01 定时器测试 flaky 修复

> 专题：把「依赖真实时钟」的测试改成「显式驱动每一拍」。
> 涉及提交：`bea479d`（定时器可注入 + 断言统一 require）、`01a90d6`（场景空闲超时改虚拟时钟）、`80b0e55`（玩家心跳等条件）、`3f07d63`（中心服虚拟时钟）。
> 结论：用例耗时 150ms → 8ms，30 次 `-race` 重复全绿；断言从「约几次」变成「几拍就是几次」。

## 1. 现象：CI 上偶发红，本地从不红

场景管理器（`internal/scenemap/mgr`）的定时器用例 originally 这样写：

```go
// 改之前的写法（意会）
start := time.Now()
time.Sleep(150 * time.Millisecond)
ticks := m.loopTicks
require.Greater(t, ticks, 10) // 150ms / 15ms 一拍 ≈ 10 拍
```

问题不在代码，在**断言本身**：它测的是「真实时间里定时器跑了约几拍」。CI runner 负载一高，同样 150ms 可能只跑到 3 拍——用例红得毫无信息量，重试又绿。这类 flaky 比没有测试更糟：大家开始习惯性重跑 CI。

## 2. 根因：被测系统拥有时间，测试没有

定时器的「拥有者」是 MGR 自己（真实 `time.Timer`），测试只能旁观。要让断言确定，必须把**驱动时间的能力**交给测试。修法分两步：

1. 抽出 `Timer` 接口 + `Deps.NewTimer` 工厂（生产仍用真实定时器，行为不变）；
2. 测试注入 `fakeTimers`，**逐拍投递**：投 12 拍，就断言恰好 12 次。

## 3. 接口设计：为什么不用 Ticker，为什么 Reset 要排空

`internal/scenemap/mgr/timer.go` 的注释把两个关键决策写得很清楚，这里原文摘录并解释：

```go
// Timer 是 MGR 的定时器抽象。
//
// 生产用真实 time.Timer（repeatingTimer）。抽出接口只为一件事：让测试
// 能显式驱动时间。用真实时钟断言"150ms 里跑了约 20 次"在 CI 上必然
// flaky——机器负载高时同样 150ms 可能只跑到 3 次，这不是代码的问题。
type Timer interface {
	C() <-chan time.Time
	SetInterval(iv time.Duration) // 只改间隔，不重置当前这一拍
	Reset()                       // 已触发，排下一拍
	Stop()
}
```

**为什么不用 `time.Ticker`**（`timer.go` 原注释）：

> Ticker 的间隔在创建后不可修改。倍速一切换（1000→2000）就得 Stop 旧的、New 一个新的，而新旧交替之间必然丢/多一拍，且 Ticker 会丢弃"来不及处理"的 tick（对游戏循环来说，丢一拍意味着 buff 少结算一次）。Timer 每次触发后由我们自己 Reset(新间隔)，语义精确：**下一拍的间隔就是当前倍速**。

**为什么 `Reset` 必须 Stop + 排空**：

```go
func (r *repeatingTimer) Reset() {
	if !r.t.Stop() {
		// 已触发但值还没被取走：排空，避免下一拍立即触发。
		select {
		case <-r.t.C:
		default:
		}
	}
	r.t.Reset(r.interval)
}
```

这是 Go 1.23 之前定时器的经典坑：如果 timer 已触发但 channel 里的值还没被 select 取走，直接 `Reset` 会让旧值留着，下一次 select **立刻又触发一次**——表现为「越跑越快」。代码注释明确写了「本包保守按老规则写，兼容所有 1.22+ 运行时」。

倍速换算也值得一提（整数运算，无浮点误差）：

```go
// scale 把基础间隔换算成当前倍速下的实际间隔。
// 倍速用千分比：1000=1x，2000=2x。间隔 = 基础 × 1000 / 倍速，
// 即 2 倍速时 1s 变 500ms。
func (m *Mgr) scale(d time.Duration) time.Duration { … }
```

## 4. 注入点：一个字段，零生产行为变化

`mgr.go` 的改动只有把四个定时器字段的类型从具体类型改成接口，并在 `Deps` 里加工厂：

```go
type Deps struct {
	// …原有依赖…
	// NewTimer 定时器工厂。nil 时用真实 time.Timer。
	//
	// 存在的唯一理由是测试：断言"倍速 2x 时 loop 跑 twice as often"
	// 若依赖真实时钟，CI 负载一高就 flaky。注入假定时器后测试显式
	// 驱动每一拍，结果完全确定。
	NewTimer func(iv time.Duration) Timer
}
```

生产代码 `New(cfg, deps)` 时 `deps.NewTimer == nil` 走 `realTimerFactory`——**默认路径一行没变**，注入只为测试存在。这是可测试性改造的纪律：接口的默认实现必须等于原行为。

## 5. fakeTimers：测试怎么「逐拍驱动」

```go
// fakeTimers 由测试显式投喂节拍（mgr_test.go 内的测试替身，意会）
type fakeTimers struct{ ch chan time.Time }

func (f *fakeTimers) C() <-chan time.Time      { return f.ch }
func (f *fakeTimers) SetInterval(time.Duration) {}
func (f *fakeTimers) Reset()                    {}
func (f *fakeTimers) Stop()                     {}
func (f *fakeTimers) tick()                     { f.ch <- time.Now() }
```

断言形态从「睡 150ms 数数」变成：

```go
for i := 0; i < 12; i++ { ft.tick() }   // 显式投 12 拍
require.EqualValues(t, 12, m.loopTicks) // 就必须恰好 12 次
```

配合改动：`handleLoop` 等处理函数抽成普通方法，`CommandTick` 可以「手动跑一拍」，测试连定时器都不用等。

## 6. 同一场战役的另外三处

flaky 不止 scenemap 一处，同一轮用同样思路清掉：

| 提交 | 位置 | 改法 |
| --- | --- | --- |
| `3f07d63` | 中心服 relay/registry/matcher | `time.Sleep` 全部替换为虚拟时钟（`Config.Clock` 注入，测试用 `atomic.Int64` 推进） |
| `01a90d6` | 场景空闲超时用例 | 改虚拟时钟，不睡真实时间 |
| `80b0e55` | 玩家心跳用例 | 从「睡固定时间」改成「等条件成立」（轮询 + 超时兜底），不等一个具体的毫秒数 |

三种手法对应三种情况：**能驱动时间就驱动（定时器）、有内部时钟就替换（虚拟时钟）、只能等外部条件就等条件（轮询）**。共同点是消除「两个 timer 竞争」——详见 [10 停机 flaky](10-停机flaky与持续集成.md) 里同根问题的结构化同步解法。

## 7. 验证

- 三个定时器用例：150ms → 8ms，30 次 `-race` 重复全绿；
- 全量 `go test ./...` 与 `-race` 通过（`bea479d` 提交信息）；
- 变异验证：把 `fakeTimers.tick` 的投递改成随机丢一拍，断言立即失败——证明「12 拍 = 12 次」真的在约束行为。

## 8. 经验沉淀

1. **测试不睡真实时间**。凡是 `time.Sleep` + 数数的断言，都是把机器的负载卷进了测试结果。
2. **注入点的默认值 = 原行为**。可测试性改造不能悄悄改生产语义，`NewTimer == nil` 走真实定时器就是保险丝。
3. **接口只为准测试存在时，要在注释里说清**。`Timer` 的注释第一句就是「抽出接口只为一件事」，后来者不会误以为是要支持多种定时器策略。
