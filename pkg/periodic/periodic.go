// Package periodic 周期任务循环：每隔 interval 执行一次 Fn，直到 ctx 取消。
//
// 收口前全仓有 8 段逐字重复的骨架：
//
//	ticker := time.NewTicker(d)
//	defer ticker.Stop()
//	for {
//	    select {
//	    case <-ctx.Done():
//	        return
//	    case <-ticker.C:
//	        <gate>   // 轮到本实例执行了吗（leader / metrics 关闭 / 索引未就绪）
//	        <fn>     // 失败之后是 continue 还是 return
//	    }
//	}
//
// 重复的不是行数，而是三件每处都得重新想一遍的事：
//
//  1. ticker 必须 Stop。Ticker 底层持有 runtime 定时器，不 Stop 会泄漏到
//     GC 兜底；漏掉这一行不会报错，只会在长跑进程里慢慢变胖。center 的
//     scheduler 专门写了一行注释强调这件事——注释在强调，说明作者知道它
//     容易漏，但也说明它只能靠人记。
//  2. Interval 必须为正。time.NewTicker 收到 <=0 直接 panic，而周期常常
//     来自配置：配错一个 0 的表现是启动期 panic，不是「任务不跑」。
//  3. ctx.Done 必须与 ticker.C 在同一个 select 里。先 sleep 再判 ctx 会让
//     取消多等一整拍，关服总预算就被这点点耐心吃掉。
//
// 本包**不覆盖**的三类循环，别硬套：
//
//   - 单循环多节拍（internal/center 的 tickLoop：1s 基础节拍上挂心跳巡检 /
//     匹配放宽 / 中转重试 / 指标上报，内部再按时间差节流）。它的意图就是
//     「一个 goroutine 多频合一」，拆成 N 个 Run 只会多出 N 份 goroutine
//     和 N 个时钟，正是它想避免的。
//   - 事件循环（internal/worldmap 的场景 actor、pkg/actor 的分片调度）：
//     ticker 只是 select 里的一个来源，旁边的命令通道/队列/关闭信号才是
//     主角，周期不构成骨架。pkg/actor 的分片循环还有一条额外理由：它是
//     最底层的调度器，让它反过来依赖一个应用层工具包，依赖方向就是错的。
//   - 刷盘器（pkg/flush.Coalescer）：它按 Close 而非 ctx 生命周期运行，
//     并且 Flush 刻意用 context.Background()——调用方的 ctx 取消了不代表
//     可以写一半就停。本包把 ctx 传给 Fn，套上去恰好破坏这个语义。
//
// 与 pkg/backoff.Loop 的区别：那个是「失败后按退避重试到成功或放弃」，
// 本包是「不管上一步成败，按固定节拍继续」。前者关心间隔怎么变，后者
// 关心间隔固定时怎么不泄漏、不错过取消。
package periodic

import (
	"context"
	"time"
)

// Logger 只要求 Warn：周期任务的失败是「这一拍没成」，下一拍自然重试，
// 不属于需要 Error 级别惊动值班的故障。收窄到单方法，让 runtime.Logger
// 这类自带日志接口的包也能直接传进来，不必包一层适配。
type Logger interface {
	Warn(msg string, args ...any)
}

// Options 一个周期任务的全部决定。
//
// 三个「每处各写一遍」的判断在这里显式化：Gate 决定「这一拍轮到谁」，
// StopOnError 决定「失败之后还活不活」，Done 决定「除了 ctx 还有谁能
// 叫停」。它们在原来的 8 段循环里都是隐式的——读代码要看到 fn 里的
// continue 才知道失败不退出，看到 select 里多一个 case 才知道还有别的
// 退出通道。
type Options struct {
	// Name 任务名，用于日志字段 task 与消息前缀（"reconcile tick failed"）。
	// 空则回落 "periodic"——日志里出现无主错误比缺一条日志更难查。
	Name string

	// Interval 触发周期。<=0 时不启动并记 Warn（而不是 panic）。
	Interval time.Duration

	// Fn 每一拍的动作。返回 error 表示「这一拍失败」，由 StopOnError 决定
	// 后续：false 记 Warn 继续，true 退出循环。
	//
	// Fn 拿到的 ctx 就是 Run 的 ctx，取消后应立即返回；本包会区分「ctx 取消
	// 导致的失败」与「任务本身失败」，前者不记 Warn 也不触发 StopOnError。
	Fn func(ctx context.Context) error

	// Gate 可选。返回 false 时跳过这一拍（不算失败、不记日志）。
	// nil 表示无条件执行。
	//
	// 典型用法是 leader 选举：非 leader 期间空转。注意「没有接选举的部署」
	// 应该传 nil 而不是恒 false 的闭包——前者是「不需要 gate」，后者是
	// 「永远不执行」，两者在日志上无法区分。
	//
	// 若开关本身可能是 nil 且「nil 必须表示不执行」，用 Guard 转换：
	// Gate 的零值语义固定为「放行」，不承担业务上的降级判断。
	Gate func() bool

	// Done 可选。额外的退出通道，与 ctx.Done 等价。
	// nil 时该 select 分支永远阻塞，等于「没有额外退出通道」。
	//
	// 心跳这类「循环生命周期 = 某条连接生命周期」的场景用它：连接断了
	// 就关掉 Done，不必为此 cancel 整个 ctx。
	Done <-chan struct{}

	// StopOnError Fn 报错后是否退出循环。
	//
	// false（零值）：记 Warn 继续。适用于「下一拍重试有意义」的任务——
	// 归档、对账、指标上报都属此类，退出等于放弃本实例的职责。
	// true：退出。适用于错误语义是「通道已断」的任务（心跳 Send 失败），
	// 继续跑只会每秒失败一次。
	StopOnError bool

	// Logger 可选。nil 则不记日志——只在 Fn 自己已经记过、或调用方
	// 确实不关心时使用；否则「任务静默失败」比没有任务更难发现。
	Logger Logger
}

// Guard 把可能为 nil 的开关转成 Gate：nil 视为永不放行。
//
// 存在的理由：Options.Gate 的零值是「无条件执行」，这对多数任务是自然语义；
// 但 leader 类任务的语义恰好相反——「没接选举就别跑」，nil 必须意味着不执行。
// 同一个 nil 不能同时表示两件事，否则把一个 Service 从 leader 任务改成无条件
// 任务时，没人会想到要去删那个传 nil 的调用点。所以让 Options.Gate 保持零值
// 放行，把「缺省即禁止」这个业务判断显式地写在 Guard 这个名字上。
//
// 三处 leader 任务（archive / scheduler / milestone）都用它，让「isLeader 为 nil
// 怎么办」只有一个答案。
func Guard(sw func() bool) func() bool {
	if sw == nil {
		return func() bool { return false }
	}
	return sw
}

// Run 阻塞运行周期任务，直到 ctx 取消、Done 关闭，或（StopOnError 时）
// Fn 首次报错。返回即代表循环已停，调用方可以安全地回收 Fn 持有的资源。
//
// 第一拍发生在 Interval 之后，不是启动瞬间。这一点有意不做成开关：
// 「启动就跑一次」在语义上是「立即执行一次 + 之后按周期」，用 Run 之前
// 直接调一次 Fn 更清楚，也避免一个 bool 让时序变成两种。
func Run(ctx context.Context, o Options) {
	name := o.Name
	if name == "" {
		name = "periodic"
	}
	if o.Fn == nil {
		if o.Logger != nil {
			o.Logger.Warn("periodic task has no body, not started", "task", name)
		}
		return
	}
	if o.Interval <= 0 {
		// 不 panic：周期来自配置时（reconcile_interval 之类），让错误在
		// 启动日志里显形，比让进程带着 stack trace 退出更容易定位。
		if o.Logger != nil {
			o.Logger.Warn("periodic task interval is not positive, not started",
				"task", name, "interval", o.Interval)
		}
		return
	}

	ticker := time.NewTicker(o.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-o.Done: // nil channel 永远阻塞
			return
		case <-ticker.C:
			if o.Gate != nil && !o.Gate() {
				continue
			}
			err := o.Fn(ctx)
			if err == nil {
				continue
			}
			// 取消引发的失败不算任务失败：关服时 Fn 拿到的是已取消的 ctx，
			// 几乎必然返回 context.Canceled。把它记成 Warn 会让每次正常停机
			// 都留一条假告警，久了就没人看这个字段。
			if ctx.Err() != nil {
				return
			}
			if o.StopOnError {
				if o.Logger != nil {
					o.Logger.Warn(name+" stopped on error", "task", name, "err", err)
				}
				return
			}
			if o.Logger != nil {
				o.Logger.Warn(name+" tick failed", "task", name, "err", err)
			}
		}
	}
}
