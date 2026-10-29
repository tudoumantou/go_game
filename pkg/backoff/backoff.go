// Package backoff 指数退避与「常驻重连」循环。
//
// # 为什么要有这个包
//
// 「断开 → 等一会 → 再连」这个循环在项目里有三份：game 到中心服的 Report
// 流（internal/game/center/client.go）、game 到跨服节点的 Channel 流
// （internal/game/cross/client.go）、中心服的选主竞选
// （internal/center/leader/leader.go）。三份的结构完全一致——非阻塞查退出、
// 执行一次、翻倍退避、等待——连注释都在互相引用对方的退避起点。
//
// 更麻烦的是它们已经开始分叉：cross 与 center 的实现是「翻倍后按 max
// 封顶」，leader 的实现是「翻倍前判 max」——后者在 cur=20s 时会翻到 40s，
// 越过自己注释里写的 30s 上限。同一个意图写了三遍，就一定会有一遍是错的。
//
// # 语义
//
// 退避值只在「一轮结束」时推进，与「这一轮成功还是失败」无关：流正常
// 返回（对端关闭、ctx 未取消）也需要重连，不该把退避重置成 base。要重置
// 就显式调 Reset——由调用方在「确认连上并稳定工作过」之后决定。
//
// # 不覆盖的场景
//
// pkg/mq 的消费重试是**线性**退避（100/200/300ms）且有次数上限
// （maxRetry 次后进死信）。它与这里的常驻循环目的相反：消息本身有问题时
// 应当快速失败并把消息交出去，指数拉长间隔只是让死信更晚到。两者不是
// 「同一件事的两种写法」，所以不合并。
package backoff

import (
	"context"
	"time"
)

// 默认下限：base 传 0 或负数时用它。
// 100ms 够快（本地联调不用等），又不至于把故障对端打成 SYN 洪水。
const defaultBase = 100 * time.Millisecond

// Backoff 指数退避状态。
//
// 不是并发安全的：一个重连循环一个实例，只在那个 goroutine 里用。
type Backoff struct {
	base time.Duration
	max  time.Duration
	cur  time.Duration
}

// New 创建退避器：首次等待 base，之后每次翻倍，封顶 max。
//
// base <= 0 取 100ms；max <= base 时退化为「固定 base 间隔」——
// 配置成 max < base 几乎一定是笔误，与其让每次等待变得不可预期，
// 不如当作「不要退避」处理。
func New(base, max time.Duration) *Backoff {
	if base <= 0 {
		base = defaultBase
	}
	if max <= base {
		max = base
	}
	return &Backoff{base: base, max: max, cur: base}
}

// Next 返回**本次**应等待的时长，并把内部状态推进到下一次。
//
// 顺序是「先返回再翻倍」：这样第一次等待一定是 base（而不是 base*2），
// 否则 minBackoff 这个名字就在说谎。
func (b *Backoff) Next() time.Duration {
	wait := b.cur
	if b.cur < b.max {
		b.cur *= 2
		if b.cur > b.max {
			b.cur = b.max
		}
	}
	return wait
}

// Current 返回下一次 Next 会给出的值（不推进状态）。
// 用于日志：想在等待之前打出「即将等多久」时用它。
func (b *Backoff) Current() time.Duration { return b.cur }

// Reset 回到 base。
//
// 调用时机是「连接已确认可用并稳定工作」，不是「这一轮没报错」——
// 对端在握手成功后立刻断开时，runOnce 也返回 nil，这时重置退避会让
// 故障对端被高频重连。是否算「稳定」由调用方判断。
func (b *Backoff) Reset() { b.cur = b.base }

// Loop 常驻执行 fn：每轮结束后按退避等待，直到 ctx 取消或 stop 关闭。
//
// 返回值：ctx 取消 → ctx.Err()；stop 关闭 → nil。两者的区分对调用方有意义
// （前者是进程退出，后者是主动停机）。
//
// fn 的错误不参与控制流——Loop 不解释错误语义，只负责节奏。要记日志就在
// fn 里记（各处的日志字段不同：节点地址、选区 id、退避值），或者用 onRetry。
//
// stop 传 nil 表示只有 ctx 一条退出路径（select 里 nil channel 永久阻塞，
// 不会误触发）。
func Loop(
	ctx context.Context,
	stop <-chan struct{},
	b *Backoff,
	fn func(context.Context) error,
	onRetry func(err error, wait time.Duration),
) error {
	for {
		// 进入新一轮前先查退出：避免「已经取消了还去建一次连接」。
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-stop:
			return nil
		default:
		}

		err := fn(ctx)
		wait := b.Next()
		if onRetry != nil && err != nil {
			onRetry(err, wait)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-stop:
			return nil
		case <-time.After(wait):
		}
	}
}
