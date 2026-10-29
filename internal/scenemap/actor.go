package scenemap

import (
	"context"
	"errors"
	"fmt"
	"time"

	"game_study/pkg/mailbox"
)

// Mailbox 有界消息邮箱（实现见 pkg/mailbox）。
//
// # 为什么只剩一层包装
//
// 本包与 worldmap/runtime 曾各有一份 Mailbox[T]：方法名完全相同、逻辑都是
// 「RWMutex + chan + closed + Once」，只有错误哨兵名不同。两份 doc.go 都写着
// 「互不 import」，但互不 import 说明的是业务模型不同（坐标系、并发模型、
// 持久化路径），不等于该各造一个邮箱——邮箱是纯通道边界逻辑，与领域无关。
//
// # 为什么不是类型别名
//
// 本包的 ErrClosed 不只表示「邮箱关了」：房间池关闭、房间管理器关闭都用它
// （见 aoi/pool.go、room/manager.go）。如果直接暴露 mailbox.ErrClosed，
// 上层就要同时识别两个「已关闭」哨兵；而如果让 mailbox 用本包的哨兵，
// 又变成基础设施包认识业务错误。所以这里做一层映射：实现共用，语义留在
// 各自的领域里。
type Mailbox[T any] struct{ *mailbox.Mailbox[T] }

// NewMailbox 创建容量为 capacity 的邮箱；capacity <= 0 时取 1。
func NewMailbox[T any](capacity int) *Mailbox[T] {
	return &Mailbox[T]{Mailbox: mailbox.New[T](capacity)}
}

// TrySend 非阻塞投递：满返回 ErrOverloaded，关闭返回 ErrClosed。
func (m *Mailbox[T]) TrySend(v T) error {
	return mapMailboxErr(m.Mailbox.TrySend(v))
}

// Send 阻塞投递，带上下文取消。用于「必须送达」的控制消息（关闭、迁移）。
func (m *Mailbox[T]) Send(ctx context.Context, v T) error {
	return mapMailboxErr(m.Mailbox.Send(ctx, v))
}

// mapMailboxErr 把基础设施哨兵映射成本包的领域哨兵。
//
// 只映射这两个：ctx 的错误（Send 的超时/取消）原样返回——那是调用方的
// 决策依据，不该被改写成「过载」或「已关闭」。
func mapMailboxErr(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, mailbox.ErrClosed):
		return ErrClosed
	case errors.Is(err, mailbox.ErrOverloaded):
		return ErrOverloaded
	default:
		return err
	}
}

// Call 是「带超时的同步调用」辅助。
//
// 契约：fn 负责把结果写入 reply（容量必须 >= 1），Call 只等结果。
// 超时返回 ErrTimeout —— **结果未知**，调用方不能当失败重试。
//
// 为什么不让调用方直接等 channel：超时后那个 goroutine 仍会写 reply，
// 如果 reply 无缓冲，写方会永久阻塞（goroutine 泄漏）。这里强制容量 1
// 并检查，把「忘记给缓冲」变成构造期错误。
func Call[T any](ctx context.Context, timeout time.Duration, reply chan T, fn func() error) (T, error) {
	var zero T
	if cap(reply) < 1 {
		return zero, fmt.Errorf("%w: reply 必须至少 1 个缓冲", ErrInvalidArgument)
	}
	if err := fn(); err != nil {
		return zero, err
	}
	if timeout <= 0 {
		timeout = 3 * time.Second // 默认 3s：与协议超时同量级
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case v := <-reply:
		return v, nil
	case <-ctx.Done():
		return zero, ctx.Err()
	case <-timer.C:
		return zero, ErrTimeout
	}
}

// SafeRun 在独立 goroutine 里运行 fn，并捕获 panic。
//
// 为什么必须有：Go 的 goroutine panic 会直接杀进程；而一个 MGR 里
// 可能承载几百只怪与几十个玩家，因为某个玩法的越界 panic 把整个进程
// 带走是不可接受的。捕获后交给 onPanic（通常是「关掉这张图 + 上报」）。
func SafeRun(name string, onPanic func(error), fn func()) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				err := fmt.Errorf("scenemap: %s panic: %v", name, r)
				if onPanic != nil {
					onPanic(err)
				}
			}
		}()
		fn()
	}()
}
