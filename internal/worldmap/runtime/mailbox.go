package runtime

import (
	"context"
	"errors"

	"game_study/pkg/mailbox"
)

// ErrMailboxClosed 邮箱已关闭：说明场景正在退出，调用方不应重试。
var ErrMailboxClosed = errors.New("worldmap/runtime: 邮箱已关闭")

// Mailbox 是有界邮箱（文档 4.2），实现见 pkg/mailbox。
//
// 本包与 scenemap 曾各有一份 Mailbox[T]：方法名完全相同、逻辑都是
// 「RWMutex + chan + closed + Once」。两份 doc.go 都写着「互不 import」，
// 但互不 import 说明的是业务模型不同（坐标系、并发模型、持久化路径），
// 不等于该各造一个邮箱。
//
// 只保留一层错误映射而不是类型别名：本包的 ErrOverloaded 还承载「场景有
// 在途事务」「事务池满」等语义（scene.go、transaction_pool.go），让它等于
// 基础设施包的哨兵会把几类不同的过载混成一个；反过来让 mailbox 用本包的
// 哨兵，又变成基础设施包认识业务。所以实现共用、语义各留。
type Mailbox[T any] struct{ *mailbox.Mailbox[T] }

// NewMailbox 创建容量为 capacity 的邮箱；capacity <= 0 时取 1。
func NewMailbox[T any](capacity int) *Mailbox[T] {
	return &Mailbox[T]{Mailbox: mailbox.New[T](capacity)}
}

// TrySend 非阻塞入队：满返回 ErrOverloaded，关闭返回 ErrMailboxClosed。
func (m *Mailbox[T]) TrySend(v T) error {
	return mapMailboxErr(m.Mailbox.TrySend(v))
}

// Send 阻塞投递，带上下文取消。用于「必须送达」的控制消息（关闭、迁移）。
func (m *Mailbox[T]) Send(ctx context.Context, v T) error {
	return mapMailboxErr(m.Mailbox.Send(ctx, v))
}

// mapMailboxErr 把基础设施哨兵映射成本包的哨兵。
// ctx 的错误（Send 超时/取消）原样返回——那是调用方的决策依据。
func mapMailboxErr(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, mailbox.ErrClosed):
		return ErrMailboxClosed
	case errors.Is(err, mailbox.ErrOverloaded):
		return ErrOverloaded
	default:
		return err
	}
}
