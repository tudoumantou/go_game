// pkg/actor/mailbox.go —— 有界信箱
package actor

import (
	"fmt"
	"sync/atomic"
)

// mailbox 有界信箱，背压策略：满时丢弃最旧消息并计数。
//
// 并发说明：push 会被多个发送方 goroutine 并发调用，故 dropped 用 atomic；
// len/close 由单一所有者（分片循环 / remove）调用。
type mailbox struct {
	queue   chan envelope
	dropped atomic.Int64
}

// newMailbox 创建信箱，size <= 0 时使用默认 1024。
func newMailbox(size int) *mailbox {
	if size <= 0 {
		size = 1024
	}
	return &mailbox{queue: make(chan envelope, size)}
}

// push 投递消息，满时丢弃最旧。
func (m *mailbox) push(env envelope) error {
	select {
	case m.queue <- env:
		return nil
	default:
		// 背压：丢弃最旧
		select {
		case <-m.queue:
			m.dropped.Add(1)
		default:
		}
		select {
		case m.queue <- env:
			return nil
		default:
			m.dropped.Add(1)
			return fmt.Errorf("mailbox still full after drop")
		}
	}
}

// pop 非阻塞取出消息，返回 ok=false 表示当前为空。
func (m *mailbox) pop() (envelope, bool) {
	select {
	case env, ok := <-m.queue:
		return env, ok
	default:
		return envelope{}, false
	}
}

// len 当前积压消息数（channel 的 len 是原子读，可在其他 goroutine 安全调用）。
func (m *mailbox) len() int { return len(m.queue) }

// droppedCount 累计丢弃消息数（背压指标，满信箱时丢最旧）。
func (m *mailbox) droppedCount() int64 { return m.dropped.Load() }

// close 关闭信箱。
func (m *mailbox) close() { close(m.queue) }
