// pkg/actor/mailbox.go —— 有界信箱
package actor

import (
	"fmt"
)

// mailbox 有界信箱，背压策略：满时丢弃最旧消息并计数。
type mailbox struct {
	queue   chan envelope
	dropped int64
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
			m.dropped++
		default:
		}
		select {
		case m.queue <- env:
			return nil
		default:
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

// close 关闭信箱。
func (m *mailbox) close() { close(m.queue) }
