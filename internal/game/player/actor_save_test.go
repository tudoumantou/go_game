package player

import (
	"testing"
	"time"

	"game_study/pkg/log"
)

// TestSaveMsgRepliesResult 保存请求必须回执真实结果，调用方不能靠「投递成功」猜测已落库。
func TestSaveMsgRepliesResult(t *testing.T) {
	// repo 为 nil：没有数据库可写，保存视为成功（单测环境）。
	a := NewActor(9, nil, log.New("test"), nil, nil, nil)
	done := make(chan error, 1)
	a.onSave(nil, &SaveMsg{Done: done})

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("save ack: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("save did not reply")
	}
}

// TestReplySaveDoesNotBlock 调用方超时离开后，回执不能阻塞 Actor。
func TestReplySaveDoesNotBlock(t *testing.T) {
	done := make(chan error) // 无缓冲且无人接收
	replySave(done, nil)     // 必须立即返回；若阻塞则测试超时
}
