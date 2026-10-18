// internal/center/relay/relay_test.go —— 中转路由测试
package relay

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

type fakePusher struct {
	id    string
	fail  bool
	mu    sync.Mutex
	got   []*PushPayload
}

func (f *fakePusher) NodeID() string { return f.id }

func (f *fakePusher) SendPush(p *PushPayload) error {
	if f.fail {
		return fmt.Errorf("node %s down", f.id)
	}
	f.mu.Lock()
	f.got = append(f.got, p)
	f.mu.Unlock()
	return nil
}

func (f *fakePusher) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.got)
}

func TestRelayPointToPoint(t *testing.T) {
	r := NewRouter(3, 10*time.Millisecond)
	p := &fakePusher{id: "game-07"}

	n := r.Relay(Payload{FromNode: "game-03", ToNode: "game-07", Body: []byte("hello")}, []Pusher{p})
	if n != 1 {
		t.Fatalf("expect 1 delivered, got %d", n)
	}
	if p.count() != 1 {
		t.Fatalf("target should receive 1 push, got %d", p.count())
	}
	if got := p.got[0]; got.FromNode != "game-03" || string(got.Payload) != "hello" {
		t.Fatalf("payload mismatch: %+v", got)
	}
	// relay_id 自动生成
	if p.got[0].RelayID == "" {
		t.Fatal("relay_id should be auto-generated")
	}

	sent, delivered, failed, _ := r.StatsSnapshot()
	if sent != 1 || delivered != 1 || failed != 0 {
		t.Fatalf("stats mismatch: sent=%d delivered=%d failed=%d", sent, delivered, failed)
	}
	if r.PendingCount() != 0 {
		t.Fatalf("no pending expected, got %d", r.PendingCount())
	}
}

func TestRelayRetriesOnFailure(t *testing.T) {
	r := NewRouter(2, 10*time.Millisecond)
	bad := &fakePusher{id: "game-07", fail: true}

	n := r.Relay(Payload{RelayID: "r1", FromNode: "game-03", ToNode: "game-07"}, []Pusher{bad})
	if n != 0 {
		t.Fatalf("expect 0 delivered, got %d", n)
	}
	if r.PendingCount() != 1 {
		t.Fatalf("failed relay should be pending, got %d", r.PendingCount())
	}

	// 到期后取重试队列
	time.Sleep(15 * time.Millisecond)
	due := r.NextRetry(time.Now())
	if len(due) != 1 || due[0].RelayID != "r1" {
		t.Fatalf("expect r1 due for retry, got %+v", due)
	}
	// 重新投递仍失败 → 仍在 pending（等下一轮）
	r.Relay(due[0], []Pusher{bad})
	if r.PendingCount() != 1 {
		t.Fatalf("still-failing relay should stay pending, got %d", r.PendingCount())
	}

	// 模拟真实循环：每次拿到到期条目就重新投递（仍失败），直到超过上限被丢弃
	maxRounds := r.maxRetry + 3
	for i := 0; i < maxRounds && r.PendingCount() > 0; i++ {
		time.Sleep(15 * time.Millisecond)
		due := r.NextRetry(time.Now())
		for _, payload := range due {
			r.Relay(payload, []Pusher{bad}) // 目标仍不可达
		}
	}
	if r.PendingCount() != 0 {
		t.Fatalf("pending should be cleared after max retries, got %d", r.PendingCount())
	}
	_, _, failed, _ := r.StatsSnapshot()
	if failed == 0 {
		t.Fatal("failed counter should be positive")
	}
}

func TestRelayBroadcastNoRetry(t *testing.T) {
	r := NewRouter(3, 10*time.Millisecond)
	ok := &fakePusher{id: "game-01"}
	bad := &fakePusher{id: "game-02", fail: true}

	n := r.Relay(Payload{Broadcast: true, FromNode: "center", GroupID: "g1"},
		[]Pusher{ok, bad})
	if n != 1 {
		t.Fatalf("expect 1 of 2 delivered, got %d", n)
	}
	// 广播不重试（避免故障时放大流量）
	if r.PendingCount() != 0 {
		t.Fatalf("broadcast must not be retried, got %d pending", r.PendingCount())
	}
	_, _, failed, bcast := r.StatsSnapshot()
	if bcast != 1 {
		t.Fatalf("broadcast counter should be 1, got %d", bcast)
	}
	if failed != 1 {
		t.Fatalf("failed should be 1, got %d", failed)
	}
}

func TestRelayNoTargets(t *testing.T) {
	r := NewRouter(3, 10*time.Millisecond)
	n := r.Relay(Payload{ToNode: "ghost"}, nil)
	if n != 0 {
		t.Fatalf("expect 0 delivered with no targets, got %d", n)
	}
	if r.PendingCount() != 0 {
		t.Fatal("no pending when there were no targets")
	}
}
