// presence_test.go —— R3.1 跨服在线归属的原子占位
package cache

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestPresence(t *testing.T) (*Presence, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })
	return NewPresence(rdb), mr
}

// TestTrySetOnlineFirstClaim 空状态可以被任意节点占用。
func TestTrySetOnlineFirstClaim(t *testing.T) {
	p, _ := newTestPresence(t)
	ok, owner, err := p.TrySetOnline(context.Background(), 1, "game-1", time.Minute)
	if err != nil || !ok || owner != "game-1" {
		t.Fatalf("first claim: ok=%v owner=%q err=%v", ok, owner, err)
	}
	if node, online := p.IsOnline(context.Background(), 1); !online || node != "game-1" {
		t.Fatalf("presence not set: %q %v", node, online)
	}
}

// TestTrySetOnlineBlocksOtherNode 别的节点持有时拒绝，并返回真实持有者。
func TestTrySetOnlineBlocksOtherNode(t *testing.T) {
	p, _ := newTestPresence(t)
	ctx := context.Background()
	if ok, _, err := p.TrySetOnline(ctx, 1, "game-1", time.Minute); err != nil || !ok {
		t.Fatal("setup claim failed")
	}
	ok, owner, err := p.TrySetOnline(ctx, 1, "game-2", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if ok || owner != "game-1" {
		t.Fatalf("other node must be rejected: ok=%v owner=%q", ok, owner)
	}
	// 归属没有被改写
	if node, _ := p.IsOnline(ctx, 1); node != "game-1" {
		t.Fatalf("ownership overwritten: %q", node)
	}
}

// TestTrySetOnlineAllowsSameNodeReconnect 同节点重连（旧会话未过期）必须放行，
// 否则玩家掉线重连会被自己的残留在线状态挡住。
func TestTrySetOnlineAllowsSameNodeReconnect(t *testing.T) {
	p, _ := newTestPresence(t)
	ctx := context.Background()
	if ok, _, err := p.TrySetOnline(ctx, 1, "game-1", time.Minute); err != nil || !ok {
		t.Fatal("setup claim failed")
	}
	ok, owner, err := p.TrySetOnline(ctx, 1, "game-1", time.Minute)
	if err != nil || !ok || owner != "game-1" {
		t.Fatalf("same node reclaim: ok=%v owner=%q err=%v", ok, owner, err)
	}
}

// TestTrySetOnlineAfterExpiry 持有者过期后可以被其它节点接手。
func TestTrySetOnlineAfterExpiry(t *testing.T) {
	p, mr := newTestPresence(t)
	ctx := context.Background()
	if ok, _, err := p.TrySetOnline(ctx, 1, "game-1", time.Second); err != nil || !ok {
		t.Fatal("setup claim failed")
	}
	mr.FastForward(2 * time.Second)
	if ok, _, err := p.TrySetOnline(ctx, 1, "game-2", time.Minute); err != nil || !ok {
		t.Fatalf("expired ownership should be claimable: ok=%v err=%v", ok, err)
	}
}
