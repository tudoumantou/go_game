package natsx

import (
	"testing"
	"time"

	internalv2 "game_study/pkg/pb/internalv2"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"google.golang.org/protobuf/proto"
)

// startServer 起一个内嵌 nats-server（随机端口），返回 url 与关闭函数。
func startServer(t *testing.T) (string, func()) {
	t.Helper()
	ns, err := natsserver.NewServer(&natsserver.Options{
		Host: "127.0.0.1",
		Port: -1, // 随机端口
	})
	if err != nil {
		t.Fatalf("new nats server: %v", err)
	}
	go ns.Start()
	if !ns.ReadyForConnections(3 * time.Second) {
		t.Fatal("nats server not ready")
	}
	return ns.ClientURL(), ns.Shutdown
}

// TestBusPubSub 验证三条 subject 的发布/订阅与 protobuf 编解码闭环。
func TestBusPubSub(t *testing.T) {
	url, stop := startServer(t)
	defer stop()

	pub, err := Connect(Config{URL: url, Timeout: time.Second})
	if err != nil {
		t.Fatalf("connect pub: %v", err)
	}
	defer pub.Close()
	sub, err := Connect(Config{URL: url, Timeout: time.Second})
	if err != nil {
		t.Fatalf("connect sub: %v", err)
	}
	defer sub.Close()

	got := make(chan *internalv2.CrossScoreDelta, 1)
	if err := sub.Subscribe(DeltaSubject(7),
		func() proto.Message { return &internalv2.CrossScoreDelta{} },
		func(m proto.Message) error {
			got <- m.(*internalv2.CrossScoreDelta)
			return nil
		}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := sub.nc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	want := &internalv2.CrossScoreDelta{
		PlayerId: 12345, Delta: -1, ToServer: 7,
		FromNode: "game-01", RelayId: "sd:game-01:1", GroupId: 3,
	}
	if err := pub.Publish(DeltaSubject(7), want); err != nil {
		t.Fatalf("publish: %v", err)
	}

	select {
	case m := <-got:
		if !proto.Equal(m, want) {
			t.Fatalf("payload mismatch: got %v want %v", m, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for message")
	}
}

// TestBusWildcard 验证中心服用 score.* 一条订阅收所有服的上报。
func TestBusWildcard(t *testing.T) {
	url, stop := startServer(t)
	defer stop()

	bus, err := Connect(Config{URL: url, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer bus.Close()

	seen := make(chan int32, 2)
	if err := bus.Subscribe(ScoreWildcard,
		func() proto.Message { return &internalv2.CrossScoreBatch{} },
		func(m proto.Message) error {
			b := m.(*internalv2.CrossScoreBatch)
			if len(b.GetEntries()) > 0 {
				seen <- b.GetEntries()[0].GetServerId()
			}
			return nil
		}); err != nil {
		t.Fatal(err)
	}
	if err := bus.nc.Flush(); err != nil {
		t.Fatal(err)
	}

	for _, sid := range []int32{1, 2} {
		if err := bus.Publish(ScoreSubject(sid), &internalv2.CrossScoreBatch{
			Entries: []*internalv2.CrossScoreEntry{{ServerId: sid, PlayerId: int64(sid)}},
		}); err != nil {
			t.Fatal(err)
		}
	}
	got := map[int32]bool{}
	for i := 0; i < 2; i++ {
		select {
		case sid := <-seen:
			got[sid] = true
		case <-time.After(2 * time.Second):
			t.Fatal("timed out")
		}
	}
	if !got[1] || !got[2] {
		t.Fatalf("wildcard missed a server: %v", got)
	}
}

// TestConnectEmptyURL 验证空 URL 降级为 (nil, nil)，与 Kafka 语义一致。
func TestConnectEmptyURL(t *testing.T) {
	b, err := Connect(Config{})
	if err != nil || b != nil {
		t.Fatalf("empty url should give (nil, nil), got (%v, %v)", b, err)
	}
}
