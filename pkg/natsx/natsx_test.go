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
	if err := sub.Subscribe(sub.DeltaSubject(7),
		func() proto.Message { return &internalv2.CrossScoreDelta{} },
		func(m proto.Message) error {
			got <- m.(*internalv2.CrossScoreDelta)
			return nil
		}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := sub.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	want := &internalv2.CrossScoreDelta{
		PlayerId: 12345, Delta: -1, ToServer: 7,
		FromNode: "game-01", RelayId: "sd:game-01:1", GroupId: 3,
	}
	if err := pub.Publish(pub.DeltaSubject(7), want); err != nil {
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
	if err := bus.Subscribe(bus.ScoreWildcard(),
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
	if err := bus.Flush(); err != nil {
		t.Fatal(err)
	}

	for _, sid := range []int32{1, 2} {
		if err := bus.Publish(bus.ScoreSubject(sid), &internalv2.CrossScoreBatch{
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

// TestRelaySubjects 验证通用 relay 的点对点与广播 subject 发布/订阅闭环。
// 载荷用 RelayPush，与跨服玩法的 subject 前缀隔离。
func TestRelaySubjects(t *testing.T) {
	url, stop := startServer(t)
	defer stop()

	bus, err := Connect(Config{URL: url, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer bus.Close()

	nodeCh := make(chan *internalv2.RelayPush, 1)
	groupCh := make(chan *internalv2.RelayPush, 1)
	if err := bus.Subscribe(bus.RelayNodeSubject("game-01"),
		func() proto.Message { return &internalv2.RelayPush{} },
		func(m proto.Message) error { nodeCh <- m.(*internalv2.RelayPush); return nil }); err != nil {
		t.Fatal(err)
	}
	if err := bus.Subscribe(bus.RelayGroupSubject("g1"),
		func() proto.Message { return &internalv2.RelayPush{} },
		func(m proto.Message) error { groupCh <- m.(*internalv2.RelayPush); return nil }); err != nil {
		t.Fatal(err)
	}
	if err := bus.Flush(); err != nil {
		t.Fatal(err)
	}

	nodeMsg := &internalv2.RelayPush{RelayId: "r1", FromNode: "game-02", Payload: []byte("p2p")}
	groupMsg := &internalv2.RelayPush{RelayId: "r2", FromNode: "game-02", Payload: []byte("bcast")}
	if err := bus.Publish(bus.RelayNodeSubject("game-01"), nodeMsg); err != nil {
		t.Fatal(err)
	}
	if err := bus.Publish(bus.RelayGroupSubject("g1"), groupMsg); err != nil {
		t.Fatal(err)
	}

	select {
	case m := <-nodeCh:
		if !proto.Equal(m, nodeMsg) {
			t.Fatalf("node payload mismatch: %v vs %v", m, nodeMsg)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for node relay")
	}
	select {
	case m := <-groupCh:
		if !proto.Equal(m, groupMsg) {
			t.Fatalf("group payload mismatch: %v vs %v", m, groupMsg)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for group relay")
	}
}

// TestConnectEmptyURL 验证空 URL 降级为 (nil, nil)，与 Kafka 语义一致。
func TestConnectEmptyURL(t *testing.T) {
	b, err := Connect(Config{})
	if err != nil || b != nil {
		t.Fatalf("empty url should give (nil, nil), got (%v, %v)", b, err)
	}
}

// TestSubjectPrefix 验证命名空间前缀的两个性质：归一化正确、跨前缀隔离生效。
//
// 隔离是这套机制存在的唯一理由：多环境共用一套 NATS 集群时，两个环境都有
// game-01，不带前缀就会订到同一条 relay.node.game-01，消息直接串台。
func TestSubjectPrefix(t *testing.T) {
	url, stop := startServer(t)
	defer stop()

	dev, err := Connect(Config{URL: url, Prefix: "dev", Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer dev.Close()
	// 末尾带点号也必须归一成单点，否则会拼出 "prod..relay.node.x"
	prod, err := Connect(Config{URL: url, Prefix: "prod.", Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()

	if got, want := dev.RelayNodeSubject("game-01"), "dev.relay.node.game-01"; got != want {
		t.Fatalf("dev subject = %q, want %q", got, want)
	}
	if got, want := prod.ScoreSubject(7), "prod.crossplay.score.7"; got != want {
		t.Fatalf("prod subject = %q, want %q", got, want)
	}
	if got, want := prod.ScoreWildcard(), "prod.crossplay.score.*"; got != want {
		t.Fatalf("prod wildcard = %q, want %q", got, want)
	}

	// 同前缀可达
	devCh := make(chan struct{}, 1)
	if err := dev.Subscribe(dev.RelayNodeSubject("game-01"),
		func() proto.Message { return &internalv2.RelayPush{} },
		func(proto.Message) error { devCh <- struct{}{}; return nil }); err != nil {
		t.Fatal(err)
	}
	// 跨前缀不可达：prod 订同名节点，dev 发布，prod 不该收到
	prodCh := make(chan struct{}, 1)
	if err := prod.Subscribe(prod.RelayNodeSubject("game-01"),
		func() proto.Message { return &internalv2.RelayPush{} },
		func(proto.Message) error { prodCh <- struct{}{}; return nil }); err != nil {
		t.Fatal(err)
	}
	if err := dev.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := prod.Flush(); err != nil {
		t.Fatal(err)
	}

	if err := dev.Publish(dev.RelayNodeSubject("game-01"),
		&internalv2.RelayPush{RelayId: "r1", FromNode: "game-01"}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-devCh:
		// 同前缀收到，正确
	case <-time.After(2 * time.Second):
		t.Fatal("同前缀未收到消息")
	}
	select {
	case <-prodCh:
		t.Fatal("跨前缀串台：prod 收到了 dev 的消息")
	case <-time.After(300 * time.Millisecond):
		// 预期：隔离生效
	}
}

// TestSubjectNoPrefix 前缀为空时 subject 保持原样（与旧部署兼容）。
func TestSubjectNoPrefix(t *testing.T) {
	url, stop := startServer(t)
	defer stop()

	bus, err := Connect(Config{URL: url, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer bus.Close()

	if got, want := bus.RelayGroupSubject("g1"), "relay.group.g1"; got != want {
		t.Fatalf("subject = %q, want %q", got, want)
	}
	if got, want := bus.GroupWildcard(), "crossplay.group.*"; got != want {
		t.Fatalf("wildcard = %q, want %q", got, want)
	}
}
