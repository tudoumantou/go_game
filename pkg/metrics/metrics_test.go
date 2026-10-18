// pkg/metrics/metrics_test.go —— 指标注册与端点测试
package metrics

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

// TestInitRegisters 指标初始化后可正常读写。
func TestInitRegisters(t *testing.T) {
	Init("unit-test")

	if !Enabled() {
		t.Fatal("metrics should be enabled after Init")
	}
	if Service() != "unit-test" {
		t.Fatalf("service name mismatch: %s", Service())
	}

	// 计数器
	before := testutil.ToFloat64(msgCounter.WithLabelValues("unit-test", "in", "ok"))
	MsgInc("in", "ok")
	if after := testutil.ToFloat64(msgCounter.WithLabelValues("unit-test", "in", "ok")); after != before+1 {
		t.Fatalf("MsgInc: before=%v after=%v", before, after)
	}

	// Gauge
	ConnInc()
	ConnInc()
	if v := testutil.ToFloat64(connGauge.WithLabelValues("unit-test")); v != 2 {
		t.Fatalf("ConnInc: expect 2, got %v", v)
	}
	ConnDec()
	if v := testutil.ToFloat64(connGauge.WithLabelValues("unit-test")); v != 1 {
		t.Fatalf("ConnDec: expect 1, got %v", v)
	}

	// 拒绝连接累计
	ConnRejected("max_conns")
	if v := testutil.ToFloat64(connTotal.WithLabelValues("unit-test", "rejected_max_conns")); v != 1 {
		t.Fatalf("ConnRejected: expect 1, got %v", v)
	}

	// 直方图
	MsgObserve("in", 15*time.Millisecond, 512)
	if n := testutil.CollectAndCount(msgLatency); n == 0 {
		t.Fatal("msgLatency should be collected")
	}

	// Actor
	ActorCountAdd(1)
	ActorProcessed(true)
	ActorObserve(time.Millisecond)
	ActorMailbox(3, 42)
	if v := testutil.ToFloat64(actorMailbox.WithLabelValues("unit-test", "3")); v != 42 {
		t.Fatalf("ActorMailbox: expect 42, got %v", v)
	}
	ActorPanicInc()
	ActorDroppedAdd(3)
	ActorDroppedAdd(0) // 0 不应改变计数
	if v := testutil.ToFloat64(actorDropped.WithLabelValues("unit-test")); v != 3 {
		t.Fatalf("ActorDroppedAdd: expect 3, got %v", v)
	}

	// Flusher
	FlushDirty(1, 7)
	FlushObserve(20 * time.Millisecond)
	FlushRows(true, 7)
	FlushFail(2)
	FlushLag(0, 1.5)
	if v := testutil.ToFloat64(flushDirty.WithLabelValues("unit-test", "1")); v != 7 {
		t.Fatalf("FlushDirty: expect 7, got %v", v)
	}

	// Channel 池
	ChannelStreams(12)
	ChannelPoolSize(3)
	ChannelError("send")
	if v := testutil.ToFloat64(channelStreams.WithLabelValues("unit-test")); v != 12 {
		t.Fatalf("ChannelStreams: expect 12, got %v", v)
	}

	// Kafka
	KafkaConsumed("gs.mail", "ok")
	KafkaProduced("gs.mail", true)
	KafkaProduced("gs.mail", false)
	KafkaLag("gs.mail", 128)
	if v := testutil.ToFloat64(kafkaLag.WithLabelValues("unit-test", "gs.mail")); v != 128 {
		t.Fatalf("KafkaLag: expect 128, got %v", v)
	}
}

// TestServerEndpoints /metrics、/healthz、/readyz 行为。
func TestServerEndpoints(t *testing.T) {
	Init("unit-test-http")

	ready := true
	srv, err := NewServer("127.0.0.1:0", func(ctx context.Context) error {
		if ready {
			return nil
		}
		return errors.New("dependency down")
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	srv.Start()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Stop(ctx)
	}()

	base := "http://" + srv.Addr()

	// /metrics 应含注册的自定义指标
	body := mustGet(t, base+"/metrics")
	if !strings.Contains(body, "gs_actor_processed_total") {
		t.Fatalf("/metrics missing gs_actor_processed_total, body head: %.200s", body)
	}
	if !strings.Contains(body, "go_goroutines") {
		t.Fatalf("/metrics missing go runtime metrics")
	}

	// /healthz 永远 200（存活探针不查依赖）
	if got := mustStatus(t, base+"/healthz"); got != http.StatusOK {
		t.Fatalf("/healthz expect 200, got %d", got)
	}

	// /readyz 依赖正常 → 200
	if got := mustStatus(t, base+"/readyz"); got != http.StatusOK {
		t.Fatalf("/readyz expect 200, got %d", got)
	}

	// /readyz 依赖故障 → 503
	ready = false
	if got := mustStatus(t, base+"/readyz"); got != http.StatusServiceUnavailable {
		t.Fatalf("/readyz expect 503, got %d", got)
	}
}

// TestServerDisabled addr 为空不启动。
func TestServerDisabled(t *testing.T) {
	srv, err := NewServer("", nil)
	if err != nil {
		t.Fatalf("empty addr should not error: %v", err)
	}
	if srv != nil {
		t.Fatal("empty addr should return nil server")
	}
	// nil server 上的方法调用必须安全
	srv.Start()
	if srv.Addr() != "" {
		t.Fatal("nil server addr should be empty")
	}
	if err := srv.Stop(context.Background()); err != nil {
		t.Fatalf("nil server stop: %v", err)
	}
}

func mustGet(t *testing.T, url string) string {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return string(b)
}

func mustStatus(t *testing.T, url string) int {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	return resp.StatusCode
}
