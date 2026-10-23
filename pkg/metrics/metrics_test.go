// pkg/metrics/metrics_test.go —— 指标注册与端点测试
package metrics

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// TestInitRegisters 指标初始化后可正常读写。
func TestInitRegisters(t *testing.T) {
	Init("unit-test")

	require.False(t, !Enabled(), "metrics should be enabled after Init")
	require.EqualValues(t, "unit-test", Service())

	// 计数器
	before := testutil.ToFloat64(msgCounter.WithLabelValues("unit-test", "in", "ok"))
	MsgInc("in", "ok")
	after := testutil.ToFloat64(msgCounter.WithLabelValues("unit-test", "in", "ok"))
	require.EqualValues(t, before+1, after, fmt.Sprintf("MsgInc: before=%v after=%v", before, after))

	// Gauge
	ConnInc()
	ConnInc()
	v := testutil.ToFloat64(connGauge.WithLabelValues("unit-test"))
	require.EqualValues(t, 2, v, fmt.Sprintf("ConnInc: expect 2, got %v", v))
	ConnDec()
	v = testutil.ToFloat64(connGauge.WithLabelValues("unit-test"))
	require.EqualValues(t, 1, v, fmt.Sprintf("ConnDec: expect 1, got %v", v))

	// 拒绝连接累计
	ConnRejected("max_conns")
	v = testutil.ToFloat64(connTotal.WithLabelValues("unit-test", "rejected_max_conns"))
	require.EqualValues(t, 1, v, fmt.Sprintf("ConnRejected: expect 1, got %v", v))

	// 直方图
	MsgObserve("in", 15*time.Millisecond, 512)
	n := testutil.CollectAndCount(msgLatency)
	require.NotEqualValues(t, 0, n, "msgLatency should be collected")

	// Actor
	ActorCountAdd(1)
	ActorProcessed(true)
	ActorObserve(time.Millisecond)
	ActorMailbox(3, 42)
	v = testutil.ToFloat64(actorMailbox.WithLabelValues("unit-test", "3"))
	require.EqualValues(t, 42, v, fmt.Sprintf("ActorMailbox: expect 42, got %v", v))
	ActorPanicInc()
	ActorRejectedAdd(3)
	ActorRejectedAdd(0) // 0 不应改变计数
	v = testutil.ToFloat64(actorRejected.WithLabelValues("unit-test"))
	require.EqualValues(t, 3, v, fmt.Sprintf("ActorRejectedAdd: expect 3, got %v", v))

	// Flusher
	FlushDirty(1, 7)
	FlushObserve(20 * time.Millisecond)
	FlushRows(true, 7)
	FlushFail(2)
	FlushLag(0, 1.5)
	v = testutil.ToFloat64(flushDirty.WithLabelValues("unit-test", "1"))
	require.EqualValues(t, 7, v, fmt.Sprintf("FlushDirty: expect 7, got %v", v))

	// Channel 池
	ChannelStreams(12)
	ChannelPoolSize(3)
	ChannelError("send")
	v = testutil.ToFloat64(channelStreams.WithLabelValues("unit-test"))
	require.EqualValues(t, 12, v, fmt.Sprintf("ChannelStreams: expect 12, got %v", v))

	// Kafka
	KafkaConsumed("gs.mail", "ok")
	KafkaProduced("gs.mail", true)
	KafkaProduced("gs.mail", false)
	KafkaLag("gs.mail", 128)
	v = testutil.ToFloat64(kafkaLag.WithLabelValues("unit-test", "gs.mail"))
	require.EqualValues(t, 128, v, fmt.Sprintf("KafkaLag: expect 128, got %v", v))
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
	require.NoError(t, err, "new server")
	srv.Start()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Stop(ctx)
	}()

	base := "http://" + srv.Addr()

	// /metrics 应含注册的自定义指标
	body := mustGet(t, base+"/metrics")
	require.Contains(t, body, "gs_actor_processed_total", fmt.Sprintf("/metrics missing gs_actor_processed_total, body head: %.200s", body))
	require.Contains(t, body, "go_goroutines", "/metrics missing go runtime metrics")

	// /healthz 永远 200（存活探针不查依赖）
	got := mustStatus(t, base+"/healthz")
	require.EqualValues(t, http.StatusOK, got, fmt.Sprintf("/healthz expect 200, got %d", got))

	// /readyz 依赖正常 → 200
	got = mustStatus(t, base+"/readyz")
	require.EqualValues(t, http.StatusOK, got, fmt.Sprintf("/readyz expect 200, got %d", got))

	// /readyz 依赖故障 → 503
	ready = false
	got = mustStatus(t, base+"/readyz")
	require.EqualValues(t, http.StatusServiceUnavailable, got, fmt.Sprintf("/readyz expect 503, got %d", got))
}

// TestServerDisabled addr 为空不启动。
func TestServerDisabled(t *testing.T) {
	srv, err := NewServer("", nil)
	require.NoError(t, err, "empty addr should not error")
	require.Nil(t, srv, "empty addr should return nil server")
	// nil server 上的方法调用必须安全
	srv.Start()
	require.EqualValues(t, "", srv.Addr(), "nil server addr should be empty")
	err = srv.Stop(context.Background())
	require.NoError(t, err, fmt.Sprintf("nil server stop: %v", err))
}

func mustGet(t *testing.T, url string) string {
	t.Helper()
	resp, err := http.Get(url)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	require.NoError(t, err, "read body")
	return string(b)
}

func mustStatus(t *testing.T, url string) int {
	t.Helper()
	resp, err := http.Get(url)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	return resp.StatusCode
}
