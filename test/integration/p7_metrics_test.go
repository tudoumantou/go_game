//go:build integration

// test/integration/p7_metrics_test.go —— P7 可观测：指标端点与健康探针
//
// 验证三件事（设计文档 20.3 / 19.6 探针表）：
//  1. 服务启动后 /metrics 可抓取，含自定义 gs_* 指标与正确 service 标签；
//  2. /healthz 永远 200（存活探针不查依赖）；
//  3. /readyz 反映依赖状态（Redis 断开 → 503，但 /healthz 仍 200）。
//
// 注意启动顺序：metrics 包的 service 名是进程级单例（生产一进程一服务），
// 而 Prometheus 的 label 值在首次 WithLabelValues 时固化。因此本测试刻意
// 「先起 login → 触发埋点 → 校验 → 再起 gateway → 触发埋点 → 校验」，
// 保证每个服务的指标带的是自己的 service 标签。
package integration

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"game_study/internal/gateway"
	"game_study/internal/login"
	"game_study/pkg/config"

	"github.com/stretchr/testify/require"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
)

func TestP7MetricsEndpoints(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// ① 中间件
	// 容器由 TestMain 起一次共享（见 support_test.go 的 isolatedDSN 说明），
	// 这里只取一条指向本用例独立库的 DSN：库独立才是「建表不撞车」的关键。
	mysqlDSN := freshEnv(t)
	etcdEndpoint := testEtcdAddr

	// Redis 这里不能共享：下面的 login_readyz_redis_down 子测试要 Stop() 掉
	// 它来验证「依赖故障时 readyz 503、healthz 仍 200」。停掉共享实例会把
	// 后续用例一起带崩，所以本用例独占一个。
	redisC, err := tcredis.Run(ctx, "redis:7-alpine")
	require.NoError(t, err, "start redis")
	defer func() { _ = redisC.Terminate(context.Background()) }()
	redisHost, _ := redisC.Host(ctx)
	redisPort, _ := redisC.MappedPort(ctx, "6379/tcp")
	redisAddr := fmt.Sprintf("%s:%s", redisHost, redisPort.Port())

	chdirProjectRoot(t)

	// ② 配置
	loginHTTP := freePort(t)
	loginMetrics := freePort(t)
	gwTCP := freePort(t)
	gwHTTP := freePort(t)
	gwMetrics := freePort(t)

	baseCfg := &config.Config{}
	baseCfg.MySQL.DSN = mysqlDSN
	baseCfg.MySQL.MaxOpenConns = 10
	baseCfg.MySQL.MaxIdleConns = 3
	baseCfg.Redis.Addrs = []string{redisAddr}
	baseCfg.Redis.PoolSize = 10
	baseCfg.Redis.MinIdleConn = 2
	baseCfg.Etcd.Endpoints = []string{etcdEndpoint}
	baseCfg.Etcd.LeaseTTL = 10
	baseCfg.Log.Level = "warn"
	baseCfg.Actor.ShardCount = 16
	baseCfg.Actor.MailboxSize = 128
	baseCfg.Flush.ShardCount = 4
	baseCfg.Flush.Interval = "2s"
	baseCfg.Flush.Stagger = "200ms"
	baseCfg.Flush.BatchRows = 50
	baseCfg.Server.MaxConns = 100
	baseCfg.Metrics.Enable = true

	loginCfg := *baseCfg
	loginCfg.Server.Name = "login-p7"
	loginCfg.Server.HTTPAddr = fmt.Sprintf("127.0.0.1:%d", loginHTTP)
	loginCfg.Metrics.Addr = fmt.Sprintf("127.0.0.1:%d", loginMetrics)
	loginCfg.Token.TTL = "24h"

	gwCfg := *baseCfg
	gwCfg.Server.Name = "gateway-p7"
	gwCfg.Server.TCPAddr = fmt.Sprintf("127.0.0.1:%d", gwTCP)
	gwCfg.Server.HTTPAddr = fmt.Sprintf("127.0.0.1:%d", gwHTTP)
	gwCfg.Metrics.Addr = fmt.Sprintf("127.0.0.1:%d", gwMetrics)

	svcCtx, svcCancel := context.WithCancel(ctx)
	defer svcCancel()

	// ③ 先起登录服 → 触发一次业务请求（产生埋点）→ 校验指标
	loginSrv := login.New(&loginCfg)
	startAndWait(t, "login", func() error { return loginSrv.Start(svcCtx) })
	time.Sleep(1200 * time.Millisecond)

	// 触发埋点：guest 登录（/v1/ 前缀，会被中间件统计）
	guestURL := fmt.Sprintf("http://127.0.0.1:%d/v1/auth/guest", loginHTTP)
	_, err = httpPostJSON(guestURL, `{"device_id":"p7-metrics-1"}`)
	require.NoError(t, err, fmt.Sprintf("trigger guest login: %v", err))

	loginBase := fmt.Sprintf("http://127.0.0.1:%d", loginMetrics)

	t.Run("login_metrics", func(t *testing.T) {
		body := httpGet(t, loginBase+"/metrics")

		// Go 运行时 / 进程指标（registry 装配正确）
		for _, want := range []string{"go_goroutines", "process_start_time_seconds"} {
			require.Contains(t, body, want, fmt.Sprintf("/metrics missing %s", want))
		}
		// 自定义指标 + service 标签（埋点已在上方请求中产生）
		require.Contains(t, body, "gs_message_total", "/metrics missing gs_message_total（登录服 HTTP 埋点未生效）")
		require.Contains(t, body, `service="login-p7"`)
		require.Contains(t, body, `direction="in"`)
	})

	t.Run("login_healthz", func(t *testing.T) {
		code := httpStatus(t, loginBase+"/healthz")
		require.EqualValues(t, http.StatusOK, code, fmt.Sprintf("/healthz expect 200, got %d", code))
	})

	t.Run("login_readyz_ok", func(t *testing.T) {
		code := httpStatus(t, loginBase+"/readyz")
		require.EqualValues(t, http.StatusOK, code, fmt.Sprintf("/readyz expect 200, got %d", code))
	})

	// ④ 再起网关 → 建一条 TCP 连接（触发 ConnInc）→ 校验网关指标
	gwSrv := gateway.New(&gwCfg)
	startAndWait(t, "gateway", func() error { return gwSrv.Start(svcCtx) })
	time.Sleep(1200 * time.Millisecond)

	conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", gwTCP), 3*time.Second)
	require.NoError(t, err, "dial gateway")
	defer func() { _ = conn.Close() }()
	time.Sleep(300 * time.Millisecond)

	gwBase := fmt.Sprintf("http://127.0.0.1:%d", gwMetrics)

	t.Run("gateway_metrics", func(t *testing.T) {
		body := httpGet(t, gwBase+"/metrics")
		require.Contains(t, body, "gs_gateway_connections", "/metrics missing gs_gateway_connections（网关埋点未生效）")
		require.Contains(t, body, `service="gateway-p7"`)
	})

	t.Run("gateway_healthz", func(t *testing.T) {
		code := httpStatus(t, gwBase+"/healthz")
		require.EqualValues(t, http.StatusOK, code, fmt.Sprintf("/healthz expect 200, got %d", code))
	})

	// ⑤ 依赖故障 → readyz 503，但 healthz 仍 200
	t.Run("login_readyz_redis_down", func(t *testing.T) {
		err := redisC.Stop(context.Background(), nil)
		require.NoError(t, err, fmt.Sprintf("stop redis: %v", err))
		time.Sleep(600 * time.Millisecond)

		code := httpStatus(t, loginBase+"/readyz")
		require.EqualValues(t, http.StatusServiceUnavailable, code, fmt.Sprintf("/readyz expect 503 after redis down, got %d", code))
		// 关键不变量：存活探针不查依赖，避免中间件抖动引发重启风暴
		code = httpStatus(t, loginBase+"/healthz")
		require.EqualValues(t, http.StatusOK, code, fmt.Sprintf("/healthz expect 200 even when redis down, got %d", code))
	})

	// ⑥ 优雅关闭
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer shutdownCancel()
	_ = gwSrv.Shutdown(shutdownCtx)
	_ = loginSrv.Shutdown(shutdownCtx)
}

func httpGet(t *testing.T, url string) string {
	t.Helper()
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(url)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.EqualValues(t, http.StatusOK, resp.StatusCode)
	b, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return string(b)
}

func httpStatus(t *testing.T, url string) int {
	t.Helper()
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(url)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	return resp.StatusCode
}

func httpPostJSON(url, body string) (int, error) {
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode, nil
}
