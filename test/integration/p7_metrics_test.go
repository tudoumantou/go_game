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
	"os"
	"strings"
	"testing"
	"time"

	"game_study/internal/gateway"
	"game_study/internal/login"
	"game_study/pkg/config"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mysql"
	"github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestP7MetricsEndpoints(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// ① 中间件
	mysqlC, err := mysql.Run(ctx, "mysql:8.0",
		testcontainers.CustomizeRequest(testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Env: map[string]string{
					"MYSQL_ROOT_PASSWORD": "root",
					"MYSQL_DATABASE":      "game_study",
					"MYSQL_USER":          "game",
					"MYSQL_PASSWORD":      "game_study",
				},
			},
		}),
	)
	require.NoError(t, err, "start mysql")
	defer func() { _ = mysqlC.Terminate(context.Background()) }()

	redisC, err := redis.Run(ctx, "redis:7-alpine")
	require.NoError(t, err, "start redis")
	defer func() { _ = redisC.Terminate(context.Background()) }()

	etcdC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "quay.io/coreos/etcd:v3.5.10",
			ExposedPorts: []string{"2379/tcp"},
			Cmd: []string{
				"/usr/local/bin/etcd",
				"--advertise-client-urls=http://0.0.0.0:2379",
				"--listen-client-urls=http://0.0.0.0:2379",
			},
			WaitingFor: wait.ForListeningPort("2379/tcp").WithStartupTimeout(60 * time.Second),
		},
		Started: true,
	})
	require.NoError(t, err, "start etcd")
	defer func() { _ = etcdC.Terminate(context.Background()) }()

	mysqlDSN, err := mysqlC.ConnectionString(ctx, "parseTime=true", "multiStatements=true", "loc=Local")
	require.NoError(t, err, "mysql dsn")
	redisHost, _ := redisC.Host(ctx)
	redisPort, _ := redisC.MappedPort(ctx, "6379/tcp")
	etcdHost, _ := etcdC.Host(ctx)
	etcdPort, _ := etcdC.MappedPort(ctx, "2379/tcp")

	initSchema(t, mysqlDSN)

	if err := os.Chdir(projectRoot(t)); err != nil {
		t.Fatalf("chdir: %v", err)
	}

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
	baseCfg.Redis.Addrs = []string{fmt.Sprintf("%s:%s", redisHost, redisPort.Port())}
	baseCfg.Redis.PoolSize = 10
	baseCfg.Redis.MinIdleConn = 2
	baseCfg.Etcd.Endpoints = []string{fmt.Sprintf("%s:%s", etcdHost, etcdPort.Port())}
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
	if _, err := httpPostJSON(guestURL, `{"device_id":"p7-metrics-1"}`); err != nil {
		t.Fatalf("trigger guest login: %v", err)
	}

	loginBase := fmt.Sprintf("http://127.0.0.1:%d", loginMetrics)

	t.Run("login_metrics", func(t *testing.T) {
		body := httpGet(t, loginBase+"/metrics")

		// Go 运行时 / 进程指标（registry 装配正确）
		for _, want := range []string{"go_goroutines", "process_start_time_seconds"} {
			if !strings.Contains(body, want) {
				t.Fatalf("/metrics missing %s", want)
			}
		}
		// 自定义指标 + service 标签（埋点已在上方请求中产生）
		if !strings.Contains(body, "gs_message_total") {
			t.Fatal("/metrics missing gs_message_total（登录服 HTTP 埋点未生效）")
		}
		if !strings.Contains(body, `service="login-p7"`) {
			t.Fatal(`/metrics missing service="login-p7" label`)
		}
		if !strings.Contains(body, `direction="in"`) {
			t.Fatal(`/metrics missing direction="in" label`)
		}
	})

	t.Run("login_healthz", func(t *testing.T) {
		if code := httpStatus(t, loginBase+"/healthz"); code != http.StatusOK {
			t.Fatalf("/healthz expect 200, got %d", code)
		}
	})

	t.Run("login_readyz_ok", func(t *testing.T) {
		if code := httpStatus(t, loginBase+"/readyz"); code != http.StatusOK {
			t.Fatalf("/readyz expect 200, got %d", code)
		}
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
		if !strings.Contains(body, "gs_gateway_connections") {
			t.Fatal("/metrics missing gs_gateway_connections（网关埋点未生效）")
		}
		if !strings.Contains(body, `service="gateway-p7"`) {
			t.Fatalf(`/metrics missing service="gateway-p7" label`)
		}
	})

	t.Run("gateway_healthz", func(t *testing.T) {
		if code := httpStatus(t, gwBase+"/healthz"); code != http.StatusOK {
			t.Fatalf("/healthz expect 200, got %d", code)
		}
	})

	// ⑤ 依赖故障 → readyz 503，但 healthz 仍 200
	t.Run("login_readyz_redis_down", func(t *testing.T) {
		if err := redisC.Stop(context.Background(), nil); err != nil {
			t.Fatalf("stop redis: %v", err)
		}
		time.Sleep(600 * time.Millisecond)

		if code := httpStatus(t, loginBase+"/readyz"); code != http.StatusServiceUnavailable {
			t.Fatalf("/readyz expect 503 after redis down, got %d", code)
		}
		// 关键不变量：存活探针不查依赖，避免中间件抖动引发重启风暴
		if code := httpStatus(t, loginBase+"/healthz"); code != http.StatusOK {
			t.Fatalf("/healthz expect 200 even when redis down, got %d", code)
		}
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
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s: status %d", url, resp.StatusCode)
	}
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read %s: %v", url, err)
	}
	return string(b)
}

func httpStatus(t *testing.T, url string) int {
	t.Helper()
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
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
