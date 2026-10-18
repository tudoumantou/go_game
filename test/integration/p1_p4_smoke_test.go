//go:build integration

// test/integration/p1_p4_smoke_test.go —— P1-P4 全链路冒烟：登录 → 网关握手 → 绑定游戏服 → 心跳
package integration

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"game_study/internal/gateway"
	"game_study/internal/game"
	"game_study/internal/login"
	"game_study/pkg/config"
	"game_study/pkg/db"
	"game_study/test/robot"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mysql"
	"github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestP1toP4Smoke(t *testing.T) {
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
	if err != nil {
		t.Fatalf("start mysql: %v", err)
	}
	defer func() { _ = mysqlC.Terminate(context.Background()) }()

	redisC, err := redis.Run(ctx, "redis:7-alpine")
	if err != nil {
		t.Fatalf("start redis: %v", err)
	}
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
	if err != nil {
		t.Fatalf("start etcd: %v", err)
	}
	defer func() { _ = etcdC.Terminate(context.Background()) }()

	// ② DSN
	mysqlDSN, err := mysqlC.ConnectionString(ctx, "parseTime=true", "multiStatements=true", "loc=Local")
	if err != nil {
		t.Fatalf("mysql dsn: %v", err)
	}
	redisHost, _ := redisC.Host(ctx)
	redisPort, _ := redisC.MappedPort(ctx, "6379/tcp")
	etcdHost, _ := etcdC.Host(ctx)
	etcdPort, _ := etcdC.MappedPort(ctx, "2379/tcp")

	// ③ 初始化 schema
	initSchema(t, mysqlDSN)

	// ④ 构造配置（端口用随机可用端口）
	loginPort := freePort(t)
	gwPort := freePort(t)
	gwHTTPPort := freePort(t)
	gameGRPCPort := freePort(t)

	baseCfg := &config.Config{}
	baseCfg.MySQL.DSN = mysqlDSN
	baseCfg.MySQL.MaxOpenConns = 20
	baseCfg.MySQL.MaxIdleConns = 5
	baseCfg.Redis.Addrs = []string{fmt.Sprintf("%s:%s", redisHost, redisPort.Port())}
	baseCfg.Redis.PoolSize = 20
	baseCfg.Redis.MinIdleConn = 5
	baseCfg.Etcd.Endpoints = []string{fmt.Sprintf("%s:%s", etcdHost, etcdPort.Port())}
	baseCfg.Etcd.LeaseTTL = 10
	baseCfg.Log.Level = "info"
	baseCfg.Actor.ShardCount = 64
	baseCfg.Actor.MailboxSize = 256
	baseCfg.Flush.ShardCount = 10
	baseCfg.Flush.Interval = "2s"
	baseCfg.Flush.Stagger = "200ms"
	baseCfg.Flush.BatchRows = 50
	baseCfg.Server.MaxConns = 1000

	// login
	loginCfg := *baseCfg
	loginCfg.Server.Name = "login-test"
	loginCfg.Server.HTTPAddr = fmt.Sprintf("127.0.0.1:%d", loginPort)
	loginCfg.Token.TTL = "24h"

	// gateway
	gwCfg := *baseCfg
	gwCfg.Server.Name = "gateway-test"
	gwCfg.Server.TCPAddr = fmt.Sprintf("127.0.0.1:%d", gwPort)
	gwCfg.Server.HTTPAddr = fmt.Sprintf("127.0.0.1:%d", gwHTTPPort)

	// game
	gameCfg := *baseCfg
	gameCfg.Server.Name = "game-test"
	gameCfg.Server.GRPCAddr = fmt.Sprintf("127.0.0.1:%d", gameGRPCPort)

	// 切到项目根目录（gateway 加载 keys/ 相对路径）
	if err := os.Chdir(projectRoot(t)); err != nil {
		t.Fatalf("chdir: %v", err)
	}

	// ⑤ 启动三服务
	svcCtx, svcCancel := context.WithCancel(ctx)
	defer svcCancel()

	loginSrv := login.New(&loginCfg)
	startAndWait(t, "login", func() error { return loginSrv.Start(svcCtx) })

	gameSrv := game.New(&gameCfg)
	startAndWait(t, "game", func() error { return gameSrv.Start(svcCtx) })

	gwSrv := gateway.New(&gwCfg)
	startAndWait(t, "gateway", func() error { return gwSrv.Start(svcCtx) })

	// 等待 etcd 注册完成
	time.Sleep(2 * time.Second)

	// ⑥ 机器人全链路
	r := robot.New(fmt.Sprintf("http://127.0.0.1:%d", loginPort), fmt.Sprintf("127.0.0.1:%d", gwPort))
	if err := r.GuestLogin("robot-device-1"); err != nil {
		t.Fatalf("guest login: %v", err)
	}
	if err := r.Connect(); err != nil {
		t.Fatalf("connect gateway: %v", err)
	}
	defer r.Close()

	if err := r.Handshake(); err != nil {
		t.Fatalf("handshake: %v", err)
	}
	resp, err := r.SendHandshakeReq("1.0.0", "pc")
	if err != nil {
		t.Fatalf("handshake req: %v", err)
	}
	if resp.Code != 0 {
		t.Fatalf("handshake resp code=%d", resp.Code)
	}

	// 心跳
	hb, err := r.Heartbeat()
	if err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	if hb.ServerTime == 0 {
		t.Fatalf("heartbeat server_time is 0")
	}

	// ⑦ 优雅关闭
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()
	_ = gwSrv.Shutdown(shutdownCtx)
	_ = gameSrv.Shutdown(shutdownCtx)
	_ = loginSrv.Shutdown(shutdownCtx)
}

// startAndWait 启动服务，等待端口可连接。
func startAndWait(t *testing.T, name string, start func() error) {
	t.Helper()
	errCh := make(chan error, 1)
	go func() { errCh <- start() }()
	select {
	case err := <-errCh:
		if err != nil && err != context.Canceled {
			t.Fatalf("%s start failed: %v", name, err)
		}
	case <-time.After(3 * time.Second):
		// 正常情况：Start 阻塞
	}
}

// freePort 获取一个空闲端口。
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("free port: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	return port
}

// initSchema 执行 sql/schema 下全部 DDL。
func initSchema(t *testing.T, dsn string) {
	t.Helper()
	database, err := db.New(db.Config{DSN: dsn, MaxOpen: 5, MaxIdle: 2})
	if err != nil {
		t.Fatalf("connect mysql: %v", err)
	}
	defer func() { _ = database.Close() }()

	root := projectRoot(t)
	files, err := filepath.Glob(filepath.Join(root, "sql/schema/*.sql"))
	if err != nil {
		t.Fatalf("glob schema: %v", err)
	}
	if len(files) == 0 {
		t.Fatalf("no schema files found under %s", root)
	}
	for _, f := range files {
		content, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("read %s: %v", f, err)
		}
		if _, err := database.Exec(string(content)); err != nil {
			t.Fatalf("exec %s: %v", f, err)
		}
	}
	// 种子
	seed, err := os.ReadFile(filepath.Join(root, "sql/seed/seed_dev.sql"))
	if err == nil {
		_, _ = database.Exec(string(seed))
	}
}

// projectRoot 返回项目根目录。
func projectRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("project root not found")
		}
		dir = parent
	}
}
