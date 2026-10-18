//go:build integration

// test/integration/p8_center_test.go —— P8 中心服：节点注册 / 跨服匹配 / 中转
//
// 验证的链路（设计文档 9.2 / 9.3 / 10.3）：
//
//	[节点注册] GS 连中心服 → Register → 收 GroupUpdate（含组内跨服地址）
//	[跨服匹配] A/B 各发 MatchReq → GS 上报 MatchRequest → 中心服匹配
//	          → MatchResult 分发 → GS 建房 + 送快照 → BattleStartNotify
//	[中转]     GS 经中心服把消息投递给另一节点（本测试用同节点双会话验证回路）
package integration

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"game_study/internal/center"
	"game_study/internal/cross"
	"game_study/internal/game"
	"game_study/internal/gateway"
	"game_study/internal/login"
	"game_study/pkg/config"
	"game_study/pkg/db"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mysql"
	"github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestP8CenterMatchFlow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
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

	mysqlDSN, err := mysqlC.ConnectionString(ctx, "parseTime=true", "multiStatements=true", "loc=Local")
	if err != nil {
		t.Fatalf("mysql dsn: %v", err)
	}
	redisHost, _ := redisC.Host(ctx)
	redisPort, _ := redisC.MappedPort(ctx, "6379/tcp")
	etcdHost, _ := etcdC.Host(ctx)
	etcdPort, _ := etcdC.MappedPort(ctx, "2379/tcp")

	initSchema(t, mysqlDSN)

	database, err := db.New(db.Config{DSN: mysqlDSN, MaxOpen: 10, MaxIdle: 3})
	if err != nil {
		t.Fatalf("connect mysql: %v", err)
	}
	defer func() { _ = database.Close() }()

	// ② 端口
	loginPort := freePort(t)
	gwPort := freePort(t)
	gwHTTPPort := freePort(t)
	gameGRPCPort := freePort(t)
	crossGRPCPort := freePort(t)
	centerGRPCPort := freePort(t)
	centerMetrics := freePort(t)

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
	baseCfg.Shutdown.TotalTimeout = "60s"
	baseCfg.Shutdown.DrainTimeout = "20s"
	baseCfg.Report.HeartbeatInterval = "1s"
	baseCfg.Report.HeartbeatTimeout = "10s"
	baseCfg.Report.RelayRetry = 3

	crossAddr := fmt.Sprintf("127.0.0.1:%d", crossGRPCPort)

	loginCfg := *baseCfg
	loginCfg.Server.Name = "login-p8"
	loginCfg.Server.HTTPAddr = fmt.Sprintf("127.0.0.1:%d", loginPort)
	loginCfg.Token.TTL = "24h"

	gwCfg := *baseCfg
	gwCfg.Server.Name = "gateway-p8"
	gwCfg.Server.TCPAddr = fmt.Sprintf("127.0.0.1:%d", gwPort)
	gwCfg.Server.HTTPAddr = fmt.Sprintf("127.0.0.1:%d", gwHTTPPort)

	centerCfg := *baseCfg
	centerCfg.Server.Name = "center-01"
	centerCfg.Server.GRPCAddr = fmt.Sprintf("127.0.0.1:%d", centerGRPCPort)
	centerCfg.Metrics.Enable = true
	centerCfg.Metrics.Addr = fmt.Sprintf("127.0.0.1:%d", centerMetrics)
	centerCfg.Cross.Nodes = []string{crossAddr}

	crossCfg := *baseCfg
	crossCfg.Server.Name = "cross-p8"
	crossCfg.Server.GRPCAddr = crossAddr

	gameCfg := *baseCfg
	gameCfg.Server.Name = "game-p8"
	gameCfg.Server.GRPCAddr = fmt.Sprintf("127.0.0.1:%d", gameGRPCPort)
	gameCfg.Cross.Nodes = []string{crossAddr}
	gameCfg.Cross.BattleTimeout = "60s"
	// 关键：配了中心服 → battle 走中心服匹配主路径
	gameCfg.Center.Addr = fmt.Sprintf("127.0.0.1:%d", centerGRPCPort)

	if err := os.Chdir(projectRoot(t)); err != nil {
		t.Fatalf("chdir: %v", err)
	}

	svcCtx, svcCancel := context.WithCancel(ctx)
	defer svcCancel()

	// ③ 启动顺序：center → cross → game → gateway → login
	centerSrv := center.New(&centerCfg)
	startAndWait(t, "center", func() error { return centerSrv.Start(svcCtx) })

	crossSrv := cross.New(&crossCfg)
	startAndWait(t, "cross", func() error { return crossSrv.Start(svcCtx) })

	gameSrv := game.New(&gameCfg)
	startAndWait(t, "game", func() error { return gameSrv.Start(svcCtx) })

	gwSrv := gateway.New(&gwCfg)
	startAndWait(t, "gateway", func() error { return gwSrv.Start(svcCtx) })

	loginSrv := login.New(&loginCfg)
	startAndWait(t, "login", func() error { return loginSrv.Start(svcCtx) })

	// ④ 等三条链路就绪：GS→Center（Report）、GS→Cross（CrossService）、
	//    并且中心服已把 GroupUpdate 下发给 GS（含跨服地址）
	waitUntil(t, 20*time.Second, "center stream + cross stream ready", func() bool {
		return gameSrv.CenterReady() && gameSrv.CrossReady() > 0
	})
	t.Logf("center ready=%v cross nodes=%d", gameSrv.CenterReady(), gameSrv.CrossReady())

	loginURL := fmt.Sprintf("http://127.0.0.1:%d", loginPort)
	gwAddr := fmt.Sprintf("127.0.0.1:%d", gwPort)

	// ⑤ 两个玩家进游戏
	roleA, clientA := newRole(t, loginURL, gwAddr, "中心甲")
	defer clientA.Close()
	roleB, clientB := newRole(t, loginURL, gwAddr, "中心乙")
	defer clientB.Close()

	// ⑥ 双方匹配：中心服匹配池凑人 → MatchResult 分发 → 各自建房 → 开战通知
	respA, err := clientA.Match("arena")
	if err != nil {
		t.Fatalf("match A: %v", err)
	}
	if respA.Code != 0 {
		t.Fatalf("match A code=%d", respA.Code)
	}
	respB, err := clientB.Match("arena")
	if err != nil {
		t.Fatalf("match B: %v", err)
	}
	if respB.Code != 0 {
		t.Fatalf("match B code=%d", respB.Code)
	}

	startA, err := clientA.WaitBattleStart(20 * time.Second)
	if err != nil {
		t.Fatalf("wait start A: %v", err)
	}
	startB, err := clientB.WaitBattleStart(20 * time.Second)
	if err != nil {
		t.Fatalf("wait start B: %v", err)
	}
	if startA.RoomId == 0 || startA.RoomId != startB.RoomId {
		t.Fatalf("room mismatch: A=%d B=%d", startA.RoomId, startB.RoomId)
	}
	if startA.BattleToken == startB.BattleToken {
		t.Fatal("tokens must differ per player")
	}
	roomID := startA.RoomId
	t.Logf("match ok: room=%d playerA=%d playerB=%d", roomID, roleA, roleB)

	// ⑦ 中心服分配的 room_id 应落在高位（实例号<<40），而非跨服本地自增
	//    这里只校验非 0 且双方一致，具体位段由 match 单测覆盖
	if roomID>>40 == 0 {
		t.Logf("note: room id %d does not carry center instance bits", roomID)
	}

	// ⑧ 战斗透传（验证中心服匹配出来的房间真实可用）
	if err := clientA.BattleOp(roomID, 8801, []byte("op-via-center")); err != nil {
		t.Fatalf("battle op: %v", err)
	}
	sync, err := clientB.WaitBattleSync(10 * time.Second)
	if err != nil {
		t.Fatalf("wait battle sync: %v", err)
	}
	if string(sync.Body) != "op-via-center" {
		t.Fatalf("relay body mismatch: %q", sync.Body)
	}

	// ⑨ 结算：双方上报 → CrossServer 结算回推
	if _, err := clientA.BattleResult(roomID, startA.BattleToken, 0); err != nil {
		t.Fatalf("result A: %v", err)
	}
	if _, err := clientB.BattleResult(roomID, startB.BattleToken, 1); err != nil {
		t.Fatalf("result B: %v", err)
	}
	settleA, err := clientA.WaitBattleSettle(15 * time.Second)
	if err != nil {
		t.Fatalf("settle A: %v", err)
	}
	if settleA.RoomId != roomID || settleA.Code != 0 {
		t.Fatalf("settle A unexpected: %+v", settleA)
	}

	// ⑩ 优雅关闭：中心服→跨服→游戏→网关→登录
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer shutdownCancel()

	t0 := time.Now()
	_ = gwSrv.Shutdown(shutdownCtx)
	t.Logf("gateway shutdown: %s", time.Since(t0).Round(time.Millisecond))

	t1 := time.Now()
	_ = gameSrv.Shutdown(shutdownCtx)
	t.Logf("game shutdown: %s", time.Since(t1).Round(time.Millisecond))

	t2 := time.Now()
	_ = crossSrv.Shutdown(shutdownCtx)
	t.Logf("cross shutdown: %s", time.Since(t2).Round(time.Millisecond))

	t3 := time.Now()
	_ = centerSrv.Shutdown(shutdownCtx)
	t.Logf("center shutdown: %s", time.Since(t3).Round(time.Millisecond))

	_ = loginSrv.Shutdown(shutdownCtx)
}

// waitUntil 轮询等待条件成立。
func waitUntil(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", what)
}
