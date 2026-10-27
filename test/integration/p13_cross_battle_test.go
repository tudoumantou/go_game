//go:build integration

// test/integration/p13_cross_battle_test.go —— P13 跨服战斗全链路
//
// 覆盖链路（设计文档 10.3 / 4.9.3）：
//
//	玩家A/B 匹配 → GS 本地凑人 → 向 CrossServer 建房 → 送快照 → ROOM_READY
//	→ 双方 BattleStartNotify → 操作透传（A 的操作到 B）→ 双向上报 → 结算回推
//
// 同时验证：
//   - 重复上报幂等（不重复结算）；
//   - 伪造令牌被拒；
//   - CrossServer 不暴露给客户端（客户端全程只连网关）。
package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"game_study/internal/cross"
	"game_study/internal/game"
	"game_study/internal/gateway"
	"game_study/internal/login"
	"game_study/pkg/config"
	"game_study/pkg/db"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mysql"
	"github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestP13CrossBattleFlow(t *testing.T) {
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
			WaitingFor: wait.ForListeningPort("2379/tcp").WithStartupTimeout(3 * time.Minute),
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

	database, err := db.New(db.Config{DSN: mysqlDSN, MaxOpen: 10, MaxIdle: 3})
	require.NoError(t, err, "connect mysql")
	defer func() { _ = database.Close() }()

	// ② 配置
	loginPort := freePort(t)
	gwPort := freePort(t)
	gwHTTPPort := freePort(t)
	gameGRPCPort := freePort(t)
	crossGRPCPort := freePort(t)
	crossMetricsPort := freePort(t)

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
	baseCfg.Shutdown.DrainTimeout = "30s"

	loginCfg := *baseCfg
	loginCfg.Server.Name = "login-p13"
	loginCfg.Server.HTTPAddr = fmt.Sprintf("127.0.0.1:%d", loginPort)
	loginCfg.Token.TTL = "24h"

	gwCfg := *baseCfg
	gwCfg.Server.Name = "gateway-p13"
	gwCfg.Server.TCPAddr = fmt.Sprintf("127.0.0.1:%d", gwPort)
	gwCfg.Server.HTTPAddr = fmt.Sprintf("127.0.0.1:%d", gwHTTPPort)

	// 跨服节点先起（游戏服要连它）
	crossCfg := *baseCfg
	crossCfg.Server.Name = "cross-p13"
	crossCfg.Server.GRPCAddr = fmt.Sprintf("127.0.0.1:%d", crossGRPCPort)
	crossCfg.Metrics.Enable = true
	crossCfg.Metrics.Addr = fmt.Sprintf("127.0.0.1:%d", crossMetricsPort)

	gameCfg := *baseCfg
	gameCfg.Server.Name = "game-p13"
	gameCfg.Server.GRPCAddr = fmt.Sprintf("127.0.0.1:%d", gameGRPCPort)
	gameCfg.Cross.Nodes = []string{fmt.Sprintf("127.0.0.1:%d", crossGRPCPort)}
	gameCfg.Cross.BattleTimeout = "60s"

	chdirProjectRoot(t)

	svcCtx, svcCancel := context.WithCancel(ctx)
	defer svcCancel()

	loginSrv := login.New(&loginCfg)
	startAndWait(t, "login", func() error { return loginSrv.Start(svcCtx) })

	crossSrv := cross.New(&crossCfg)
	startAndWait(t, "cross", func() error { return crossSrv.Start(svcCtx) })

	gameSrv := game.New(&gameCfg)
	startAndWait(t, "game", func() error { return gameSrv.Start(svcCtx) })

	gwSrv := gateway.New(&gwCfg)
	startAndWait(t, "gateway", func() error { return gwSrv.Start(svcCtx) })

	// 等跨服流建立（GS → CrossServer BIND）：轮询而非死等
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if gameSrv.CrossReady() > 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	require.NotEqualValues(t, 0, gameSrv.CrossReady(), "game server failed to bind to cross node within 15s")
	t.Logf("cross stream bound, ready nodes=%d", gameSrv.CrossReady())

	loginURL := fmt.Sprintf("http://127.0.0.1:%d", loginPort)
	gwAddr := fmt.Sprintf("127.0.0.1:%d", gwPort)

	// ③ 两个玩家进游戏
	_, clientA := newRole(t, loginURL, gwAddr, "斗士甲")
	defer clientA.Close()
	_, clientB := newRole(t, loginURL, gwAddr, "斗士乙")
	defer clientB.Close()

	// ④ 双方发起匹配：A 先入队（等），B 入队触发建房
	respA, err := clientA.Match("arena")
	require.NoError(t, err, "match A")
	require.EqualValues(t, 0, respA.Code)
	require.EqualValues(t, 1, respA.Queued)

	respB, err := clientB.Match("arena")
	require.NoError(t, err, "match B")
	require.EqualValues(t, 0, respB.Code)

	// ⑤ 双方收到开战通知
	startA, err := clientA.WaitBattleStart(15 * time.Second)
	require.NoError(t, err, "wait start A")
	startB, err := clientB.WaitBattleStart(15 * time.Second)
	require.NoError(t, err, "wait start B")
	require.NotEqualValues(t, 0, startA.RoomId, fmt.Sprintf("room mismatch: A=%d B=%d", startA.RoomId, startB.RoomId))
	require.EqualValues(t, startB.RoomId, startA.RoomId)
	require.NotEqualValues(t, "", startA.BattleToken, "battle token must not be empty")
	require.NotEqualValues(t, "", startB.BattleToken)
	require.NotEqualValues(t, startB.BattleToken, startA.BattleToken, "each player must get its own token")
	require.False(t, startA.TimeoutSec <= 0, fmt.Sprintf("timeout_sec must be positive, got %d", startA.TimeoutSec))
	roomID := startA.RoomId

	// ⑥ 操作透传：A 的攻击应出现在 B 的 BattleSyncNotify
	opBody := []byte("skill-fireball-9001")
	err = clientA.BattleOp(roomID, 9001, opBody)
	require.NoError(t, err, fmt.Sprintf("battle op: %v", err))
	sync, err := clientB.WaitBattleSync(10 * time.Second)
	require.NoError(t, err, "wait battle sync")
	require.EqualValues(t, roomID, sync.RoomId)
	require.EqualValues(t, 9001, sync.MsgId)
	require.EqualValues(t, string(opBody), string(sync.Body))
	require.NotEqualValues(t, 0, sync.PlayerId, "sync must carry source player id")

	// ⑦ 伪造令牌上报 → 被拒（6002）
	bad, err := clientA.BattleResult(roomID, "forged-token", 0)
	require.NoError(t, err, "bad token report")
	require.EqualValues(t, 6002, bad.Code)

	// ⑧ 正常上报（A 胜）
	resA, err := clientA.BattleResult(roomID, startA.BattleToken, 0)
	require.NoError(t, err, "result A")
	require.EqualValues(t, 0, resA.Code)

	// 重复上报 → 6003（幂等）
	dup, err := clientA.BattleResult(roomID, startA.BattleToken, 0)
	require.NoError(t, err, "dup result A")
	require.EqualValues(t, 6003, dup.Code)

	// B 上报（败）→ 房间收齐双方，CrossServer 结算回推
	resB, err := clientB.BattleResult(roomID, startB.BattleToken, 1)
	require.NoError(t, err, "result B")
	require.EqualValues(t, 0, resB.Code)

	// ⑨ 双方都收到兜底结算推送（5011）
	settleA, err := clientA.WaitBattleSettle(15 * time.Second)
	require.NoError(t, err, "wait settle A")
	require.EqualValues(t, roomID, settleA.RoomId, fmt.Sprintf("settle A unexpected: %+v", settleA))
	require.EqualValues(t, 0, settleA.Code)
	settleB, err := clientB.WaitBattleSettle(15 * time.Second)
	require.NoError(t, err, "wait settle B")
	require.EqualValues(t, roomID, settleB.RoomId, fmt.Sprintf("settle B unexpected: %+v", settleB))
	require.EqualValues(t, 0, settleB.Code)

	// ⑩ 优雅关闭（跨服先 drain 房间，再停 gRPC）—— 分段计时便于定位耗时
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
	_ = loginSrv.Shutdown(shutdownCtx)
	t.Logf("login shutdown: %s", time.Since(t3).Round(time.Millisecond))
}
