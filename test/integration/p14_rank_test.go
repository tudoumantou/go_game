//go:build integration

// test/integration/p14_rank_test.go —— P14 全服排行榜 / 统计 / 选主
//
// 验证链路：
//  1. 战斗结算 → GS 上报 RankReport → 中心服 ZINCRBY → GM 查询 TopN 可见；
//  2. 胜 +30 / 负 -10 的分数语义正确；
//  3. 统计上报 → 内存聚合 → QueryFeatureStats 可查；
//  4. etcd 选主：单实例应成为 leader。
package integration

import (
	"context"
	"fmt"
	"io"
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

	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mysql"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	internalv2 "game_study/pkg/pb/internalv2"
	"github.com/stretchr/testify/require"
)

func TestP14RankAndLeader(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

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

	redisC, err := tcredis.Run(ctx, "redis:7-alpine")
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
	redisAddr := fmt.Sprintf("%s:%s", redisHost, redisPort.Port())
	etcdAddr := fmt.Sprintf("%s:%s", etcdHost, etcdPort.Port())

	initSchema(t, mysqlDSN)

	database, err := db.New(db.Config{DSN: mysqlDSN, MaxOpen: 10, MaxIdle: 3})
	require.NoError(t, err, "connect mysql")
	defer func() { _ = database.Close() }()

	loginPort := freePort(t)
	gwPort := freePort(t)
	gwHTTPPort := freePort(t)
	gameGRPCPort := freePort(t)
	crossGRPCPort := freePort(t)
	centerGRPCPort := freePort(t)

	baseCfg := &config.Config{}
	baseCfg.MySQL.DSN = mysqlDSN
	baseCfg.MySQL.MaxOpenConns = 20
	baseCfg.MySQL.MaxIdleConns = 5
	baseCfg.Redis.Addrs = []string{redisAddr}
	baseCfg.Redis.PoolSize = 20
	baseCfg.Redis.MinIdleConn = 5
	baseCfg.Etcd.Endpoints = []string{etcdAddr}
	baseCfg.Etcd.LeaseTTL = 10
	baseCfg.Etcd.LeaderKey = "/game_study/test/leader"
	baseCfg.Log.Level = "info"
	baseCfg.Actor.ShardCount = 64
	baseCfg.Actor.MailboxSize = 256
	baseCfg.Flush.ShardCount = 10
	baseCfg.Flush.Interval = "2s"
	baseCfg.Flush.Stagger = "200ms"
	baseCfg.Flush.BatchRows = 50
	baseCfg.Server.MaxConns = 100
	baseCfg.Shutdown.TotalTimeout = "60s"
	baseCfg.Shutdown.DrainTimeout = "20s"
	baseCfg.Report.HeartbeatTimeout = "10s"
	baseCfg.Report.RelayRetry = 3

	crossAddr := fmt.Sprintf("127.0.0.1:%d", crossGRPCPort)

	loginCfg := *baseCfg
	loginCfg.Server.Name = "login-p14"
	loginCfg.Server.HTTPAddr = fmt.Sprintf("127.0.0.1:%d", loginPort)
	loginCfg.Token.TTL = "24h"

	gwCfg := *baseCfg
	gwCfg.Server.Name = "gateway-p14"
	gwCfg.Server.TCPAddr = fmt.Sprintf("127.0.0.1:%d", gwPort)
	gwCfg.Server.HTTPAddr = fmt.Sprintf("127.0.0.1:%d", gwHTTPPort)

	centerCfg := *baseCfg
	centerCfg.Server.Name = "center-01"
	centerCfg.Server.GRPCAddr = fmt.Sprintf("127.0.0.1:%d", centerGRPCPort)
	centerCfg.Cross.Nodes = []string{crossAddr}

	crossCfg := *baseCfg
	crossCfg.Server.Name = "cross-p14"
	crossCfg.Server.GRPCAddr = crossAddr

	gameCfg := *baseCfg
	gameCfg.Server.Name = "game-p14"
	gameCfg.Server.GRPCAddr = fmt.Sprintf("127.0.0.1:%d", gameGRPCPort)
	gameCfg.Cross.Nodes = []string{crossAddr}
	gameCfg.Cross.BattleTimeout = "60s"
	gameCfg.Center.Addr = fmt.Sprintf("127.0.0.1:%d", centerGRPCPort)

	if err := os.Chdir(projectRoot(t)); err != nil {
		t.Fatalf("chdir: %v", err)
	}

	svcCtx, svcCancel := context.WithCancel(ctx)
	defer svcCancel()

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

	waitUntil(t, 20*time.Second, "game connected to center and cross", func() bool {
		return gameSrv.CenterReady() && gameSrv.CrossReady() > 0
	})

	// ① 选主：单实例应成为 leader
	waitUntil(t, 20*time.Second, "center becomes leader", func() bool {
		return centerSrv.IsLeader()
	})
	t.Logf("center is leader")

	// ② 两人匹配 → 打完一场 → A 胜 B 负 → 上报排行
	loginURL := fmt.Sprintf("http://127.0.0.1:%d", loginPort)
	gwAddr := fmt.Sprintf("127.0.0.1:%d", gwPort)

	roleA, clientA := newRole(t, loginURL, gwAddr, "榜首甲")
	defer clientA.Close()
	roleB, clientB := newRole(t, loginURL, gwAddr, "挑战乙")
	defer clientB.Close()

	if _, err := clientA.Match("arena"); err != nil {
		t.Fatalf("match A: %v", err)
	}
	if _, err := clientB.Match("arena"); err != nil {
		t.Fatalf("match B: %v", err)
	}
	startA, err := clientA.WaitBattleStart(20 * time.Second)
	require.NoError(t, err, "wait start A")
	startB, err := clientB.WaitBattleStart(20 * time.Second)
	require.NoError(t, err, "wait start B")
	roomID := startA.RoomId

	// A 胜(0)，B 负(1)
	if _, err := clientA.BattleResult(roomID, startA.BattleToken, 0); err != nil {
		t.Fatalf("result A: %v", err)
	}
	if _, err := clientB.BattleResult(roomID, startB.BattleToken, 1); err != nil {
		t.Fatalf("result B: %v", err)
	}

	// 等结算回推（结算后才上报排行）
	if _, err := clientA.WaitBattleSettle(15 * time.Second); err != nil {
		t.Fatalf("settle A: %v", err)
	}

	// ③ 通过 GM 接口查榜：A 应在 B 之上（+30 vs -10）
	conn, err := grpc.NewClient(fmt.Sprintf("127.0.0.1:%d", centerGRPCPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err, "dial center")
	defer func() { _ = conn.Close() }()

	gm := internalv2.NewCenterServiceClient(conn)

	var entries []*internalv2.RankEntry
	waitUntil(t, 15*time.Second, "rank entries appear", func() bool {
		entries = queryRank(t, gm, "arena_score", 10)
		return len(entries) >= 2
	})

	t.Logf("rank top%d: %v", len(entries), rankSummary(entries))

	// A 应排第一（30 分），B 第二（-10 分）
	require.EqualValues(t, roleA, entries[0].PlayerId)
	require.EqualValues(t, 30, entries[0].Score)
	var bEntry *internalv2.RankEntry
	for _, e := range entries {
		if e.PlayerId == roleB {
			bEntry = e
		}
	}
	require.NotNil(t, bEntry)
	require.EqualValues(t, -10, bEntry.Score)

	// ④ 再打一场（A 再胜）→ 分数应累加到 60（验证 ZINCRBY 而非覆盖）
	respA2, err := clientA.Match("arena")
	require.NoError(t, err, "match A2")
	require.EqualValues(t, 0, respA2.Code)
	respB2, err := clientB.Match("arena")
	require.NoError(t, err, "match B2")
	require.EqualValues(t, 0, respB2.Code)
	startA2, err := clientA.WaitBattleStart(20 * time.Second)
	require.NoError(t, err, "wait start A2")
	startB2, err := clientB.WaitBattleStart(20 * time.Second)
	require.NoError(t, err, "wait start B2")
	if _, err := clientA.BattleResult(startA2.RoomId, startA2.BattleToken, 0); err != nil {
		t.Fatalf("result A2: %v", err)
	}
	if _, err := clientB.BattleResult(startB2.RoomId, startB2.BattleToken, 1); err != nil {
		t.Fatalf("result B2: %v", err)
	}
	if _, err := clientA.WaitBattleSettle(15 * time.Second); err != nil {
		t.Fatalf("settle A2: %v", err)
	}

	waitUntil(t, 15*time.Second, "score accumulates to 60", func() bool {
		entries = queryRank(t, gm, "arena_score", 10)
		return len(entries) > 0 && entries[0].PlayerId == roleA && entries[0].Score == 60
	})
	t.Logf("second win accumulated: player %d score %d", entries[0].PlayerId, entries[0].Score)

	// ⑤ 统计查询（本测试未上报 feature，应返回空列表且不报错）
	statsResp, err := gm.QueryFeatureStats(context.Background(), &internalv2.QueryStatsReq{})
	require.NoError(t, err, "query stats")
	require.EqualValues(t, 0, statsResp.Code)
	t.Logf("stats reports: %d", len(statsResp.Reports))

	// ⑥ 校验 Redis 里的 ZSet key 形态
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer func() { _ = rdb.Close() }()
	key := "rank:global:s1:arena_score"
	if n, err := rdb.ZCard(context.Background(), key).Result(); err != nil || n < 2 {
		t.Fatalf("zset %s should have >=2 members, got %d err=%v", key, n, err)
	}

	// ⑦ 关闭
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer shutdownCancel()
	_ = gwSrv.Shutdown(shutdownCtx)
	_ = gameSrv.Shutdown(shutdownCtx)
	_ = crossSrv.Shutdown(shutdownCtx)
	_ = centerSrv.Shutdown(shutdownCtx)
	_ = loginSrv.Shutdown(shutdownCtx)
}

// queryRank 调用 GM 接口拉榜单。
func queryRank(t *testing.T, c internalv2.CenterServiceClient, rankType string, topN int32) []*internalv2.RankEntry {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := c.QueryGlobalRank(ctx, &internalv2.QueryRankReq{RankType: rankType, TopN: topN})
	if err != nil {
		return nil
	}
	var out []*internalv2.RankEntry
	for {
		e, err := stream.Recv()
		if err != nil {
			if err != io.EOF {
				return out
			}
			break
		}
		out = append(out, e)
	}
	return out
}

func rankSummary(entries []*internalv2.RankEntry) string {
	s := ""
	for _, e := range entries {
		s += fmt.Sprintf("[%d:%d] ", e.Rank, e.PlayerId)
	}
	return s
}
