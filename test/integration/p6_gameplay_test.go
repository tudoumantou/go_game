//go:build integration

// test/integration/p6_gameplay_test.go —— P6 业务闭环：创角 → 进游戏 → 邮件领取 → 背包 → 使用道具
package integration

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"game_study/internal/game"
	"game_study/internal/gateway"
	"game_study/internal/login"
	"game_study/pkg/config"
	"game_study/pkg/db"
	"game_study/test/robot"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mysql"
	"github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/testcontainers/testcontainers-go/wait"
)

// TestP6GameplayFlow P6 业务闭环（第一批）：创角 → 进游戏 → 邮件领取 → 背包增量
// → 使用道具 → 场景移动 → 榜单降级语义。
//
// 编排：三服务 + 三中间件（testcontainers）；网关 RSA 密钥在临时目录自签生成，
// 不依赖开发机 keys/，也不与并行用例抢同一把私钥。带附件邮件直接落库构造，
// 模拟 Kafka gs.mail 的投递结果，避免为一条邮件拉起整条 MQ 链路。
//
// 关键断言：
//   - 空账号进游戏 2003、创角后 role_id 非 0、重名 2004；
//   - 领附件后钻石恰好 +50（以登录礼包基线做增量，避免绝对值抖动）；
//   - 重复领取 5002（幂等）；使用道具受配置单次上限钳制；
//   - 越界坐标钳到地图上界(99)、跨图瞬移 4004 并回权威坐标、离开后同步 4005；
//   - 未起中心服时榜单回 6702 且 entries 为空 —— 必须区分「暂不可用」与「空榜」，
//     否则玩家会误以为自己没有成绩（R8.1/R11.1）。
func TestP6GameplayFlow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// ① 中间件
	mysqlC, err := mysql.Run(ctx, "mysql:8.0",
		testcontainers.WithWaitStrategyAndDeadline(3*time.Minute, wait.ForLog("port: 3306  MySQL Community Server").WithStartupTimeout(3*time.Minute)),
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
			WaitingFor: wait.ForLog("ready to serve client requests").WithStartupTimeout(3 * time.Minute),
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

	// 数据库句柄（造邮件用）
	database, err := db.New(db.Config{DSN: mysqlDSN, MaxOpen: 10, MaxIdle: 3})
	require.NoError(t, err, "connect mysql")
	defer func() { _ = database.Close() }()

	// ② 配置
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

	loginCfg := *baseCfg
	loginCfg.Server.Name = "login-p6"
	loginCfg.Server.HTTPAddr = fmt.Sprintf("127.0.0.1:%d", loginPort)
	loginCfg.Token.TTL = "24h"

	gwCfg := *baseCfg
	gwCfg.Server.Name = "gateway-p6"
	gwCfg.Server.TCPAddr = fmt.Sprintf("127.0.0.1:%d", gwPort)
	gwCfg.Server.HTTPAddr = fmt.Sprintf("127.0.0.1:%d", gwHTTPPort)
	// 使用独立临时密钥，不依赖开发机的 keys/ 或已有部署私钥。
	keyDir := t.TempDir()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	privatePath := filepath.Join(keyDir, "gateway_rsa.pem")
	err = os.WriteFile(privatePath, pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}), 0600)
	require.NoError(t, err)
	publicDER, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	require.NoError(t, err)
	err = os.WriteFile(filepath.Join(keyDir, "gateway_rsa_pub.pem"), pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: publicDER}), 0644)
	require.NoError(t, err)
	gwCfg.Crypto.RSAPrivateKey = privatePath

	gameCfg := *baseCfg
	gameCfg.Server.Name = "game-p6"
	gameCfg.Server.GRPCAddr = fmt.Sprintf("127.0.0.1:%d", gameGRPCPort)

	err = os.Chdir(projectRoot(t))
	require.NoError(t, err, fmt.Sprintf("chdir: %v", err))

	// ③ 起服务
	svcCtx, svcCancel := context.WithCancel(ctx)
	defer svcCancel()

	loginSrv := login.New(&loginCfg)
	startAndWait(t, "login", func() error { return loginSrv.Start(svcCtx) })
	gameSrv := game.New(&gameCfg)
	startAndWait(t, "game", func() error { return gameSrv.Start(svcCtx) })
	gwSrv := gateway.New(&gwCfg)
	startAndWait(t, "gateway", func() error { return gwSrv.Start(svcCtx) })
	time.Sleep(2 * time.Second)

	// ④ 机器人：登录 → 握手 → 进游戏（无角色）
	r := robot.New(fmt.Sprintf("http://127.0.0.1:%d", loginPort), fmt.Sprintf("127.0.0.1:%d", gwPort))
	err = r.GuestLogin(fmt.Sprintf("p6-device-%d", time.Now().UnixNano()))
	require.NoError(t, err, fmt.Sprintf("guest login: %v", err))
	err = r.Connect()
	require.NoError(t, err, fmt.Sprintf("connect: %v", err))
	defer r.Close()
	err = r.Handshake()
	require.NoError(t, err, fmt.Sprintf("handshake: %v", err))
	hs, err := r.SendHandshakeReq("1.0.0", "pc")
	require.NoError(t, err, "handshake req")
	require.EqualValues(t, 0, hs.Code, fmt.Sprintf("handshake resp code=%d account=%d", hs.Code, hs.AccountId))
	require.NotEqualValues(t, 0, hs.AccountId)

	// 无角色 → 2003
	eg, err := r.EnterGame()
	require.NoError(t, err, "enter game")
	require.EqualValues(t, 2003, eg.Code)

	// ⑤ 创角
	roleName := fmt.Sprintf("勇士%d", time.Now().UnixNano()%1000000)
	cr, err := r.CreateRole(roleName, 1)
	require.NoError(t, err, "create role")
	require.EqualValues(t, 0, cr.Code, fmt.Sprintf("create role resp code=%d role=%d", cr.Code, cr.RoleId))
	require.NotEqualValues(t, 0, cr.RoleId)
	roleID := cr.RoleId

	// 重名 → 2004（换一个账号，避免同账号多角色限制先触发）
	r2 := robot.New(fmt.Sprintf("http://127.0.0.1:%d", loginPort), fmt.Sprintf("127.0.0.1:%d", gwPort))
	err = r2.GuestLogin(fmt.Sprintf("p6-device2-%d", time.Now().UnixNano()))
	require.NoError(t, err, fmt.Sprintf("guest login 2: %v", err))
	err = r2.Connect()
	require.NoError(t, err, fmt.Sprintf("connect 2: %v", err))
	defer r2.Close()
	err = r2.Handshake()
	require.NoError(t, err, fmt.Sprintf("handshake 2: %v", err))
	_, err = r2.SendHandshakeReq("1.0.0", "pc")
	require.NoError(t, err, fmt.Sprintf("handshake req 2: %v", err))
	dup, err := r2.CreateRole(roleName, 1)
	require.NoError(t, err, "create duplicate role")
	require.EqualValues(t, 2004, dup.Code)

	// ⑥ 玩家信息
	info, err := r.PlayerInfo()
	require.NoError(t, err, "player info")
	require.EqualValues(t, roleID, info.RoleId, fmt.Sprintf("player info mismatch: %+v", info))
	require.EqualValues(t, roleName, info.Name)
	require.EqualValues(t, 1, info.Level)
	if len(info.Attrs) != 5 || info.Attrs[2].Value != 50 { // atk=50（战士）
		t.Fatalf("attrs unexpected: %+v", info.Attrs)
	}

	// 登录礼包会在进游戏时先发一笔钻石，这里记录基线，后续按增量断言
	bagBase, err := r.BagList()
	require.NoError(t, err, "bag list (baseline)")
	var diamondBase int32
	for _, it := range bagBase.Items {
		if it.ItemId == 1001 {
			diamondBase = it.Count
		}
	}

	// ⑦ 造一封带附件的邮件（模拟 Kafka gs.mail 投递结果）
	mailID := insertTestMail(t, database, roleID)

	list, err := r.MailList()
	require.NoError(t, err, "mail list")
	require.False(t, len(list.Mails) != 1 || !list.Mails[0].HasAttachment, fmt.Sprintf("mail list unexpected: %+v", list.Mails))

	// ⑧ 领取附件
	take, err := r.TakeMail(mailID)
	require.NoError(t, err, "take mail")
	require.EqualValues(t, 0, take.Code, fmt.Sprintf("take mail code=%d rewards=%d", take.Code, len(take.Rewards)))
	require.NotEmpty(t, take.Rewards)

	// 幂等：重复领取 → 5002
	take2, err := r.TakeMail(mailID)
	require.NoError(t, err, "take mail again")
	require.EqualValues(t, 5002, take2.Code)

	// ⑨ 背包
	bag, err := r.BagList()
	require.NoError(t, err, "bag list")
	var diamond int32
	for _, it := range bag.Items {
		if it.ItemId == 1001 {
			diamond = it.Count
		}
	}
	require.EqualValues(t, diamondBase+50, diamond)

	// ⑩ 使用道具（1001 配置 Col3=1 → 单次上限 1 个）
	use, err := r.UseItem(1001, 10)
	require.NoError(t, err, "use item")
	require.EqualValues(t, 0, use.Code, fmt.Sprintf("use item code=%d remain=%d (base %d)", use.Code, use.RemainCount, diamondBase))
	require.EqualValues(t, diamondBase+49, use.RemainCount)

	// ⑪ 场景：越界钳制 → 近距离移动成功 → 瞬移被拒 → 离开后同步报「不在场景」
	sceneEnt, err := r.SceneEnter(1, 150, 150)
	require.NoError(t, err, "scene enter")
	require.EqualValues(t, 0, sceneEnt.Code)
	// 地图 100x100，坐标上界是开区间：150 必须钳制到 99。
	require.EqualValues(t, 99, sceneEnt.X, fmt.Sprintf("scene enter clamped to (%d,%d), want (99,99)", sceneEnt.X, sceneEnt.Y))
	require.EqualValues(t, 99, sceneEnt.Y)

	nearMove, err := r.SceneMove(97, 99, 1)
	require.NoError(t, err, "scene move")
	require.EqualValues(t, 0, nearMove.Code, fmt.Sprintf("near move = %+v", nearMove))
	require.EqualValues(t, 97, nearMove.X)

	// 跨地图瞬移：超出「距上次移动时间 × 速度上限」，应被拒并回权威坐标。
	farMove, err := r.SceneMove(10, 10, 2)
	require.NoError(t, err, "scene move (far)")
	require.EqualValues(t, 4004, farMove.Code)
	require.EqualValues(t, 97, farMove.X, fmt.Sprintf("far move authority = (%d,%d), want (97,99)", farMove.X, farMove.Y))
	require.EqualValues(t, 99, farMove.Y)

	_, err = r.SceneLeave()
	require.NoError(t, err, fmt.Sprintf("scene leave: %v", err))
	syncResp, err := r.SceneSync(1)
	require.NoError(t, err, "scene sync after leave")
	require.EqualValues(t, 4005, syncResp.Code)

	// ⑫ 榜单查询：本用例没有启动中心服，必须回「暂不可用」（6702）而不是空榜。
	// 空榜会让玩家误以为自己没有成绩，是列表类协议最容易做错的语义（R8.1/R11.1）。
	rank, err := r.RankList("arena_score")
	require.NoError(t, err, "rank list")
	require.EqualValues(t, 6702, rank.Code)
	require.Len(t, rank.Entries, 0)

	// ⑬ 优雅关闭
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()
	_ = gwSrv.Shutdown(shutdownCtx)
	_ = gameSrv.Shutdown(shutdownCtx)
	_ = loginSrv.Shutdown(shutdownCtx)
}

// insertTestMail 直接落库一封带附件邮件。
func insertTestMail(t *testing.T, database *db.DB, playerID int64) int64 {
	t.Helper()
	res, err := database.Exec(
		`INSERT INTO mail (player_id, template_id, title, content, channel, status) VALUES (?, 1, '系统补偿', '附件请查收', 0, 0)`,
		playerID)
	require.NoError(t, err, "insert mail")
	mailID, err := res.LastInsertId()
	require.NoError(t, err, "mail id")
	if _, err := database.Exec(
		`INSERT INTO mail_attachment (mail_id, item_id, count) VALUES (?, 1001, 50)`, mailID); err != nil {
		t.Fatalf("insert attachment: %v", err)
	}
	return mailID
}
