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

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mysql"
	"github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/testcontainers/testcontainers-go/wait"
)

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
			WaitingFor: wait.ForLog("ready to serve client requests").WithStartupTimeout(3 * time.Minute),
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

	// 数据库句柄（造邮件用）
	database, err := db.New(db.Config{DSN: mysqlDSN, MaxOpen: 10, MaxIdle: 3})
	if err != nil {
		t.Fatalf("connect mysql: %v", err)
	}
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
	if err != nil {
		t.Fatal(err)
	}
	privatePath := filepath.Join(keyDir, "gateway_rsa.pem")
	if err := os.WriteFile(privatePath, pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}), 0600); err != nil {
		t.Fatal(err)
	}
	publicDER, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(keyDir, "gateway_rsa_pub.pem"), pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: publicDER}), 0644); err != nil {
		t.Fatal(err)
	}
	gwCfg.Crypto.RSAPrivateKey = privatePath

	gameCfg := *baseCfg
	gameCfg.Server.Name = "game-p6"
	gameCfg.Server.GRPCAddr = fmt.Sprintf("127.0.0.1:%d", gameGRPCPort)

	if err := os.Chdir(projectRoot(t)); err != nil {
		t.Fatalf("chdir: %v", err)
	}

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
	if err := r.GuestLogin(fmt.Sprintf("p6-device-%d", time.Now().UnixNano())); err != nil {
		t.Fatalf("guest login: %v", err)
	}
	if err := r.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer r.Close()
	if err := r.Handshake(); err != nil {
		t.Fatalf("handshake: %v", err)
	}
	hs, err := r.SendHandshakeReq("1.0.0", "pc")
	if err != nil {
		t.Fatalf("handshake req: %v", err)
	}
	if hs.Code != 0 || hs.AccountId == 0 {
		t.Fatalf("handshake resp code=%d account=%d", hs.Code, hs.AccountId)
	}

	// 无角色 → 2003
	eg, err := r.EnterGame()
	if err != nil {
		t.Fatalf("enter game: %v", err)
	}
	if eg.Code != 2003 {
		t.Fatalf("expect 2003 for empty account, got %d", eg.Code)
	}

	// ⑤ 创角
	roleName := fmt.Sprintf("勇士%d", time.Now().UnixNano()%1000000)
	cr, err := r.CreateRole(roleName, 1)
	if err != nil {
		t.Fatalf("create role: %v", err)
	}
	if cr.Code != 0 || cr.RoleId == 0 {
		t.Fatalf("create role resp code=%d role=%d", cr.Code, cr.RoleId)
	}
	roleID := cr.RoleId

	// 重名 → 2004（换一个账号，避免同账号多角色限制先触发）
	r2 := robot.New(fmt.Sprintf("http://127.0.0.1:%d", loginPort), fmt.Sprintf("127.0.0.1:%d", gwPort))
	if err := r2.GuestLogin(fmt.Sprintf("p6-device2-%d", time.Now().UnixNano())); err != nil {
		t.Fatalf("guest login 2: %v", err)
	}
	if err := r2.Connect(); err != nil {
		t.Fatalf("connect 2: %v", err)
	}
	defer r2.Close()
	if err := r2.Handshake(); err != nil {
		t.Fatalf("handshake 2: %v", err)
	}
	if _, err := r2.SendHandshakeReq("1.0.0", "pc"); err != nil {
		t.Fatalf("handshake req 2: %v", err)
	}
	dup, err := r2.CreateRole(roleName, 1)
	if err != nil {
		t.Fatalf("create duplicate role: %v", err)
	}
	if dup.Code != 2004 {
		t.Fatalf("expect 2004 for duplicate name, got %d", dup.Code)
	}

	// ⑥ 玩家信息
	info, err := r.PlayerInfo()
	if err != nil {
		t.Fatalf("player info: %v", err)
	}
	if info.RoleId != roleID || info.Name != roleName || info.Level != 1 {
		t.Fatalf("player info mismatch: %+v", info)
	}
	if len(info.Attrs) != 5 || info.Attrs[2].Value != 50 { // atk=50（战士）
		t.Fatalf("attrs unexpected: %+v", info.Attrs)
	}

	// ⑦ 造一封带附件的邮件（模拟 Kafka gs.mail 投递结果）
	mailID := insertTestMail(t, database, roleID)

	list, err := r.MailList()
	if err != nil {
		t.Fatalf("mail list: %v", err)
	}
	if len(list.Mails) != 1 || !list.Mails[0].HasAttachment {
		t.Fatalf("mail list unexpected: %+v", list.Mails)
	}

	// ⑧ 领取附件
	take, err := r.TakeMail(mailID)
	if err != nil {
		t.Fatalf("take mail: %v", err)
	}
	if take.Code != 0 || len(take.Rewards) == 0 {
		t.Fatalf("take mail code=%d rewards=%d", take.Code, len(take.Rewards))
	}

	// 幂等：重复领取 → 5002
	take2, err := r.TakeMail(mailID)
	if err != nil {
		t.Fatalf("take mail again: %v", err)
	}
	if take2.Code != 5002 {
		t.Fatalf("expect 5002 for repeated take, got %d", take2.Code)
	}

	// ⑨ 背包
	bag, err := r.BagList()
	if err != nil {
		t.Fatalf("bag list: %v", err)
	}
	var diamond int32
	for _, it := range bag.Items {
		if it.ItemId == 1001 {
			diamond = it.Count
		}
	}
	if diamond != 50 {
		t.Fatalf("expect 50 diamonds from mail, got %d (%+v)", diamond, bag.Items)
	}

	// ⑩ 使用道具（1001 配置 Col3=1 → 单次上限 1 个）
	use, err := r.UseItem(1001, 10)
	if err != nil {
		t.Fatalf("use item: %v", err)
	}
	if use.Code != 0 || use.RemainCount != 49 {
		t.Fatalf("use item code=%d remain=%d", use.Code, use.RemainCount)
	}

	// ⑪ 优雅关闭
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
	if err != nil {
		t.Fatalf("insert mail: %v", err)
	}
	mailID, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("mail id: %v", err)
	}
	if _, err := database.Exec(
		`INSERT INTO mail_attachment (mail_id, item_id, count) VALUES (?, 1001, 50)`, mailID); err != nil {
		t.Fatalf("insert attachment: %v", err)
	}
	return mailID
}
