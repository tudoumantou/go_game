//go:build integration

// test/integration/p12_preload_test.go —— P12 登录预加载
//
// 验证三件事（设计文档 12.6）：
//  1. 预加载把「角色 + 属性 + 背包 + 装备 + 活动」并行拉全，模块 OnLogin 不再回源：
//     已穿装备的属性加成在登录后立即可见（说明用的是预加载快照，不是事后补的）；
//  2. 登录活动进度在预加载基础上 +1；
//  3. 离线暂存队列（pend:chat:*）在登录后被 drain 并补投给客户端。
package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"game_study/internal/game"
	"game_study/internal/gateway"
	"game_study/internal/login"
	"game_study/pkg/config"
	"game_study/pkg/db"
	pkgmq "game_study/pkg/mq"
	"game_study/test/robot"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mysql"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestP12LoginPreload(t *testing.T) {
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

	initSchema(t, mysqlDSN)

	database, err := db.New(db.Config{DSN: mysqlDSN, MaxOpen: 10, MaxIdle: 3})
	require.NoError(t, err, "connect mysql")
	defer func() { _ = database.Close() }()

	loginPort := freePort(t)
	gwPort := freePort(t)
	gwHTTPPort := freePort(t)
	gameGRPCPort := freePort(t)

	baseCfg := &config.Config{}
	baseCfg.MySQL.DSN = mysqlDSN
	baseCfg.MySQL.MaxOpenConns = 20
	baseCfg.MySQL.MaxIdleConns = 5
	baseCfg.Redis.Addrs = []string{redisAddr}
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
	baseCfg.Server.MaxConns = 100

	loginCfg := *baseCfg
	loginCfg.Server.Name = "login-p12"
	loginCfg.Server.HTTPAddr = fmt.Sprintf("127.0.0.1:%d", loginPort)
	loginCfg.Token.TTL = "24h"

	gwCfg := *baseCfg
	gwCfg.Server.Name = "gateway-p12"
	gwCfg.Server.TCPAddr = fmt.Sprintf("127.0.0.1:%d", gwPort)
	gwCfg.Server.HTTPAddr = fmt.Sprintf("127.0.0.1:%d", gwHTTPPort)

	gameCfg := *baseCfg
	gameCfg.Server.Name = "game-p12"
	gameCfg.Server.GRPCAddr = fmt.Sprintf("127.0.0.1:%d", gameGRPCPort)

	if err := os.Chdir(projectRoot(t)); err != nil {
		t.Fatalf("chdir: %v", err)
	}

	svcCtx, svcCancel := context.WithCancel(ctx)
	defer svcCancel()

	loginSrv := login.New(&loginCfg)
	startAndWait(t, "login", func() error { return loginSrv.Start(svcCtx) })
	gameSrv := game.New(&gameCfg)
	startAndWait(t, "game", func() error { return gameSrv.Start(svcCtx) })
	gwSrv := gateway.New(&gwCfg)
	startAndWait(t, "gateway", func() error { return gwSrv.Start(svcCtx) })
	time.Sleep(2 * time.Second)

	loginURL := fmt.Sprintf("http://127.0.0.1:%d", loginPort)
	gwAddr := fmt.Sprintf("127.0.0.1:%d", gwPort)

	// ① 用固定 device_id 登录并创角（后续重连必须用同一 device 才能命中同一账号）
	deviceID := fmt.Sprintf("p12-device-%d", time.Now().UnixNano())
	roleID, client1 := createRoleWithDevice(t, loginURL, gwAddr, deviceID, "预载者")

	// ② 落「已穿装备」+「活动记录」到库，模拟老角色数据
	//    战士基础 atk=50；新手剑(3001) 加成 Col4=10 → 预加载后 atk 应为 60
	if _, err := database.Exec(
		`INSERT INTO player_equip (player_id, equip_id, enhance_level, quality, equipped) VALUES (?, 3001, 0, 1, 1)`,
		roleID); err != nil {
		t.Fatalf("seed equip: %v", err)
	}
	if _, err := database.Exec(
		`INSERT INTO activity_record (player_id, activity_id, progress, reward_mask, joined_at)
		 VALUES (?, 1, 2, 0, NOW())
		 ON DUPLICATE KEY UPDATE progress = 2`,
		roleID); err != nil {
		t.Fatalf("seed activity: %v", err)
	}

	// ③ 往离线暂存队列塞一条私聊（模拟离线期间别的服发来的消息）
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer func() { _ = rdb.Close() }()

	offlineMsg := &pkgmq.ChatCrossMsg{
		MsgID:    987654321,
		FromID:   999001,
		FromName: "离线发送者",
		ToID:     roleID,
		Channel:  2,
		Content:  "你不在的时候我发了一条",
		SendTime: time.Now().UnixMilli(),
	}
	raw, err := json.Marshal(offlineMsg)
	require.NoError(t, err, "marshal offline msg")
	pendKey := fmt.Sprintf("pend:chat:%d", roleID)
	if err := rdb.RPush(context.Background(), pendKey, raw).Err(); err != nil {
		t.Fatalf("push pend: %v", err)
	}

	// ④ 断开 → 用同一 device 重连（触发一次完整预加载）
	client1.Close()
	time.Sleep(500 * time.Millisecond)

	client2 := robot.New(loginURL, gwAddr)
	if err := client2.GuestLogin(deviceID); err != nil {
		t.Fatalf("relogin: %v", err)
	}
	if err := client2.Connect(); err != nil {
		t.Fatalf("reconnect: %v", err)
	}
	defer client2.Close()
	if err := client2.Handshake(); err != nil {
		t.Fatalf("rehandshake: %v", err)
	}
	if _, err := client2.SendHandshakeReq("1.0.0", "pc"); err != nil {
		t.Fatalf("rehandshake req: %v", err)
	}
	eg, err := client2.EnterGame()
	require.NoError(t, err, "enter game")
	if eg.RoleId != roleID {
		t.Fatalf("expect same role %d on reconnect, got %d", roleID, eg.RoleId)
	}

	// ⑤ 校验装备加成（预加载路径生效）
	info, err := client2.PlayerInfo()
	require.NoError(t, err, "player info")
	var atk int64
	for _, a := range info.Attrs {
		if a.AttrId == 3 { // atk
			atk = a.Value
		}
	}
	if atk != 60 {
		t.Fatalf("atk should be 60 (base 50 + sword 10 from preload), got %d", atk)
	}
	t.Logf("装备加成生效：atk=%d", atk)

	// ⑥ 校验登录活动进度推进（预置 2，登录后 ≥3）
	var progress int32
	if err := database.QueryRow(
		`SELECT progress FROM activity_record WHERE player_id = ? AND activity_id = 1`, roleID).Scan(&progress); err != nil {
		t.Fatalf("query activity progress: %v", err)
	}
	if progress < 3 {
		t.Fatalf("login activity progress should increase, got %d", progress)
	}
	t.Logf("活动进度推进到 %d", progress)

	// ⑦ 校验离线暂存被 drain 并补投
	chat, err := client2.WaitChatNotify(10 * time.Second)
	require.NoError(t, err, "expect offline chat redelivered")
	if chat.Content != "你不在的时候我发了一条" {
		t.Fatalf("offline chat content mismatch: %q", chat.Content)
	}
	if chat.SenderName != "离线发送者" {
		t.Fatalf("offline chat sender mismatch: %q", chat.SenderName)
	}
	t.Logf("离线消息已补投：msg_id=%s from=%s", chat.MsgId, chat.SenderName)

	// 队列应已清空
	if n, err := rdb.LLen(context.Background(), pendKey).Result(); err != nil || n != 0 {
		t.Fatalf("pend queue should be drained, len=%d err=%v", n, err)
	}

	// ⑧ 关闭
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()
	_ = gwSrv.Shutdown(shutdownCtx)
	_ = gameSrv.Shutdown(shutdownCtx)
	_ = loginSrv.Shutdown(shutdownCtx)
}

// createRoleWithDevice 用指定 device_id 登录并创角。
func createRoleWithDevice(t *testing.T, loginURL, gwAddr, deviceID, namePrefix string) (int64, *robot.Client) {
	t.Helper()
	c := robot.New(loginURL, gwAddr)
	if err := c.GuestLogin(deviceID); err != nil {
		t.Fatalf("%s guest login: %v", namePrefix, err)
	}
	if err := c.Connect(); err != nil {
		t.Fatalf("%s connect: %v", namePrefix, err)
	}
	if err := c.Handshake(); err != nil {
		t.Fatalf("%s handshake: %v", namePrefix, err)
	}
	if _, err := c.SendHandshakeReq("1.0.0", "pc"); err != nil {
		t.Fatalf("%s handshake req: %v", namePrefix, err)
	}
	roleName := fmt.Sprintf("%s%d", namePrefix, time.Now().UnixNano()%1000000)
	cr, err := c.CreateRole(roleName, 1)
	if err != nil {
		t.Fatalf("%s create role: %v", namePrefix, err)
	}
	if cr.Code != 0 || cr.RoleId == 0 {
		t.Fatalf("%s create role code=%d role=%d", namePrefix, cr.Code, cr.RoleId)
	}
	return cr.RoleId, c
}
