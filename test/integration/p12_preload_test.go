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
	"github.com/redis/go-redis/v9"
	"testing"
	"time"

	"game_study/internal/game"
	"game_study/internal/gateway"
	"game_study/internal/login"
	"game_study/pkg/config"
	"game_study/pkg/db"
	pkgmq "game_study/pkg/mq"
	"game_study/test/robot"

	"github.com/stretchr/testify/require"
)

func TestP12LoginPreload(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	// 容器由 TestMain 起一次共享（见 support_test.go 的 isolatedDSN 说明），
	// 这里只取一条指向本用例独立库的 DSN：库独立才是「建表不撞车」的关键。
	mysqlDSN := freshEnv(t)
	redisAddr := testRedisAddr
	etcdEndpoint := testEtcdAddr

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
	baseCfg.Etcd.Endpoints = []string{etcdEndpoint}
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

	chdirProjectRoot(t)

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
	// equip 已改为整份 JSON 落库（player_equip.equips），不再是一行一件。
	const wornSword = `[{"uid":1,"equip_id":3001,"enhance_level":0,"quality":1,"equipped":true}]`
	if _, err := database.Exec(
		`INSERT INTO player_equip (player_id, equips) VALUES (?, ?)`, roleID, wornSword); err != nil {
		t.Fatalf("seed equip: %v", err)
	}
	// 活动状态同样整份 JSON 落库（activity_record.records）。
	const activityRec = `{"1":{"progress":2,"reward_mask":0}}`
	if _, err := database.Exec(
		`INSERT INTO activity_record (player_id, records) VALUES (?, ?)
		 ON DUPLICATE KEY UPDATE records = VALUES(records)`,
		roleID, activityRec); err != nil {
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
	err = rdb.RPush(context.Background(), pendKey, raw).Err()
	require.NoError(t, err, fmt.Sprintf("push pend: %v", err))

	// ④ 断开 → 用同一 device 重连（触发一次完整预加载）
	client1.Close()
	time.Sleep(500 * time.Millisecond)

	client2 := robot.New(loginURL, gwAddr)
	err = client2.GuestLogin(deviceID)
	require.NoError(t, err, fmt.Sprintf("relogin: %v", err))
	err = client2.Connect()
	require.NoError(t, err, fmt.Sprintf("reconnect: %v", err))
	defer client2.Close()
	err = client2.Handshake()
	require.NoError(t, err, fmt.Sprintf("rehandshake: %v", err))
	_, err = client2.SendHandshakeReq("1.0.0", "pc")
	require.NoError(t, err, fmt.Sprintf("rehandshake req: %v", err))
	eg, err := client2.EnterGame()
	require.NoError(t, err, "enter game")
	require.EqualValues(t, roleID, eg.RoleId)

	// ⑤ 校验装备加成（预加载路径生效）
	info, err := client2.PlayerInfo()
	require.NoError(t, err, "player info")
	var atk int64
	for _, a := range info.Attrs {
		if a.AttrId == 3 { // atk
			atk = a.Value
		}
	}
	require.EqualValues(t, 60, atk)
	t.Logf("装备加成生效：atk=%d", atk)

	// ⑥ 校验登录活动进度推进（预置 2，登录后 ≥3）
	// activity_record 是整份 JSON（{"<活动ID>":{progress,reward_mask}}），
	// 读出来在 Go 侧解析，SQL 里没有 progress 列。
	//
	// 必须用 Eventually：登录推进只改内存快照，落库由 write-behind 异步完成，
	// 登录后立即查库读到的是上一轮落库的值。
	var progress int32
	require.Eventually(t, func() bool {
		var records string
		if err := database.QueryRow(
			`SELECT records FROM activity_record WHERE player_id = ?`, roleID).Scan(&records); err != nil {
			return false
		}
		var decoded map[string]struct {
			Progress int32 `json:"progress"`
		}
		if err := json.Unmarshal([]byte(records), &decoded); err != nil {
			return false
		}
		progress = decoded["1"].Progress
		return progress >= 3
	}, 10*time.Second, 100*time.Millisecond, "login activity progress should increase past the seeded 2")
	t.Logf("活动进度推进到 %d", progress)

	// ⑦ 校验离线暂存被 drain 并补投
	chat, err := client2.WaitChatNotify(10 * time.Second)
	require.NoError(t, err, "expect offline chat redelivered")
	require.EqualValues(t, "你不在的时候我发了一条", chat.Content)
	require.EqualValues(t, "离线发送者", chat.SenderName)
	t.Logf("离线消息已补投：msg_id=%s from=%s", chat.MsgId, chat.SenderName)

	// 队列应已清空
	n, err := rdb.LLen(context.Background(), pendKey).Result()
	require.NoError(t, err, fmt.Sprintf("pend queue should be drained, len=%d err=%v", n, err))
	require.EqualValues(t, 0, n)

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
	err := c.GuestLogin(deviceID)
	require.NoError(t, err, fmt.Sprintf("%s guest login: %v", namePrefix, err))
	err = c.Connect()
	require.NoError(t, err, fmt.Sprintf("%s connect: %v", namePrefix, err))
	err = c.Handshake()
	require.NoError(t, err, fmt.Sprintf("%s handshake: %v", namePrefix, err))
	_, err = c.SendHandshakeReq("1.0.0", "pc")
	require.NoError(t, err, fmt.Sprintf("%s handshake req: %v", namePrefix, err))
	roleName := fmt.Sprintf("%s%d", namePrefix, time.Now().UnixNano()%1000000)
	cr, err := c.CreateRole(roleName, 1)
	require.NoError(t, err)
	require.EqualValues(t, 0, cr.Code, fmt.Sprintf("%s create role code=%d role=%d", namePrefix, cr.Code, cr.RoleId))
	require.NotEqualValues(t, 0, cr.RoleId)
	return cr.RoleId, c
}
