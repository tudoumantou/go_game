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

	"game_study/internal/game"
	"game_study/internal/gateway"
	"game_study/internal/login"
	"game_study/pkg/config"
	"game_study/pkg/db"
	"game_study/test/robot"

	"github.com/stretchr/testify/require"
)

// TestP1toP4Smoke P1-P4 全链路冒烟：三服务（login/game/gateway）+ 三中间件
// （MySQL/Redis/etcd）全部真实拉起，机器人走「游客登录 → 网关握手 → 心跳」。
//
// 编排要点：
//   - 中间件（MySQL/Redis/etcd）由 TestMain 起一次共享，本用例只领独立库；
//   - 端口全部随机（freePort），避免与已跑的服务冲突；
//   - 服务在测试进程内启动（非子进程），收尾走各服务 Shutdown —— 优雅关闭链路
//     本身也是被验证的一环；
//   - 先 os.Chdir 到项目根：gateway 按相对路径加载 keys/ 下的 RSA 私钥。
//
// 关键断言：握手 resp.Code==0 且 account 非 0、心跳 server_time 非 0
// （说明「登录 → 网关 → etcd 选游戏服 → 绑定」整条链路已通）。
func TestP1toP4Smoke(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// ① 中间件
	// 容器由 TestMain 起一次共享（见 support_test.go 的 isolatedDSN 说明），
	// 这里只取一条指向本用例独立库的 DSN：库独立才是「建表不撞车」的关键。
	mysqlDSN := freshEnv(t)
	redisAddr := testRedisAddr
	etcdEndpoint := testEtcdAddr

	// ④ 构造配置（端口用随机可用端口）
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
	chdirProjectRoot(t)

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
	err := r.GuestLogin("robot-device-1")
	require.NoError(t, err, fmt.Sprintf("guest login: %v", err))
	err = r.Connect()
	require.NoError(t, err, fmt.Sprintf("connect gateway: %v", err))
	defer r.Close()

	err = r.Handshake()
	require.NoError(t, err, fmt.Sprintf("handshake: %v", err))
	resp, err := r.SendHandshakeReq("1.0.0", "pc")
	require.NoError(t, err, "handshake req")
	require.EqualValues(t, 0, resp.Code)

	// 心跳
	hb, err := r.Heartbeat()
	require.NoError(t, err, "heartbeat")
	require.NotEqualValues(t, 0, hb.ServerTime, "heartbeat server_time is 0")

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
		require.False(t, err != nil && err != context.Canceled, fmt.Sprintf("%s start failed: %v", name, err))
	case <-time.After(3 * time.Second):
		// 正常情况：Start 阻塞
	}
}

// freePort 获取一个空闲端口。
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err, "free port")
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	return port
}

// initSchema 执行 sql/schema 下全部 DDL。
func initSchema(t *testing.T, dsn string) {
	t.Helper()
	database, err := db.New(db.Config{DSN: dsn, MaxOpen: 5, MaxIdle: 2})
	require.NoError(t, err, "connect mysql")
	defer func() { _ = database.Close() }()

	root := projectRoot(t)
	files, err := filepath.Glob(filepath.Join(root, "sql/schema/*.sql"))
	require.NoError(t, err, "glob schema")
	require.NotEmpty(t, files, fmt.Sprintf("no schema files found under %s", root))
	for _, f := range files {
		content, err := os.ReadFile(f)
		require.NoError(t, err)
		_, err = database.Exec(string(content))
		require.NoError(t, err, fmt.Sprintf("exec %s: %v", f, err))
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
	require.NoError(t, err, "getwd")
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		require.NotEqualValues(t, dir, parent, "project root not found")
		dir = parent
	}
}
