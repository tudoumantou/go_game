//go:build integration

// Package integration 主链路集成测试（设计文档 23.3）
//
// 运行方式（需本机 Docker）：
//
//	go test -tags=integration ./test/integration/ -v -count=1
//
// 环境：TestMain 用 testcontainers 起**一套**真实 MySQL 8.0 + Redis 7 + etcd，
// 全包 30 个用例共用；每个用例通过 freshEnv 拿到「独立数据库 + 清空 Redis/etcd」。
//
// 为什么不每用例起一套：MySQL 冷启动 20~30 秒，10 个重型用例就是 250 秒纯等待，
// CI 上还会把 docker daemon 拖到不响应（exec inspect 超时），表现为「etcd 起不
// 来」的假故障。容器共享后整包从超时打满（>15min）降到约 7 分钟。
//
// P0 覆盖范围（骨架期）：容器环境 → 建表 → 灌数 → CRUD 闭环 → 幂等防线。
// 模块级主链路（登录→进游戏→聊天→跨服战斗，23.3 的 6 步）待对应模块落地后
// 逐段启用，当前以 t.Skip 标注 TODO(P2)，避免空转失败。
package integration

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql" // 注册 driver + ParseDSN
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mysql"
	"github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/testcontainers/testcontainers-go/wait"

	"game_study/pkg/model"
	"github.com/stretchr/testify/require"
)

var (
	testDB        *sql.DB
	testDSN       string
	testRedisAddr string
	// testEtcdAddr 共享 etcd 的 client 地址（TestMain 起一次，
	// 由 p1_p4/p6b/p7/p8/p12/p13/p14 共用）。leader_test 例外：
	// 选举用例需要独占的 key 空间，它自己起。
	testEtcdAddr string

	// testRootDB root 连接（无默认库）：isolatedDSN 要给每个用例
	// CREATE DATABASE，而 testcontainers 的默认用户在初始化时只被授权了
	// MYSQL_DATABASE 指定的那一个库，建库会 Access denied。
	testRootDB *sql.DB
)

const (
	testDBName = "game_study"
	testDBUser = "root"
	testDBPass = "game_study"

	// testMySQLRootPass testcontainers mysql 模块的 root 密码。
	//
	// 模块的 ContainerRequest 只设 MYSQL_USER/MYSQL_PASSWORD/MYSQL_DATABASE
	// （默认都是 test），之后 WithDefaultCredentials 把 MYSQL_PASSWORD 复制给
	// MYSQL_ROOT_PASSWORD——所以 root 密码恒等于 MYSQL_PASSWORD 的默认值，
	// 而 CustomizeRequest 里设的 MYSQL_ROOT_PASSWORD 会被这步覆盖。
	//
	// 模块没导出这个常量，只能写死；升级模块改了默认值时，TestMain 的
	// root ping 会立刻报 1045，不会静默降级。
	testMySQLRootPass = "test"
)

// TestMain 起一套容器环境（23.3 第 1 步：起环境 → 建表 → 灌数）
func TestMain(m *testing.M) {
	ctx := context.Background()

	// --- MySQL ---
	mysqlC, err := mysql.Run(ctx, "mysql:8.0",
		testcontainers.WithWaitStrategyAndDeadline(3*time.Minute, wait.ForLog("port: 3306  MySQL Community Server").WithStartupTimeout(3*time.Minute)),
		testcontainers.CustomizeRequest(testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Env: map[string]string{
					"MYSQL_ROOT_PASSWORD": testDBPass,
					"MYSQL_DATABASE":      testDBName,
				},
			},
		}),
	)
	if err != nil {
		fmt.Fprintln(os.Stderr, "start mysql container:", err)
		os.Exit(1)
	}
	defer func() { _ = mysqlC.Terminate(context.Background()) }()

	// --- Redis ---
	redisC, err := redis.Run(ctx, "redis:7-alpine")
	if err != nil {
		fmt.Fprintln(os.Stderr, "start redis container:", err)
		os.Exit(1)
	}
	defer func() { _ = redisC.Terminate(context.Background()) }()
	host, err := redisC.Host(ctx)
	if err != nil {
		panic(err)
	}
	port, err := redisC.MappedPort(ctx, "6379/tcp")
	if err != nil {
		panic(err)
	}
	testRedisAddr = fmt.Sprintf("%s:%s", host, port.Port())

	// --- etcd ---
	//
	// 起一次给所有用例共用：etcd 单节点启动只要几秒，但 7 个用例各起一次
	// 就是几十秒的纯等待，且每个容器都要拉一遍镜像。
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
	if err != nil {
		fmt.Fprintln(os.Stderr, "start etcd container:", err)
		os.Exit(1)
	}
	defer func() { _ = etcdC.Terminate(context.Background()) }()
	etcdHost, err := etcdC.Host(ctx)
	if err != nil {
		panic(err)
	}
	etcdPort, err := etcdC.MappedPort(ctx, "2379/tcp")
	if err != nil {
		panic(err)
	}
	testEtcdAddr = fmt.Sprintf("%s:%s", etcdHost, etcdPort.Port())

	// 连接串（multiStatements 支持整文件执行 DDL；parseTime 支持时间列扫描）
	// 不写 loc：让 pkg/db.normalizeDSN 补 time_zone='+00:00'，与驱动默认的
	// loc=UTC 对齐。写 loc=Local 时，宿主是 +0800 而容器是 UTC，读出来的时间
	// 整整早 8 小时——那是测试自己造的偏差，不是产品问题，但会让本地跑出的
	// 「时间列差 8 小时」看起来像真 bug（owner_index_test 正是在测这件事）。
	dsn, err := mysqlC.ConnectionString(ctx, "multiStatements=true", "parseTime=true")
	if err != nil {
		fmt.Fprintln(os.Stderr, "mysql connection string:", err)
		os.Exit(1)
	}
	var db *sql.DB
	for i := 0; i < 30; i++ {
		db, err = sql.Open("mysql", dsn)
		if err == nil {
			err = db.Ping()
		}
		if err == nil {
			break
		}
		time.Sleep(time.Second)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "mysql not ready:", err)
		os.Exit(1)
	}
	testDB = db
	testDSN = dsn

	rootCfg, err := mysqlDriver.ParseDSN(dsn)
	if err != nil {
		fmt.Fprintln(os.Stderr, "parse dsn for root:", err)
		os.Exit(1)
	}
	rootCfg.User = "root"
	rootCfg.Passwd = testMySQLRootPass
	rootCfg.DBName = ""
	rootDB, err := sql.Open("mysql", rootCfg.FormatDSN())
	if err != nil {
		fmt.Fprintln(os.Stderr, "open root connection:", err)
		os.Exit(1)
	}
	if err := rootDB.Ping(); err != nil {
		fmt.Fprintln(os.Stderr, "root ping:", err)
		os.Exit(1)
	}
	testRootDB = rootDB
	defer func() { _ = db.Close() }()

	// 建表 + 灌数（19.5 清单第 4 步的容器等价物）
	if err := execSQLDir(ctx, db, "../../sql/schema"); err != nil {
		fmt.Fprintln(os.Stderr, "apply schema:", err)
		os.Exit(1)
	}
	if err := execSQLDir(ctx, db, "../../sql/seed"); err != nil {
		fmt.Fprintln(os.Stderr, "apply seed:", err)
		os.Exit(1)
	}

	os.Exit(m.Run())
}

// execSQLDir 顺序执行目录下全部 .sql（multiStatements=true）
func execSQLDir(ctx context.Context, db *sql.DB, dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			return err
		}
		if _, err := db.ExecContext(ctx, string(raw)); err != nil {
			return fmt.Errorf("%s: %w", e.Name(), err)
		}
	}
	return nil
}

// TestSchemaApplied 断言 22+ 张表全部建成（DDL 可执行性是本测试核心价值）
func TestSchemaApplied(t *testing.T) {
	rows, err := testDB.Query("SELECT table_name FROM information_schema.tables WHERE table_schema = ?", testDBName)
	require.NoError(t, err)
	defer rows.Close()
	got := map[string]bool{}
	for rows.Next() {
		var name string
		err := rows.Scan(&name)
		require.NoError(t, err)
		got[name] = true
	}
	want := []string{
		"account", "player", "player_attr", "player_item", "player_equip",
		"player_skill", "player_partner", "player_quest", "guild", "guild_member",
		"mail", "mail_attachment", "team", "team_member", "copy_record",
		"shop_record", "activity_record", "friend", "friend_apply", "chat_record",
		"recharge_order", "recharge_record", "login_log", "rank_history", "feature_stats",
	}
	for _, w := range want {
		require.False(t, !got[w], fmt.Sprintf("表 %s 未建成", w))
	}
}

// TestSeedLoaded 断言种子数据可查（23.3 第 1 步灌数验证）
func TestSeedLoaded(t *testing.T) {
	acc, err := model.FindAccountByPK(context.Background(), testDB, 1)
	require.NoError(t, err, "FindAccountByPK(1)")
	require.EqualValues(t, "test001", acc.Account)
}

// TestCRUDAndIdempotent 第 4 步等价验证：业务闭环 → 触发落库 → 查 MySQL 终态；
// 幂等：Upsert 重复执行不产生重复行（18.2 防线②）
func TestCRUDAndIdempotent(t *testing.T) {
	ctx := context.Background()

	// 插入一个测试角色（player 表，来自 seed 之外的独立数据）
	p := &model.Player{Id: 1001, AccountId: 1, Name: "集成测试", Level: 1, Coin: 100}
	_, err := model.UpsertPlayerByPK(ctx, testDB, p)
	require.NoError(t, err, fmt.Sprintf("UpsertPlayerByPK: %v", err))
	// 重复 Upsert（幂等）
	_, err = model.UpsertPlayerByPK(ctx, testDB, p)
	require.NoError(t, err, fmt.Sprintf("重复 UpsertPlayerByPK: %v", err))
	// 更新走 Update
	p.Coin = 200
	_, err = model.UpdatePlayerByPK(ctx, testDB, p)
	require.NoError(t, err, fmt.Sprintf("UpdatePlayerByPK: %v", err))
	// 终态断言
	got, err := model.FindPlayerByPK(ctx, testDB, 1001)
	require.NoError(t, err, "FindPlayerByPK")
	require.EqualValues(t, 200, got.Coin, fmt.Sprintf("终态异常: coin=%d name=%q", got.Coin, got.Name))
	require.EqualValues(t, "集成测试", got.Name)
	// 唯一性：同 account_id 下重复 name 不拦，但 player.id 唯一键冲突应报错
	dup := &model.Player{Id: 1001, AccountId: 1, Name: "重复"}
	_, err = model.InsertPlayer(ctx, testDB, dup)
	require.Error(t, err, "重复 InsertPlayer 应触发唯一键冲突")

	// 聊天记录幂等（uk_owner_msg，17.3 双端落库不重复）
	msg := &model.ChatRecord{OwnerId: 1001, PeerId: 1002, MsgId: 500001, Direction: 0, Channel: 1, Content: "hello"}
	_, err = model.InsertChatRecord(ctx, testDB, msg)
	require.NoError(t, err, fmt.Sprintf("InsertChatRecord: %v", err))
	_, err = model.InsertChatRecord(ctx, testDB, msg)
	require.Error(t, err, "重复 chat_record 应触发 uk_owner_msg 冲突")

	// 可空时间列（player_item.expire_at，DATETIME NULL）：
	// ① 零值 sql.NullTime 写入应为 NULL（曾因 Go 零值 time.Time 越界报 1292）；
	// ② 读出 NULL 不应报 converting 错误
	item := &model.PlayerItem{Id: 9001, PlayerId: 1001, ItemId: 10001, Count: 5}
	_, err = model.InsertPlayerItem(ctx, testDB, item)
	require.NoError(t, err, fmt.Sprintf("InsertPlayerItem: %v", err))
	gotItem, err := model.FindPlayerItemByPK(ctx, testDB, 9001)
	require.NoError(t, err, "FindPlayerItemByPK")
	require.False(t, gotItem.ExpireAt.Valid, "未设过期时间的道具 expire_at 应为 NULL")
	// 显式设置过期时间后应可往返
	expire := time.Now().Add(24 * time.Hour).Truncate(time.Second)
	item.ExpireAt = sql.NullTime{Time: expire, Valid: true}
	_, err = model.UpdatePlayerItemByPK(ctx, testDB, item)
	require.NoError(t, err, fmt.Sprintf("UpdatePlayerItemByPK: %v", err))
	gotItem2, err := model.FindPlayerItemByPK(ctx, testDB, 9001)
	require.NoError(t, err, "FindPlayerItemByPK(2)")
	require.False(t, !gotItem2.ExpireAt.Valid || !gotItem2.ExpireAt.Time.Equal(expire), fmt.Sprintf("expire_at 往返异常: %+v", gotItem2.ExpireAt))

	// 清理
	_, err = model.DeletePlayerByPK(ctx, testDB, 1001)
	require.NoError(t, err, fmt.Sprintf("DeletePlayerByPK: %v", err))
}

// TestModuleLevelFlow 模块级主链路（23.3 第 2-6 步）：待 P2 模块落地启用
func TestModuleLevelFlow(t *testing.T) {
	t.Skip("TODO(P2): 登录签发 Token → RSA/AES 握手 → EnterGame → UseItem → 跨服聊天/战斗，待模块落地")
}
