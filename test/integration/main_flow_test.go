//go:build integration

// Package integration 主链路集成测试（设计文档 23.3）
//
// 运行方式（需本机 Docker）：
//
//	go test -tags=integration ./test/integration/ -v -count=1
//
// 环境：testcontainers 拉起真实 MySQL 8.0 + Redis 7，执行 sql/schema 建表、
// sql/seed 灌初始数据，再用 pkg/model CRUD 验证终态。
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

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mysql"
	"github.com/testcontainers/testcontainers-go/modules/redis"
	_ "github.com/go-sql-driver/mysql" // 注册 mysql driver

	"game_study/pkg/model"
)

var (
	testDB  *sql.DB
	testDSN string
)

const (
	testDBName = "game_study"
	testDBUser = "root"
	testDBPass = "game_study"
)

// TestMain 起一套容器环境（23.3 第 1 步：起环境 → 建表 → 灌数）
func TestMain(m *testing.M) {
	ctx := context.Background()

	// --- MySQL ---
	mysqlC, err := mysql.Run(ctx, "mysql:8.0",
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

	// 连接串（multiStatements 支持整文件执行 DDL；parseTime 支持时间列扫描）
	dsn, err := mysqlC.ConnectionString(ctx, "multiStatements=true", "parseTime=true", "loc=Local")
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
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	got := map[string]bool{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
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
		if !got[w] {
			t.Errorf("表 %s 未建成", w)
		}
	}
}

// TestSeedLoaded 断言种子数据可查（23.3 第 1 步灌数验证）
func TestSeedLoaded(t *testing.T) {
	acc, err := model.FindAccountByPK(context.Background(), testDB, 1)
	if err != nil {
		t.Fatalf("FindAccountByPK(1): %v", err)
	}
	if acc.Account != "test001" {
		t.Errorf("account = %q, want test001", acc.Account)
	}
}

// TestCRUDAndIdempotent 第 4 步等价验证：业务闭环 → 触发落库 → 查 MySQL 终态；
// 幂等：Upsert 重复执行不产生重复行（18.2 防线②）
func TestCRUDAndIdempotent(t *testing.T) {
	ctx := context.Background()

	// 插入一个测试角色（player 表，来自 seed 之外的独立数据）
	p := &model.Player{Id: 1001, AccountId: 1, Name: "集成测试", Level: 1, Coin: 100}
	if _, err := model.UpsertPlayerByPK(ctx, testDB, p); err != nil {
		t.Fatalf("UpsertPlayerByPK: %v", err)
	}
	// 重复 Upsert（幂等）
	if _, err := model.UpsertPlayerByPK(ctx, testDB, p); err != nil {
		t.Fatalf("重复 UpsertPlayerByPK: %v", err)
	}
	// 更新走 Update
	p.Coin = 200
	if _, err := model.UpdatePlayerByPK(ctx, testDB, p); err != nil {
		t.Fatalf("UpdatePlayerByPK: %v", err)
	}
	// 终态断言
	got, err := model.FindPlayerByPK(ctx, testDB, 1001)
	if err != nil {
		t.Fatalf("FindPlayerByPK: %v", err)
	}
	if got.Coin != 200 || got.Name != "集成测试" {
		t.Errorf("终态异常: coin=%d name=%q", got.Coin, got.Name)
	}
	// 唯一性：同 account_id 下重复 name 不拦，但 player.id 唯一键冲突应报错
	dup := &model.Player{Id: 1001, AccountId: 1, Name: "重复"}
	if _, err := model.InsertPlayer(ctx, testDB, dup); err == nil {
		t.Error("重复 InsertPlayer 应触发唯一键冲突")
	}

	// 聊天记录幂等（uk_owner_msg，17.3 双端落库不重复）
	msg := &model.ChatRecord{OwnerId: 1001, PeerId: 1002, MsgId: 500001, Direction: 0, Channel: 1, Content: "hello"}
	if _, err := model.InsertChatRecord(ctx, testDB, msg); err != nil {
		t.Fatalf("InsertChatRecord: %v", err)
	}
	if _, err := model.InsertChatRecord(ctx, testDB, msg); err == nil {
		t.Error("重复 chat_record 应触发 uk_owner_msg 冲突")
	}

	// 清理
	if _, err := model.DeletePlayerByPK(ctx, testDB, 1001); err != nil {
		t.Fatalf("DeletePlayerByPK: %v", err)
	}
}

// TestModuleLevelFlow 模块级主链路（23.3 第 2-6 步）：待 P2 模块落地启用
func TestModuleLevelFlow(t *testing.T) {
	t.Skip("TODO(P2): 登录签发 Token → RSA/AES 握手 → EnterGame → UseItem → 跨服聊天/战斗，待模块落地")
}
