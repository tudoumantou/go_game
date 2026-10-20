package service

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"game_study/pkg/db"
	"game_study/pkg/gamedata"
	"game_study/pkg/log"
	"github.com/DATA-DOG/go-sqlmock"
)

// TestBuildInitialItems 覆盖初始背包配置的合并、截断与容错：
//   - 同一道具多行合并；
//   - 数量超过 item.xml 堆叠上限时截断；
//   - 未知道具、非法 ID/数量只跳过，不阻断创角。
func TestBuildInitialItems(t *testing.T) {
	logger := log.New("role-service-test", log.WithLevel("error"))
	got := buildInitialItems(logger, []gamedata.InitBagCfg{
		{Id: 1, Col1: 2001, Col2: 5},   // 新手剑上限 1 → 截断为 1
		{Id: 2, Col1: 1001, Col2: 100}, // 金币
		{Id: 3, Col1: 1001, Col2: 200}, // 与上一行合并 → 300
		{Id: 4, Col1: 9999, Col2: 1},   // item.xml 不存在 → 跳过
		{Id: 5, Col1: 0, Col2: 1},      // 非法道具 ID → 跳过
		{Id: 6, Col1: 1002, Col2: -1},  // 非法数量 → 跳过
	})
	want := []initItem{{itemID: 1001, count: 300}, {itemID: 2001, count: 1}}
	if len(got) != len(want) {
		t.Fatalf("items = %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("items[%d] = %+v, want %+v (all: %+v)", i, got[i], want[i], got)
		}
	}
}

// TestInitialItemsFromConfig 校验线上生效的 init_bag.xml：
// 至少配置一件道具，且每件都在 item.xml 中存在、数量不超过堆叠上限。
func TestInitialItemsFromConfig(t *testing.T) {
	logger := log.New("role-service-test", log.WithLevel("error"))
	items := initialItems(logger)
	if len(items) == 0 {
		t.Fatal("init_bag.xml 未配置任何初始道具")
	}
	for _, it := range items {
		cfg, ok := gamedata.GetItemCfg(int64(it.itemID))
		if !ok {
			t.Fatalf("初始道具 %d 不在 item.xml", it.itemID)
		}
		if it.count <= 0 || (cfg.Col2 > 0 && it.count > cfg.Col2) {
			t.Fatalf("初始道具 %d 数量 %d 非法（上限 %d）", it.itemID, it.count, cfg.Col2)
		}
	}
}

// TestCreateWritesInitialBagInTx 验证创角事务的完整 SQL 序列：
// player → player_attr → 初始道具（按 item_id 升序），最后提交。
func TestCreateWritesInitialBagInTx(t *testing.T) {
	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer sqlDB.Close()
	svc := NewRoleService(&db.DB{DB: sqlDB}, log.New("role-service-test", log.WithLevel("error")))

	const accountID, roleID = int64(1001), int64(10001)
	mock.ExpectQuery("SELECT id, account_id, name").WithArgs(accountID).WillReturnError(sql.ErrNoRows)
	mock.ExpectBegin()
	mock.ExpectExec(`INSERT INTO player \(`).WithArgs(accountID, "张三").
		WillReturnResult(sqlmock.NewResult(roleID, 1))
	// 战士基础属性。
	mock.ExpectExec("INSERT INTO player_attr").
		WithArgs(roleID, int32(1), int32(1000), int32(100), int32(50), int32(30), int32(10)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	// init_bag.xml：1001×1000、1002×5、2001×1（升序写入）。
	mock.ExpectExec("INSERT INTO player_item").
		WithArgs(roleID, int32(1001), int64(1000)).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO player_item").
		WithArgs(roleID, int32(1002), int64(5)).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO player_item").
		WithArgs(roleID, int32(2001), int64(1)).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	got, err := svc.Create(context.Background(), accountID, "张三", 1)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if got != roleID {
		t.Fatalf("role id = %d, want %d", got, roleID)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// TestCreateRollbackOnInitialItemFailure 初始道具写入失败时整笔事务回滚，
// 不能留下「有角色但没有新手道具」的半成品。
func TestCreateRollbackOnInitialItemFailure(t *testing.T) {
	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer sqlDB.Close()
	svc := NewRoleService(&db.DB{DB: sqlDB}, log.New("role-service-test", log.WithLevel("error")))

	const accountID = int64(1001)
	mock.ExpectQuery("SELECT id, account_id, name").WithArgs(accountID).WillReturnError(sql.ErrNoRows)
	mock.ExpectBegin()
	mock.ExpectExec(`INSERT INTO player \(`).WithArgs(accountID, "张三").
		WillReturnResult(sqlmock.NewResult(10001, 1))
	mock.ExpectExec("INSERT INTO player_attr").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO player_item").WillReturnError(errors.New("disk full"))
	mock.ExpectRollback()

	if _, err := svc.Create(context.Background(), accountID, "张三", 1); err == nil {
		t.Fatal("expect error when initial item insert fails")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
