package player

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"game_study/internal/game/module"
	"game_study/internal/game/module/bag"
	"game_study/pkg/actor"
	"game_study/pkg/db"
	"game_study/pkg/log"
	gamev1 "game_study/pkg/pb/game/v1"
	"github.com/DATA-DOG/go-sqlmock"
	"google.golang.org/protobuf/proto"
)

func TestCommitItemsAtomic(t *testing.T) {
	for _, fail := range []string{"", "business", "items", "commit"} {
		t.Run(fail, func(t *testing.T) {
			sqlDB, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer sqlDB.Close()
			r := NewRepository(&db.DB{DB: sqlDB})
			s := NewState(7)
			s.SeedItem(1001, 10)
			s.AddItem(1001, 5) // 尚未落库的已有背包变化也必须一起保存。
			boom := errors.New("injected failure")
			mock.ExpectBegin()
			mock.ExpectQuery("SELECT id FROM player.*FOR UPDATE").WithArgs(int64(7)).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(7))
			business := mock.ExpectExec("INSERT INTO claim_test").WithArgs(int64(7))
			if fail == "business" {
				business.WillReturnError(boom)
				mock.ExpectRollback()
			} else {
				business.WillReturnResult(sqlmock.NewResult(1, 1))
				items := mock.ExpectExec("INSERT INTO `?player_item`?").WithArgs(int64(7), int32(1001), int64(35))
				if fail == "items" {
					items.WillReturnError(boom)
					mock.ExpectRollback()
				} else {
					items.WillReturnResult(sqlmock.NewResult(1, 1))
					if fail == "commit" {
						mock.ExpectCommit().WillReturnError(boom)
					} else {
						mock.ExpectCommit()
					}
				}
			}
			err = r.CommitItems(context.Background(), s, []module.ItemGrant{{ItemID: 1001, Count: 20}}, func(tx *sql.Tx) error {
				_, err := tx.Exec("INSERT INTO claim_test (player_id) VALUES (?)", int64(7))
				return err
			})
			if fail == "" {
				if err != nil {
					t.Fatal(err)
				}
				if s.GetItemCount(1001) != 35 || len(s.DirtyItems()) != 0 {
					t.Fatal("committed reward not synchronized")
				}
			} else {
				if err == nil {
					t.Fatal("expected failure")
				}
				if s.GetItemCount(1001) != 15 || s.DirtyItems()[1001] != 15 {
					t.Fatal("failed transaction mutated memory")
				}
				if (fail == "commit") != errors.Is(err, ErrCommitUnknown) {
					t.Fatalf("wrong commit uncertainty: %v", err)
				}
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestReloadItemsPreservesOtherActorState(t *testing.T) {
	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer sqlDB.Close()
	s := NewState(7)
	s.SetProfile(module.Profile{Name: "hero", Level: 9, Exp: 17})
	s.AddBonus("atk", 77)
	s.AddItem(1001, 15)
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT id FROM player.*FOR UPDATE").WithArgs(int64(7)).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(7))
	mock.ExpectQuery("SELECT item_id, count FROM player_item").WithArgs(int64(7)).WillReturnRows(sqlmock.NewRows([]string{"item_id", "count"}).AddRow(1001, 35).AddRow(1002, 1))
	mock.ExpectCommit()
	if err := NewRepository(&db.DB{DB: sqlDB}).ReloadItems(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	if s.GetItemCount(1001) != 35 || s.GetItemCount(1002) != 1 || len(s.DirtyItems()) != 0 {
		t.Fatal("backpack not recovered")
	}
	if s.Profile().Level != 9 || s.Profile().Exp != 17 || s.GetAttr("atk") != 77 {
		t.Fatal("recovery replaced unrelated player state")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestActorUnknownCommitSkipsSaveAndRecoversBeforeNextPacket(t *testing.T) {
	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer sqlDB.Close()
	r := NewRepository(&db.DB{DB: sqlDB})
	logger := log.New("grant-test", log.WithLevel("error"))
	registry := module.NewRegistry()
	registry.Register(bag.New(logger))
	response := make(chan *gamev1.BagListResp, 1)
	a := NewActor(7, registry, logger, r, nil, func(_ uint64, id uint32, body []byte) error {
		if id == 3004 {
			out := &gamev1.BagListResp{}
			if err := proto.Unmarshal(body, out); err != nil {
				return err
			}
			response <- out
		}
		return nil
	})
	a.loaded = true
	a.online = true
	a.sessionID = 1
	a.state.SeedItem(1001, 15)
	a.state.AddBonus("atk", 77)
	// 发奖前保存已经存在的状态。
	mock.ExpectBegin()
	mock.ExpectExec("UPDATE player SET").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO player_attr").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	// 模拟服务端可能成功、客户端收到 COMMIT 错误。
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT id FROM player.*FOR UPDATE").WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(7))
	mock.ExpectExec("INSERT INTO claim_test").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT INTO player_item").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit().WillReturnError(errors.New("connection lost"))
	err = a.moduleCtx(nil).CommitItems(context.Background(), []module.ItemGrant{{ItemID: 1001, Count: 20}}, func(tx *sql.Tx) error { _, err := tx.Exec("INSERT INTO claim_test VALUES (7)"); return err })
	if !errors.Is(err, ErrCommitUnknown) || !a.needsReload {
		t.Fatalf("uncertainty not retained: %v", err)
	}
	// SaveMsg 必须跳过。下一条背包请求先锁行恢复，再读取新数量。
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT id FROM player.*FOR UPDATE").WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(7))
	mock.ExpectQuery("SELECT item_id, count FROM player_item").WillReturnRows(sqlmock.NewRows([]string{"item_id", "count"}).AddRow(1001, 35))
	mock.ExpectCommit()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sys := actor.NewSystem(actor.Config{ShardCount: 1, MailboxSize: 8})
	sys.Start(ctx)
	defer sys.Stop()
	pid, err := sys.Spawn("unknown-commit", a)
	if err != nil {
		t.Fatal(err)
	}
	if err := sys.Send(pid, &SaveMsg{}); err != nil {
		t.Fatal(err)
	}
	if err := sys.Send(pid, &PacketMsg{MsgID: 3003}); err != nil {
		t.Fatal(err)
	}
	select {
	case out := <-response:
		if len(out.Items) != 1 || out.Items[0].Count != 35 {
			t.Fatalf("stale backpack response: %v", out)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("recovery response timeout")
	}
	sys.Stop()
	if a.needsReload || a.state.GetAttr("atk") != 77 {
		t.Fatal("recovery did not preserve actor state")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
