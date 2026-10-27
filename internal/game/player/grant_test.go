package player

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"game_study/internal/game/module"
	"game_study/internal/game/module/bag"
	"game_study/pkg/actor"
	"game_study/pkg/db"
	"game_study/pkg/log"
	gamev1 "game_study/pkg/pb/game/v1"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// playerRow 一行完整的 player 记录。
//
// 生成函数（@lockby/@findby）取的是全部列，不再是手写 SQL 的 `SELECT id`——
// 测试要跟着「表结构」走，而不是跟着某条手写语句走。
func playerRow(id int64) *sqlmock.Rows {
	now := time.Now()
	return sqlmock.NewRows([]string{"id", "account_id", "name", "level", "exp", "coin", "scene_id", "created_at", "updated_at"}).
		AddRow(id, int64(1001), "hero", 1, 0, 0, 0, now, now)
}

// itemRows 背包行（ListPlayerItemByPlayer 取 player_item 全部列）。
func itemRows(items ...[2]int32) *sqlmock.Rows {
	now := time.Now()
	r := sqlmock.NewRows([]string{"id", "player_id", "item_id", "count", "expire_at", "created_at", "updated_at"})
	for i, it := range items {
		r.AddRow(int64(i+1), int64(7), it[0], it[1], nil, now, now)
	}
	return r
}

func TestCommitItemsAtomic(t *testing.T) {
	for _, fail := range []string{"", "business", "items", "commit"} {
		t.Run(fail, func(t *testing.T) {
			sqlDB, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer sqlDB.Close()
			r := NewRepository(&db.DB{DB: sqlDB})
			s := NewState(7)
			s.SeedItem(1001, 10)
			s.AddItem(1001, 5) // 尚未落库的已有背包变化也必须一起保存。
			boom := errors.New("injected failure")
			mock.ExpectBegin()
			mock.ExpectQuery("FROM player WHERE id = \\? ORDER BY id LIMIT 1 FOR UPDATE").WithArgs(int64(7)).WillReturnRows(playerRow(7))
			business := mock.ExpectExec("INSERT INTO claim_test").WithArgs(int64(7))
			if fail == "business" {
				business.WillReturnError(boom)
				mock.ExpectRollback()
			} else {
				business.WillReturnResult(sqlmock.NewResult(1, 1))
				items := mock.ExpectExec("INSERT INTO player_item \\(player_id, item_id, count\\)").WithArgs(int64(7), int32(1001), int64(35))
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
				require.NoError(t, err)
				require.EqualValues(t, 35, s.GetItemCount(1001), "committed reward not synchronized")
				require.Empty(t, s.DirtyItems())
			} else {
				require.Error(t, err, "expected failure")
				require.EqualValues(t, 15, s.GetItemCount(1001), "failed transaction mutated memory")
				require.EqualValues(t, 15, s.DirtyItems()[1001])
				require.EqualValues(t, errors.Is(err, ErrCommitUnknown), (fail == "commit"))
			}
			err = mock.ExpectationsWereMet()
			require.NoError(t, err)
		})
	}
}

func TestReloadItemsPreservesOtherActorState(t *testing.T) {
	sqlDB, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer sqlDB.Close()
	s := NewState(7)
	s.SetProfile(module.Profile{Name: "hero", Level: 9, Exp: 17})
	s.AddBonus("atk", 77)
	s.AddItem(1001, 15)
	mock.ExpectBegin()
	mock.ExpectQuery("FROM player WHERE id = \\? ORDER BY id LIMIT 1 FOR UPDATE").WithArgs(int64(7)).WillReturnRows(playerRow(7))
	mock.ExpectQuery("FROM player_item WHERE player_id = \\?").WithArgs(int64(7)).WillReturnRows(itemRows([2]int32{1001, 35}, [2]int32{1002, 1}))
	mock.ExpectCommit()
	err = NewRepository(&db.DB{DB: sqlDB}).ReloadItems(context.Background(), s)
	require.NoError(t, err)
	require.EqualValues(t, 35, s.GetItemCount(1001), "backpack not recovered")
	require.EqualValues(t, 1, s.GetItemCount(1002))
	require.Empty(t, s.DirtyItems())
	require.EqualValues(t, 9, s.Profile().Level, "recovery replaced unrelated player state")
	require.EqualValues(t, 17, s.Profile().Exp)
	require.EqualValues(t, 77, s.GetAttr("atk"))
	err = mock.ExpectationsWereMet()
	require.NoError(t, err)
}

func TestActorUnknownCommitSkipsSaveAndRecoversBeforeNextPacket(t *testing.T) {
	sqlDB, mock, err := sqlmock.New()
	require.NoError(t, err)
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
	mock.ExpectQuery("FROM player WHERE id = \\? ORDER BY id LIMIT 1 FOR UPDATE").WillReturnRows(playerRow(7))
	mock.ExpectExec("INSERT INTO claim_test").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT INTO player_item").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit().WillReturnError(errors.New("connection lost"))
	err = a.moduleCtx(nil).CommitItems(context.Background(), []module.ItemGrant{{ItemID: 1001, Count: 20}}, func(tx *sql.Tx) error { _, err := tx.Exec("INSERT INTO claim_test VALUES (7)"); return err })
	require.False(t, !errors.Is(err, ErrCommitUnknown) || !a.needsReload, fmt.Sprintf("uncertainty not retained: %v", err))
	// SaveMsg 必须跳过。下一条背包请求先锁行恢复，再读取新数量。
	mock.ExpectBegin()
	mock.ExpectQuery("FROM player WHERE id = \\? ORDER BY id LIMIT 1 FOR UPDATE").WillReturnRows(playerRow(7))
	mock.ExpectQuery("FROM player_item WHERE player_id = \\?").WillReturnRows(itemRows([2]int32{1001, 35}))
	mock.ExpectCommit()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sys := actor.NewSystem(actor.Config{ShardCount: 1, MailboxSize: 8})
	sys.Start(ctx)
	defer sys.Stop()
	pid, err := sys.Spawn("unknown-commit", a)
	require.NoError(t, err)
	err = sys.Send(pid, &SaveMsg{})
	require.NoError(t, err)
	err = sys.Send(pid, &PacketMsg{MsgID: 3003})
	require.NoError(t, err)
	select {
	case out := <-response:
		require.Len(t, out.Items, 1, fmt.Sprintf("stale backpack response: %v", out))
		require.EqualValues(t, 35, out.Items[0].Count)
	case <-time.After(5 * time.Second):
		t.Fatal("recovery response timeout")
	}
	sys.Stop()
	require.False(t, a.needsReload || a.state.GetAttr("atk") != 77, "recovery did not preserve actor state")
	err = mock.ExpectationsWereMet()
	require.NoError(t, err)
}
