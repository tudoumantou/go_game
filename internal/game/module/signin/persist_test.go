package signin

import (
	"context"
	"encoding/json"
	"testing"

	"game_study/pkg/writebehind"

	"github.com/DATA-DOG/go-sqlmock"
)

// 落库由生成代码拼 SQL：业务只组装字段，表名/列名改动会在这里体现。
func TestPersistBatchWritesAnchorsAndClaims(t *testing.T) {
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	mock.ExpectBegin()
	tx, err := database.Begin()
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectExec("INSERT INTO player_signin").WithArgs(int64(7), int64(20000)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO signin_claim").
		WithArgs(int64(7), int64(20000), int64(20000), int32(1), `[{"item_id":1001,"count":100}]`).
		WillReturnResult(sqlmock.NewResult(0, 1))

	data := Data{AnchorDay: 20000, Claims: map[int64][]writebehind.ItemGrant{
		20000: {{ItemID: 1001, Count: 100}},
	}}
	raw, err := json.Marshal(data)
	if err != nil {
		t.Fatal(err)
	}
	rows := []writebehind.ModuleRow{{PlayerID: 7, Data: raw}}
	if err := (persister{}).PersistBatch(context.Background(), tx, rows); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// 多个玩家合成一批写入，仍是两条语句（每表一条多行 UPSERT）。
func TestPersistBatchBatchesPlayersTogether(t *testing.T) {
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	mock.ExpectBegin()
	tx, err := database.Begin()
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectExec("INSERT INTO player_signin").WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectExec("INSERT INTO signin_claim").WillReturnResult(sqlmock.NewResult(0, 2))

	rows := make([]writebehind.ModuleRow, 0, 2)
	for _, pid := range []int64{7, 8} {
		raw, err := json.Marshal(Data{AnchorDay: 20000, Claims: map[int64][]writebehind.ItemGrant{
			20000: {{ItemID: 1001, Count: 100}},
		}})
		if err != nil {
			t.Fatal(err)
		}
		rows = append(rows, writebehind.ModuleRow{PlayerID: pid, Data: raw})
	}
	if err := (persister{}).PersistBatch(context.Background(), tx, rows); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
