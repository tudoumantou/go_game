package signin

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"game_study/internal/game/module"
	"game_study/pkg/db"
	"github.com/DATA-DOG/go-sqlmock"
)

func TestClaimDuplicateUsesStoredRewards(t *testing.T) {
	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer sqlDB.Close()
	r := NewRepo(&db.DB{DB: sqlDB})
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO player_signin").WithArgs(int64(7), int64(20000)).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("SELECT anchor_day.*FOR UPDATE").WithArgs(int64(7)).WillReturnRows(sqlmock.NewRows([]string{"anchor_day"}).AddRow(20000))
	mock.ExpectQuery("SELECT rewards FROM signin_claim").WithArgs(int64(7), int64(20000)).WillReturnRows(sqlmock.NewRows([]string{"rewards"}).AddRow(`[{"item_id":1001,"count":100}]`))
	mock.ExpectRollback()
	tx, err := sqlDB.Begin()
	if err != nil {
		t.Fatal(err)
	}
	stored, err := r.Claim(context.Background(), tx, 7, 20000, 20000, 1, []module.ItemGrant{{ItemID: 1001, Count: 999}})
	if !errors.Is(err, errAlready) || len(stored) != 1 || stored[0].Count != 100 {
		t.Fatalf("stored=%v err=%v", stored, err)
	}
	_ = tx.Rollback()
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestLoadNeverStartsCycle(t *testing.T) {
	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer sqlDB.Close()
	r := NewRepo(&db.DB{DB: sqlDB})
	mock.ExpectQuery("SELECT anchor_day FROM player_signin").WithArgs(int64(7)).WillReturnError(sql.ErrNoRows)
	view, err := r.Load(context.Background(), 7, 20000)
	if err != nil || view.Start != 20000 || view.Day != 1 || len(view.Claims) != 0 {
		t.Fatalf("view=%+v err=%v", view, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
