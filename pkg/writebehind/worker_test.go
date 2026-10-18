package writebehind

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestSQLFenceSkipsOlderSnapshot(t *testing.T) {
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	mock.ExpectBegin()
	mock.ExpectExec("INSERT IGNORE INTO player_writebehind").WithArgs(int64(7)).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("SELECT player_id, version FROM player_writebehind.*FOR UPDATE").WithArgs(int64(7)).WillReturnRows(sqlmock.NewRows([]string{"player_id", "version"}).AddRow(7, 2))
	mock.ExpectCommit()
	worker := NewWorker(nil, database, 100)
	if err := worker.Persist(context.Background(), []Record{{Version: 1, State: Snapshot{PlayerID: 7, Items: map[int32]int64{1001: 100}}}}); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestSQLFailureRollsBackFenceAndState(t *testing.T) {
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	mock.ExpectBegin()
	mock.ExpectExec("INSERT IGNORE INTO player_writebehind").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("SELECT player_id, version FROM player_writebehind.*FOR UPDATE").WillReturnRows(sqlmock.NewRows([]string{"player_id", "version"}).AddRow(7, 0))
	mock.ExpectExec("UPDATE player SET").WillReturnError(errors.New("database unavailable"))
	mock.ExpectRollback()
	if err := NewWorker(nil, database, 100).Persist(context.Background(), []Record{{Version: 1, State: Snapshot{PlayerID: 7}}}); err == nil {
		t.Fatal("failure lost")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
