package flush

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

// 空输入不产生任何 SQL：模块本次没数据时不应该打扰数据库。
func TestUpsertBatchSkipsEmptyInput(t *testing.T) {
	database, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer database.Close()
	mock.ExpectBegin()
	tx, err := database.Begin()
	require.NoError(t, err)
	rows := []int32(nil)
	if err := UpsertBatch(context.Background(), tx, "demo", []string{"id"}, rows, func(v int32) []any {
		return []any{v}
	}); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// 超过单批行数上限时自动切分成多条语句。
func TestUpsertBatchChunksByRowLimit(t *testing.T) {
	database, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer database.Close()
	mock.ExpectBegin()
	tx, err := database.Begin()
	require.NoError(t, err)
	mock.ExpectExec("INSERT INTO demo").WillReturnResult(sqlmock.NewResult(0, DefaultBatchRows))
	mock.ExpectExec("INSERT INTO demo").WillReturnResult(sqlmock.NewResult(0, 1))

	rows := make([]int32, DefaultBatchRows+1)
	for i := range rows {
		rows[i] = int32(i + 1)
	}
	if err := UpsertBatch(context.Background(), tx, "demo", []string{"id"}, rows, func(v int32) []any {
		return []any{v}
	}); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestDeleteByPlayerUsesPlayerColumn(t *testing.T) {
	database, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer database.Close()
	mock.ExpectBegin()
	tx, err := database.Begin()
	require.NoError(t, err)
	mock.ExpectExec("DELETE FROM demo WHERE player_id = ?").WithArgs(int64(7)).WillReturnResult(sqlmock.NewResult(0, 3))
	if err := DeleteByPlayer(context.Background(), tx, "demo", "player_id", 7); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
