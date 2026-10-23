package writebehind

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

// demoPersister 是测试用模块，用来证明「注册即接入」不需要改框架代码。
type demoPersister struct {
	name     string
	validate func(json.RawMessage) error
	persist  func(context.Context, *sql.Tx, []ModuleRow) error
	load     func(context.Context, *sql.Tx, int64) (json.RawMessage, error)
}

func (d demoPersister) Name() string { return d.name }
func (d demoPersister) Validate(raw json.RawMessage) error {
	if d.validate != nil {
		return d.validate(raw)
	}
	return nil
}
func (d demoPersister) PersistBatch(ctx context.Context, tx *sql.Tx, rows []ModuleRow) error {
	if d.persist == nil {
		return nil
	}
	return d.persist(ctx, tx, rows)
}
func (d demoPersister) Load(ctx context.Context, tx *sql.Tx, playerID int64) (json.RawMessage, error) {
	if d.load == nil {
		return nil, nil
	}
	return d.load(ctx, tx, playerID)
}

func expectCommonBatch(mock sqlmock.Sqlmock, players ...int64) {
	mock.ExpectBegin()
	mock.ExpectExec("INSERT IGNORE INTO player_writebehind").WillReturnResult(sqlmock.NewResult(0, 0))
	rows := sqlmock.NewRows([]string{"player_id", "version"})
	for _, id := range players {
		rows.AddRow(id, 0)
	}
	mock.ExpectQuery("SELECT player_id, version FROM player_writebehind").WillReturnRows(rows)
	mock.ExpectExec("UPDATE player SET").WillReturnResult(sqlmock.NewResult(0, int64(len(players))))
	count := sqlmock.NewRows([]string{"count"})
	count.AddRow(int64(len(players)))
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(count)
	mock.ExpectExec("INSERT INTO player_attr").WillReturnResult(sqlmock.NewResult(0, int64(len(players))))
	mock.ExpectExec("DELETE FROM player_item").WillReturnResult(sqlmock.NewResult(0, 0))
}

// 注册一个模块后，框架自动把它的 Ext 交给它落库 —— 不需要改 worker 的任何逻辑。
func TestRegisteredPersisterReceivesItsRows(t *testing.T) {
	resetPersisters()
	t.Cleanup(resetPersisters)

	var got []ModuleRow
	RegisterPersister(demoPersister{
		name: "demo",
		persist: func(ctx context.Context, tx *sql.Tx, rows []ModuleRow) error {
			got = append(got, rows...)
			_, err := tx.ExecContext(ctx, "INSERT INTO demo_state (player_id, payload) VALUES (?,?)", rows[0].PlayerID, string(rows[0].Data))
			return err
		},
	})

	database, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer database.Close()
	expectCommonBatch(mock, 7, 8)
	mock.ExpectExec("INSERT INTO demo_state").WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectExec("INSERT INTO player_writebehind").WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectCommit()

	records := []Record{
		{Version: 1, State: Snapshot{PlayerID: 7, Ext: map[string]json.RawMessage{"demo": json.RawMessage(`{"counter":1}`)}}},
		{Version: 2, State: Snapshot{PlayerID: 8, Ext: map[string]json.RawMessage{"demo": json.RawMessage(`{"counter":2}`)}}},
	}
	if err := NewWorker(nil, database, 100).Persist(context.Background(), records); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].PlayerID != 7 || got[1].PlayerID != 8 {
		t.Fatalf("persister rows = %+v", got)
	}
	if string(got[0].Data) != `{"counter":1}` {
		t.Fatalf("row data = %s", got[0].Data)
	}
}

// 未注册的模块状态必须拒绝写入：宁可整批回滚，也不能把数据写到没人认领的地方。
func TestUnregisteredModuleStateRejected(t *testing.T) {
	resetPersisters()
	t.Cleanup(resetPersisters)

	err := Snapshot{PlayerID: 7, Ext: map[string]json.RawMessage{"ghost": json.RawMessage(`{}`)}}.Validate()
	if err == nil || !strings.Contains(err.Error(), "ghost") {
		t.Fatalf("unregistered module accepted: %v", err)
	}

	database, mock, dbErr := sqlmock.New()
	if dbErr != nil {
		t.Fatal(dbErr)
	}
	defer database.Close()
	mock.ExpectBegin()
	mock.ExpectExec("INSERT IGNORE INTO player_writebehind").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("SELECT player_id, version FROM player_writebehind").WillReturnRows(sqlmock.NewRows([]string{"player_id", "version"}).AddRow(7, 0))
	mock.ExpectRollback()
	r := Record{Version: 1, State: Snapshot{PlayerID: 7, Ext: map[string]json.RawMessage{"ghost": json.RawMessage(`{}`)}}}
	if err := NewWorker(nil, database, 100).Persist(context.Background(), []Record{r}); err == nil {
		t.Fatal("unregistered module persisted")
	}
}
