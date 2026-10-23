//go:build integration

// test/integration/r12_save_ack_test.go —— R1.2 保存确认与关服排空
//
// 验证两件事：
//  1. 关服 FlushAll 必须排空全部批次（脏数据 > batch_rows 时不能只落第一批）；
//  2. 真实 MySQL 不可用时保存必须报错并保留脏标记，恢复后可重新落库。
package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"game_study/internal/game/player"
	"game_study/pkg/db"
	"game_study/pkg/flush"
	"game_study/pkg/log"
	"github.com/stretchr/testify/require"
)

func insertTestPlayer(t *testing.T, database *db.DB, id int64) {
	t.Helper()
	if _, err := database.ExecContext(context.Background(),
		"INSERT INTO player (id, account_id, name) VALUES (?,?,?)",
		id, id, fmt.Sprintf("p%d", id)); err != nil {
		t.Fatalf("insert player %d: %v", id, err)
	}
}

// TestR12FlushAllDrainsAllBatches 5 个玩家、批次 2：必须分 3 轮全部落库。
func TestR12FlushAllDrainsAllBatches(t *testing.T) {
	database, _, _ := startStorage(t)
	ctx := context.Background()
	repo := player.NewRepository(database)

	const n = 5
	ids := make([]int64, 0, n)
	for i := 0; i < n; i++ {
		id := int64(910000 + i)
		insertTestPlayer(t, database, id)
		ids = append(ids, id)
	}

	f := flush.New(flush.Config{ShardCount: 1, Interval: time.Minute, BatchRows: 2},
		func(ctx context.Context, id int64) error {
			st, err := repo.Load(ctx, id)
			if err != nil {
				return err
			}
			st.AddItem(1001, 1)
			return repo.Save(ctx, st)
		}, log.New("test"))

	for _, id := range ids {
		f.MarkDirty(id)
	}
	if err := f.FlushAll(ctx); err != nil {
		t.Fatalf("flush all: %v", err)
	}
	if got := f.DirtyCount(); got != 0 {
		t.Fatalf("dirty left after FlushAll: %d", got)
	}
	for _, id := range ids {
		var count int64
		if err := database.QueryRowContext(ctx,
			"SELECT count FROM player_item WHERE player_id=? AND item_id=1001", id).Scan(&count); err != nil {
			t.Fatalf("player %d item row: %v", id, err)
		}
		require.EqualValues(t, 1, count)
	}
}

// TestR12SaveFailureKeepsDirtyThenRecovers MySQL 断连时保存失败、脏标记保留；
// 恢复连接后同一玩家可以重新落库，数据未丢。
func TestR12SaveFailureKeepsDirtyThenRecovers(t *testing.T) {
	database, _, dsn := startStorage(t)
	ctx := context.Background()
	id := int64(910100)
	insertTestPlayer(t, database, id)

	repo := player.NewRepository(database)
	persist := func(repo *player.Repository) flush.PersistFunc {
		return func(ctx context.Context, pid int64) error {
			st, err := repo.Load(ctx, pid)
			if err != nil {
				return err
			}
			st.AddItem(1001, 2)
			return repo.Save(ctx, st)
		}
	}

	f := flush.New(flush.Config{ShardCount: 1, Interval: time.Minute, BatchRows: 10},
		persist(repo), log.New("test"))
	f.MarkDirty(id)

	// 制造真实故障：连接池关闭后任何查询都返回 "sql: database is closed"。
	if err := database.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}
	if err := f.FlushAll(ctx); err == nil {
		t.Fatal("FlushAll must fail when MySQL is unavailable")
	}
	if got := f.DirtyCount(); got != 1 {
		t.Fatalf("failed save must keep dirty flag, got %d", got)
	}

	// 恢复：同一 DSN 新建连接池，重新落库成功。
	db2, err := db.New(db.Config{DSN: dsn, MaxOpen: 5, MaxIdle: 2})
	require.NoError(t, err, "reconnect")
	t.Cleanup(func() { _ = db2.Close() })
	f2 := flush.New(flush.Config{ShardCount: 1, Interval: time.Minute, BatchRows: 10},
		persist(player.NewRepository(db2)), log.New("test"))
	f2.MarkDirty(id)
	if err := f2.FlushAll(ctx); err != nil {
		t.Fatalf("recovery flush: %v", err)
	}

	var count int64
	if err := db2.QueryRowContext(ctx,
		"SELECT count FROM player_item WHERE player_id=? AND item_id=1001", id).Scan(&count); err != nil {
		t.Fatalf("verify item: %v", err)
	}
	require.EqualValues(t, 2, count)
}
