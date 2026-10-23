//go:build integration

// test/integration/friend_test.go —— 好友最小闭环（R5.1/R5.2）
//
// 验证的是「双边一致」，不是「接口返回 200」：
//   - 申请后双方可见（申请方在我的好友里看不到，接收方在待处理里看得到）；
//   - 同意后双方列表各有一行，且数据库里正好两行；
//   - 删除后两行都没了；
//   - 重复申请/重复同意/重复删除都幂等；
//   - 互相申请直接成为好友，不留第二条待处理；
//   - 好友满时拒绝，且不产生单边关系；
//   - 过期申请同意被拒，且状态落为已拒绝。
package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"game_study/internal/game/module/friend"
	"game_study/pkg/db"
	"github.com/stretchr/testify/require"
)

// insertFriendPlayers 批量造测试玩家：好友列表的 name 来自 player 表，
// 没有玩家行时名字为空（TestFriendFlow 里对此有断言）。
func insertFriendPlayers(t *testing.T, database *db.DB, ids ...int64) {
	t.Helper()
	for _, id := range ids {
		insertTestPlayer(t, database, id)
	}
}

// friendRows 某玩家的好友行数（直接查库，不经过 Repo）。
func friendRows(t *testing.T, database *db.DB, playerID int64) int {
	t.Helper()
	var n int
	if err := database.QueryRow(
		`SELECT COUNT(*) FROM friend WHERE player_id = ?`, playerID).Scan(&n); err != nil {
		t.Fatalf("count friend rows: %v", err)
	}
	return n
}

// TestFriendFlow 好友最小闭环（R5.1/R5.2）：断言一律落在数据库行数与状态上，
// 而不是接口返回值。
//
// 六个子场景各盯一类真实漏洞：
//   - 申请→同意→互见→删除：双边一致（恰好两行）、删除双向清理、重复操作幂等；
//   - 互相申请：直接建关系，不留第二条待处理；
//   - 好友满：拒绝（6804）且不产生单边关系 —— 「一边有一边没有」是最典型的事故形态；
//   - 过期申请：同意被拒（6808）且状态落为已拒绝（2），防止补同意历史申请；
//   - 拒绝后重新申请：复用同一行（唯一键），状态回到待处理。
//
// 好友上限用直接插 50 行构造，避免造 50 个玩家拖慢用例。
func TestFriendFlow(t *testing.T) {
	database, _, _ := startStorage(t)
	ctx := context.Background()
	repo := friend.NewRepo(database)

	t.Run("申请→同意→互见→删除", func(t *testing.T) {
		const a, b = int64(930001), int64(930002)
		insertFriendPlayers(t, database, a, b)

		code, err := repo.Apply(ctx, a, b)
		require.NoError(t, err, fmt.Sprintf("apply: code=%d err=%v", code, err))
		require.EqualValues(t, 0, code)
		// 申请方自己还看不到好友，接收方有一条待处理。
		n := friendRows(t, database, a)
		require.EqualValues(t, 0, n, fmt.Sprintf("apply must not create friendship, got %d rows", n))
		_, applies, err := repo.List(ctx, b)
		require.NoError(t, err, fmt.Sprintf("apply list = %+v err=%v", applies, err))
		require.Len(t, applies, 1)
		require.EqualValues(t, a, applies[0].FromID)

		// 重复申请：幂等，不产生第二条。
		if code, err = repo.Apply(ctx, a, b); err != nil || code != 6805 {
			t.Fatalf("duplicate apply: code=%d err=%v", code, err)
		}

		if code, err = repo.Handle(ctx, b, a, true, 7*24*time.Hour); err != nil || code != 0 {
			t.Fatalf("accept: code=%d err=%v", code, err)
		}
		// 双边两行，且双方列表互见。
		require.EqualValues(t, 1, friendRows(t, database, a), fmt.Sprintf("expect one row each: a=%d b=%d", friendRows(t, database, a), friendRows(t, database, b)))
		require.EqualValues(t, 1, friendRows(t, database, b))
		fa, _, _ := repo.List(ctx, a)
		fb, _, _ := repo.List(ctx, b)
		require.Len(t, fa, 1, fmt.Sprintf("mutual list broken: a=%+v b=%+v", fa, fb))
		require.EqualValues(t, b, fa[0].PlayerID)
		require.Len(t, fb, 1)
		require.EqualValues(t, a, fb[0].PlayerID)
		require.NotEqualValues(t, "", fa[0].Name, "friend name must be filled from player table")

		// 已是好友再申请 / 再同意都幂等。
		if code, _ = repo.Apply(ctx, a, b); code != 6803 {
			t.Fatalf("apply when already friend: code=%d", code)
		}
		if code, _ = repo.Handle(ctx, b, a, true, 7*24*time.Hour); code != 6803 {
			t.Fatalf("accept twice: code=%d", code)
		}

		// 删除后双方都空；重复删除仍成功。
		err = repo.Delete(ctx, a, b)
		require.NoError(t, err, fmt.Sprintf("delete: %v", err))
		require.EqualValues(t, 0, friendRows(t, database, a), "delete must remove both directions")
		require.EqualValues(t, 0, friendRows(t, database, b))
		err = repo.Delete(ctx, a, b)
		require.NoError(t, err, fmt.Sprintf("delete must be idempotent: %v", err))
	})

	t.Run("互相申请直接成为好友", func(t *testing.T) {
		const a, b = int64(930011), int64(930012)
		insertFriendPlayers(t, database, a, b)

		code, err := repo.Apply(ctx, a, b)
		require.NoError(t, err, fmt.Sprintf("a→b: code=%d err=%v", code, err))
		require.EqualValues(t, 0, code)
		// b 反向申请：应直接建立关系，而不是再插一条待处理。
		code, err = repo.Apply(ctx, b, a)
		require.NoError(t, err, fmt.Sprintf("b→a: code=%d err=%v", code, err))
		require.EqualValues(t, 0, code)
		require.EqualValues(t, 1, friendRows(t, database, a), "mutual apply must create friendship directly")
		require.EqualValues(t, 1, friendRows(t, database, b))
		_, applies, _ := repo.List(ctx, b)
		require.Len(t, applies, 0)
	})

	t.Run("好友满拒绝且不产生单边", func(t *testing.T) {
		const a, b = int64(930021), int64(930022)
		insertFriendPlayers(t, database, a, b)

		// 直接塞满上限（默认 50），避免造 50 个玩家。
		for i := 0; i < 50; i++ {
			if _, err := database.Exec(
				`INSERT INTO friend (player_id, friend_id) VALUES (?, ?)`, a, 950000+i); err != nil {
				t.Fatalf("seed friend %d: %v", i, err)
			}
		}
		code, err := repo.Apply(ctx, a, b)
		require.NoError(t, err, fmt.Sprintf("apply when full: code=%d err=%v", code, err))
		require.EqualValues(t, 6804, code)
		n := friendRows(t, database, b)
		require.EqualValues(t, 0, n, fmt.Sprintf("must not create one-sided friendship: %d", n))
	})

	t.Run("过期申请不能同意", func(t *testing.T) {
		const a, b = int64(930031), int64(930032)
		insertFriendPlayers(t, database, a, b)

		// 造一条 8 天前的申请。
		if _, err := database.Exec(`
			INSERT INTO friend_apply (from_id, to_id, status, created_at)
			VALUES (?, ?, 0, NOW() - INTERVAL 8 DAY)`, a, b); err != nil {
			t.Fatalf("seed expired apply: %v", err)
		}
		code, err := repo.Handle(ctx, b, a, true, 7*24*time.Hour)
		require.NoError(t, err, fmt.Sprintf("expired accept: code=%d err=%v", code, err))
		require.EqualValues(t, 6808, code)
		require.EqualValues(t, 0, friendRows(t, database, a), "expired apply must not create friendship")
		var status int32
		if err := database.QueryRow(
			`SELECT status FROM friend_apply WHERE from_id = ? AND to_id = ?`, a, b).Scan(&status); err != nil {
			t.Fatal(err)
		}
		require.EqualValues(t, 2, status)
	})

	t.Run("拒绝后需重新申请", func(t *testing.T) {
		const a, b = int64(930041), int64(930042)
		insertFriendPlayers(t, database, a, b)

		code, _ := repo.Apply(ctx, a, b)
		require.EqualValues(t, 0, code, fmt.Sprintf("apply: %d", code))
		code, _ = repo.Handle(ctx, b, a, false, 7*24*time.Hour)
		require.EqualValues(t, 0, code, fmt.Sprintf("reject: %d", code))
		require.EqualValues(t, 0, friendRows(t, database, a), "reject must not create friendship")
		// 再次同意旧申请：申请已拒绝，应返回「不存在/已处理」。
		code, _ = repo.Handle(ctx, b, a, true, 7*24*time.Hour)
		require.EqualValues(t, 6806, code, fmt.Sprintf("accept rejected apply: code=%d", code))
		// 重新申请复用同一行（唯一键），状态回到待处理。
		code, err := repo.Apply(ctx, a, b)
		require.NoError(t, err, fmt.Sprintf("re-apply: code=%d err=%v", code, err))
		require.EqualValues(t, 0, code)
		var status int32
		if err := database.QueryRow(
			`SELECT status FROM friend_apply WHERE from_id = ? AND to_id = ?`, a, b).Scan(&status); err != nil {
			t.Fatal(err)
		}
		require.EqualValues(t, 0, status)
	})
}
