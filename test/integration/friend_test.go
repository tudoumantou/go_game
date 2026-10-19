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
	"testing"
	"time"

	"game_study/internal/game/module/friend"
	"game_study/pkg/db"
)

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

func TestFriendFlow(t *testing.T) {
	database, _, _ := startStorage(t)
	ctx := context.Background()
	repo := friend.NewRepo(database)

	t.Run("申请→同意→互见→删除", func(t *testing.T) {
		const a, b = int64(930001), int64(930002)
		insertFriendPlayers(t, database, a, b)

		code, err := repo.Apply(ctx, a, b)
		if err != nil || code != 0 {
			t.Fatalf("apply: code=%d err=%v", code, err)
		}
		// 申请方自己还看不到好友，接收方有一条待处理。
		if n := friendRows(t, database, a); n != 0 {
			t.Fatalf("apply must not create friendship, got %d rows", n)
		}
		_, applies, err := repo.List(ctx, b)
		if err != nil || len(applies) != 1 || applies[0].FromID != a {
			t.Fatalf("apply list = %+v err=%v", applies, err)
		}

		// 重复申请：幂等，不产生第二条。
		if code, err = repo.Apply(ctx, a, b); err != nil || code != 6805 {
			t.Fatalf("duplicate apply: code=%d err=%v", code, err)
		}

		if code, err = repo.Handle(ctx, b, a, true, 7*24*time.Hour); err != nil || code != 0 {
			t.Fatalf("accept: code=%d err=%v", code, err)
		}
		// 双边两行，且双方列表互见。
		if friendRows(t, database, a) != 1 || friendRows(t, database, b) != 1 {
			t.Fatalf("expect one row each: a=%d b=%d", friendRows(t, database, a), friendRows(t, database, b))
		}
		fa, _, _ := repo.List(ctx, a)
		fb, _, _ := repo.List(ctx, b)
		if len(fa) != 1 || fa[0].PlayerID != b || len(fb) != 1 || fb[0].PlayerID != a {
			t.Fatalf("mutual list broken: a=%+v b=%+v", fa, fb)
		}
		if fa[0].Name == "" {
			t.Fatal("friend name must be filled from player table")
		}

		// 已是好友再申请 / 再同意都幂等。
		if code, _ = repo.Apply(ctx, a, b); code != 6803 {
			t.Fatalf("apply when already friend: code=%d", code)
		}
		if code, _ = repo.Handle(ctx, b, a, true, 7*24*time.Hour); code != 6803 {
			t.Fatalf("accept twice: code=%d", code)
		}

		// 删除后双方都空；重复删除仍成功。
		if err := repo.Delete(ctx, a, b); err != nil {
			t.Fatalf("delete: %v", err)
		}
		if friendRows(t, database, a) != 0 || friendRows(t, database, b) != 0 {
			t.Fatal("delete must remove both directions")
		}
		if err := repo.Delete(ctx, a, b); err != nil {
			t.Fatalf("delete must be idempotent: %v", err)
		}
	})

	t.Run("互相申请直接成为好友", func(t *testing.T) {
		const a, b = int64(930011), int64(930012)
		insertFriendPlayers(t, database, a, b)

		if code, err := repo.Apply(ctx, a, b); err != nil || code != 0 {
			t.Fatalf("a→b: code=%d err=%v", code, err)
		}
		// b 反向申请：应直接建立关系，而不是再插一条待处理。
		if code, err := repo.Apply(ctx, b, a); err != nil || code != 0 {
			t.Fatalf("b→a: code=%d err=%v", code, err)
		}
		if friendRows(t, database, a) != 1 || friendRows(t, database, b) != 1 {
			t.Fatal("mutual apply must create friendship directly")
		}
		_, applies, _ := repo.List(ctx, b)
		if len(applies) != 0 {
			t.Fatalf("no pending apply should remain: %+v", applies)
		}
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
		if err != nil || code != 6804 {
			t.Fatalf("apply when full: code=%d err=%v", code, err)
		}
		if n := friendRows(t, database, b); n != 0 {
			t.Fatalf("must not create one-sided friendship: %d", n)
		}
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
		if err != nil || code != 6808 {
			t.Fatalf("expired accept: code=%d err=%v", code, err)
		}
		if friendRows(t, database, a) != 0 {
			t.Fatal("expired apply must not create friendship")
		}
		var status int32
		if err := database.QueryRow(
			`SELECT status FROM friend_apply WHERE from_id = ? AND to_id = ?`, a, b).Scan(&status); err != nil {
			t.Fatal(err)
		}
		if status != 2 {
			t.Fatalf("expired apply status = %d, want 2 (rejected)", status)
		}
	})

	t.Run("拒绝后需重新申请", func(t *testing.T) {
		const a, b = int64(930041), int64(930042)
		insertFriendPlayers(t, database, a, b)

		if code, _ := repo.Apply(ctx, a, b); code != 0 {
			t.Fatalf("apply: %d", code)
		}
		if code, _ := repo.Handle(ctx, b, a, false, 7*24*time.Hour); code != 0 {
			t.Fatalf("reject: %d", code)
		}
		if friendRows(t, database, a) != 0 {
			t.Fatal("reject must not create friendship")
		}
		// 再次同意旧申请：申请已拒绝，应返回「不存在/已处理」。
		if code, _ := repo.Handle(ctx, b, a, true, 7*24*time.Hour); code != 6806 {
			t.Fatalf("accept rejected apply: code=%d", code)
		}
		// 重新申请复用同一行（唯一键），状态回到待处理。
		if code, err := repo.Apply(ctx, a, b); err != nil || code != 0 {
			t.Fatalf("re-apply: code=%d err=%v", code, err)
		}
		var status int32
		if err := database.QueryRow(
			`SELECT status FROM friend_apply WHERE from_id = ? AND to_id = ?`, a, b).Scan(&status); err != nil {
			t.Fatal(err)
		}
		if status != 0 {
			t.Fatalf("re-apply status = %d, want 0", status)
		}
	})
}
