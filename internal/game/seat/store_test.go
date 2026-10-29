// internal/game/seat/store_test.go —— 无 DB 时的仓储行为
//
// 这里只测一件事：没有数据库连接时，五个入口都返回 ErrNoDB 而不是
// nil deref。真正值钱的 SQL 语义（换座删除条件带 seat_index）由
// writer 的合并测试覆盖，不重复。
package seat

import (
	"context"
	"errors"
	"testing"

	"game_study/internal/game/module"
	pkgseat "game_study/pkg/seat"
)

// TestStoreWithoutDBReturnsErrNoDB 无连接时每个入口都要报「没库」。
//
// 反向验证过：把 Save 里的 Ready 判断去掉，本用例以 panic 失败
// （nil pointer dereference），不是静默通过。
func TestStoreWithoutDBReturnsErrNoDB(t *testing.T) {
	s := NewStore(nil)
	ctx := context.Background()

	if s.Ready() {
		t.Fatal("NewStore(nil).Ready() = true, want false")
	}
	if err := s.EnsureRooms(ctx, []pkgseat.RoomSnapshot{{ID: 1}}); !errors.Is(err, module.ErrNoDB) {
		t.Errorf("EnsureRooms err = %v, want ErrNoDB", err)
	}
	if _, err := s.Load(ctx); !errors.Is(err, module.ErrNoDB) {
		t.Errorf("Load err = %v, want ErrNoDB", err)
	}
	if err := s.Save(ctx, pkgseat.RoomSnapshot{ID: 1}); !errors.Is(err, module.ErrNoDB) {
		t.Errorf("Save err = %v, want ErrNoDB", err)
	}
	if err := s.SavePlayer(ctx, 7, pkgseat.SeatRef{RoomID: 1, SeatIndex: 2}, 100); !errors.Is(err, module.ErrNoDB) {
		t.Errorf("SavePlayer err = %v, want ErrNoDB", err)
	}
	if err := s.DeletePlayer(ctx, 7, pkgseat.SeatRef{RoomID: 1, SeatIndex: 2}); !errors.Is(err, module.ErrNoDB) {
		t.Errorf("DeletePlayer err = %v, want ErrNoDB", err)
	}
}
