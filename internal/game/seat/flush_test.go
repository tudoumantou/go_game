// internal/game/seat/flush_test.go —— 合并写与失败重试
package seat

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"game_study/pkg/log"
	pkgseat "game_study/pkg/seat"
)

type saverCall struct {
	kind string // room / save / delete
	id   int64
	ref  pkgseat.SeatRef
}

// fakeSaver 记录落库调用；fail 为真时全部返回错误。
type fakeSaver struct {
	calls []saverCall
	fail  bool
}

func (f *fakeSaver) Save(_ context.Context, snap pkgseat.RoomSnapshot) error {
	f.calls = append(f.calls, saverCall{kind: "room", id: int64(snap.ID)})
	if f.fail {
		return errors.New("db down")
	}
	return nil
}

func (f *fakeSaver) SavePlayer(_ context.Context, playerID int64, ref pkgseat.SeatRef, _ int64) error {
	f.calls = append(f.calls, saverCall{kind: "save", id: playerID, ref: ref})
	if f.fail {
		return errors.New("db down")
	}
	return nil
}

func (f *fakeSaver) DeletePlayer(_ context.Context, playerID int64, ref pkgseat.SeatRef) error {
	f.calls = append(f.calls, saverCall{kind: "delete", id: playerID, ref: ref})
	if f.fail {
		return errors.New("db down")
	}
	return nil
}

func newTestWriter(s saver) *writer {
	logger := log.New("seat-test", log.WithLevel("error"))
	return newWriter(s, logger, func(roomID int32) (pkgseat.RoomSnapshot, bool) {
		return pkgseat.RoomSnapshot{ID: roomID}, true
	})
}

// 同一房间多次变更只落最后一次；同一玩家多次变更只保留最终意图。
func TestFlushMergesDirtyMarks(t *testing.T) {
	f := &fakeSaver{}
	w := newTestWriter(f)

	w.markRoom(100)
	w.markRoom(100)
	w.markRoom(200)

	a := pkgseat.SeatRef{RoomID: 100, SeatIndex: 1}
	b := pkgseat.SeatRef{RoomID: 200, SeatIndex: 3}
	w.markPlayer(7, a, 1000, true)
	w.markPlayer(7, b, 2000, true) // 后写覆盖先写

	require.NoError(t, w.flush(context.Background()))

	// map 遍历无序，房间顺序不做断言；只校验去重结果与玩家取最终值。
	require.Len(t, f.calls, 3, "两个房间各一次 + 玩家一次")
	var roomIDs []int64
	for _, c := range f.calls {
		if c.kind == "room" {
			roomIDs = append(roomIDs, c.id)
		}
	}
	require.ElementsMatch(t, []int64{100, 200}, roomIDs, "同一房间多次变更只落一次")
	require.Contains(t, f.calls, saverCall{kind: "save", id: 7, ref: b}, "玩家只落最终座位")
	require.Zero(t, w.rooms.Len(), "落库成功后脏集合应清空")
	require.Zero(t, w.players.Len())
}

// 占座后立刻离开（同一轮窗口内）：只应发出删除，不应先写后删。
func TestFlushDeleteOverridesOccupy(t *testing.T) {
	f := &fakeSaver{}
	w := newTestWriter(f)

	ref := pkgseat.SeatRef{RoomID: 100, SeatIndex: 1}
	w.markPlayer(7, ref, 1000, true)
	w.markPlayer(7, ref, 0, false)

	require.NoError(t, w.flush(context.Background()))
	require.Equal(t, []saverCall{{kind: "delete", id: 7, ref: ref}}, f.calls)
}

// 写库失败：脏标记必须保留，下一轮重试成功后才清空。
func TestFlushKeepsDirtyOnFailure(t *testing.T) {
	f := &fakeSaver{fail: true}
	w := newTestWriter(f)

	w.markRoom(100)
	ref := pkgseat.SeatRef{RoomID: 100, SeatIndex: 1}
	w.markPlayer(7, ref, 1000, true)

	require.Error(t, w.flush(context.Background()))
	require.Equal(t, 1, w.rooms.Len(), "失败后房间脏标记必须保留")
	require.Equal(t, 1, w.players.Len(), "失败后玩家脏标记必须保留")

	f.fail = false
	require.NoError(t, w.flush(context.Background()))
	require.Zero(t, w.rooms.Len())
	require.Zero(t, w.players.Len())
}

// 空集合不应产生任何 SQL。
func TestFlushNoopWhenClean(t *testing.T) {
	f := &fakeSaver{}
	w := newTestWriter(f)
	require.NoError(t, w.flush(context.Background()))
	require.Empty(t, f.calls)
}
