// internal/game/seat/flush.go —— 合并落库（write-behind）
//
// 为什么需要这一层：座位状态变更原本在房间 Actor 的 Receive 里同步写
// MySQL——一次占座两次往返（seat_room + seat_player），期间该房间的
// 所有消息（他人占座、1h 超时、恢复）全部排队，DB 抖动会直接表现为
// 「抢不到座」。这里把 DB 往返移出 Actor：
//
//	房间 Actor --mark（纯内存）--> writer.dirty --每秒合并--> Store
//
// 合并为什么安全：
//   - seat_room 是整房幂等覆盖（Save 只按 room_id 更新 slots），
//     连续 N 次变更只落最后一次，和逐次落库结果相同；
//   - seat_player 按 player_id 只保留最后一次变更（含 upsert/delete 的
//     意图），删除条件带 room_id + seat_index，不会误删换座后的新行。
//
// 失败重试：写失败的 key 重新并回待写集合，下一轮重试；进程崩溃最多
// 丢一个 flushInterval 的内存状态，重启后以库里的快照为准。
package seat

import (
	"context"
	"errors"
	"sync"
	"time"

	"game_study/pkg/log"
	pkgseat "game_study/pkg/seat"
)

const (
	// flushInterval 合并窗口。10 个房间上限 = 每秒至多 10 条 upsert，
	// 对数据库可忽略；崩溃窗口也只有 1s。
	flushInterval = time.Second
	// flushTimeout 单轮落库超时（与房间 Actor 彻底解耦，慢查询不会
	// 反压到业务消息）。
	flushTimeout = 3 * time.Second
)

// saver 落库出口（生产实现只有 *Store）。
//
// 抽成接口只为单测能验证「合并写 / 失败重试」这两条分支——没有第二个
// 生产实现，不是为将来扩展预留的抽象。
type saver interface {
	Save(ctx context.Context, snap pkgseat.RoomSnapshot) error
	SavePlayer(ctx context.Context, playerID int64, ref pkgseat.SeatRef, occupiedAt int64) error
	DeletePlayer(ctx context.Context, playerID int64, ref pkgseat.SeatRef) error
}

// seatOp 玩家数据的最后一次变更（occupied=false 表示该行应被删除）。
type seatOp struct {
	ref      pkgseat.SeatRef
	at       int64
	occupied bool
}

// writer 合并落库 worker。
type writer struct {
	saver  saver
	logger *log.Logger
	// snap 落库时现取房间最新快照：这样连续变更只写最终态，
	// 也不需要把快照在管理器里再存一份。
	snap func(roomID int32) (pkgseat.RoomSnapshot, bool)

	mu      sync.Mutex
	rooms   map[int32]struct{}
	players map[int64]seatOp

	stopCh   chan struct{}
	done     chan struct{}
	stopOnce sync.Once
}

func newWriter(s saver, logger *log.Logger, snap func(int32) (pkgseat.RoomSnapshot, bool)) *writer {
	return &writer{
		saver:   s,
		logger:  logger,
		snap:    snap,
		rooms:   make(map[int32]struct{}, 16),
		players: make(map[int64]seatOp, 16),
		stopCh:  make(chan struct{}),
		done:    make(chan struct{}),
	}
}

// start 拉起后台 worker（每次 NewManager 一次）。
func (w *writer) start() { go w.loop() }

// markRoom 登记房间脏标记（房间 Actor 内调用，纯内存）。
func (w *writer) markRoom(roomID int32) {
	w.mu.Lock()
	w.rooms[roomID] = struct{}{}
	w.mu.Unlock()
}

// markPlayer 登记玩家数据变更（后写覆盖先写，只落最后一次意图）。
func (w *writer) markPlayer(playerID int64, ref pkgseat.SeatRef, at int64, occupied bool) {
	w.mu.Lock()
	w.players[playerID] = seatOp{ref: ref, at: at, occupied: occupied}
	w.mu.Unlock()
}

func (w *writer) loop() {
	defer close(w.done)
	t := time.NewTicker(flushInterval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			if err := w.flushOnce(context.Background()); err != nil {
				w.logger.Warn("seat flush failed, will retry", "err", err)
			}
		case <-w.stopCh:
			return
		}
	}
}

// stop 停 worker 并做最后一次落库（关服路径：Actor 已 Drain，不会再有新标记）。
func (w *writer) stop() error {
	w.stopOnce.Do(func() { close(w.stopCh) })
	<-w.done
	return w.flushOnce(context.Background())
}

// flushOnce 取走当前脏集合写库；失败的 key 并回集合等下一轮。
//
// 先换出新 map 再解锁写库：写库可能耗时，不能持锁做 IO。
func (w *writer) flushOnce(ctx context.Context) error {
	w.mu.Lock()
	rooms, players := w.rooms, w.players
	w.rooms = make(map[int32]struct{}, len(rooms))
	w.players = make(map[int64]seatOp, len(players))
	w.mu.Unlock()

	if len(rooms) == 0 && len(players) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, flushTimeout)
	defer cancel()

	var errs []error
	for id := range rooms {
		snap, ok := w.snap(id)
		if !ok {
			continue // 房间已被移除：没有可写的状态
		}
		if err := w.saver.Save(ctx, snap); err != nil {
			w.logger.Warn("seat room flush failed", "room_id", id, "err", err)
			errs = append(errs, err)
			w.retryRoom(id)
		}
	}
	for playerID, op := range players {
		var err error
		if op.occupied {
			err = w.saver.SavePlayer(ctx, playerID, op.ref, op.at)
		} else {
			err = w.saver.DeletePlayer(ctx, playerID, op.ref)
		}
		if err != nil {
			w.logger.Warn("seat player flush failed", "player_id", playerID, "err", err)
			errs = append(errs, err)
			w.retryPlayer(playerID, op)
		}
	}
	return errors.Join(errs...)
}

// retryRoom 把失败的房间放回待写集合。
func (w *writer) retryRoom(roomID int32) {
	w.mu.Lock()
	w.rooms[roomID] = struct{}{}
	w.mu.Unlock()
}

// retryPlayer 把失败的玩家放回待写集合。
//
// 只在集合里没有该玩家时放回：本轮写库期间可能已经有更新的变更，
// 直接覆盖会让「新状态被旧状态顶掉」。
func (w *writer) retryPlayer(playerID int64, op seatOp) {
	w.mu.Lock()
	if _, ok := w.players[playerID]; !ok {
		w.players[playerID] = op
	}
	w.mu.Unlock()
}
