// internal/game/seat/writer.go —— 座位快照的异步落库器（脏合并）
//
// 为什么不在 onChange 里同步写库：onChange 由房间 Actor 在自己的 goroutine
// 里同步调用，一次 MySQL 抖动就会把该房间所有玩家的占座/离开一起卡住。
// 这里改成「记录脏房间 + 定时合并写」，把 Actor 与 I/O 解耦。
//
// 为什么可以合并：落库的是「整房快照」而不是增量，后到的快照天然覆盖前一个，
// 中间态没有保留价值。这与落库子系统「先内存合并再批量 UPSERT」的思路一致。
package seat

import (
	"context"
	"sync"
	"time"

	"game_study/pkg/log"
	pkgseat "game_study/pkg/seat"
)

// seatFlushInterval 合并窗口：200ms 内的多次变更合并为一次写。
//
// 取值取舍：太短起不到合并作用；太长则崩溃时丢失窗口内的变更。
// 座位变更是低频操作（玩家手动点击），200ms 足够合并且丢窗口可接受——
// 真丢了也只是「重启后这几个座位变空」，玩家重抢即可，不会影响奖励正确性。
const seatFlushInterval = 200 * time.Millisecond

// writer 座位快照异步落库器（单 goroutine + 脏表合并）。
type writer struct {
	store  *Store
	logger *log.Logger

	mu    sync.Mutex
	dirty map[string]pkgseat.RoomSnapshot // 房间名 → 最新快照

	stop chan struct{}
	done chan struct{}
	once sync.Once
}

func newWriter(store *Store, logger *log.Logger) *writer {
	w := &writer{
		store:  store,
		logger: logger,
		dirty:  make(map[string]pkgseat.RoomSnapshot),
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
	}
	go w.loop()
	return w
}

// mark 标记房间为脏（只保留最新快照），非阻塞。
func (w *writer) mark(snap pkgseat.RoomSnapshot) {
	name := pkgseat.RoomName(snap.Tier, snap.RoomIndex)
	w.mu.Lock()
	w.dirty[name] = snap
	w.mu.Unlock()
}

func (w *writer) loop() {
	defer close(w.done)
	ticker := time.NewTicker(seatFlushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			w.flush()
		case <-w.stop:
			w.flush() // 退出前再刷一次，尽量不丢最后一次变更
			return
		}
	}
}

// flush 把当前脏表整体写出。
func (w *writer) flush() {
	w.mu.Lock()
	if len(w.dirty) == 0 {
		w.mu.Unlock()
		return
	}
	batch := w.dirty
	w.dirty = make(map[string]pkgseat.RoomSnapshot, len(batch))
	w.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	for name, snap := range batch {
		if err := w.store.SaveRoom(ctx, snap); err != nil {
			w.logger.Warn("seat slot persist failed", "room", name, "err", err)
			// 放回重试，但绝不覆盖期间产生的更新快照——否则会把已落库的
			// 新状态用旧状态盖回去。
			w.mu.Lock()
			if _, exists := w.dirty[name]; !exists {
				w.dirty[name] = snap
			}
			w.mu.Unlock()
		}
	}
}

// Close 停止后台协程并做最后一次刷盘（幂等）。
func (w *writer) Close() {
	w.once.Do(func() {
		close(w.stop)
		<-w.done
	})
}
