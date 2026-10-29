// internal/game/seat/flush.go —— 座位状态的合并落库
//
// 为什么需要这一层：座位状态变更原本在房间 Actor 的 Receive 里同步写
// MySQL——一次占座两次往返（seat_room + seat_player），期间该房间的
// 所有消息（他人占座、1h 超时、恢复）全部排队，DB 抖动会直接表现为
// 「抢不到座」。这里把 DB 往返移出 Actor：
//
//	房间 Actor --mark（纯内存）--> Coalescer --每秒合并--> Store
//
// 合并为什么安全：
//   - seat_room 是整房幂等覆盖（Save 只按 room_id 更新 slots），
//     连续 N 次变更只落最后一次，和逐次落库结果相同；
//   - seat_player 按 player_id 只保留最后一次变更（含 upsert/delete 的
//     意图），删除条件带 room_id + seat_index，不会误删换座后的新行。
//
// 合并机制本身在 pkg/flush.Coalescer：这套「键级合并 + 失败放回」的逻辑
// 与玩法无关，原先在这里写了 150 行。本文件只剩座位特有的那部分——
// 脏集合分两级、每级写什么。
package seat

import (
	"context"
	"errors"
	"time"

	"game_study/pkg/flush"
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

// writer 座位落库器：两级脏集合，两种合并语义。
//
//   - rooms：只关心「这间房脏了」，落库时现取房间最新快照。这样连续变更
//     只写最终态，也不需要把快照在管理器里再存一份。
//   - players：关心「最后意图」（占座还是离开，坐在哪），所以值不是零值
//     而是 seatOp。
//
// 分两个 Coalescer 而不是一个带联合结构体的：两级键类型不同（int32/int64）、
// 失败重试互不牵连，合成一个只会让「房间写失败导致玩家也重试」。
type writer struct {
	saver  saver
	logger *log.Logger
	snap   func(roomID int32) (pkgseat.RoomSnapshot, bool)

	rooms   *flush.Coalescer[int32, struct{}]
	players *flush.Coalescer[int64, seatOp]
}

func newWriter(s saver, logger *log.Logger, snap func(int32) (pkgseat.RoomSnapshot, bool)) *writer {
	w := &writer{saver: s, logger: logger, snap: snap}
	w.rooms = flush.NewCoalescer(
		flush.CoalesceConfig{Interval: flushInterval, Timeout: flushTimeout},
		func(ctx context.Context, roomID int32, _ struct{}) error {
			snap, ok := w.snap(roomID)
			if !ok {
				return nil // 房间已被移除：没有可写的状态
			}
			return w.saver.Save(ctx, snap)
		}, logger)
	w.players = flush.NewCoalescer(
		flush.CoalesceConfig{Interval: flushInterval, Timeout: flushTimeout},
		func(ctx context.Context, playerID int64, op seatOp) error {
			if !op.occupied {
				return w.saver.DeletePlayer(ctx, playerID, op.ref)
			}
			return w.saver.SavePlayer(ctx, playerID, op.ref, op.at)
		}, logger)
	return w
}

// start 拉起后台 worker（每次 NewManager 一次）。
func (w *writer) start() {
	w.rooms.Start()
	w.players.Start()
}

// markRoom 登记房间脏标记（房间 Actor 内调用，纯内存）。
func (w *writer) markRoom(roomID int32) { w.rooms.Mark(roomID, struct{}{}) }

// markPlayer 登记玩家数据变更（后写覆盖先写，只落最后一次意图）。
func (w *writer) markPlayer(playerID int64, ref pkgseat.SeatRef, at int64, occupied bool) {
	w.players.Mark(playerID, seatOp{ref: ref, at: at, occupied: occupied})
}

// flush 立即排空两级脏集合（测试与关服路径共用）。
//
// 两级各自 Flush 而不是合成一批：房间和玩家行的写互不依赖，
// 一个失败不该让另一批也重试。
func (w *writer) flush(ctx context.Context) error {
	return errors.Join(w.rooms.Flush(ctx), w.players.Flush(ctx))
}

// stop 停 worker 并做最后一次落库（关服路径：Actor 已 Drain，不会再有新标记）。
//
// 两级都要排空：只停 rooms 会让「关服瞬间的玩家行变更」留在内存里。
func (w *writer) stop() error {
	return errors.Join(w.rooms.Stop(), w.players.Stop())
}
