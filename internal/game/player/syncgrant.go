// internal/game/player/syncgrant.go —— A 类数据（充值/交易）的同步落库
//
// 问题：这笔数据必须「MySQL 提交后才回执」（丢了要赔钱），但事务在
// Actor 里跑，一次 DB 抖动（5s 超时）会把**同分片的其他玩家**一起卡住——
// 分片是 1 个 goroutine 串行跑该分片所有 Actor 的消息。
//
// 解法：Actor 只做三件事——克隆状态、交给工作线程、等回投的结果消息。
//
//	Actor goroutine:  clone() ──┐
//	worker goroutine:            └─ SaveForce(clone) ──> post(grantDoneMsg)
//	Actor goroutine:  处理 grantDoneMsg（更新版本、重建缓存、回执）
//
// 为什么必须克隆：工作线程跑事务期间 Actor 继续处理别的消息，直接读
// a.state 会读到改到一半的状态（脏写），并撞上 map 并发读写。
//
// 为什么同一玩家的同步发货要串行：两次并发事务都会写
// player_writebehind.applied_grants（内存为准），后提交的会覆盖前一笔的
// 幂等键，重投时就查不到记录、可能二次发放。串行用 syncBusy + 队列在
// Actor 内部完成，等待期间不占分片（消息还躺在邮箱里）。
package player

import (
	"context"
	"fmt"
	"time"

	"game_study/pkg/actor"
)

// syncSaveSlots 同时在跑的同步事务上限。
//
// 限制的是「并发数」不是「排队」：超出的请求在自己的 goroutine 里等信号量，
// Actor 不受影响；真正的背压来自 DB 连接池，这里只是别让等待者无限膨胀。
const syncSaveSlots = 64

var syncSaveSem = make(chan struct{}, syncSaveSlots)

// grantDoneMsg 同步落库结果（worker → Actor）。
type grantDoneMsg struct {
	msg     *GrantItemsMsg
	version int64
	err     error
}

// grantSync 发起 A 类落库。同一玩家串行，不同玩家互不影响。
func (a *Actor) grantSync(msg *GrantItemsMsg) {
	if a.repo == nil {
		// 只在测试装配里出现；同步发货必须能落库，宁可明确失败。
		a.replyGrant(msg, fmt.Errorf("player %d sync grant without repository", a.playerID))
		return
	}
	if a.syncBusy {
		a.syncQueue = append(a.syncQueue, msg)
		return
	}
	a.syncBusy = true

	// 没有回投通道（单测直接调用）：保持原语义，内联执行完整流程。
	if a.post == nil || a.self == nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		version, err := a.repo.SaveForce(ctx, a.state)
		cancel()
		a.finishSyncGrant(&grantDoneMsg{msg: msg, version: version, err: err})
		return
	}

	clone := a.state.clone()
	go func() {
		syncSaveSem <- struct{}{}
		defer func() { <-syncSaveSem }()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		version, err := a.repo.SaveForce(ctx, clone)
		cancel()

		if postErr := a.post(a.self, &grantDoneMsg{msg: msg, version: version, err: err}); postErr != nil {
			// Actor 已经不在（下线/迁移）：消费者会在 8s 后超时重投，
			// 幂等键会挡住重复发放。
			a.logger.Warn("sync grant result post failed",
				"source", msg.Source, "biz_id", msg.BizID, "err", postErr)
		}
	}()
}

// finishSyncGrant 在 Actor 内处理落库结果：更新版本、重建缓存、回执。
//
// 不清脏标记的原因：克隆期间 Actor 可能已经改了别的字段（例如玩家同时
// 用了个道具），清掉会把这段新改动从落库链路里抹掉。留着标记，下一轮
// 提交会把内存的新值再写一遍——多一次幂等写，换的是零丢失。
func (a *Actor) finishSyncGrant(done *grantDoneMsg) {
	msg := done.msg
	switch {
	case done.err != nil:
		a.needsReload = true
		a.logger.Error("sync grant save failed",
			"source", msg.Source, "biz_id", msg.BizID, "err", done.err)
		a.replyGrant(msg, done.err)
	default:
		if done.version > a.cacheVersion {
			a.cacheVersion = done.version
		}
		if err := a.resyncCache(done.version); err != nil {
			a.logger.Error("resync cache after sync grant failed",
				"source", msg.Source, "biz_id", msg.BizID, "err", err)
			a.replyGrant(msg, err)
		} else {
			a.logger.Info("sync grant committed",
				"source", msg.Source, "biz_id", msg.BizID, "version", done.version)
			a.replyGrant(msg, nil)
		}
	}

	a.syncBusy = false
	a.nextSync()
}

// nextSync 处理排队中的下一笔（同一玩家串行）。
func (a *Actor) nextSync() {
	if len(a.syncQueue) == 0 {
		return
	}
	msg := a.syncQueue[0]
	a.syncQueue = a.syncQueue[1:]
	a.grantSync(msg)
}

// WithPost 注入「给自己投递消息」的通道（组装层传 sys.Send）。
func (a *Actor) WithPost(fn func(pid *actor.PID, msg any) error) *Actor {
	a.post = fn
	return a
}
