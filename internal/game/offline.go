// internal/game/offline.go —— 离线暂存 drain（设计文档 12.6.2 第 6 条 / 13.3 模式②）
//
// 玩家离线期间，跨服私聊等消息进 `pend:{biz}:{pid}` 队列（Redis List，有上限 + TTL）。
// 登录后统一消费并补投，避免「离线期间的消息永远不会到」。
//
// 架构职责：只做「搬运」——把暂存队列里的消息翻译成协议推送，不改任何
// 玩家状态，因此不经过 Actor（不占玩家处理窗口，也不需要快照 CAS）。
//
// 协作关系：chat.Messenger.PendOffline 负责「存」（server.go），
// handleEnterGame 在进游戏回执后触发本文件的「取」。
//
// 设计取舍：drain 是尽力而为——PendDrain 取出即清空（至少一次语义），
// 推送失败不重试、剩余消息随清空丢弃；用短超时的独立 ctx，Redis 卡住
// 不能无限拖住登录流程。
package game

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"game_study/internal/game/player"
	pkgmq "game_study/pkg/mq"
	pkgseat "game_study/pkg/seat"

	gamev1 "game_study/pkg/pb/game/v1"
	"google.golang.org/protobuf/proto"
)

// 离线暂存队列的业务标识（与 mq 层的 PendPush 保持一致）。
const (
	pendBizChat = "chat"
	pendBizSeat = "seat"
)

// offlineSeatReward 座位奖励的离线载荷：与在线投递同一份字段，
// 登录时按 occupy_id 做幂等键重放。
type offlineSeatReward struct {
	PlayerID int64 `json:"player_id"`
	OccupyID int64 `json:"occupy_id"`
	Tier     int32 `json:"tier"`
	Minutes  int64 `json:"minutes"`
	Amount   int64 `json:"amount"`
}

// drainTimeout 单个业务队列 drain 的超时（登录期不能无限等）。
const drainTimeout = 2 * time.Second

// drainOffline 登录后补投离线消息。
//
// 设计取舍：drain 放在组装层而非 Actor 内 —— 补投只做「推送」，不改玩家状态，
// 走 sendToClient 即可，不必占用 Actor 处理窗口；失败仅记日志，不阻断登录。
func (s *Server) drainOffline(playerID int64, sessionID uint64) {
	if s.presence == nil {
		return
	}
	// 独立 ctx + 短超时：补投是尽力而为的收尾工作，Redis 卡住不能无限等
	// （调用方是登录流程里的 goroutine，超时即放弃本次补投）。
	ctx, cancel := context.WithTimeout(context.Background(), drainTimeout)
	defer cancel()

	s.drainOfflineChat(ctx, playerID, sessionID)
	s.drainOfflineSeat(ctx, playerID)
}

// drainOfflineSeat 补发离线期间的座位奖励。
//
// 走玩家 Actor 的 GrantItemsMsg（BizID = occupy_id），与在线路径同一通道、
// 同一幂等键：即使这段奖励在线时已投递过又重放，也不会二次发放。
func (s *Server) drainOfflineSeat(ctx context.Context, playerID int64) {
	msgs, err := s.presence.PendDrain(ctx, pendBizSeat, playerID)
	if err != nil {
		s.logger.Warn("drain offline seat failed", "player_id", playerID, "err", err)
		return
	}
	if len(msgs) == 0 {
		return
	}
	pid, ok := s.players.Get(playerID)
	if !ok {
		return // 正常不会发生：drain 在进游戏后触发
	}

	granted := 0
	for _, raw := range msgs {
		var ev offlineSeatReward
		if err := json.Unmarshal(raw, &ev); err != nil {
			s.logger.Warn("bad offline seat payload", "player_id", playerID, "err", err)
			continue
		}
		if ev.Amount <= 0 {
			continue
		}
		if err := s.actorSys.Send(pid, &player.GrantItemsMsg{
			BizID:  fmt.Sprintf("%d", ev.OccupyID),
			Source: "seat",
			Items:  []player.GrantItem{{ItemID: pkgseat.RewardItem, Count: ev.Amount}},
		}); err != nil {
			s.logger.Warn("grant offline seat reward failed", "player_id", playerID, "err", err)
			continue
		}
		granted++
	}
	s.logger.Info("offline seat drained", "player_id", playerID, "count", granted)
}

// drainOfflineChat 补投离线私聊。
//
// 由 handleEnterGame 以 goroutine 触发（登录流程不阻塞在补投上）；
// ctx 的短超时保证这个 goroutine 最迟 2 秒内退出，不会泄漏。
func (s *Server) drainOfflineChat(ctx context.Context, playerID int64, sessionID uint64) {
	// PendDrain 在同一个管道里 LRange + Del：取出即清空，语义是「至少一次」
	// 而非「恰好一次」。所以推送失败不重试——消息已离开 Redis，重试只会放大
	// 延迟；单条失败立即 return，剩余消息随清空丢弃（离线补投本就尽力而为）。
	msgs, err := s.presence.PendDrain(ctx, pendBizChat, playerID)
	if err != nil {
		s.logger.Warn("drain offline chat failed", "player_id", playerID, "err", err)
		return
	}
	if len(msgs) == 0 {
		return
	}

	delivered := 0
	for _, raw := range msgs {
		var cm pkgmq.ChatCrossMsg
		if err := json.Unmarshal(raw, &cm); err != nil {
			s.logger.Warn("bad offline chat payload", "player_id", playerID, "err", err)
			continue
		}
		// 6010 = ChatMessageNotify（4.9 Demo 客户端协议契约，跨服扇出同格式）。
		// SendTime 毫秒 → 秒：协议侧时间戳是秒级。
		notify := &gamev1.ChatMessageNotify{
			MsgId:      formatMsgID(cm.MsgID),
			Channel:    cm.Channel,
			SenderId:   cm.FromID,
			SenderName: cm.FromName,
			PeerId:     cm.ToID,
			Content:    cm.Content,
			SentAt:     cm.SendTime / 1000,
		}
		body, err := proto.Marshal(notify)
		if err != nil {
			continue
		}
		if err := s.sendToClient(sessionID, 6010, body); err != nil {
			s.logger.Warn("push offline chat failed", "player_id", playerID, "err", err)
			return
		}
		delivered++
	}
	s.logger.Info("offline chat drained", "player_id", playerID, "count", delivered)
}

// formatMsgID 把 int64 消息号转成协议里的字符串形式。
func formatMsgID(v int64) string { return strconv.FormatInt(v, 10) }
