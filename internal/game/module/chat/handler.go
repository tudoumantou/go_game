// handler.go —— 协议处理器：发送、历史记录。
package chat

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	pkgmq "game_study/pkg/mq"
	"google.golang.org/protobuf/proto"
	"strconv"
	"time"
	"unicode/utf8"

	"game_study/internal/common/errcode"
	"game_study/internal/game/module"

	gamev1 "game_study/pkg/pb/game/v1"
)

// onSend 发送私聊（17.3：本端 out → 对方在线本服直投 / 跨服走 Kafka / 离线 pend）。
func (m *Module) onSend(ctx *module.Context, req *gamev1.ChatSendReq, resp *gamev1.ChatSendResp) error {
	*resp = gamev1.ChatSendResp{}
	// channel=0 按私聊兼容（老客户端不传频道字段），其余频道 P6 未开放
	if req.Channel != channelPrivate && req.Channel != 0 {
		return ctx.FailCode(resp, errcode.ErrChannelClosed) // P6 仅开放私聊
	}
	if req.Content == "" || req.PeerId <= 0 {
		return ctx.FailCode(resp, errcode.ErrContentEmpty)
	}
	if len(req.Content) > maxContentBytes || utf8.RuneCountInString(req.Content) > maxContentRunes {
		return ctx.FailCode(resp, errcode.ErrContentTooLong)
	}
	// 频控放在内容校验之后：被拒绝的脏请求不占用额度（否则玩家打字打错
	// 一次就要等一秒），只有真正要发出去的才计数。
	if m.chatLimiter != nil && !m.chatLimiter.Allow(strconv.FormatInt(ctx.State.PlayerID(), 10)) {
		return ctx.FailCode(resp, errcode.ErrChatTooFast)
	}

	// msgID 先于一切 IO 生成：历史行与投递消息必须共用同一个 ID，
	// 否则 uk_owner_msg 幂等与「撤回/引用」都失去锚点
	msgID := nextMsgID()
	now := time.Now().Unix()
	// 身份一律取 Actor 内会话，不信协议体携带的发送者字段（防伪造他人发言）
	fromID := ctx.State.PlayerID()
	fromName := ctx.State.Profile().Name

	// ① 本端历史（direction=out）
	if m.repo != nil {
		// 用 context.Background() 而非请求 ctx：onSend 返回后 ctx 即取消，
		// 会把这次落库误杀；历史丢失只 Warn——不回错误码、不阻塞发送体验
		if err := m.repo.Insert(context.Background(), fromID, req.PeerId, msgID, DirectionOut, channelPrivate, req.Content); err != nil {
			m.logger.Warn("save outbound chat failed", "err", err)
		}
	}

	// ② 投递对端
	notify := &gamev1.ChatMessageNotify{
		MsgId:      fmt.Sprintf("%d", msgID),
		Channel:    channelPrivate,
		SenderId:   fromID,
		SenderName: fromName,
		PeerId:     req.PeerId,
		Content:    req.Content,
		SentAt:     now,
	}
	// 简单 proto 消息（无 map/oneof）Marshal 不会失败，忽略错误的务实取舍
	raw, _ := proto.Marshal(notify)
	// 注意单位换算只在这一处：协议 SentAt 用秒（客户端契约），
	// MQ 跨消息 SendTime 用毫秒（pkgmq 契约）， downstream 各自解各自的
	crossMsg := &pkgmq.ChatCrossMsg{
		MsgID: msgID, FromID: fromID, FromName: fromName,
		ToID: req.PeerId, Channel: channelPrivate, Content: req.Content, SendTime: now * 1000,
	}
	// 投递失败不影响回包：私聊是尽力而为链路，回包只表示「服务端已受理」
	m.deliver(req.PeerId, raw, crossMsg)

	resp.Code = int32(errcode.OK)
	resp.MsgId = fmt.Sprintf("%d", msgID)
	resp.SentAt = now
	return nil
}

// deliver 按在线状态选择投递路径。
//
// 三路互斥：离线 → Redis 暂存；本服在线 → 落 in 历史 + Actor 推送；
// 跨服在线 → Kafka 生产（对端所在节点的广播消费者落地后处理）。

// onHistory 拉取与某好友的聊天历史。
func (m *Module) onHistory(ctx *module.Context, req *gamev1.ChatHistoryReq, resp *gamev1.ChatHistoryResp) error {
	*resp = gamev1.ChatHistoryResp{Code: int32(errcode.OK)}
	if !m.ready() {
		return nil
	}
	lines, err := m.repo.ListRecent(context.Background(), ctx.State.PlayerID(), req.PeerId, int(req.Limit))
	if err != nil {
		return err
	}
	// 查询是倒序，回包按时间正序
	// 逆序遍历把 DESC 结果反转：比在 SQL 里再套一层子查询省一次排序
	selfID := ctx.State.PlayerID()
	for i := len(lines) - 1; i >= 0; i-- {
		l := lines[i]
		msg := &gamev1.ChatMessageNotify{
			MsgId:   fmt.Sprintf("%d", l.MsgID),
			Channel: channelPrivate,
			Content: l.Content,
		}
		// 用 direction 还原对话视图：一行 out 一行 in 共享同一 msg_id，
		// 比每行都存 sender_id 省一半存储；回包时按 owner 视角反推收发方
		if l.Direction == DirectionOut {
			msg.SenderId = selfID
			msg.PeerId = req.PeerId
		} else {
			msg.SenderId = req.PeerId
			msg.PeerId = selfID
		}
		resp.Messages = append(resp.Messages, msg)
	}
	// 取满一页即提示可能还有更多：精确判断要额外 count，
	// 私聊翻页场景用「取满」近似足够（客户端可再拉一页确认）
	resp.HasMore = len(lines) >= int(req.Limit)
	return nil
}

// nextMsgID 生成消息 ID（时间戳毫秒 << 20 | 20 位随机）。
//
// 位运算组装的三重考量：
//  1. 毫秒时间戳占高位 → ID 整体随时间单调递增，MySQL 主键顺序插入、
//     索引页分裂最少；当前毫秒约 2^40.7，左移 20 位后约 2^60.7 < 2^63，
//     不会溢出成负数；
//  2. 低 20 位（1,048,576 个槽位）放同毫秒随机数：这是**概率唯一**而非
//     绝对唯一，碰撞概率上界 n(n-1)/2^21——每毫秒 10 条约 0.004%，
//     100 条约 0.5%，1000 条约 48%。私聊实际峰值远低于每毫秒 100 条，
//     所以够用；真要支撑每毫秒上千条，必须换成「节点 ID + 序列」发号器
//     （或中心化发号），当前位宽不够；
//  3. 用 crypto/rand 而非 math/rand：msgID 会暴露给客户端且是 Kafka 幂等键，
//     不要求密码学强度，但要避免可预测的序列被客户端猜中后伪造重复消息。
//
// 碰撞的后果：chat_record 的 uk_owner_msg（owner_id, msg_id）会把后一条
// 静默忽略，Kafka 侧同 ID 也会被去重——即「极低概率丢一条消息」，不是
// 串号。这是选择无中心化发号器的已知代价。
func nextMsgID() int64 {
	var b [4]byte
	_, _ = rand.Read(b[:])
	// 只取 4 字节的低 20 位：保证随机段不侵占时间戳的位域
	rnd := int64(binary.BigEndian.Uint32(b[:]) & 0xFFFFF)
	return time.Now().UnixMilli()<<20 | rnd
}
