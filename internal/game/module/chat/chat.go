// internal/game/module/chat/chat.go —— 聊天模块（私聊全链路，17.3）
package chat

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"time"

	"game_study/internal/game/module"
	"game_study/pkg/codec"
	"game_study/pkg/db"
	"game_study/pkg/log"
	pkgmq "game_study/pkg/mq"

	gamev1 "game_study/pkg/pb/game/v1"
	"google.golang.org/protobuf/proto"
)

// 协议号（4.9：6001/6002 发送，6010 推送，6020/6021 历史）。
const (
	msgIDSendReq     = codec.MsgChatSendReq
	msgIDSendResp    = codec.MsgChatSendResp
	msgIDNotify      = codec.MsgChatMessageNotify
	msgIDHistoryReq  = codec.MsgChatHistoryReq
	msgIDHistoryResp = codec.MsgChatHistoryResp
)

// 业务码（4.10）。
const (
	codeOK            int32 = 0
	codeChannelClosed int32 = 4001
	codeContentEmpty  int32 = 4005
)

// 私聊频道。
const channelPrivate int32 = 2

// 消息方向。
const (
	DirectionOut int32 = 0 // 发出
	DirectionIn  int32 = 1 // 收到
)

// Messenger 投递能力（由游戏服组装层实现：在线判定 + 本服推送 + 跨服生产 + 离线暂存）。
type Messenger interface {
	SelfNode() string
	OnlineNode(ctx context.Context, playerID int64) (nodeID string, ok bool)
	Push(ctx context.Context, playerID int64, msgID uint32, body []byte) bool
	ProduceCross(ctx context.Context, msg *pkgmq.ChatCrossMsg) error
	PendOffline(ctx context.Context, playerID int64, msg *pkgmq.ChatCrossMsg) error
}

// Repo 聊天记录仓储。
type Repo struct {
	db *db.DB
}

// NewRepo 创建仓储。
func NewRepo(database *db.DB) *Repo {
	return &Repo{db: database}
}

// Insert 落库一条聊天记录（uk_owner_msg 幂等，重复插入忽略）。
func (r *Repo) Insert(ctx context.Context, ownerID, peerID, msgID int64, direction int32, channel int32, content string) error {
	const q = `INSERT INTO chat_record (owner_id, peer_id, msg_id, direction, channel, content)
	           VALUES (?, ?, ?, ?, ?, ?)
	           ON DUPLICATE KEY UPDATE id = id`
	if _, err := r.db.ExecContext(ctx, q, ownerID, peerID, msgID, direction, channel, content); err != nil {
		return fmt.Errorf("insert chat_record: %w", err)
	}
	return nil
}

// ChatLine 聊天记录行。
type ChatLine struct {
	MsgID     int64
	Direction int32
	Content   string
}

// ListRecent 读取与某好友的最近 N 条记录。
func (r *Repo) ListRecent(ctx context.Context, ownerID, peerID int64, limit int) ([]ChatLine, error) {
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	const q = `SELECT msg_id, direction, content
	           FROM chat_record
	           WHERE owner_id = ? AND peer_id = ?
	           ORDER BY id DESC LIMIT ?`
	rows, err := r.db.QueryContext(ctx, q, ownerID, peerID, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var out []ChatLine
	for rows.Next() {
		var c ChatLine
		if err := rows.Scan(&c.MsgID, &c.Direction, &c.Content); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// Module 聊天模块。
type Module struct {
	repo      *Repo
	messenger Messenger
	logger    *log.Logger
}

// New 创建聊天模块（messenger 可为 nil：单机降级为只落库）。
func New(repo *Repo, messenger Messenger, logger *log.Logger) *Module {
	return &Module{repo: repo, messenger: messenger, logger: logger}
}

// Name 实现 module.Module。
func (m *Module) Name() string { return "chat" }

// MsgIDs 实现 module.Module。
func (m *Module) MsgIDs() []uint32 { return []uint32{msgIDSendReq, msgIDHistoryReq} }

// Init 实现 module.Module。
func (m *Module) Init(ctx *module.Context) error { return nil }

// OnMessage 实现 module.Module。
func (m *Module) OnMessage(ctx *module.Context, msgID uint32, body []byte) error {
	switch msgID {
	case msgIDSendReq:
		return m.onSend(ctx, body)
	case msgIDHistoryReq:
		return m.onHistory(ctx, body)
	default:
		return fmt.Errorf("chat: unexpected msg %d", msgID)
	}
}

// OnSave 实现 module.Module。
func (m *Module) OnSave(ctx context.Context) error { return nil }

// OnLogin 实现 module.Module。
func (m *Module) OnLogin(ctx *module.Context) error { return nil }

// OnLogout 实现 module.Module。
func (m *Module) OnLogout(ctx *module.Context) error {
	ctx.State.MarkDirty(m.Name())
	return nil
}

// DeliverInbound 投递一条「收到」方向的跨服私聊（Kafka 消费调用）。
func (m *Module) DeliverInbound(ctx context.Context, msg *pkgmq.ChatCrossMsg) error {
	if m.repo == nil {
		return nil
	}
	return m.repo.Insert(ctx, msg.ToID, msg.FromID, msg.MsgID, DirectionIn, msg.Channel, msg.Content)
}

// onSend 发送私聊（17.3：本端 out → 对方在线本服直投 / 跨服走 Kafka / 离线 pend）。
func (m *Module) onSend(ctx *module.Context, body []byte) error {
	var req gamev1.ChatSendReq
	if err := proto.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("decode ChatSendReq: %w", err)
	}
	resp := &gamev1.ChatSendResp{}
	if req.Channel != channelPrivate && req.Channel != 0 {
		resp.Code = codeChannelClosed // P6 仅开放私聊
		return m.reply(ctx, msgIDSendResp, resp)
	}
	if req.Content == "" || req.PeerId <= 0 {
		resp.Code = codeContentEmpty
		return m.reply(ctx, msgIDSendResp, resp)
	}

	msgID := nextMsgID()
	now := time.Now().Unix()
	fromID := ctx.State.PlayerID()
	fromName := ctx.State.Profile().Name

	// ① 本端历史（direction=out）
	if m.repo != nil {
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
	raw, _ := proto.Marshal(notify)
	crossMsg := &pkgmq.ChatCrossMsg{
		MsgID: msgID, FromID: fromID, FromName: fromName,
		ToID: req.PeerId, Channel: channelPrivate, Content: req.Content, SendTime: now * 1000,
	}
	m.deliver(req.PeerId, raw, crossMsg)

	resp.Code = codeOK
	resp.MsgId = fmt.Sprintf("%d", msgID)
	resp.SentAt = now
	return m.reply(ctx, msgIDSendResp, resp)
}

// deliver 按在线状态选择投递路径。
func (m *Module) deliver(toID int64, raw []byte, crossMsg *pkgmq.ChatCrossMsg) {
	if m.messenger == nil {
		return
	}
	bg := context.Background()
	node, online := m.messenger.OnlineNode(bg, toID)
	switch {
	case !online:
		if err := m.messenger.PendOffline(bg, toID, crossMsg); err != nil {
			m.logger.Warn("pend offline chat failed", "to", toID, "err", err)
		}
	case node == m.messenger.SelfNode():
		// 本服在线：落 in + 推送
		if m.repo != nil {
			if err := m.repo.Insert(bg, toID, crossMsg.FromID, crossMsg.MsgID, DirectionIn, channelPrivate, crossMsg.Content); err != nil {
				m.logger.Warn("save inbound chat failed", "err", err)
			}
		}
		m.messenger.Push(bg, toID, msgIDNotify, raw)
	default:
		if err := m.messenger.ProduceCross(bg, crossMsg); err != nil {
			m.logger.Warn("produce cross chat failed", "to", toID, "err", err)
		}
	}
}

// onHistory 拉取与某好友的聊天历史。
func (m *Module) onHistory(ctx *module.Context, body []byte) error {
	var req gamev1.ChatHistoryReq
	if err := proto.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("decode ChatHistoryReq: %w", err)
	}
	resp := &gamev1.ChatHistoryResp{Code: codeOK}
	if m.repo == nil {
		return m.reply(ctx, msgIDHistoryResp, resp)
	}
	lines, err := m.repo.ListRecent(context.Background(), ctx.State.PlayerID(), req.PeerId, int(req.Limit))
	if err != nil {
		return err
	}
	// 查询是倒序，回包按时间正序
	selfID := ctx.State.PlayerID()
	for i := len(lines) - 1; i >= 0; i-- {
		l := lines[i]
		msg := &gamev1.ChatMessageNotify{
			MsgId:   fmt.Sprintf("%d", l.MsgID),
			Channel: channelPrivate,
			Content: l.Content,
		}
		if l.Direction == DirectionOut {
			msg.SenderId = selfID
			msg.PeerId = req.PeerId
		} else {
			msg.SenderId = req.PeerId
			msg.PeerId = selfID
		}
		resp.Messages = append(resp.Messages, msg)
	}
	resp.HasMore = len(lines) >= int(req.Limit)
	return m.reply(ctx, msgIDHistoryResp, resp)
}

func (m *Module) reply(ctx *module.Context, msgID uint32, msg proto.Message) error {
	body, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	return ctx.Reply(msgID, body)
}

// nextMsgID 生成全局消息 ID（时间戳毫秒 << 20 | 20 位随机）。
func nextMsgID() int64 {
	var b [4]byte
	_, _ = rand.Read(b[:])
	rnd := int64(binary.BigEndian.Uint32(b[:]) & 0xFFFFF)
	return time.Now().UnixMilli()<<20 | rnd
}
