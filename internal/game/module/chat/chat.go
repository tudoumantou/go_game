// internal/game/module/chat/chat.go —— 聊天模块（P5：跨服私聊双端落库）
package chat

import (
	"context"
	"fmt"

	"game_study/internal/game/module"
	"game_study/pkg/db"
	"game_study/pkg/log"
	pkgmq "game_study/pkg/mq"
)

// 消息方向。
const (
	DirectionOut int32 = 0 // 发出
	DirectionIn  int32 = 1 // 收到
)

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

// ListRecent 读取与某好友的最近 N 条记录（按 id 升序）。
func (r *Repo) ListRecent(ctx context.Context, ownerID, peerID int64, limit int) ([]ChatLine, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	const q = `SELECT msg_id, direction, content, created_at
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
		if err := rows.Scan(&c.MsgID, &c.Direction, &c.Content, &c.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// ChatLine 聊天记录行。
type ChatLine struct {
	MsgID     int64
	Direction int32
	Content   string
	CreatedAt string
}

// Module 聊天模块。
type Module struct {
	repo   *Repo
	logger *log.Logger
}

// New 创建聊天模块。
func New(repo *Repo, logger *log.Logger) *Module {
	return &Module{repo: repo, logger: logger}
}

// Name 实现 module.Module。
func (m *Module) Name() string { return "chat" }

// Init 实现 module.Module。
func (m *Module) Init(ctx *module.Context) error { return nil }

// OnMessage 实现 module.Module。
func (m *Module) OnMessage(ctx *module.Context, msgID uint32, body []byte) error {
	_ = ctx
	_ = msgID
	_ = body
	return nil
}

// OnSave 实现 module.Module。
func (m *Module) OnSave(ctx context.Context) error {
	_ = ctx
	return nil
}

// OnLogin 实现 module.Module。
func (m *Module) OnLogin(ctx *module.Context) error { return nil }

// OnLogout 实现 module.Module。
func (m *Module) OnLogout(ctx *module.Context) error {
	ctx.State.MarkDirty(m.Name())
	return nil
}

// DeliverInbound 投递一条「收到」方向的跨服私聊（Kafka 消费调用）。
func (m *Module) DeliverInbound(ctx context.Context, msg *pkgmq.ChatCrossMsg) error {
	return m.repo.Insert(ctx, msg.ToID, msg.FromID, msg.MsgID, DirectionIn, msg.Channel, msg.Content)
}
