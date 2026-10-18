// internal/game/module/mail/mail.go —— 邮件模块（P5：Kafka 幂等投递落库）
package mail

import (
	"context"
	"database/sql"
	"fmt"

	"game_study/internal/game/module"
	"game_study/pkg/db"
	"game_study/pkg/log"
	"game_study/pkg/model"
	pkgmq "game_study/pkg/mq"
)

// Repo 邮件仓储。
type Repo struct {
	db *db.DB
}

// NewRepo 创建仓储。
func NewRepo(database *db.DB) *Repo {
	return &Repo{db: database}
}

// Insert 插入邮件，返回邮件 ID。
func (r *Repo) Insert(ctx context.Context, m *model.Mail) (int64, error) {
	res, err := model.InsertMail(ctx, r.db.DB, m)
	if err != nil {
		return 0, fmt.Errorf("insert mail: %w", err)
	}
	return res.LastInsertId()
}

// InsertAttachment 插入附件。
func (r *Repo) InsertAttachment(ctx context.Context, mailID int64, itemID, count int32) error {
	const q = `INSERT INTO mail_attachment (mail_id, item_id, count) VALUES (?, ?, ?)`
	_, err := r.db.ExecContext(ctx, q, mailID, itemID, count)
	return err
}

// UnreadCount 未读数。
func (r *Repo) UnreadCount(ctx context.Context, playerID int64) (int64, error) {
	const q = `SELECT COUNT(*) FROM mail WHERE player_id = ? AND status = 0`
	var n int64
	err := r.db.QueryRowContext(ctx, q, playerID).Scan(&n)
	if err != nil && err != sql.ErrNoRows {
		return 0, err
	}
	return n, nil
}

// Module 邮件模块。
type Module struct {
	repo   *Repo
	logger *log.Logger
}

// New 创建邮件模块。
func New(repo *Repo, logger *log.Logger) *Module {
	return &Module{repo: repo, logger: logger}
}

// Name 实现 module.Module。
func (m *Module) Name() string { return "mail" }

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
func (m *Module) OnLogin(ctx *module.Context) error {
	n, err := m.repo.UnreadCount(context.Background(), ctx.State.PlayerID())
	if err != nil {
		return err
	}
	if n > 0 {
		m.logger.Debug("unread mails", "player_id", ctx.State.PlayerID(), "count", n)
	}
	return nil
}

// OnLogout 实现 module.Module。
func (m *Module) OnLogout(ctx *module.Context) error {
	ctx.State.MarkDirty(m.Name())
	return nil
}

// Deliver 投递一封邮件（Kafka 消费调用）。
func (m *Module) Deliver(ctx context.Context, msg *pkgmq.MailMsg) (int64, error) {
	mailModel := &model.Mail{
		PlayerId:   msg.PlayerID,
		TemplateId: msg.TemplateID,
		Title:      msg.Title,
		Content:    msg.Content,
		Channel:    msg.Channel,
		Status:     0,
	}
	mailID, err := m.repo.Insert(ctx, mailModel)
	if err != nil {
		return 0, err
	}
	for _, it := range msg.Items {
		if it.Count <= 0 {
			continue
		}
		if err := m.repo.InsertAttachment(ctx, mailID, it.ItemID, int32(it.Count)); err != nil {
			m.logger.Warn("insert attachment failed", "mail_id", mailID, "err", err)
		}
	}
	return mailID, nil
}
