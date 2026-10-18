// internal/game/module/mail/mail.go —— 邮件模块（P5 投递 / P6 列表与领取）
package mail

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"game_study/internal/game/module"
	"game_study/pkg/db"
	"game_study/pkg/log"
	"game_study/pkg/model"
	pkgmq "game_study/pkg/mq"

	gamev1 "game_study/pkg/pb/game/v1"
	"google.golang.org/protobuf/proto"
)

// 协议号（4.9：7001/7002 列表，7020/7021 领取）。
const (
	msgIDMailListReq  uint32 = 7001
	msgIDMailListResp uint32 = 7002
	msgIDMailTakeReq  uint32 = 7020
	msgIDMailTakeResp uint32 = 7021
)

// 业务码（4.10）。
const (
	codeOK           int32 = 0
	codeMailNotExist int32 = 5001
	codeMailTaken    int32 = 5002
	codeNoAttachment int32 = 5003
)

// 仓储级错误。
var (
	ErrMailNotExist = errors.New("mail not exist")
	ErrAlreadyTaken = errors.New("attachment already taken")
	ErrNoAttachment = errors.New("mail has no attachment")
)

// MailRow 邮件列表行（含附件标记）。
type MailRow struct {
	ID            int64
	Title         string
	Channel       int32
	Status        int32
	ExpireAt      sql.NullTime
	CreatedAt     time.Time
	HasAttachment bool
}

// Attachment 附件行。
type Attachment struct {
	ItemID int32
	Count  int32
}

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

// ListByPlayer 邮件列表（按时间倒序，最多 100 封）。
func (r *Repo) ListByPlayer(ctx context.Context, playerID int64, limit int) ([]MailRow, error) {
	if limit <= 0 || limit > 100 {
		limit = 100
	}
	const q = `SELECT m.id, m.title, m.channel, m.status, m.expire_at, m.created_at,
	                  EXISTS(SELECT 1 FROM mail_attachment a WHERE a.mail_id = m.id) AS has_att
	           FROM mail m
	           WHERE m.player_id = ?
	           ORDER BY m.id DESC LIMIT ?`
	rows, err := r.db.QueryContext(ctx, q, playerID, limit)
	if err != nil {
		return nil, fmt.Errorf("list mail: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []MailRow
	for rows.Next() {
		var row MailRow
		var hasAtt int
		if err := rows.Scan(&row.ID, &row.Title, &row.Channel, &row.Status,
			&row.ExpireAt, &row.CreatedAt, &hasAtt); err != nil {
			return nil, err
		}
		row.HasAttachment = hasAtt == 1
		out = append(out, row)
	}
	return out, rows.Err()
}

// UnreadCount 未读数。
func (r *Repo) UnreadCount(ctx context.Context, playerID int64) (int64, error) {
	const q = `SELECT COUNT(*) FROM mail WHERE player_id = ? AND status = 0`
	var n int64
	err := r.db.QueryRowContext(ctx, q, playerID).Scan(&n)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return 0, err
	}
	return n, nil
}

// TakeAttachments 领取附件（事务 + 行锁）：
//  1. 校验邮件归属与状态（5001/5002/5003）
//  2. 附件 claimed 0→1，mail.status → 2
//  3. 返回附件明细供发奖
func (r *Repo) TakeAttachments(ctx context.Context, playerID, mailID int64) ([]Attachment, error) {
	var result []Attachment
	err := r.db.Tx(ctx, func(tx *sql.Tx) error {
		var ownerID int64
		var status int32
		err := tx.QueryRowContext(ctx,
			`SELECT player_id, status FROM mail WHERE id = ? FOR UPDATE`, mailID).Scan(&ownerID, &status)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrMailNotExist
		}
		if err != nil {
			return err
		}
		if ownerID != playerID {
			return ErrMailNotExist
		}
		if status == 2 {
			return ErrAlreadyTaken
		}

		rows, err := tx.QueryContext(ctx,
			`SELECT item_id, count FROM mail_attachment WHERE mail_id = ? AND claimed = 0 FOR UPDATE`, mailID)
		if err != nil {
			return err
		}
		for rows.Next() {
			var a Attachment
			if err := rows.Scan(&a.ItemID, &a.Count); err != nil {
				_ = rows.Close()
				return err
			}
			result = append(result, a)
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return err
		}
		_ = rows.Close()

		if len(result) == 0 {
			return ErrNoAttachment
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE mail_attachment SET claimed = 1 WHERE mail_id = ? AND claimed = 0`, mailID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE mail SET status = 2 WHERE id = ?`, mailID); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
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

// MsgIDs 实现 module.Module。
func (m *Module) MsgIDs() []uint32 { return []uint32{msgIDMailListReq, msgIDMailTakeReq} }

// Init 实现 module.Module。
func (m *Module) Init(ctx *module.Context) error { return nil }

// OnMessage 实现 module.Module。
func (m *Module) OnMessage(ctx *module.Context, msgID uint32, body []byte) error {
	switch msgID {
	case msgIDMailListReq:
		return m.onList(ctx)
	case msgIDMailTakeReq:
		return m.onTake(ctx, body)
	default:
		return fmt.Errorf("mail: unexpected msg %d", msgID)
	}
}

// OnSave 实现 module.Module。
func (m *Module) OnSave(ctx context.Context) error { return nil }

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

// onList 邮件列表。
func (m *Module) onList(ctx *module.Context) error {
	rows, err := m.repo.ListByPlayer(context.Background(), ctx.State.PlayerID(), 100)
	if err != nil {
		return err
	}
	resp := &gamev1.MailListResp{Code: codeOK, Mails: make([]*gamev1.MailSummary, 0, len(rows))}
	for _, r := range rows {
		s := &gamev1.MailSummary{
			MailId:        r.ID,
			Title:         r.Title,
			SentAt:        r.CreatedAt.Unix(),
			HasAttachment: r.HasAttachment,
			Read:          r.Status >= 1,
		}
		if r.ExpireAt.Valid {
			s.ExpireAt = r.ExpireAt.Time.Unix()
		}
		resp.Mails = append(resp.Mails, s)
	}
	body, err := proto.Marshal(resp)
	if err != nil {
		return err
	}
	return ctx.Reply(msgIDMailListResp, body)
}

// onTake 领取附件（幂等：重复领取回 5002）。
func (m *Module) onTake(ctx *module.Context, body []byte) error {
	var req gamev1.MailTakeAttachmentReq
	if err := proto.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("decode MailTakeAttachmentReq: %w", err)
	}

	resp := &gamev1.MailTakeAttachmentResp{}
	if req.MailId <= 0 {
		resp.Code = codeMailNotExist
		return m.replyTake(ctx, resp)
	}

	atts, err := m.repo.TakeAttachments(context.Background(), ctx.State.PlayerID(), req.MailId)
	if err != nil {
		switch {
		case errors.Is(err, ErrMailNotExist):
			resp.Code = codeMailNotExist
		case errors.Is(err, ErrAlreadyTaken):
			resp.Code = codeMailTaken
		case errors.Is(err, ErrNoAttachment):
			resp.Code = codeNoAttachment
		default:
			return err
		}
		return m.replyTake(ctx, resp)
	}

	// 发奖（同 Actor 内直接改内存，安全）
	changes := make([]*gamev1.ItemChange, 0, len(atts))
	for _, a := range atts {
		ctx.State.AddItem(a.ItemID, int64(a.Count))
		changes = append(changes, &gamev1.ItemChange{
			ItemUid: int64(a.ItemID),
			ItemId:  a.ItemID,
			Count:   int32(ctx.State.GetItemCount(a.ItemID)),
			Change:  1,
		})
	}
	ctx.State.MarkDirty("bag")

	resp.Code = codeOK
	if raw, err := proto.Marshal(&gamev1.BagChangeNotify{Changes: changes}); err == nil {
		resp.Rewards = raw
	}
	return m.replyTake(ctx, resp)
}

func (m *Module) replyTake(ctx *module.Context, resp *gamev1.MailTakeAttachmentResp) error {
	body, err := proto.Marshal(resp)
	if err != nil {
		return err
	}
	return ctx.Reply(msgIDMailTakeResp, body)
}
