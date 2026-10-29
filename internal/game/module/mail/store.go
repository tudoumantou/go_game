// store.go —— mail / mail_attachment 两张表的访问层。
//
// 这是本模块唯一的 SQL 出口：只调 tools/gendb 生成的函数，剩下的手写语句
// 是生成器表达不了的只读查询（列表的 EXISTS 子查询、附件 EXISTS 的 IN 批量）。
// 每条都登记在 docs/功能实现状态.md 的例外清单里。
package mail

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"game_study/internal/game/module"
	"game_study/pkg/db"
	"game_study/pkg/model"
)

// Repo 邮件仓储。
//
// MySQL 直写（不过 Persister）：邮件是流水型数据、与玩家快照解耦，
// db 由组装层注入；模块层入口对 repo==nil 的处理见各方法注释。
type Repo struct {
	*module.Shared
}

// NewRepo 创建仓储。
func NewRepo(database *db.DB) *Repo {
	return &Repo{Shared: module.NewShared(database, "mail")}
}

// Insert 插入邮件，返回邮件 ID。
//
// 直接用生成代码 model.InsertMail：`biz_id` 可空 + 唯一键（uk_player_biz）的陷阱
// 已由生成器统一解决——可空字符串列映射 sql.NullString，零值写 NULL 而非空串，
// 唯一索引对 NULL 不生效，同一玩家的多封普通邮件不再互撞（历史 Error 1062）。
func (r *Repo) Insert(ctx context.Context, m *model.Mail) (int64, error) {
	res, err := model.InsertMail(ctx, r.SQL(), m)
	if err != nil {
		return 0, fmt.Errorf("insert mail: %w", err)
	}
	return res.LastInsertId()
}

// InsertAttachment 插入附件。
//
// 逐条插入而非批量：一封邮件附件是个位数，逐条 INSERT 简单且错误定位清晰；
// 失败由 Deliver 的循环只 Warn——主邮件已建成，缺附件好过整封丢。
func (r *Repo) InsertAttachment(ctx context.Context, mailID int64, itemID, count int32) error {
	_, err := model.InsertMailAttachment(ctx, r.SQL(), &model.MailAttachment{
		MailId: mailID,
		ItemId: itemID,
		Count:  count,
	})
	return err
}

// HasAttachments 批量判断「这些邮件里哪些有附件」。
//
// 一次 IN 查询换掉列表里的 N 次 EXISTS：缓存展开路径拿到一批 id 后，
// 只关心「有没有」，不需要附件本身，DISTINCT 把多附件邮件折成一行。
// 与 SQL 列表版本的 EXISTS 口径一致：只看存在性，不看 claimed。
func (r *Repo) HasAttachments(ctx context.Context, mailIDs []int64) (map[int64]bool, error) {
	if len(mailIDs) == 0 {
		return nil, nil
	}
	//nolint:gosec // 只有占位符拼接，id 走参数绑定
	q := `SELECT DISTINCT mail_id FROM mail_attachment WHERE mail_id IN (` +
		sqlPlaceholders(len(mailIDs)) + `)`
	args := make([]any, len(mailIDs))
	for i, id := range mailIDs {
		args[i] = id
	}
	rows, err := r.SQL().QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("query mail attachments: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := make(map[int64]bool, len(mailIDs))
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out[id] = true
	}
	return out, rows.Err()
}

// sqlPlaceholders 生成 n 个逗号分隔的占位符（IN 子句）。
func sqlPlaceholders(n int) string {
	if n <= 0 {
		return ""
	}
	return strings.TrimSuffix(strings.Repeat("?,", n), ",")
}

// ListByPlayer 邮件列表（按时间倒序，最多 100 封）。
//
// limit 钳制：越界/非法回落 100（产品约定的单页上限，与聊天的 50 不同）。
// EXISTS 子查询是关键优化：100 封列表若逐封查附件是 101 次查询
// （经典 N+1），EXISTS 让 DB 用一次半连接算完，列表接口只发 1 条 SQL。
// ORDER BY m.id DESC：自增主键即时间序，走主键索引倒序扫描，无 filesort。
func (r *Repo) ListByPlayer(ctx context.Context, playerID int64, limit int) ([]MailRow, error) {
	if limit <= 0 || limit > 100 {
		limit = 100
	}
	const q = `SELECT m.id, m.title, m.channel, m.status, m.expire_at, m.created_at,
	                  EXISTS(SELECT 1 FROM mail_attachment a WHERE a.mail_id = m.id) AS has_att
	           FROM mail m
	           WHERE m.player_id = ?
	           ORDER BY m.id DESC LIMIT ?`
	rows, err := r.SQL().QueryContext(ctx, q, playerID, limit)
	if err != nil {
		return nil, fmt.Errorf("list mail: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []MailRow
	for rows.Next() {
		var row MailRow
		// EXISTS 在 MySQL 返回 0/1，database/sql 扫不进 bool——
		// 用 int 中转再折算是惯用法
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
//
// 生成的 CountMailByPlayerIdStatus 就是 `SELECT COUNT(*) ... WHERE player_id=?
// AND status=?`；COUNT 恒返回一行，因此不需要 ErrNoRows 分支。
func (r *Repo) UnreadCount(ctx context.Context, playerID int64) (int64, error) {
	return model.CountMailByPlayerIdStatus(ctx, r.SQL(), playerID, 0)
}

// FindAttachments 只读附件：校验归属与有无附件，不写库。
//
// 领取事实由玩家快照的业务键 mail:<id> 保证（A 类幂等），因此这里不再用
// 「SQL 先标记已领」——那条路径在 Redis 提交失败时会丢奖励（R2.2）。
//
// 归属校验刻意不区分「邮件不存在」与「不是你的邮件」：两种情况都回
// ErrMailNotExist，避免用错误码差异向调用方泄露邮件 id 是否存在。
// claimed = 0 过滤是双轨幂等的第二道：已领附件不再出现在结果里。
func (r *Repo) FindAttachments(ctx context.Context, playerID, mailID int64) ([]Attachment, error) {
	m, err := model.FindMailByPK(ctx, r.SQL(), mailID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrMailNotExist
	}
	if err != nil {
		return nil, err
	}
	if m.PlayerId != playerID {
		return nil, ErrMailNotExist
	}
	rows, err := model.ListMailAttachmentByMailIdClaimed(ctx, r.SQL(), mailID, 0)
	if err != nil {
		return nil, err
	}
	out := make([]Attachment, 0, len(rows))
	for _, a := range rows {
		out = append(out, Attachment{ItemID: a.ItemId, Count: a.Count})
	}
	// 查不到未领附件 = 无附件可领（ErrNoAttachment → 5003）：
	// 与「已领过」区分开，客户端据此给不同提示
	if len(out) == 0 {
		return nil, ErrNoAttachment
	}
	return out, nil
}

// MarkTaken 尽力把附件标记为已领，只用于列表展示与排查。
// 失败不影响正确性：重复领取由快照里的业务键拦截。
//
// 事务包两条更新（附件 claimed + 邮件 status）保持展示一致：
// 不能出现「附件显示已领、邮件还显示有附件」的割裂态。
// 附件那条不再带 `AND claimed = 0` 过滤——全量置 1 本来就幂等，
// 带上过滤只是少改几行，语义没有区别。
func (r *Repo) MarkTaken(ctx context.Context, mailID int64) error {
	return r.Tx(ctx, func(tx *sql.Tx) error {
		if _, err := model.UpdateMailAttachmentClaimedByMailId(ctx, tx, &model.MailAttachment{
			MailId:  mailID,
			Claimed: 1,
		}); err != nil {
			return err
		}
		_, err := model.UpdateMailStatusByPK(ctx, tx, &model.Mail{Id: mailID, Status: 2})
		return err
	})
}

// Delete 删除一封邮件及其附件（同一事务两条 DELETE）。
//
// 归属校验放进 WHERE 而不是「先查再删」：一次往返、无竞态，且
// RowsAffected=0 天然把「不存在」与「不是你的」合成一个结果——与
// FindAttachments 同一口径，不向调用方泄露别人的邮件 id 是否存在。
// 这条复合条件删除由 @deleteby:id,player_id 生成，条件与手写版逐字一致。
func (r *Repo) Delete(ctx context.Context, playerID, mailID int64) error {
	return r.Tx(ctx, func(tx *sql.Tx) error {
		res, err := model.DeleteMailByIdPlayerId(ctx, tx, mailID, playerID)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if n == 0 {
			// 主邮件没删到 → 整笔回滚，不留下孤儿附件
			return ErrMailNotExist
		}
		_, err = model.DeleteMailAttachmentByMailId(ctx, tx, mailID)
		return err
	})
}

// DeliverOffline 离线发放：以 biz_id 幂等写一封带附件的系统邮件。
//
// 用于玩家不在线时的 Tier-1 发放（充值到账等）：邮件与附件在同一 SQL 事务里
// 落库，biz_id 唯一键保证重投不会重复建单。返回 true 表示本次新建。
//
// 幂等实现从「INSERT IGNORE + RowsAffected==0」改成「普通 INSERT + 1062 视为
// 已存在」：两者语义等价（mail 表只有 uk_player_biz 会冲突），但后者不依赖
// MySQL 方言，且冲突判定走 db.IsDuplicate 的类型断言而不是错误文本。
// 冲突时直接 return nil：没有新行，LastInsertId 不可信，绝不能拿去插附件
// （会插出 mail_id=0 的孤儿行）。
// biz_id 空校验是 fail-fast：幂等键缺失时直接报错让上游重试，
// 而不是建一封可能重复的发奖单——充值链路「报错可重试」远好过「重复发奖」。
func (r *Repo) DeliverOffline(ctx context.Context, playerID int64, bizID, title, content string, items []Attachment) (int64, bool, error) {
	if bizID == "" {
		return 0, false, fmt.Errorf("offline delivery requires biz_id")
	}
	var newID int64
	created := false
	err := r.Tx(ctx, func(tx *sql.Tx) error {
		res, err := model.InsertMail(ctx, tx, &model.Mail{
			PlayerId: playerID,
			Title:    title,
			Content:  content,
			BizId:    sql.NullString{String: bizID, Valid: true},
		})
		if err != nil {
			if db.IsDuplicate(err) {
				return nil // 已发放过（同一 biz_id）
			}
			return err
		}
		mailID, err := res.LastInsertId()
		if err != nil {
			return err
		}
		for _, it := range items {
			if _, err := model.InsertMailAttachment(ctx, tx, &model.MailAttachment{
				MailId: mailID,
				ItemId: it.ItemID,
				Count:  it.Count,
			}); err != nil {
				return err
			}
		}
		newID = mailID
		created = true
		return nil
	})
	if err != nil {
		return 0, false, err
	}
	return newID, created, nil
}

// mailRepo 是模块对仓储的全部依赖（接口隔离）。
//
// 抽成接口只为一件事：让「领取幂等」「满包不发奖」这类纯业务规则能在内存里
