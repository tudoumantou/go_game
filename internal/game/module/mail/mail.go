// internal/game/module/mail/mail.go —— 邮件模块（P5 投递 / P6 列表与领取）
//
// 架构职责：邮件的 Kafka 投递、列表查询、附件领取，对应设计文档 4.9.3 / R2.2。
//  1. 投递：Deliver（Kafka 消费）建主邮件 + 附件；DeliverOffline（Tier-1
//     发放，充值到账等）以 biz_id 幂等建带附件的系统邮件；
//  2. 列表：onList 倒序取最近 100 封，附件有无用 EXISTS 子查询一次带出；
//  3. 领取：onTake 走玩家快照的 ApplyOnce 业务键（A 类幂等），
//     与道具在同一次 Redis CAS 提交，SQL 侧 claimed 只做尽力展示标记。
//
// Ext 槽位：无（邮件数据全在 MySQL）。但领取幂等「寄生」在玩家快照上：
// 业务键 mail:<id> 随快照 CAS 原子提交——这是与签到 ApplyOnce 同构的
// 「快照业务键幂等」模式，也是 R2.2 修复的核心：SQL 先标记会在 Redis
// 提交失败时丢奖励，快照业务键不会。
//
// 事件订阅：无。
//
// 配置表关系：邮件正文走模板（template_id 关联配置），demo 期直接存
// Kafka 消息里的标题/正文，不查模板表。
//
// 懒初始化：不适用（邮件行由投递动作显式产生）。
package mail

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"game_study/internal/common/errcode"
	"game_study/internal/game/module"
	"game_study/pkg/cache"
	"game_study/pkg/db"
	"game_study/pkg/gamedata"
	"game_study/pkg/log"
	"game_study/pkg/model"
	pkgmq "game_study/pkg/mq"

	gamev1 "game_study/pkg/pb/game/v1"
	"github.com/samber/lo"
	"google.golang.org/protobuf/proto"
)

// mailListLimit 单页列表上限（产品约定，与聊天的 50 不同）。
const mailListLimit = 100

// 仓储级错误。
//
// 哨兵 error 三兄弟：模块层用 errors.Is 判定转业务码，
// 仓储层保持裸返回（不 %w 包装），保证哨兵链可读。
var (
	ErrMailNotExist = errors.New("mail not exist")
	ErrAlreadyTaken = errors.New("attachment already taken")
	ErrNoAttachment = errors.New("mail has no attachment")
)

// MailRow 邮件列表行（含附件标记）。
//
// 字段语义：
//   - Status：0=未读，1=已读，2=已领（附件取完后由 MarkTaken 尽力更新）；
//   - ExpireAt：可空；过期邮件仍会查出，由客户端决定是否展示；
//   - HasAttachment：由 EXISTS 子查询得出，避免列表再查一次附件表。
type MailRow struct {
	// ID 邮件自增主键（领取业务键 mail:<id> 的组成部分）
	ID int64
	// Title 标题（投递时快照进邮件行，模板改版不影响已发邮件）
	Title string
	// Channel 渠道（0=系统邮件，业务自定义）
	Channel int32
	// Status 0=未读 1=已读 2=已领；Read 回包时按 >=1 折算
	Status int32
	// ExpireAt 可空过期时间；SQL NULL 必须用 sql.NullTime 承接——
	// 直接 Scan 进 time.Time 遇 NULL 会报错
	ExpireAt sql.NullTime
	// CreatedAt 发送时间（回包 SentAt 取 Unix 秒）
	CreatedAt time.Time
	// HasAttachment 是否有未领附件（EXISTS 子查询的 0/1 折算）
	HasAttachment bool
}

// Attachment 附件行。
type Attachment struct {
	// ItemID 道具配置 id（gamedata item 表主键）
	ItemID int32
	// Count 数量（int32：附件数量是小整数，与协议宽度一致）
	Count int32
}

// Repo 邮件仓储。
//
// MySQL 直写（不过 Persister）：邮件是流水型数据、与玩家快照解耦，
// db 由组装层注入；模块层入口对 repo==nil 的处理见各方法注释。
type Repo struct {
	db *db.DB
}

// NewRepo 创建仓储。
func NewRepo(database *db.DB) *Repo {
	return &Repo{db: database}
}

// Insert 插入邮件，返回邮件 ID。
//
// 直接用生成代码 model.InsertMail：`biz_id` 可空 + 唯一键（uk_player_biz）的陷阱
// 已由生成器统一解决——可空字符串列映射 sql.NullString，零值写 NULL 而非空串，
// 唯一索引对 NULL 不生效，同一玩家的多封普通邮件不再互撞（历史 Error 1062）。
func (r *Repo) Insert(ctx context.Context, m *model.Mail) (int64, error) {
	res, err := model.InsertMail(ctx, r.db.DB, m)
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
	_, err := model.InsertMailAttachment(ctx, r.db.DB, &model.MailAttachment{
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
	rows, err := r.db.QueryContext(ctx, q, args...)
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
	rows, err := r.db.QueryContext(ctx, q, playerID, limit)
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
	return model.CountMailByPlayerIdStatus(ctx, r.db.DB, playerID, 0)
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
	m, err := model.FindMailByPK(ctx, r.db.DB, mailID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrMailNotExist
	}
	if err != nil {
		return nil, err
	}
	if m.PlayerId != playerID {
		return nil, ErrMailNotExist
	}
	rows, err := model.ListMailAttachmentByMailIdClaimed(ctx, r.db.DB, mailID, 0)
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
	return r.db.Tx(ctx, func(tx *sql.Tx) error {
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
	return r.db.Tx(ctx, func(tx *sql.Tx) error {
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
	err := r.db.Tx(ctx, func(tx *sql.Tx) error {
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
// 测，不必为一条断言拉起 MySQL。*Repo 隐式实现它，装配层代码不变。
type mailRepo interface {
	UnreadCount(ctx context.Context, playerID int64) (int64, error)
	Insert(ctx context.Context, m *model.Mail) (int64, error)
	InsertAttachment(ctx context.Context, mailID int64, itemID, count int32) error
	HasAttachments(ctx context.Context, mailIDs []int64) (map[int64]bool, error)
	ListByPlayer(ctx context.Context, playerID int64, limit int) ([]MailRow, error)
	FindAttachments(ctx context.Context, playerID, mailID int64) ([]Attachment, error)
	MarkTaken(ctx context.Context, mailID int64) error
	Delete(ctx context.Context, playerID, mailID int64) error
	DeliverOffline(ctx context.Context, playerID int64, bizID, title, content string, items []Attachment) (int64, bool, error)
}

// Module 邮件模块。
//
// 无内存态、无锁：邮件数据全在 MySQL，OnMessage 在玩家 Actor 内串行执行，
// 跨玩家一致性由 DB 约束（唯一键）与快照业务键分工保证。
type Module struct {
	*module.Thin

	// repo 仓储；nil 表示无 DB 环境（列表/领取降级，离线发放报错，见各方法）
	repo   mailRepo
	logger *log.Logger

	// index 归属索引（by_owner:mail）。nil = 不启用，列表回落直查 SQL。
	//
	// 邮件是「一人多行」里最标准的一个：player_id 单一归属、新增/删除只动
	// 自己那一份。因此它是第一个真正消费归属索引的模块——新增时 Add、
	// 列表时 Expand，从而把「索引 → 展开 → 回填 → 自愈」整条链路跑在真实
	// 业务上，而不是只活在单测里。
	index *cache.OwnerIndex
}

// SetOwnerIndex 注入归属索引（组装层在 Register 之前调用）。
//
// nil 表示不启用（单测/无 Redis 环境）：列表自动回落 ListByPlayer，
// 行为与接入索引之前完全一致——这正是「索引是加速层，不是正确性前提」
// 在模块侧的体现。
func (m *Module) SetOwnerIndex(oi *cache.OwnerIndex) { m.index = oi }

// removeIndex 摘除索引（邮件行已删除）。
//
// 与 dropEntity 的分工：这里行没了，索引也要没（否则展开时会走一遍
// 「查不到 → 自愈 Remove」的弯路）；dropEntity 是行还在、内容变了。
func (m *Module) removeIndex(mailID int64) {
	if m.index == nil || mailID <= 0 {
		return
	}
	if err := m.index.RemoveEntity(context.Background(), mailID); err != nil {
		m.logger.Warn("mail index remove failed", "mail_id", mailID, "err", err)
	}
}

// dropEntity 丢弃缓存里的实体行（领取后状态变了）。
//
// 失败只影响下次多打一次库，不让领取失败——与 indexAdd 同一取舍。
func (m *Module) dropEntity(mailID int64) {
	if m.index == nil || mailID <= 0 {
		return
	}
	if err := m.index.DropEntity(context.Background(), mailID); err != nil {
		m.logger.Warn("mail cache drop failed", "mail_id", mailID, "err", err)
	}
}

// indexAdd 尽力登记新邮件。
//
// 失败只记日志：索引漏了这一条，下次 Reconcile（10 分钟）会重建补上，
// 期间列表回落 SQL 仍能读到——不值得为一次缓存写让发邮件失败。
func (m *Module) indexAdd(ctx context.Context, playerID, mailID int64) {
	if m.index == nil {
		return
	}
	if err := m.index.AddEntity(ctx, playerID, mailID); err != nil {
		m.logger.Warn("mail index add failed",
			"player_id", playerID, "mail_id", mailID, "err", err)
	}
}

// New 创建邮件模块。
func New(repo mailRepo, logger *log.Logger) *Module {
	m := &Module{repo: repo, logger: logger}
	m.Thin = module.NewThin("mail",
		module.ThinLogger(logger),
		module.ThinLogin(m.onLogin),
		module.ThinLogout(m.onLogout),
	)
	module.Handle(m.Thin, m.onList)
	module.Handle(m.Thin, m.onTake)
	module.Handle(m.Thin, m.onDelete)
	return m
}

// DeliverOffline 供充值等 Tier-1 发放使用：玩家离线时以 biz_id 幂等写系统邮件。
//
// 与 guild/chat 的「repo 为 nil 降级成功」刻意相反：邮件发放是奖励链路，
// 缺 DB 时必须让上游看到失败、走 Kafka 重试，而不是假装成功——
// 「报错重试」永远好过「静默丢奖励」。
func (m *Module) DeliverOffline(ctx context.Context, playerID int64, bizID, title, content string, items []Attachment) (bool, error) {
	if m.repo == nil {
		return false, fmt.Errorf("mail repository unavailable")
	}
	mailID, created, err := m.repo.DeliverOffline(ctx, playerID, bizID, title, content, items)
	if err != nil {
		return false, err
	}
	// 只有真新建才登记索引：biz_id 幂等命中时 mailID 为 0，
	// 重复 Add 同一 member 本身幂等，但没有 id 可 Add。
	if created {
		m.indexAdd(ctx, playerID, mailID)
	}
	return created, nil
}

// onLogin 登录时拉一次未读数（经 ThinLogin 挂接）。
//
// 目前只记 Debug 日志（红点以客户端列表请求为准，这里是服务端留痕，
// 供「玩家反馈没收到邮件」时对账）。
// 查询失败直接上抛：OnLogin 失败会中断登录流程——DB 故障时拒绝放行
// 好过放行后玩家发现奖励全丢。
func (m *Module) onLogin(ctx *module.Context) error {
	n, err := m.repo.UnreadCount(context.Background(), ctx.State.PlayerID())
	if err != nil {
		return err
	}
	if n > 0 {
		m.logger.Debug("unread mails", "player_id", ctx.State.PlayerID(), "count", n)
	}
	return nil
}

// onLogout 下线时触发一次常规 flush 周期（经 ThinLogout 挂接）。
//
// 邮件无 Ext 快照，MarkDirty 只是触发一次常规 flush 周期（与其他模块
// 生命周期保持一致，无害）；真正的数据在 MySQL，发送时已直写。
func (m *Module) onLogout(ctx *module.Context) error {
	ctx.State.MarkDirty("mail")
	return nil
}

// Deliver 投递一封邮件（Kafka 消费调用）。
//
// 两步落库：先插主邮件拿 mailID，再逐条插附件。
// it.Count <= 0 跳过：附件数量非正是脏数据，插进去会让领取时发 0 个
// 甚至负数道具，宁可丢这条附件。
// int32(it.Count) 是 int64→int32 收窄：附件数量在配置侧都是小整数，
// 与签到的 int32 溢出防护同源——这里选择信任配置而非加检查（数量级
// 不可能到 21 亿，加了也是死代码）。
// 附件插入失败只 Warn 不返回错误：主邮件已建成，邮件在、附件缺，
// 客服可按 mail_id 补发；若整封报错重投，Kafka 侧重试又会建一封新邮件，
// 玩家看到两封同名邮件反而更糟。
func (m *Module) Deliver(ctx context.Context, msg *pkgmq.MailMsg) (int64, error) {
	// 标题/正文优先用 mail_template 的配置，消息里非空则覆盖。
	//
	// 这样补偿邮件只需要下发「模板号 + 附件」，文案集中在配置表（运营可改）；
	// 需要个性化（如「您反馈的问题已处理」）时仍可在消息里写死。
	// 旧实现完全忽略模板表：改文案必须改发送端代码。
	title, content := msg.Title, msg.Content
	if cfg, ok := gamedata.GetMailTemplateCfg(int64(msg.TemplateID)); ok {
		if title == "" {
			title = cfg.Col1
		}
		if content == "" {
			content = cfg.Col2
		}
	}

	mailModel := &model.Mail{
		PlayerId:   msg.PlayerID,
		TemplateId: msg.TemplateID,
		Title:      title,
		Content:    content,
		Channel:    msg.Channel,
		Status:     0,
	}
	mailID, err := m.repo.Insert(ctx, mailModel)
	if err != nil {
		return 0, err
	}
	// 先落库再登记索引：构建期只从库里取材，反过来会漏行。
	m.indexAdd(ctx, msg.PlayerID, mailID)
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

// listRows 取邮件列表：归属索引展开优先，未启用/失败回落 SQL。
//
// 为什么值得走缓存：SQL 版本是「100 行 × 100 次 EXISTS 探测」；缓存版本是
// 「1 次 ZRANGEBYSCORE + 1 次 pipeline HGETALL + 1 次 IN 查附件」——邮件越多，
// EXISTS 的次数越多，而 IN 始终一次。少的那 100 次探测就是这里的收益。
//
// 回落条件刻意宽（索引为 nil、展开报错都回落）：列表是读路径，任何缓存
// 故障都不该让玩家看不到邮件——顶多多打一次库。
func (m *Module) listRows(ctx context.Context, playerID int64) ([]MailRow, error) {
	if m.index != nil {
		entityRows, removed, err := m.index.Expand(ctx, playerID)
		if err != nil {
			m.logger.Warn("mail expand failed, falling back to sql",
				"player_id", playerID, "err", err)
		} else {
			if len(removed) > 0 {
				m.logger.Info("mail index self-healed",
					"player_id", playerID, "removed", len(removed))
			}
			return m.rowsFromCache(ctx, entityRows)
		}
	}
	return m.repo.ListByPlayer(ctx, playerID, mailListLimit)
}

// rowsFromCache 把展开出的实体行组装成列表行。
//
// 与 SQL 版本的两处差异，都是有意为之：
//   - 倒序与限长在内存做（SQL 是 ORDER BY id DESC LIMIT）：索引返回升序，
//     而邮件量级是几十到几百，内存排序的代价远小于一次额外往返；
//   - 附件标记多一次 IN 查询：e:mail:{id} 只缓存 mail 表的列，
//     mail_attachment 是另一张表。不做冗余字段——那会在每次领取附件时
//     多一处必须同步的缓存写，而「不同步」正是这套设计最难查的问题。
func (m *Module) rowsFromCache(ctx context.Context, rows []cache.EntityRow) ([]MailRow, error) {
	if len(rows) == 0 {
		return nil, nil
	}
	out := make([]MailRow, 0, len(rows))
	for _, er := range rows {
		row, ok := mailRowFrom(er)
		if !ok {
			// 单行字段损坏不该让整页失败：跳过它，其余照常返回。
			m.logger.Warn("mail entity row malformed", "id", er.ID)
			continue
		}
		out = append(out, row)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID > out[j].ID })
	if len(out) > mailListLimit {
		out = out[:mailListLimit]
	}
	if len(out) == 0 {
		return nil, nil
	}

	ids := make([]int64, len(out))
	for i := range out {
		ids[i] = out[i].ID
	}
	withAtt, err := m.repo.HasAttachments(ctx, ids)
	if err != nil {
		return nil, err
	}
	for i := range out {
		out[i].HasAttachment = withAtt[out[i].ID]
	}
	return out, nil
}

// mailRowFrom 把缓存里的动态列还原成列表行。
//
// 字段名是列名（`id`/`title`/…），值统一是字符串——这是 EntityRow 的约定
// （与 IDTable.LoadEntity 同构）。时间列由 toString 格式化为 RFC3339
// （见 pkg/cache 的 toString），这里按 RFC3339 解析。
func mailRowFrom(er cache.EntityRow) (MailRow, bool) {
	var row MailRow
	id, err := strconv.ParseInt(er.Fields["id"], 10, 64)
	if err != nil || id <= 0 {
		return row, false
	}
	row.ID = id
	row.Title = er.Fields["title"]
	row.Channel = parseInt32(er.Fields["channel"])
	row.Status = parseInt32(er.Fields["status"])
	if t, ok := parseTimeField(er.Fields["created_at"]); ok {
		row.CreatedAt = t
	}
	if t, ok := parseTimeField(er.Fields["expire_at"]); ok {
		row.ExpireAt = sql.NullTime{Time: t, Valid: true}
	}
	return row, true
}

// parseInt32 宽松解析：坏值按 0 处理（列表展示用，不值得让整行失败）。
func parseInt32(s string) int32 {
	n, err := strconv.ParseInt(s, 10, 32)
	if err != nil {
		return 0
	}
	return int32(n)
}

// parseTimeField 解析缓存里的时间列。
//
// 两种格式都认：RFC3339（parseTime=true 时 toString 的产出）与
// MySQL 原生 "2006-01-02 15:04:05"（parseTime=false 或 []byte 直传）。
// 只认前者会让「换一次驱动配置」变成「列表时间全变 1970」。
func parseTimeField(s string) (time.Time, bool) {
	if s == "" {
		return time.Time{}, false
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t, true
	}
	if t, err := time.ParseInLocation("2006-01-02 15:04:05", s, time.Local); err == nil {
		return t, true
	}
	return time.Time{}, false
}

// onList 邮件列表。
//
// make(..., 0, len(rows)) 预分配容量：已知行数时一次分配到位，
// 避免 append 反复增长带来的底层数组复制（Go 切片性能惯用法）。
// Read 按 Status>=1 折算：1=已读与 2=已领对「已读红点」是同义。
// ExpireAt.Valid 先判再取：sql.NullTime 的可空语义，Valid=false 时
// .Time 是零值，直接 Unix() 会回一个 1970 的过期时间骗客户端。
func (m *Module) onList(ctx *module.Context, _ *gamev1.MailListReq, resp *gamev1.MailListResp) error {
	ctxDB, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	rows, err := m.listRows(ctxDB, ctx.State.PlayerID())
	if err != nil {
		return err
	}
	*resp = gamev1.MailListResp{Code: int32(errcode.OK), Mails: make([]*gamev1.MailSummary, 0, len(rows))}
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
	return nil
}

// onTake 领取附件（幂等：重复领取回 5002）。
//
// 领取事实 = 快照里的业务键 mail:<id>，与道具同一次 CAS 提交；
// SQL 的 claimed/status 只是尽力更新，标记失败也不会重复发奖。
//
// 5002（已领）与 5003（无附件）对客户端语义不同：前者静默、后者可能
// 弹错误提示。MarkTaken 生效后重复请求必须稳定回 5002，所以 HasBiz
// 必须先于 FindAttachments——顺序颠倒是可观测的行为退化。
func (m *Module) onTake(ctx *module.Context, req *gamev1.MailTakeAttachmentReq, resp *gamev1.MailTakeAttachmentResp) error {

	*resp = gamev1.MailTakeAttachmentResp{}
	// 前置挡掉非正 id：省一次 DB 往返，也防负 id 扫表
	if req.MailId <= 0 {
		resp.Code = int32(errcode.ErrMailNotFound)
		return nil
	}

	// 已领过直接返回 5002：SQL 的 claimed 只是展示，权威事实在快照业务键上。
	// 先查键再查附件，否则 MarkTaken 生效后重复请求会拿到 5003（无附件）而不是 5002。
	key := fmt.Sprintf("mail:%d", req.MailId)
	if ctx.State.HasBiz(key) {
		resp.Code = int32(errcode.ErrMailTaken)
		return nil
	}

	atts, err := m.repo.FindAttachments(context.Background(), ctx.State.PlayerID(), req.MailId)
	if err != nil {
		switch {
		case errors.Is(err, ErrMailNotExist):
			resp.Code = int32(errcode.ErrMailNotFound)
		case errors.Is(err, ErrNoAttachment):
			resp.Code = int32(errcode.ErrNoAttachment)
		default:
			return err
		}
		return nil
	}

	applied, err := ctx.ApplyOnce(key, func() error {
		// grants 先完整构造：CanHold 需要全量清单才能按堆叠规则算容量，
		// 逐件加是「加不进去才后悔」的半途而废
		grants := make([]module.ItemGrant, 0, len(atts))
		for _, a := range atts {
			grants = append(grants, module.ItemGrant{ItemID: a.ItemID, Count: int64(a.Count)})
		}
		// 满包/超限时返回错误：ApplyOnce 不占业务键，玩家清包后可重新领取。
		// Grant 先全量校验再入包，不会留下「加到一半」的中间态。
		// 闭包内的内存修改是「暂态」：ApplyOnce 成功后统一 CAS 提交，
		// 失败则整体丢弃，不会留下改了没提交的脏状态。
		if _, err := module.Grant(ctx.State, grants); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		// 满包与数量超限都回「背包满」：玩家侧动作相同（清包再领），
		// 区分两者只会让客户端多一个用不上的分支。
		if errors.Is(err, module.ErrBagFull) || errors.Is(err, module.ErrItemLimit) {
			resp.Code = int32(errcode.ErrBagFull)
			return nil
		}
		return err
	}
	// !applied 是并发双端同时领取的第二道防线：HasBiz 与 CAS 之间被
	// 抢跑的请求在这里被 ApplyOnce 的版本号判重挡下，同样回 5002
	if !applied {
		resp.Code = int32(errcode.ErrMailTaken)
		return nil
	}
	if err := m.repo.MarkTaken(context.Background(), req.MailId); err != nil {
		// 不是错误：快照才是权威，标记只影响列表展示。
		m.logger.Warn("mail mark taken failed", "mail_id", req.MailId, "err", err)
	}
	// 缓存里的这封邮件必须作废：它的 status 还是领取前的 0。
	// 不删的话下次 onList 从缓存读到旧 status，玩家会看到「已领到东西
	// 但列表仍显示未读 + 有附件」，而且缓存条目没有 TTL，不会自己好。
	// 与 MarkTaken 成败无关：只要领奖成功（applied），展示态就该刷新。
	m.dropEntity(req.MailId)

	changes := lo.Map(atts, func(a Attachment, _ int) *gamev1.ItemChange {
		return &gamev1.ItemChange{
			// demo 期道具无唯一实例 uid，用 itemID 占位（协议字段不可省）
			ItemUid: int64(a.ItemID),
			ItemId:  a.ItemID,
			// 回包带领取后存量而非本次发放数：客户端直接刷新显示
			Count:  int32(ctx.State.GetItemCount(a.ItemID)),
			Change: 1,
		}
	})

	resp.Code = int32(errcode.OK)
	// 奖励复用 BagChangeNotify 格式塞进 Rewards：客户端一套道具变更
	// 渲染逻辑同时处理邮件奖励和背包操作，省一个专用消息类型
	if raw, err := proto.Marshal(&gamev1.BagChangeNotify{Changes: changes}); err == nil {
		resp.Rewards = raw
	}
	return nil
}

// onDelete 删除邮件。
//
// 有未领附件时拒绝（ErrMailHasUnclaimed）：删了就再也领不到，是不可逆的
// 损失；让玩家先领再删，比事后补发便宜得多。
//
// 顺序：查未领附件 → 删库 → 摘索引 → 丢缓存。前三步任一失败都不动缓存，
// 免得出现「缓存说没了、库里还在」。
func (m *Module) onDelete(c *module.Context, req *gamev1.MailDeleteReq, resp *gamev1.MailDeleteResp) error {
	if req.MailId <= 0 {
		resp.Code = int32(errcode.ErrMailNotFound)
		return nil
	}
	ctxDB, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	playerID := c.State.PlayerID()
	// FindAttachments 只返回「未领」的附件：ErrNoAttachment 正好表示可以删。
	atts, err := m.repo.FindAttachments(ctxDB, playerID, req.MailId)
	switch {
	case errors.Is(err, ErrMailNotExist):
		resp.Code = int32(errcode.ErrMailNotFound)
		return nil
	case errors.Is(err, ErrNoAttachment):
		// 没有未领附件 → 放行
	case err != nil:
		return err
	case len(atts) > 0:
		resp.Code = int32(errcode.ErrMailHasUnclaimed)
		return nil
	}

	if err := m.repo.Delete(ctxDB, playerID, req.MailId); err != nil {
		if errors.Is(err, ErrMailNotExist) {
			resp.Code = int32(errcode.ErrMailNotFound)
			return nil
		}
		return err
	}
	// 先删库再动缓存——与投递方向一致：索引与缓存永远跟着数据的真实变更走。
	m.removeIndex(req.MailId)
	m.dropEntity(req.MailId)
	resp.Code = int32(errcode.OK)
	return nil
}
