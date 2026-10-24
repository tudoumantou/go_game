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
	"time"

	"game_study/internal/game/module"
	"game_study/pkg/codec"
	"game_study/pkg/db"
	"game_study/pkg/gamedata"
	"game_study/pkg/log"
	"game_study/pkg/model"
	pkgmq "game_study/pkg/mq"

	gamev1 "game_study/pkg/pb/game/v1"
	"github.com/samber/lo"
	"google.golang.org/protobuf/proto"
)

// 协议号（4.9：7001/7002 列表，7020/7021 领取）。
const (
	msgIDMailListReq  = codec.MsgMailListReq
	msgIDMailListResp = codec.MsgMailListResp
	msgIDMailTakeReq  = codec.MsgMailTakeAttachmentReq
	msgIDMailTakeResp = codec.MsgMailTakeAttachmentResp
)

// 业务码（4.10：0 成功；5001-5004 是邮件/背包交界错误，5004 满包
// 由 module.ErrBagFull 转来，客户端弹「背包已满」）。
const (
	codeOK           int32 = 0
	codeMailNotExist int32 = 5001
	codeMailTaken    int32 = 5002
	codeNoAttachment int32 = 5003
	codeBagFull      int32 = 5004
)

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
// 委托 model.InsertMail（gendb 生成的 model 层，SQL 由代码生成维护），
// 这里只做错误包装（%w 保留底层错误链供排查）。
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
	const q = `INSERT INTO mail_attachment (mail_id, item_id, count) VALUES (?, ?, ?)`
	_, err := r.db.ExecContext(ctx, q, mailID, itemID, count)
	return err
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
// COUNT(*) 是聚合，恒返回一行、不会 ErrNoRows——下面的 errors.Is 检查
// 是防御性冗余（单行扫描的通用写法照搬），无害但不会触发。
func (r *Repo) UnreadCount(ctx context.Context, playerID int64) (int64, error) {
	const q = `SELECT COUNT(*) FROM mail WHERE player_id = ? AND status = 0`
	var n int64
	err := r.db.QueryRowContext(ctx, q, playerID).Scan(&n)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return 0, err
	}
	return n, nil
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
	var ownerID int64
	err := r.db.QueryRowContext(ctx,
		`SELECT player_id FROM mail WHERE id = ?`, mailID).Scan(&ownerID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrMailNotExist
	}
	if err != nil {
		return nil, err
	}
	if ownerID != playerID {
		return nil, ErrMailNotExist
	}
	rows, err := r.db.QueryContext(ctx,
		`SELECT item_id, count FROM mail_attachment WHERE mail_id = ? AND claimed = 0`, mailID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []Attachment
	for rows.Next() {
		var a Attachment
		if err := rows.Scan(&a.ItemID, &a.Count); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	if err := rows.Err(); err != nil {
		return nil, err
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
// 事务包两条 UPDATE（附件 claimed + 邮件 status）保持展示一致：
// 不能出现「附件显示已领、邮件还显示有附件」的割裂态。
// WHERE claimed = 0 让重复调用幂等（第二次只影响 0 行，不报错）。
func (r *Repo) MarkTaken(ctx context.Context, mailID int64) error {
	return r.db.Tx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx,
			`UPDATE mail_attachment SET claimed = 1 WHERE mail_id = ? AND claimed = 0`, mailID); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `UPDATE mail SET status = 2 WHERE id = ?`, mailID)
		return err
	})
}

// DeliverOffline 离线发放：以 biz_id 幂等写一封带附件的系统邮件。
//
// 用于玩家不在线时的 Tier-1 发放（充值到账等）：邮件与附件在同一 SQL 事务里
// 落库，biz_id 唯一键保证重投不会重复建单。返回 true 表示本次新建。
//
// INSERT IGNORE + RowsAffected()==0 是「不存在才插」的 DB 惯用法：
// 比先 SELECT 再 INSERT 少一次往返，且彻底无竞态（两条并发重投只有
// 一条能插入，另一条 IGNORE 后 RowsAffected=0）。
// biz_id 空校验是 fail-fast：幂等键缺失时直接报错让上游重试，
// 而不是建一封可能重复的发奖单——充值链路「报错可重试」远好过「重复发奖」。
// RowsAffected=0 时直接 return nil：IGNORE 未真插入，LastInsertId 不可信，
// 绝不能拿去插附件（会插出 mail_id=0 的孤儿行）。
func (r *Repo) DeliverOffline(ctx context.Context, playerID int64, bizID, title, content string, items []Attachment) (bool, error) {
	if bizID == "" {
		return false, fmt.Errorf("offline delivery requires biz_id")
	}
	created := false
	err := r.db.Tx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx,
			`INSERT IGNORE INTO mail (player_id, template_id, title, content, channel, biz_id)
			 VALUES (?, 0, ?, ?, 0, ?)`, playerID, title, content, bizID)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if n == 0 {
			return nil // 已发放过
		}
		mailID, err := res.LastInsertId()
		if err != nil {
			return err
		}
		for _, it := range items {
			if _, err := tx.ExecContext(ctx,
				`INSERT INTO mail_attachment (mail_id, item_id, count) VALUES (?, ?, ?)`,
				mailID, it.ItemID, it.Count); err != nil {
				return err
			}
		}
		created = true
		return nil
	})
	if err != nil {
		return false, err
	}
	return created, nil
}

// mailRepo 是模块对仓储的全部依赖（接口隔离）。
//
// 抽成接口只为一件事：让「领取幂等」「满包不发奖」这类纯业务规则能在内存里
// 测，不必为一条断言拉起 MySQL。*Repo 隐式实现它，装配层代码不变。
type mailRepo interface {
	UnreadCount(ctx context.Context, playerID int64) (int64, error)
	Insert(ctx context.Context, m *model.Mail) (int64, error)
	InsertAttachment(ctx context.Context, mailID int64, itemID, count int32) error
	ListByPlayer(ctx context.Context, playerID int64, limit int) ([]MailRow, error)
	FindAttachments(ctx context.Context, playerID, mailID int64) ([]Attachment, error)
	MarkTaken(ctx context.Context, mailID int64) error
	DeliverOffline(ctx context.Context, playerID int64, bizID, title, content string, items []Attachment) (bool, error)
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
}

// New 创建邮件模块。
func New(repo mailRepo, logger *log.Logger) *Module {
	m := &Module{repo: repo, logger: logger}
	m.Thin = module.NewThin("mail",
		module.ThinLogger(logger),
		module.ThinLogin(m.onLogin),
		module.ThinLogout(m.onLogout),
	)
	m.HandleR(msgIDMailListReq, msgIDMailListResp, &gamev1.MailListResp{}, m.onList)
	m.HandleR(msgIDMailTakeReq, msgIDMailTakeResp, &gamev1.MailTakeAttachmentResp{}, module.Typed(msgIDMailTakeResp, m.onTake))
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
	return m.repo.DeliverOffline(ctx, playerID, bizID, title, content, items)
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
//
// make(..., 0, len(rows)) 预分配容量：已知行数时一次分配到位，
// 避免 append 反复增长带来的底层数组复制（Go 切片性能惯用法）。
// Read 按 Status>=1 折算：1=已读与 2=已领对「已读红点」是同义。
// ExpireAt.Valid 先判再取：sql.NullTime 的可空语义，Valid=false 时
// .Time 是零值，直接 Unix() 会回一个 1970 的过期时间骗客户端。
func (m *Module) onList(ctx *module.Context, _ uint32, _ []byte) error {
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
		resp.Code = codeMailNotExist
		return nil
	}

	// 已领过直接返回 5002：SQL 的 claimed 只是展示，权威事实在快照业务键上。
	// 先查键再查附件，否则 MarkTaken 生效后重复请求会拿到 5003（无附件）而不是 5002。
	key := fmt.Sprintf("mail:%d", req.MailId)
	if ctx.State.HasBiz(key) {
		resp.Code = codeMailTaken
		return nil
	}

	atts, err := m.repo.FindAttachments(context.Background(), ctx.State.PlayerID(), req.MailId)
	if err != nil {
		switch {
		case errors.Is(err, ErrMailNotExist):
			resp.Code = codeMailNotExist
		case errors.Is(err, ErrNoAttachment):
			resp.Code = codeNoAttachment
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
			resp.Code = codeBagFull
			return nil
		}
		return err
	}
	// !applied 是并发双端同时领取的第二道防线：HasBiz 与 CAS 之间被
	// 抢跑的请求在这里被 ApplyOnce 的版本号判重挡下，同样回 5002
	if !applied {
		resp.Code = codeMailTaken
		return nil
	}
	if err := m.repo.MarkTaken(context.Background(), req.MailId); err != nil {
		// 不是错误：快照才是权威，标记只影响列表展示。
		m.logger.Warn("mail mark taken failed", "mail_id", req.MailId, "err", err)
	}

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

	resp.Code = codeOK
	// 奖励复用 BagChangeNotify 格式塞进 Rewards：客户端一套道具变更
	// 渲染逻辑同时处理邮件奖励和背包操作，省一个专用消息类型
	if raw, err := proto.Marshal(&gamev1.BagChangeNotify{Changes: changes}); err == nil {
		resp.Rewards = raw
	}
	return nil
}
