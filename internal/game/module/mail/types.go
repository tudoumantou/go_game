// types.go —— 列表行与附件的领域类型、仓储错误。
//
// 这些类型是 handler 与 store 的共同语言：handler 不接触 SQL 行，
// store 不接触 proto——中间就靠它们。
package mail

import (
	"database/sql"
	"errors"
	"time"
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
