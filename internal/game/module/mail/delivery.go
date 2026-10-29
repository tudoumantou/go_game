// delivery.go —— 投递入口：Kafka 消费（Deliver）与离线发放（DeliverOffline）。
//
// 与 handler.go 分开的原因：这两条路径不由玩家请求驱动，而是由消息队列和
// 充值回调驱动，排障时看的是另一套东西（topic、biz_id 幂等键）。
package mail

import (
	"context"
	"fmt"

	"game_study/pkg/gamedata"
	"game_study/pkg/model"
	pkgmq "game_study/pkg/mq"
)

// DeliverOffline 供充值等 Tier-1 发放使用：玩家离线时以 biz_id 幂等写系统邮件。
//
// 与 guild/chat 的「repo 为 nil 降级成功」刻意相反：邮件发放是奖励链路，
// 缺 DB 时必须让上游看到失败、走 Kafka 重试，而不是假装成功——
// 「报错重试」永远好过「静默丢奖励」。
func (m *Module) DeliverOffline(ctx context.Context, playerID int64, bizID, title, content string, items []Attachment) (bool, error) {
	if !m.ready() {
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
