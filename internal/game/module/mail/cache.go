// cache.go —— 归属索引（by_owner:mail）的维护。
//
// 三个动作对应三种事实：新邮件登记（indexAdd）、内容变了丢实体行（dropEntity）、
// 行删了连索引一起摘（removeIndex）。失败一律只 Warn——索引是加速层，
// 不是正确性前提（漏了下次 Reconcile 会补，期间回落 SQL 也能读）。
package mail

import (
	"context"
)

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
