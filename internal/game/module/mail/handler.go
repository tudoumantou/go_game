// handler.go —— 协议处理器：列表、领取附件、删除。
//
// 只做三件事：取玩家 ID → 按规则判断 → 调 store/缓存。SQL 在 store.go，
// 投递在 delivery.go，这里不该出现驱动、事务或缓存 key 的拼接。
package mail

import (
	"context"
	"errors"
	"fmt"

	"game_study/internal/common/errcode"
	"game_study/internal/game/module"

	gamev1 "game_study/pkg/pb/game/v1"
	"github.com/samber/lo"
	"google.golang.org/protobuf/proto"
)

// onList 邮件列表。
//
// make(..., 0, len(rows)) 预分配容量：已知行数时一次分配到位，
// 避免 append 反复增长带来的底层数组复制（Go 切片性能惯用法）。
// Read 按 Status>=1 折算：1=已读与 2=已领对「已读红点」是同义。
// ExpireAt.Valid 先判再取：sql.NullTime 的可空语义，Valid=false 时
// .Time 是零值，直接 Unix() 会回一个 1970 的过期时间骗客户端。
func (m *Module) onList(ctx *module.Context, _ *gamev1.MailListReq, resp *gamev1.MailListResp) error {
	ctxDB, cancel := module.QueryCtx()
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
		return ctx.FailCode(resp, errcode.ErrMailNotFound)
	}

	// 已领过直接返回 5002：SQL 的 claimed 只是展示，权威事实在快照业务键上。
	// 先查键再查附件，否则 MarkTaken 生效后重复请求会拿到 5003（无附件）而不是 5002。
	key := fmt.Sprintf("mail:%d", req.MailId)
	if ctx.State.HasBiz(key) {
		return ctx.FailCode(resp, errcode.ErrMailTaken)
	}

	atts, err := m.repo.FindAttachments(context.Background(), ctx.State.PlayerID(), req.MailId)
	if err != nil {
		switch {
		case errors.Is(err, ErrMailNotExist):
			return ctx.FailCode(resp, errcode.ErrMailNotFound)
		case errors.Is(err, ErrNoAttachment):
			return ctx.FailCode(resp, errcode.ErrNoAttachment)
		default:
			return err
		}
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
			return ctx.FailCode(resp, errcode.ErrBagFull)
		}
		return err
	}
	// !applied 是并发双端同时领取的第二道防线：HasBiz 与 CAS 之间被
	// 抢跑的请求在这里被 ApplyOnce 的版本号判重挡下，同样回 5002
	if !applied {
		return ctx.FailCode(resp, errcode.ErrMailTaken)
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
		return c.FailCode(resp, errcode.ErrMailNotFound)
	}
	ctxDB, cancel := module.QueryCtx()
	defer cancel()

	playerID := c.State.PlayerID()
	// FindAttachments 只返回「未领」的附件：ErrNoAttachment 正好表示可以删。
	atts, err := m.repo.FindAttachments(ctxDB, playerID, req.MailId)
	switch {
	case errors.Is(err, ErrMailNotExist):
		return c.FailCode(resp, errcode.ErrMailNotFound)
	case errors.Is(err, ErrNoAttachment):
		// 没有未领附件 → 放行
	case err != nil:
		return err
	case len(atts) > 0:
		return c.FailCode(resp, errcode.ErrMailHasUnclaimed)
	}

	if err := m.repo.Delete(ctxDB, playerID, req.MailId); err != nil {
		if errors.Is(err, ErrMailNotExist) {
			return c.FailCode(resp, errcode.ErrMailNotFound)
		}
		return err
	}
	// 先删库再动缓存——与投递方向一致：索引与缓存永远跟着数据的真实变更走。
	m.removeIndex(req.MailId)
	m.dropEntity(req.MailId)
	resp.Code = int32(errcode.OK)
	return nil
}
