// internal/game/module/quest/quest.go —— 任务模块（7101-7106）
//
// 状态（进度 + 领取事实）整体放在玩家快照的 Ext["quest"] 里：领奖时改内存、
// 与道具经验一起提交，Redis CAS 失败则整批回滚（契约见 docs/R2.1-缓存原子变更契约.md）。
// 不再直接写 MySQL 业务事实。
//
// 架构职责：任务进度推进 + 领奖。任务列表由 quest.xml 配置派生，
// 模块只存「接取/进度/领取」三态，不存任务定义。
// 事件订阅：消费方（EventAware）。监听 kill/use_item/copy_pass 等事件，
// 按配置 Col2 匹配后推进进度。
// 配置表：quest.xml（Col1=名称，Col2=监听事件类型，Col3=目标次数）。
// 懒初始化：未接取的任务在 Records map 里不存在，接取时才建键；
// 列表请求把「配置全集」与「已有记录」join 后下发。

package quest

import (
	"game_study/internal/common/errcode"
	"game_study/internal/game/module"
	"game_study/pkg/gamedata"
	gamev1 "game_study/pkg/pb/game/v1"

	"google.golang.org/protobuf/proto"
)

func onLogout(c *module.Ctx[Data]) error {
	// 只标脏、不编码：下线时把「内存里已经改过但还没提交」的进度催一次落库。
	// Touch 在槽位为空时什么也不做——从没接过任务的玩家不该被写一条空状态。
	return c.Repo.Touch()
}

// onEvent 推进匹配类型的任务进度（经 ThinOnEvent 挂接）。
//
// 一次事件只解析一次状态、只写回一次；进度与状态都在内存里改，
// 快照提交失败时随整条消息回滚。
//
// Go 知识点：EventAware 是可选接口，Registry 用类型断言探测
// （见 module.go DispatchEvent）——本模块实现它即「订阅全部事件」，
// 事件过滤在函数体内按 cfg.Col2 做。

func onEvent(c *module.Ctx[Data], evt module.Event) {
	// 没有任务被推进时 changed=false，不产生一次无意义的提交。
	err := c.Repo.Mutate(func(d *Data) (bool, error) {
		changed := false
		// 配置约定：Col1=任务名，Col2=监听的事件类型，Col3=目标次数。
		for _, cfg := range gamedata.QuestCfgList() {
			if cfg.Col2 != evt.Type {
				continue
			}
			id := int32(cfg.Id)
			rec, ok := d.Quests[id]
			if !ok || rec.Status != statusDoing {
				continue // 未接取或已完成的任务不推进
			}
			target := int32(cfg.Col3)
			next := rec.Progress + int32(evt.Delta)
			// 到达目标即置完成态并截断：超额计数没有意义，
			// 截断还能防止 int32 被恶意大 Delta 冲爆。
			if next >= target {
				next = target
				rec.Status = statusDone
			}
			rec.Progress = next
			d.Quests[id] = rec
			changed = true
		}
		return changed, nil
	})
	if err != nil {
		// 状态损坏只告警不抛错：OnEvent 没有返回值，
		// 不能让一个坏任务把整条业务消息的响应搞失败。
		c.Log().Warn("quest progress save failed", "player_id", c.State.PlayerID(), "err", err)
	}
}

// onList 任务列表（未接/进行中/可领奖/已领奖四态）。
//
// 注意内部 status（0/1/2）与协议 status（0-3）是两套编码：
// 内部没有「未接」状态（用键不存在表达），协议需要显式 0，
// 所以这里做一次映射，而不是把内部状态直接下发。

func onList(c *module.Ctx[Data]) error {
	resp := &gamev1.QuestListResp{}
	err := c.Repo.View(func(d *Data) error {
		for _, cfg := range gamedata.QuestCfgList() {
			entry := &gamev1.QuestEntry{
				QuestId: int32(cfg.Id),
				Name:    cfg.Col1,
				Target:  int32(cfg.Col3),
				Status:  0, // 未接
			}
			if rec, ok := d.Quests[int32(cfg.Id)]; ok {
				entry.Progress = rec.Progress
				switch rec.Status {
				case statusClaimed:
					entry.Status = 3
				case statusDone:
					entry.Status = 2
				default:
					if rec.Progress >= int32(cfg.Col3) {
						entry.Status = 2
					} else {
						entry.Status = 1
					}
				}
			}
			resp.Quests = append(resp.Quests, entry)
		}
		return nil
	})
	if err != nil {
		return c.Fail(&gamev1.QuestListResp{Code: int32(errcode.ErrSystem)}, err)
	}
	return c.Reply(resp)
}

// onAccept 接取任务。
//
// 已领奖的任务允许重新接取（日常任务的设计前提）：Status==claimed
// 时覆盖为 doing 并清零进度，实现「每日刷新」而不需要删键。

func onAccept(c *module.Ctx[Data], req *gamev1.QuestAcceptReq) error {
	if _, ok := gamedata.GetQuestCfg(int64(req.QuestId)); !ok {
		return c.FailCode(&gamev1.QuestAcceptResp{}, errcode.ErrQuestNotExist)
	}
	resp := &gamev1.QuestAcceptResp{}
	err := c.Repo.Mutate(func(d *Data) (bool, error) {
		if rec, ok := d.Quests[req.QuestId]; ok && rec.Status != statusClaimed {
			// 已在进行中：不改状态，也不产生提交。
			resp.Code = int32(errcode.ErrQuestAccepted)
			return false, nil
		}
		d.Quests[req.QuestId] = Record{Status: statusDoing}
		return true, nil
	})
	if err != nil {
		return c.Fail(&gamev1.QuestAcceptResp{Code: int32(errcode.ErrSystem)}, err)
	}
	return c.Reply(resp)
}

// onClaim 领奖（经验 + 道具）。
//
// 领取事实与奖励在同一次快照提交：已领状态写进 Ext，道具进 Items，经验进 Profile，
// Redis 提交失败时三者一起回滚，不会出现「状态已领、奖励没到账」。

func onClaim(c *module.Ctx[Data], req *gamev1.QuestClaimReq) error {
	cfg, ok := gamedata.GetQuestCfg(int64(req.QuestId))
	if !ok {
		return c.FailCode(&gamev1.QuestClaimResp{}, errcode.ErrQuestNotExist)
	}

	resp := &gamev1.QuestClaimResp{}
	// expGain 在闭包里落定、在 Mutate 成功后才发：经验写进 Profile 是另一段
	// 状态（不由本模块的 D 承载），必须等任务状态提交成功才能动，
	// 否则会出现「经验已发、领取位没置上」的重复领取。
	var expGain int64

	err := c.Repo.Mutate(func(d *Data) (bool, error) {
		rec, ok := d.Quests[req.QuestId]
		if !ok {
			resp.Code = int32(errcode.ErrQuestUnfinished)
			return false, nil
		}
		if rec.Status == statusClaimed {
			resp.Code = int32(errcode.ErrQuestClaimed)
			return false, nil
		}
		if rec.Progress < int32(cfg.Col3) {
			resp.Code = int32(errcode.ErrQuestUnfinished)
			return false, nil
		}

		// 发奖：经验 + 钻石（配置约定 target*100 经验、target*10 钻石）。
		// 满包/超限时整个领取被拒，不会出现「状态已置领、道具没发」。
		grants := []module.ItemGrant{{ItemID: 1001, Count: int64(cfg.Col3) * 10}}
		bagChanges, grantErr := module.Grant(c.State, grants)
		if grantErr != nil {
			resp.Code = int32(errcode.ErrQuestBagFull)
			return false, nil
		}

		rec.Status = statusClaimed
		d.Quests[req.QuestId] = rec
		expGain = int64(cfg.Col3) * 100
		if raw, err := proto.Marshal(&gamev1.BagChangeNotify{Changes: module.ToItemChanges(bagChanges)}); err == nil {
			resp.Rewards = raw
		}
		return true, nil
	})
	if err != nil {
		return c.Fail(&gamev1.QuestClaimResp{Code: int32(errcode.ErrSystem)}, err)
	}

	resp.ExpGain = expGain
	// 发经验（Actor 内部会推送等级变化）；任务状态已提交，不会重复领取。
	if c.AddExp != nil && expGain > 0 {
		c.AddExp(expGain)
	}
	return c.Reply(resp)
}
