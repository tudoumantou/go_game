// internal/game/module/activity/activity.go —— 活动模块（6501-6504）
//
// 活动进度与领取位图放在玩家快照 Ext["activity"]：领奖与发放在同一次 Redis CAS
// 提交，Redis 失败时两者一起回滚（契约见 docs/R2.1-缓存原子变更契约.md）。
//
// 架构职责：活动进度累加 + 领奖。进度有两个来源：登录类活动在 OnLogin
// +1，行为类活动走 OnEvent 累加；全服里程碑活动额外上报 Kafka。
// Ext 槽位：activity_record（Records JSON 列）。
// 事件订阅：消费方（EventAware），按配置 Col2 匹配事件类型。
// 配置表：activity.xml（Col1=名称，Col2=事件类型或 "login"，Col3=目标）、
// activity_period.xml（开放周期，col1=周期类型）。
// 懒初始化：未参与活动返回空 Data；窗口判定不落库——
// 「是否开放」每次请求现算（openNow），过期活动直接从列表隐藏。

package activity

import (
	"context"
	"fmt"
	"game_study/internal/common/errcode"
	"game_study/internal/game/module"
	"game_study/pkg/activity"
	"game_study/pkg/gamedata"
	pkgmq "game_study/pkg/mq"
	gamev1 "game_study/pkg/pb/game/v1"
	"game_study/pkg/timeutil"
	"time"

	"google.golang.org/protobuf/proto"
)

func SetProducer(p ProgressProducer) { producer = p }

// ProgressProducer 贡献上报能力（*pkgmq.Producer 实现）。
//
// 接口注入而非直接依赖 mq 包：单测可注入假 producer，
// 且活动模块不关心 Kafka 细节（topic/分区/重试）。

func onLogin(c *module.Ctx[Data]) error {
	// 「读状态 → 逐活动推进」；没有任何活动推进时 changed=false，不产生提交。
	err := c.Repo.Mutate(func(d *Data) (bool, error) {
		changed := false
		for _, cfg := range gamedata.ActivityCfgList() {
			if cfg.Col2 != "login" {
				continue
			}
			id := int32(cfg.Id)
			rec := d.Records[id]
			if rec.Progress >= int32(cfg.Col3) {
				continue // 已达目标，登录不再推进
			}
			rec.Progress++
			d.Records[id] = rec
			changed = true
		}
		return changed, nil
	})
	if err != nil {
		// 状态损坏不阻断登录：后续消息会再次报错，把玩家拦在登录外不能。
		c.Log().Warn("activity login progress failed", "player_id", c.State.PlayerID(), "err", err)
	}
	return nil
}

// onEvent 把玩家行为事件累加为活动进度（经 ThinOnEvent 挂接）。
//
// 与 quest.OnEvent 同构，差异有两点：
//  1. 进度到目标后仍然接受事件（只是截断），不改变任何领取态；
//  2. server_progress 类活动要额外收集上报，攒批后统一发 Kafka。

func onEvent(c *module.Ctx[Data], evt module.Event) {
	var reports [][2]int32 // 全服贡献上报：(activity_id, progress)

	err := c.Repo.Mutate(func(d *Data) (bool, error) {
		changed := false
		for _, cfg := range gamedata.ActivityCfgList() {
			if cfg.Col2 != evt.Type {
				continue
			}
			id := int32(cfg.Id)
			rec := d.Records[id]
			next := rec.Progress + int32(evt.Delta)
			if next > int32(cfg.Col3) {
				next = int32(cfg.Col3)
			}
			if next == rec.Progress {
				continue // 已封顶，无变化
			}
			rec.Progress = next
			d.Records[id] = rec
			changed = true
			if cfg.Col2 == "server_progress" {
				// 全服活动：进度变化后上报新值（绝对值，中心服按差值累加）。
				reports = append(reports, [2]int32{id, next})
			}
		}
		return changed, nil
	})
	if err != nil {
		c.Log().Warn("activity progress update failed", "player_id", c.State.PlayerID(), "err", err)
		return
	}
	// 上报在提交之后：本地进度先落，再尝试广播——
	// 广播失败不影响本地正确性（见 reportProgress）。
	for _, r := range reports {
		reportProgress(c, r[0], r[1])
	}
}

// reportProgress 上报全服活动贡献（R7.3）。
//
// 上报绝对值而不是增量：重投相同值在中心服得到差值 0，天然幂等。
// 上报失败只记日志——玩家本地进度已保存，下一次事件会带上更大的进度继续推进。

func reportProgress(c *module.Ctx[Data], activityID, progress int32) {
	if producer == nil || progress <= 0 {
		return
	}
	now := time.Now().Unix()
	instanceID := instanceIDFor(activityID, now)
	msg := pkgmq.ActivityProgressMsg{
		InstanceID: instanceID,
		ActivityID: activityID,
		PlayerID:   c.State.PlayerID(),
		Progress:   progress,
	}
	// envelope 的 key 是「实例:玩家」：同一玩家同一实例的消息进同一分区，
	// Kafka 内保序，中心服按序累加不会乱。
	env, err := pkgmq.NewEnvelope(fmt.Sprintf("%s:%d", instanceID, c.State.PlayerID()),
		pkgmq.TopicActivityProgress, instanceID, msg)
	if err != nil {
		return
	}
	if err := producer.ProduceEnvelope(context.Background(), env); err != nil {
		c.Log().Debug("report activity progress failed", "activity_id", activityID, "err", err)
	}
}

// instanceIDFor 当前活动实例：优先用中心服下发的，没有就按周期自算。
//
// 实例 ID = 活动 + 周期序号：同一活动的每一轮（每天/每周）是不同实例，
// 进度与奖励按实例隔离，上一轮的 Record 不会串到下一轮。

func instanceIDFor(activityID int32, now int64) string {
	if w, ok := DefaultRegistry().Get(activityID); ok {
		return w.InstanceID
	}
	if cfg, ok := gamedata.GetActivityPeriodCfg(int64(activityID)); ok {
		_, _, period := activity.Window(int32(cfg.Col1), now)
		return fmt.Sprintf("%d-%d", activityID, period)
	}
	// 无周期配置：按天兜底，保证实例 ID 单调不减。
	//
	// 必须用 BusinessDayUnix 而不是 now/86400：后者是 UTC 日界，
	// 与商店限购/登录礼包的北京日界差 8 小时——同一玩家会在
	// 「今天还能买」的同时看到「活动还是昨天那一轮」。
	return fmt.Sprintf("%d-%d", activityID, timeutil.BusinessDayUnix(now))
}

// onList 活动列表（配置 + 进度 + 是否已领）。
//
// 过期活动直接不下发：客户端定时刷新列表即可看到活动消失，
// 不需要服务端推「活动结束」通知——少一种推送，少一类时序问题。

func onList(c *module.Ctx[Data]) error {
	resp := &gamev1.ActivityListResp{}
	err := c.Repo.View(func(d *Data) error {
		now := time.Now().Unix()
		for _, cfg := range gamedata.ActivityCfgList() {
			// 下发的窗口已过期就不展示：客户端仍会定时刷新。
			if !openNow(int32(cfg.Id), now) {
				continue
			}
			entry := &gamev1.ActivityEntry{
				ActivityId: int32(cfg.Id),
				Name:       cfg.Col1,
				Type:       cfg.Col2,
				Target:     int32(cfg.Col3),
			}
			if rec, ok := d.Records[int32(cfg.Id)]; ok {
				entry.Progress = rec.Progress
				entry.Claimed = rec.RewardMask&1 != 0
			}
			resp.Activities = append(resp.Activities, entry)
		}
		return nil
	})
	if err != nil {
		return c.Fail(&gamev1.ActivityListResp{Code: int32(errcode.ErrSystem)}, err)
	}
	return c.Reply(resp)
}

// onClaim 领奖：进度达标 + 位图防重复，领奖事实与奖励同一次快照提交。

func onClaim(c *module.Ctx[Data], req *gamev1.ActivityClaimReq) error {
	cfg, ok := gamedata.GetActivityCfg(int64(req.ActivityId))
	if !ok {
		return c.FailCode(&gamev1.ActivityClaimResp{}, errcode.ErrActivityNotExist)
	}
	// 领取同样校验开放期：不能依赖客户端「恰好没收到结束通知」。
	// 服务端权威原则——客户端的任何展示状态都不能当准入条件。
	if !openNow(req.ActivityId, time.Now().Unix()) {
		return c.FailCode(&gamev1.ActivityClaimResp{}, errcode.ErrActivityNotOpen)
	}

	resp := &gamev1.ActivityClaimResp{}
	var bagChanges []module.BagChange

	err := c.Repo.Mutate(func(d *Data) (bool, error) {
		rec := d.Records[req.ActivityId]
		if rec.Progress < int32(cfg.Col3) {
			resp.Code = int32(errcode.ErrActivityNotFinish)
			return false, nil
		}
		// 位图判重：bit0=已领。活动天生多档位，枚举会爆炸。
		if rec.RewardMask&1 != 0 {
			resp.Code = int32(errcode.ErrActivityClaimed)
			return false, nil
		}
		// 发奖：target * 20 钻石；满包/超限时不发也不置领取位。
		gain := int64(cfg.Col3) * 20
		changes, grantErr := module.Grant(c.State, []module.ItemGrant{{ItemID: itemDiamond, Count: gain}})
		if grantErr != nil {
			resp.Code = int32(errcode.ErrActivityBagFull)
			return false, nil
		}
		rec.RewardMask |= 1 // 置位 bit0：与道具同一次快照 CAS
		d.Records[req.ActivityId] = rec
		bagChanges = changes
		return true, nil
	})
	if err != nil {
		return c.Fail(&gamev1.ActivityClaimResp{Code: int32(errcode.ErrSystem)}, err)
	}

	// 只有真正发过奖才带背包变更：失败路径保持 Rewards 为 nil，
	// 与「先 return 再构造响应」的旧行为一致。
	if len(bagChanges) > 0 {
		if raw, err := proto.Marshal(&gamev1.BagChangeNotify{Changes: module.ToItemChanges(bagChanges)}); err == nil {
			resp.Rewards = raw
		}
	}
	return c.Reply(resp)
}
