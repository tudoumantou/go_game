// handler.go —— 签到协议处理器。
//
// 只写业务流：取状态 → 校验配置 → 判幂等 → 发奖 → 改状态 → 存回 → 回包。
// 不出现 SQL、不碰 *db.DB、不拼缓存 key（由 docs/模块开发约定.md §2 约束，
// .golangci.yml 的 depguard 会在 import 层面卡住）。
package signin

import (
	"errors"
	"fmt"
	"math"
	"sort"

	"game_study/internal/common/errcode"
	"game_study/internal/game/module"
	"game_study/pkg/gamedata"
	gamev1 "game_study/pkg/pb/game/v1"
	"game_study/pkg/timeutil"
	"github.com/samber/lo"
)

// onInfo 签到信息查询：今天周几、今天是否已领、本周领取全貌。
func onInfo(c *module.Ctx[SignIn]) error {
	// 每个请求只采样一次时钟，防止处理途中跨过周零点：
	// 若在 has() 判断后重新取时间，跨过周日 24 点的请求会出现
	// 「按旧周几判定已领、按新周几发奖」的错位。
	today := timeutil.Weekday(c.Now())

	d, err := c.Repo.Get()
	if err != nil {
		return c.Fail(&gamev1.SigninInfoResp{Code: int32(errcode.ErrSystem)}, err)
	}
	return c.Reply(&gamev1.SigninInfoResp{
		Today:          today,
		ClaimedToday:   d.has(today),
		ClaimedDays:    d.Days,
		ClaimedRewards: d.Rewards,
	})
}

// onClaim 领取今天对应的奖励；已领过只回状态，不重复发奖。
//
// 顺序固定为「取状态 → 校验配置 → 幂等判断 → 发奖 → 改状态 → 存回」，
// 任何一步失败都不 Save，内存改动不会产生一次无意义的提交。
func onClaim(c *module.Ctx[SignIn]) error {
	today := timeutil.Weekday(c.Now())

	d, err := c.Repo.Get()
	if err != nil {
		return c.Fail(&gamev1.SigninClaimResp{Code: int32(errcode.ErrSystem)}, err)
	}

	// 四个分支共用一份回包：只有 Code/Already/Rewards 会变，
	// 状态字段（Day/Days/Rewards 汇总）始终回当前值。
	resp := &gamev1.SigninClaimResp{
		Day: today, ClaimedDays: d.Days, ClaimedRewards: d.Rewards,
	}

	grants, cfgErr := rewardsForDay(today)
	if cfgErr != nil {
		// resp 里已经填好了 Day/ClaimedDays/ClaimedRewards，
		// 用 FailCode 只改 Code 再回包，别清空重建（客户端要靠这些字段刷新界面）。
		return c.FailCode(resp, errcode.ErrSigninConfig)
	}
	resp.Rewards = protoRewards(grants)

	// 幂等：重复领取回「已领」状态，不再发奖。
	if d.has(today) {
		resp.Already = true
		return c.Reply(resp)
	}

	// 发奖与登记在同一个提交窗口里：Grant 失败时不改任何状态，
	// 领取位也就不会置上，玩家清包后可重新领。
	changes, grantErr := module.Grant(c.State, grants)
	if grantErr != nil {
		resp.Code = int32(grantCode(grantErr))
		return c.Reply(resp)
	}

	d.add(today, grants)
	if err := c.Repo.Save(d); err != nil {
		return c.Fail(&gamev1.SigninClaimResp{Code: int32(errcode.ErrSystem)}, err)
	}
	resp.ClaimedDays, resp.ClaimedRewards = d.Days, d.Rewards

	if err := c.Reply(resp); err != nil {
		return err
	}
	// 背包变更由 Grant 一并算出（含发放后存量），直接转协议下发。
	return c.Reply(&gamev1.BagChangeNotify{
		Changes: module.ToItemChanges(changes),
	})
}

// grantCode 把发奖失败翻译成本模块的业务码。
//
// 两种失败的处理方式不同：满包是「清包再来」，超限是「数量异常」，
// 客户端提示与客诉话术都不一样，所以不合并成一个码。
func grantCode(err error) errcode.Code {
	if errors.Is(err, module.ErrItemLimit) {
		return errcode.ErrSigninRewardLimit
	}
	return errcode.ErrSigninBagFull
}

// protoRewards 把内部发奖结构转成协议结构（按 ItemID 排序由调用方保证）。
func protoRewards(grants []module.ItemGrant) []*gamev1.SigninReward {
	return lo.Map(grants, func(g module.ItemGrant, _ int) *gamev1.SigninReward {
		return &gamev1.SigninReward{ItemId: g.ItemID, Count: g.Count}
	})
}

// rewardsForDay 汇总配置里某一天的奖励（同一天可配多种道具）。
//
// Go 知识点：先汇总进 map[int32]int64 再按 ItemID 排序输出——
// map 的 range 顺序随机，直接 append 会让每次领取的奖励顺序不同，
// 客户端展示会抖动；排序后输出稳定。同时这里做两级溢出防护：
// 单项 Count 与累计和都不得超过 int32（协议字段宽度）。
func rewardsForDay(day int32) ([]module.ItemGrant, error) {
	counts := make(map[int32]int64)
	for _, cfg := range gamedata.SigninRewardCfgList() {
		if cfg.Day != int64(day) {
			continue
		}
		if cfg.ItemId <= 0 || cfg.ItemId > math.MaxInt32 || cfg.Count <= 0 || cfg.Count > math.MaxInt32 {
			return nil, fmt.Errorf("invalid signin reward config %d", cfg.Id)
		}
		if _, ok := gamedata.GetItemCfg(cfg.ItemId); !ok {
			return nil, fmt.Errorf("signin item %d not found", cfg.ItemId)
		}
		id := int32(cfg.ItemId)
		if cfg.Count > math.MaxInt32-counts[id] {
			return nil, fmt.Errorf("signin reward count overflow")
		}
		counts[id] += cfg.Count
	}
	if len(counts) == 0 {
		return nil, fmt.Errorf("signin day %d has no reward", day)
	}
	// map → slice 再排序：lo.Keys 只负责取键，顺序仍由下面的排序保证稳定。
	out := lo.Map(lo.Keys(counts), func(id int32, _ int) module.ItemGrant {
		return module.ItemGrant{ItemID: id, Count: counts[id]}
	})
	sort.Slice(out, func(i, j int) bool { return out[i].ItemID < out[j].ItemID })
	return out, nil
}
