// Package signin 实现按北京时间自然周（周一至周日）循环的七日签到。
//
// 业务只做三件事：取状态（Repo.Get）、按规则改（has/add）、存回去（Repo.Save）。
// 编解码、标脏、Ext 槽位、落库时机全部由基座承担；持久化适配见 persist.go。
//
// 架构职责：签到信息查询 + 当日领取。与 loginreward 同为「协议少、规则强」
// 的日历玩法，但周期单位是周（weekday 1-7）而非日。
// Ext 槽位：player_signin（ClaimedDays + ClaimedRewards 两列 JSON）。
// 配置表：signin_reward.xml（day=周几，item_id/count）。
//
// 配置热更：每次请求从 gamedata 当前快照读取（不缓存到模块），运营改数值
// 后新请求立即生效；缺天/非法配置由 onClaim 回 codeConfig，启动期不做
// 「七天必须齐全」的硬校验——那会把只改某几天的热更挡在门外。配置完整性
// 由 TestRewardConfiguration 在构建期（CI）守住。
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

const (
	// stateKey 同时是 Ext 槽位键与落库归属的模块名。
	stateKey = "signin"
)

// SignIn 是签到的业务视图：本周已领取天数（1=周一…7=周日）与已领取奖励汇总。
//
// 字段语义：
//   - Days：本周已领取的星期几，落库保持升序（便于对账与 diff）；
//   - Rewards：本周已领取奖励汇总（item_id → 累计数量），只作展示/对账，不发奖。
//
// 跨周后本结构不会被自动清零——清空由周期回调模块调用 Reset 完成，
// 这是「模块不持时钟锚点」设计的已知取舍（见 docs/signin.md）。
//
// Go 知识点：两个字段都没有 json tag，序列化用 Go 字段名（"Days"/"Rewards"）。
// 这是历史契约（表里已存的数据按这个键名），补 tag 会让旧快照读不出来，
// 所以注释里显式记录「无 tag 是故意的」。
// @codec table=player_signin validate=validate
type SignIn struct {
	Days    []int32         `db:"claimed_days"`
	Rewards map[int32]int64 `db:"claimed_rewards"`
}

// M 是签到模块的唯一实例：声明在包级，路由在 init 注册。
//
// 没有 New()：日志、状态编解码、分发都由 Define 兜底，每次启动手工
// 组装一遍只是多一处漏配的机会。两个请求都满足「重放无第二次副作用」
// （Info 只读；Claim 按「今天已领」事实幂等），因此可安全声明为可丢弃。
// 测试要换时钟用 M.SetClock。
var M = module.Define[SignIn](stateKey, newSignIn).Guard(module.RequireCache)

func init() {
	M.HandleRoute(&gamev1.SigninInfoReq{}, onInfo)
	M.HandleRoute(&gamev1.SigninClaimReq{}, onClaim)
}

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
		resp.Code = int32(errcode.ErrSigninConfig)
		return c.Reply(resp)
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

// newSignIn 新玩家的空状态（空切片而非 nil，业务里不必判空）。
func newSignIn() *SignIn { return &SignIn{Days: []int32{}, Rewards: map[int32]int64{}} }

// has 今天是否已领取。
func (d *SignIn) has(day int32) bool {
	for _, v := range d.Days {
		if v == day {
			return true
		}
	}
	return false
}

// add 登记领取事实：天数保持升序落库，奖励按道具累加。
//
// Days 升序是落库契约：JSON diff 与人工对账时顺序稳定；
// append 后 sort.Slice 保证这一点（天数最多 7 个，排序成本可忽略）。
func (d *SignIn) add(day int32, grants []module.ItemGrant) {
	if d.Rewards == nil {
		d.Rewards = make(map[int32]int64, len(grants))
	}
	if !d.has(day) {
		d.Days = append(d.Days, day)
		sort.Slice(d.Days, func(i, j int) bool { return d.Days[i] < d.Days[j] })
	}
	for _, g := range grants {
		d.Rewards[g.ItemID] += g.Count
	}
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

// validate 落库前校验：天数在 1-7，奖励数量为正，坏数据整批回滚。
//
// 这类校验属于业务语义（周序号 1..7），不交给生成器：生成器只管
// 「字段 ↔ 列」的结构映射。
func validate(d *SignIn) error {
	for _, day := range d.Days {
		if day < 1 || day > timeutil.DaysPerWeek {
			return fmt.Errorf("invalid signin day %d", day)
		}
	}
	for id, count := range d.Rewards {
		if id <= 0 || count <= 0 {
			return fmt.Errorf("invalid signin reward %d x %d", id, count)
		}
	}
	return nil
}
