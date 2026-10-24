// Package signin 实现按北京时间自然周（周一至周日）循环的七日签到。
//
// 状态只有「已领取天数 + 已领取奖励汇总」，持久化由 FlatPersister 一行注册，
// 不再有独立的仓储/持久化/日历文件。周切换的清零属于周期回调模块的职责，
// 本模块不持有时钟锚点、不做定时重置；回调模块上线时调用 Reset 即可。
//
// 架构职责：签到信息查询 + 当日领取。与 loginreward 同为「协议少、
// 规则强」的日历玩法，但周期单位是周（weekday 1-7）而非日。
// Ext 槽位：player_signin（ClaimedDays + ClaimedRewards 两列 JSON）。
// 事件订阅：无（签到不消费行为事件，也不产生）。
// 配置表：signin_reward.xml（day=周几，item_id/count）。
// 懒初始化：从未签到返回空 Data（Days 空切片）；跨周清零由外部
// 调用 Reset 完成，读取路径不自动清——这是「模块不持时钟锚点」的取舍。
//
// 模块形态：嵌入 module.Thin 薄模块基座。接口样板（分发/失败响应/
// 可回滚声明/生命周期空实现）由基座预装配，本文件只剩业务：
// 周几口径、领取幂等、发奖预检。改造前 620 行里约三分之一是样板。
package signin

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"time"

	"game_study/internal/game/module"
	"game_study/pkg/codec"
	"game_study/pkg/gamedata"
	"game_study/pkg/log"
	gamev1 "game_study/pkg/pb/game/v1"
	"github.com/samber/lo"
)

const (
	// stateKey 同时是 Ext 槽位键与落库归属的模块名。
	stateKey = "signin"

	MsgInfoReq   = codec.MsgSigninInfoReq
	MsgInfoResp  = codec.MsgSigninInfoResp
	MsgClaimReq  = codec.MsgSigninClaimReq
	MsgClaimResp = codec.MsgSigninClaimResp

	codeSystem   int32 = 1
	codeConfig   int32 = 6601
	codeBagLimit int32 = 6602
	codeBagFull  int32 = 6603

	weekDays int32 = 7
)

// businessZone 固定 UTC+8：进程或容器时区不同不该改变「今天周几」。
//
// 与 loginreward 同一常量：两个日历玩法必须同口径，否则会出现
// 「登录礼包按 UTC 已跨天、签到还没跨周」的错位。
var businessZone = time.FixedZone("Asia/Shanghai", 8*60*60)

// Data 是签到的业务视图：本周已领取天数（1=周一…7=周日）与已领取奖励汇总。
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
type Data struct {
	Days    []int32
	Rewards map[int32]int64
}

// Module 签到模块；now 可注入时钟（测试跨周边界），生产用 time.Now。
//
// 嵌入 *module.Thin：Name/MsgIDs/OnMessage 分发/OnSave/OnLogin/OnLogout/
// Discardable/CacheFailureReply 全部由基座提供，本模块只注册处理器。
// 两个请求都满足「重放无第二次副作用」：Info 只读；Claim 按「今天已领」
// 事实幂等（已领过直接回状态，不第二次发奖），所以注册进 Thin 即承诺
// 可回滚——缓存提交失败时丢弃内存改动重放是安全的。
type Module struct {
	*module.Stateful[Data]
	now func() time.Time
}

// New 的可选时钟用于验证周几边界；生产默认使用服务器时间。
func New(logger *log.Logger, clock ...func() time.Time) *Module {
	m := &Module{now: time.Now}
	if len(clock) > 0 && clock[0] != nil {
		m.now = clock[0]
	}
	m.Stateful = module.NewStateful(stateKey, newData,
		module.ThinLogger(logger),
		module.ThinErrCode(codeSystem),
		// 缓存权威路径未就绪一律拒绝：签到发奖必须走快照 CAS，
		// 直写路径已移除，没有降级方案——宁可不可用，不可双写。
		module.ThinGuard(module.RequireCache),
		// 启动期校验七天奖励配置齐全，配错不等到玩家点领取才暴露。
		module.ThinInit(func(*module.Context) error {
			for day := int32(1); day <= weekDays; day++ {
				if _, err := rewardsForDay(day); err != nil {
					return err
				}
			}
			return nil
		}),
	)
	// HandleR 顺带声明响应协议：错误回包由基座克隆零值原型并置 code，
	// 模块不必再写「请求号 → 响应号 + 工厂」的 switch。
	m.HandleR(MsgInfoReq, MsgInfoResp, &gamev1.SigninInfoResp{}, m.onInfo)
	m.HandleR(MsgClaimReq, MsgClaimResp, &gamev1.SigninClaimResp{}, m.onClaim)
	return m
}

// onInfo 签到信息查询：今天周几、今天是否已领、本周领取全貌。
func (m *Module) onInfo(ctx *module.Context, _ uint32, _ []byte) error {
	// 每个请求只采样一次时钟，防止处理途中跨过周零点：
	// 若在 has() 判断后重新取时间，跨过周日 24 点的请求会出现
	// 「按旧周几判定已领、按新周几发奖」的错位。
	today := weekdayOf(m.now())
	var resp *gamev1.SigninInfoResp
	if err := m.Read(ctx, func(d *Data) {
		resp = &gamev1.SigninInfoResp{
			Today:          today,
			ClaimedToday:   d.has(today),
			ClaimedDays:    d.Days,
			ClaimedRewards: d.Rewards,
		}
	}); err != nil {
		return m.Fail(ctx, MsgInfoReq, codeSystem, err)
	}
	return m.Reply(ctx, MsgInfoResp, resp)
}

// onClaim 领取今天对应的奖励；已领过只回状态，不重复发奖。
//
// 一次 Mutate 走完「读状态 → 校验 → 登记领取事实」：闭包返回 false 表示
// 没有改动，基座不会写回 Ext，也就不会产生一次无意义的提交。
func (m *Module) onClaim(ctx *module.Context, _ uint32, _ []byte) error {
	today := weekdayOf(m.now())

	var (
		code    int32
		grants  []module.ItemGrant
		changes []module.BagChange
		already bool
		snap    Data
	)
	err := m.Mutate(ctx, func(d *Data) bool {
		// 无论走哪个分支，回包都要带当前状态，统一在这里取样。
		defer func() { snap = *d }()

		g, cfgErr := rewardsForDay(today)
		if cfgErr != nil {
			code = codeConfig
			return false
		}
		grants = g
		if d.has(today) {
			already = true // 幂等：重复领取回「已领」状态，不再发奖
			return false
		}
		// 发奖与登记在同一个提交窗口里：Grant 失败时不改任何状态，
		// 领取位也就不会置上，玩家清包后可重新领。
		c, grantErr := module.Grant(ctx.State, g)
		if grantErr != nil {
			code = grantCode(grantErr)
			return false
		}
		changes = c
		d.add(today, g)
		return true
	})
	if err != nil {
		return m.Fail(ctx, MsgClaimReq, codeSystem, err)
	}

	resp := &gamev1.SigninClaimResp{
		Code: code, Day: today, Already: already,
		Rewards:        protoRewards(grants),
		ClaimedDays:    snap.Days,
		ClaimedRewards: snap.Rewards,
	}
	if code != 0 || already {
		return m.Reply(ctx, MsgClaimResp, resp)
	}

	if err := m.Reply(ctx, MsgClaimResp, resp); err != nil {
		return err
	}
	// 背包变更由 Grant 一并算出（含发放后存量），直接转协议下发。
	return m.Reply(ctx, codec.MsgBagChangeNotify, &gamev1.BagChangeNotify{
		Changes: module.ToItemChanges(changes),
	})
}

// grantCode 把发奖失败翻译成本模块的业务码。
//
// 两种失败的处理方式不同：满包是「清包再来」，超限是「数量异常」，
// 客户端提示与客诉话术都不一样，所以不合并成一个码。
func grantCode(err error) int32 {
	if errors.Is(err, module.ErrItemLimit) {
		return codeBagLimit
	}
	return codeBagFull
}

// newData 新玩家的空状态（空切片而非 nil，业务闭包里不必判空）。
func newData() *Data { return &Data{Days: []int32{}, Rewards: map[int32]int64{}} }

// has 今天是否已领取。
func (d *Data) has(day int32) bool {
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
func (d *Data) add(day int32, grants []module.ItemGrant) {
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

// weekdayOf 返回北京时间下的周几：1=周一…7=周日。
//
// Go 知识点：time.Weekday 的零值是 Sunday（0），与「周一=1」差一天，
// 所以周日要单独映射成 7；In(zone) 把时间切到固定时区再取 Weekday，
// 避免服务器 UTC 时把周一算成周日。
func weekdayOf(now time.Time) int32 {
	wd := now.In(businessZone).Weekday()
	if wd == time.Sunday {
		return 7
	}
	return int32(wd)
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
