// internal/game/module/loginreward/loginreward.go —— 登录礼包（R3.2）
//
// 规则：按北京时间自然日，每天首次登录发放一次；连续登录天数对应
// login_reward.xml 的档位（col1），断签重置为 1，超出最大档按最大档发。
//
// 幂等靠状态本身：LastDay 与奖励在同一次快照 CAS 里提交，重连、换服、
// 进程重启都不会重复发。不占用 AppliedGrants —— 那是订单级业务键的地盘，
// 每天一个键会让快照无限膨胀（见 docs/R2.1-缓存原子变更契约.md）。
//
// 架构职责：登录时自动发奖（无客户端协议）。这是「OnLogin 侧写模块」的
// 范本——没有 MsgIDs，逻辑全部挂在登录钩子上。
// Ext 槽位：player_login（LastDay + Streak 两列）。
// 事件订阅：无。
// 配置表：login_reward.xml（col1=第几天，col2="item_id,count"）。
// 懒初始化：从未登录过返回零值 Data（LastDay=0 不等于任何自然日序号），
// 首次登录才产生第一行记录。
//
// 模块形态：嵌入 module.Thin 薄模块基座。本模块没有客户端协议
// （MsgIDs 为 nil），只挂 Init（配置校验）与 OnLogin（发奖）两个钩子。
package loginreward

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"game_study/internal/game/module"
	"game_study/pkg/gamedata"
	"game_study/pkg/log"
	"game_study/pkg/model"
	"game_study/pkg/writebehind"

	gamev1 "game_study/pkg/pb/game/v1"
	"google.golang.org/protobuf/proto"
)

// stateKey 同时是 Ext 槽位键、模块名与落库归属标签。
const stateKey = "login"

// businessZone 固定 UTC+8：与签到共用同一套业务日口径。
//
// 用 FixedZone 而不是 LoadLocation：不依赖容器里有没有 tzdata，
// 也不会因系统时区变化改变「今天」的判定。
var businessZone = time.FixedZone("Asia/Shanghai", 8*60*60)

// Data 是登录礼包的私有状态。
//
// 字段语义：
//   - LastDay：最近一次发放所在的北京时间自然日序号（见 businessDay）；
//   - Streak：连续登录天数，断签（LastDay != 今天-1）时重置为 1。
//
// 两者与道具在同一次快照 CAS 提交，是本模块唯一的幂等依据——
// 刻意不占用 AppliedGrants：每天一个业务键会让 applied_grants 无限膨胀。
//
// Go 知识点：零值即合法初始态（LastDay=0 表示「从未发放」），
// 所以 load 对空行直接返回 &Data{}，不需要额外的 initialized 标志位。
type Data struct {
	LastDay int64 `json:"last_day"`
	Streak  int32 `json:"streak"`
}

// init 注册持久化：player_login 主键为 (player_id)。
func init() {
	writebehind.RegisterFlat[model.PlayerLogin](stateKey, model.PlayerLoginFlat, validate)
}

// validate 落库前校验：天数与连续计数不能为负。
//
// 零值合法（未发放过），负数一定是数据损坏。
func validate(row *model.PlayerLogin) error {
	if row.LastDay < 0 || row.Streak < 0 {
		return fmt.Errorf("invalid login state: day=%d streak=%d", row.LastDay, row.Streak)
	}
	return nil
}

// Module 登录礼包模块（无协议：登录时自动发放）。
//
// 嵌入 *module.Thin：Module 接口与可选能力由基座预装配。
// logger 由本模块自持：Thin 内的 logger 只服务协议失败路径，
// 而本模块没有客户端协议，告警走自己的 warn。
type Module struct {
	*module.Thin
	logger *log.Logger
	now    func() time.Time // 时钟可注入：测试跨日/断签边界，生产用 time.Now
}

// New 的可选时钟用于测试跨日与断签；生产默认服务器时间。
//
// Go 知识点：clock ...func() time.Time 是「可选依赖」的惯用法——
// 可变参数 + nil 检查，比再提供一个 NewWithClock 更省 API 面积。
func New(logger *log.Logger, clock ...func() time.Time) *Module {
	m := &Module{logger: logger, now: time.Now}
	if len(clock) > 0 && clock[0] != nil {
		m.now = clock[0]
	}
	m.Thin = module.NewThin(stateKey,
		module.ThinLogger(logger),
		// 启动期校验配置可用：配置错误在这里暴露而不是玩家登录时——
		// 启动期失败会中断发布，线上失败则影响每个登录玩家，
		// 失败要早、要响。
		module.ThinInit(func(*module.Context) error {
			if len(gamedata.LoginRewardCfgList()) == 0 {
				return fmt.Errorf("login_reward.xml is empty")
			}
			for _, cfg := range gamedata.LoginRewardCfgList() {
				if cfg.Col1 <= 0 {
					return fmt.Errorf("invalid login reward day %d", cfg.Col1)
				}
				if _, err := parseReward(cfg.Col2); err != nil {
					return fmt.Errorf("login reward %d: %w", cfg.Id, err)
				}
			}
			return nil
		}),
		module.ThinLogin(m.grantOnLogin),
	)
	return m
}

// 保证 Module 满足 module.Module（Thin 已预实现全部方法）。
var _ module.Module = (*Module)(nil)

// grantOnLogin 发放当日登录礼包。
//
// 重连会再次触发 OnLogin，但 LastDay 已是今天时直接返回，不会重复发奖。
// 这是本模块幂等的全部机制：状态即事实，不需要额外业务键。
func (m *Module) grantOnLogin(ctx *module.Context) error {
	day := businessDay(m.now())
	d, err := load(ctx.State)
	if err != nil {
		return err
	}
	if d.LastDay == day {
		return nil // 今天已经领过
	}
	// 连续登录判定：昨天（day-1）领过才 +1，否则（断签/首次）重置为 1。
	if d.LastDay == day-1 {
		d.Streak++
	} else {
		d.Streak = 1 // 断签或首次登录
	}
	d.LastDay = day

	grants, err := rewardForStreak(d.Streak)
	if err != nil {
		// 配置缺失不阻断登录，但状态不前进，下次登录会重试。
		m.warn("login reward config missing", ctx, err)
		return nil
	}
	// 满包（或数量超限）时不前进状态：下次登录会重新发放，不会吞掉当天奖励。
	// 与 quest/copy 的「满包拒绝」不同：那边是玩家主动重试，
	// 这里连「重试入口」都没有，只能靠状态不前进来保奖励。
	// Grant 失败时一个道具都不会加，所以「状态没前进 = 奖励没发」始终成立。
	changes, grantErr := module.Grant(ctx.State, grants)
	if grantErr != nil {
		m.warn("login reward deferred: bag full", ctx, grantErr)
		return nil
	}
	// save 与发奖都只改内存，同一次快照 CAS 提交：
	// CAS 失败则「已发当天」的事实与道具一起丢弃，不会吞奖。
	if err := save(ctx.State, ctx.State.PlayerID(), d); err != nil {
		return err
	}

	if m.logger != nil {
		m.logger.Info("login reward granted",
			"player_id", ctx.State.PlayerID(), "day", day, "streak", d.Streak, "items", len(grants))
	}
	// 登录阶段客户端可能还没进场景，推送失败不影响正确性（背包以下次拉取为准）。
	if ctx.Send != nil {
		if raw, err := proto.Marshal(&gamev1.BagChangeNotify{Changes: module.ToItemChanges(changes)}); err == nil {
			_ = ctx.Send(3010, raw)
		}
	}
	return nil
}

// rewardForStreak 取连续第 streak 天的奖励；超过最大档按最后一档发。
//
// Go 知识点：先 make+copy 再排序，是因为 gamedata.LoginRewardCfgList()
// 返回的是配置包的内部切片——直接 sort.Slice 会把它按 Col1 重排，
// 污染所有后续读取方。copy 出私有副本再排序，是「不信任外部切片
// 所有权」的标准防护（与共享底层数组相关的经典坑）。
func rewardForStreak(streak int32) ([]module.ItemGrant, error) {
	cfgs := make([]gamedata.LoginRewardCfg, len(gamedata.LoginRewardCfgList()))
	copy(cfgs, gamedata.LoginRewardCfgList())
	sort.Slice(cfgs, func(i, j int) bool { return cfgs[i].Col1 < cfgs[j].Col1 })
	if len(cfgs) == 0 {
		return nil, fmt.Errorf("login_reward.xml is empty")
	}
	// 默认取最大档：连续天数超过配置上限时「按最大档发」，
	// 老玩家不会被惩罚。
	pick := cfgs[len(cfgs)-1]
	for _, cfg := range cfgs {
		if cfg.Col1 >= int64(streak) {
			pick = cfg
			break
		}
	}
	g, err := parseReward(pick.Col2)
	if err != nil {
		return nil, fmt.Errorf("login reward %d: %w", pick.Id, err)
	}
	return []module.ItemGrant{g}, nil
}

// parseReward 解析 "item_id,count"。
//
// 道具必须存在于 item.xml：配置引用不存在的道具要在启动期
// （Init 调本函数）暴露，而不是发奖时静默发 0 个。
func parseReward(s string) (module.ItemGrant, error) {
	parts := strings.Split(s, ",")
	if len(parts) != 2 {
		return module.ItemGrant{}, fmt.Errorf("expect \"item_id,count\", got %q", s)
	}
	id, err := strconv.ParseInt(strings.TrimSpace(parts[0]), 10, 32)
	if err != nil || id <= 0 {
		return module.ItemGrant{}, fmt.Errorf("invalid item id %q", parts[0])
	}
	count, err := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64)
	if err != nil || count <= 0 {
		return module.ItemGrant{}, fmt.Errorf("invalid count %q", parts[1])
	}
	if _, ok := gamedata.GetItemCfg(id); !ok {
		return module.ItemGrant{}, fmt.Errorf("item %d not found", id)
	}
	return module.ItemGrant{ItemID: int32(id), Count: count}, nil
}

// businessDay 北京时间自然日序号。
//
// 1970-01-01 是第 0 天；LastDay=0 的零值语义因此不等于任何自然日
// （真正的第 0 天早已过去），可以安全表示「从未发放」。
func businessDay(now time.Time) int64 { return (now.Unix() + 8*60*60) / 86400 }

// load 读取登录状态；从未登录过返回零值（LastDay=0 不等于任何自然日序号）。
func load(state writebehind.ExtState) (*Data, error) {
	row, err := writebehind.LoadExt[model.PlayerLogin](state, stateKey)
	if err != nil {
		return nil, err
	}
	if row == nil {
		return &Data{}, nil
	}
	return &Data{LastDay: row.LastDay, Streak: row.Streak}, nil
}

// save 把发放事实写回 Ext 槽位；落库调度由 Write-Behind 负责。
func save(state writebehind.ExtState, playerID int64, d *Data) error {
	return writebehind.SaveExt(state, stateKey, &model.PlayerLogin{
		PlayerId: playerID,
		LastDay:  d.LastDay,
		Streak:   d.Streak,
	})
}

// warn 统一告警入口（logger 可为 nil，单测里传 nil 不 panic）。
func (m *Module) warn(msg string, ctx *module.Context, err error) {
	if m.logger != nil {
		m.logger.Warn(msg, "player_id", ctx.State.PlayerID(), "err", err)
	}
}
