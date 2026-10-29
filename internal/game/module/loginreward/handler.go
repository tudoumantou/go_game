// handler.go —— 登录发奖流程。
//
// 本模块没有客户端协议，handler 名下的函数都由 OnLogin 钩子驱动。
package loginreward

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"game_study/internal/game/module"
	"game_study/pkg/gamedata"

	gamev1 "game_study/pkg/pb/game/v1"
	"google.golang.org/protobuf/proto"
)

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

// warn 统一告警入口（logger 可为 nil，单测里传 nil 不 panic）。
func (m *Module) warn(msg string, ctx *module.Context, err error) {
	if m.logger != nil {
		m.logger.Warn(msg, "player_id", ctx.State.PlayerID(), "err", err)
	}
}
