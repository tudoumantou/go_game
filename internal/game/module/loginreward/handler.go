// handler.go —— 登录发奖流程。
//
// 本模块没有客户端协议，handler 名下的函数都由 OnLogin 钩子驱动。
package loginreward

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"game_study/internal/game/module"
	"game_study/pkg/codec"
	"game_study/pkg/gamedata"

	gamev1 "game_study/pkg/pb/game/v1"
	"google.golang.org/protobuf/proto"
)

// onGrant 发放当日登录礼包。
//
// 重连会再次触发 OnLogin，但 LastDay 已是今天时闭包返回 changed=false，
// 不产生任何提交——这是本模块幂等的全部机制：状态即事实，不需要业务键。
//
// 闭包里三个「返回 false」的分支各有一个理由：
//   - 今天已领：本来就无事可做；
//   - 配置缺失：状态不前进，下次登录会重试（配置坏掉不该吞掉玩家的奖励）；
//   - 满包：同上。这里与 quest/copy 的「满包拒绝」不同——那边玩家能主动
//     重试，这条链路连重试入口都没有，只能靠「状态不前进」保住奖励。
//
// Grant 失败时一个道具都不会加，所以「状态没前进 = 奖励没发」始终成立。
func onGrant(c *module.Ctx[Data]) error {
	day := businessDay(c.Now())

	var (
		changes  []*gamev1.ItemChange // 背包变更，落库成功后下发
		streak   int32                // 本次连续天数（日志用）
		granted  bool
		warnMsg  string
		warnErrs error
	)

	err := c.Repo.Mutate(func(d *Data) (bool, error) {
		if d.LastDay == day {
			return false, nil // 今天已经领过
		}
		// 连续登录判定：昨天（day-1）领过才 +1，否则（断签/首次）重置为 1。
		next := int32(1)
		if d.LastDay == day-1 {
			next = d.Streak + 1
		}

		grants, cfgErr := rewardForStreak(next)
		if cfgErr != nil {
			warnMsg, warnErrs = "login reward config missing", cfgErr
			return false, nil
		}
		bagChanges, grantErr := module.Grant(c.State, grants)
		if grantErr != nil {
			warnMsg, warnErrs = "login reward deferred: bag full", grantErr
			return false, nil
		}

		d.Streak = next
		d.LastDay = day
		streak = next
		granted = true
		changes = module.ToItemChanges(bagChanges)
		return true, nil
	})
	if err != nil {
		// 状态解码/编码失败：上抛。登录流程会中断——放行后玩家会发现
		// 奖励全丢，不如当场失败让监控看到。
		return err
	}
	if warnMsg != "" {
		c.Log().Warn(warnMsg, "player_id", c.State.PlayerID(), "err", warnErrs)
	}
	if !granted {
		return nil
	}

	c.Log().Info("login reward granted",
		"player_id", c.State.PlayerID(), "day", day, "streak", streak, "items", len(changes))
	// 登录阶段客户端可能还没进场景，推送失败不影响正确性（背包以下次拉取为准）。
	return c.Context.Reply(codec.MsgBagChangeNotify, mustMarshal(&gamev1.BagChangeNotify{Changes: changes}))
}

// mustMarshal 编码推送消息；失败返回 nil（Reply 对 nil 静默跳过）。
//
// 推送是「尽力而为」：奖励已经进了玩家背包，一条通知编码失败不该让登录
// 流程报错。协议是固定结构，真正失败只可能是反射层异常，非业务可恢复。
func mustMarshal(msg proto.Message) []byte {
	raw, err := proto.Marshal(msg)
	if err != nil {
		return nil
	}
	return raw
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
// （validateConfig 调本函数）暴露，而不是发奖时静默发 0 个。
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
