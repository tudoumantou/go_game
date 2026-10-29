// types.go —— 签到状态结构与不变量。
//
// 只放「数据长什么样」和「什么算合法」，不放取数逻辑与外部调用。
package signin

import (
	"fmt"
	"sort"

	"game_study/internal/game/module"
	"game_study/pkg/timeutil"
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
