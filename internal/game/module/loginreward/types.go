// types.go —— 登录礼包的状态结构。
//
// 只放「数据长什么样」，落库转换见 persist.go，业务规则见 handler.go。
package loginreward

import "fmt"

// Data 是登录礼包的私有状态。
//
// 字段语义：
//   - LastDay：最近一次发放所在的北京时间自然日序号（见 timeutil.BusinessDay）；
//   - Streak：连续登录天数，断签（LastDay != 今天-1）时重置为 1。
//
// 两者与道具在同一次快照 CAS 提交，是本模块唯一的幂等依据——
// 刻意不占用 AppliedGrants：每天一个业务键会让 applied_grants 无限膨胀。
//
// Go 知识点：零值即合法初始态（LastDay=0 表示「从未发放」——它对应
// 1970-01-01，早于任何真实数据；见 timeutil.BusinessDay），所以 newData
// 对空槽位返回 &Data{}，不需要额外的 initialized 标志位。
//
// **json tag 是故意不写的大写字段名，不是偷懒。** 迁移前本模块的 Ext 槽位
// 存的是 model.PlayerLogin 的 JSON（那个结构体没有 json tag，序列化出的是
// `{"PlayerId":..,"LastDay":..,"Streak":..}`）。改成 Data 之后如果给字段加
// 小写 tag，线上所有玩家的槽位都会 json.Unmarshal 失败 → 读成零值 →
// 连续天数归零、当天奖励重发一次。用大写字段名让新结构能直接解旧 JSON
// （PlayerId 无对应字段，被忽略），零迁移成本。
//
// 要改成小写 tag 时必须同时写迁移：读旧格式 → 转换 → 写新格式。
// loginreward_test.go 的 TestDataDecodesLegacyExtJSON 锁住这个约定。
//
// db tag 与 json tag 是两件事：db 决定映射哪一列，json 决定快照里的键名。
// 这里只加 db，json 继续留空以保持上面的兼容契约。
// @codec table=player_login validate=validate
type Data struct {
	LastDay int64 `db:"last_day"`
	Streak  int32 `db:"streak"`
}

// validate 落库前校验：天数与连续计数不能为负。
//
// 零值合法（未发放过），负数一定是数据损坏。
func validate(d *Data) error {
	if d.LastDay < 0 || d.Streak < 0 {
		return fmt.Errorf("invalid login state: day=%d streak=%d", d.LastDay, d.Streak)
	}
	return nil
}
