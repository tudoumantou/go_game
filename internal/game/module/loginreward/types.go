// types.go —— 登录礼包的状态结构。
//
// 只放「数据长什么样」，落库转换见 persist.go，业务规则见 handler.go。
package loginreward

import "time"

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
// 所以 newData 对空槽位返回 &Data{}，不需要额外的 initialized 标志位。
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
type Data struct {
	LastDay int64
	Streak  int32
}

// businessDay 北京时间自然日序号。
//
// 1970-01-01 是第 0 天；LastDay=0 的零值语义因此不等于任何自然日
// （真正的第 0 天早已过去），可以安全表示「从未发放」。
//
// 与 timeutil.BusinessZone 的关系：这里直接算偏移而不是 LoadLocation，
// 因为 Beijing 无夏令时，+8 是常量；换成有夏令时的时区必须改写。
func businessDay(now time.Time) int64 { return (now.Unix() + 8*60*60) / 86400 }
