// types.go —— 登录礼包的状态结构。
//
// 只放「数据长什么样」，落库转换见 persist.go，业务规则见 handler.go。
package loginreward

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
