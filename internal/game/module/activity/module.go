// internal/game/module/activity/activity.go —— 活动模块（6501-6504）
//
// 活动进度与领取位图放在玩家快照 Ext["activity"]：领奖与发放在同一次 Redis CAS
// 提交，Redis 失败时两者一起回滚（契约见 docs/R2.1-缓存原子变更契约.md）。
//
// 架构职责：活动进度累加 + 领奖。进度有两个来源：登录类活动在 OnLogin
// +1，行为类活动走 OnEvent 累加；全服里程碑活动额外上报 Kafka。
// Ext 槽位：activity_record（Records JSON 列）。
// 事件订阅：消费方（EventAware），按配置 Col2 匹配事件类型。
// 配置表：activity.xml（Col1=名称，Col2=事件类型或 "login"，Col3=目标）、
// activity_period.xml（开放周期，col1=周期类型）。
// 懒初始化：未参与活动返回空 Data；窗口判定不落库——
// 「是否开放」每次请求现算（openNow），过期活动直接从列表隐藏。

package activity

import (
	"game_study/internal/game/module"
	gamev1 "game_study/pkg/pb/game/v1"
)

// stateKey 同时是 Ext 槽位键、模块名与落库归属标签。

const stateKey = "activity"

// 奖励道具固定为金币（module.ItemGold），道具 ID 定义在 module/itemid.go 一处。

// Record 单个活动的进度与领取位图。
//
// 字段语义：
//   - Progress：当前进度，由 OnEvent 累加并在配置 Col3 目标处截断；
//   - RewardMask：领取位图，bit0=第一档奖励已领；多档奖励时按位扩展。
//
// Go 知识点：位图（bitmask）把「哪几档领了」压成一个整数——
// 单档活动只用 bit0（&1 / |=1），多档时第 n 档是 1<<(n-1)，
// 比 []bool 或 map 都省空间，且整型比较可并行做多重判定。

var producer ProgressProducer

// SetProducer 注入贡献上报能力（组装层在 Register 之前调用）。

var M = module.Define[Data](stateKey, newData).
	WithLogin(onLogin).
	WithEvent(onEvent)

func init() {
	M.HandleRoute(&gamev1.ActivityListReq{}, onList)
	module.Route(M, &gamev1.ActivityClaimReq{}, onClaim)
}

// onLogin 登录类活动进度 +1，纯内存改动，随本次快照提交（经 ThinLogin 挂接）。
//
// 与 quest.OnEvent 的分工：登录是「事件」的一种，但发生在协议之外，
// 所以单独在 OnLogin 钩子里推进，而不是伪造一条 Event。
// 状态损坏时吞错继续登录：活动进度丢一次可以原谅，
// 把玩家拦在登录外不能。
