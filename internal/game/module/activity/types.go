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
	"context"
	pkgmq "game_study/pkg/mq"
)

type Record struct {
	Progress   int32 `json:"progress"`
	RewardMask int64 `json:"reward_mask"`
}

// Data 是活动模块的私有状态：activity_id -> 进度与已领位图。

type Data struct {
	Records map[int32]Record `json:"records"`
}

// producer 全服活动贡献上报（R7.3）；nil 表示不上报（单机/测试）。
//
// 包级而不是模块字段：模块本身已是进程级单例（M），注入发生在启动期
// 单线程，之后只读——与 Define 的「无 New」形态一致。

type ProgressProducer interface {
	// ProduceEnvelope 投递一条全服活动贡献消息；失败由调用方降级为日志。
	ProduceEnvelope(ctx context.Context, env *pkgmq.Envelope) error
}

// M 活动模块的唯一实例。
//
// 没有 New()：日志、编解码、分发、生命周期都由 Define 兜底；
// producer 走 SetProducer 注入。两个请求都可重放（列表只读；领奖按
// 位图幂等），因此可安全声明为可丢弃。
