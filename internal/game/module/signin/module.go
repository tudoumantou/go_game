// Package signin 实现按北京时间自然周（周一至周日）循环的七日签到。
//
// 业务只做两件事：用 c.Repo.View 读状态（纯查询），或用 c.Repo.Mutate
// 在闭包里「按规则改 + 返回改没改」（写路径）。编解码、标脏、Ext 槽位、
// 落库时机全部由基座承担；持久化适配见 persist_gen.go。
//
// 架构职责：签到信息查询 + 当日领取。与 loginreward 同为「协议少、规则强」
// 的日历玩法，但周期单位是周（weekday 1-7）而非日。
// Ext 槽位：player_signin（ClaimedDays + ClaimedRewards 两列 JSON）。
// 配置表：signin_reward.xml（day=周几，item_id/count）。
//
// 配置热更：每次请求从 gamedata 当前快照读取（不缓存到模块），运营改数值
// 后新请求立即生效；缺天/非法配置由 onClaim 回 codeConfig，启动期不做
// 「七天必须齐全」的硬校验——那会把只改某几天的热更挡在门外。配置完整性
// 由 TestRewardConfiguration 在构建期（CI）守住。
//
// 文件分工（docs/模块开发约定.md §2）：
//
//	module.go  本文件：模块声明与路由
//	types.go   状态结构与不变量校验
//	handler.go 协议处理器
package signin

import (
	"game_study/internal/game/module"
	gamev1 "game_study/pkg/pb/game/v1"
)

const (
	// stateKey 同时是 Ext 槽位键与落库归属的模块名。
	stateKey = "signin"
)

// M 是签到模块的唯一实例：声明在包级，路由在 init 注册。
//
// 没有 New()：日志、状态编解码、分发都由 Define 兜底，每次启动手工
// 组装一遍只是多一处漏配的机会。两个请求都满足「重放无第二次副作用」
// （Info 只读；Claim 按「今天已领」事实幂等），因此可安全声明为可丢弃。
// 测试要换时钟用 M.SetClock。
var M = module.Define[SignIn](stateKey, newSignIn).Guard(module.RequireCache)

func init() {
	M.HandleRoute(&gamev1.SigninInfoReq{}, onInfo)
	M.HandleRoute(&gamev1.SigninClaimReq{}, onClaim)
}
