// internal/game/module/quest/quest.go —— 任务模块（7101-7106）
//
// 状态（进度 + 领取事实）整体放在玩家快照的 Ext["quest"] 里：领奖时改内存、
// 与道具经验一起提交，Redis CAS 失败则整批回滚（契约见 docs/R2.1-缓存原子变更契约.md）。
// 不再直接写 MySQL 业务事实。
//
// 架构职责：任务进度推进 + 领奖。任务列表由 quest.xml 配置派生，
// 模块只存「接取/进度/领取」三态，不存任务定义。
// 事件订阅：消费方（EventAware）。监听 kill/use_item/copy_pass 等事件，
// 按配置 Col2 匹配后推进进度。
// 配置表：quest.xml（Col1=名称，Col2=监听事件类型，Col3=目标次数）。
// 懒初始化：未接取的任务在 Records map 里不存在，接取时才建键；
// 列表请求把「配置全集」与「已有记录」join 后下发。

package quest

import (
	"game_study/internal/game/module"
	gamev1 "game_study/pkg/pb/game/v1"
)

// stateKey 同时是 Ext 槽位键、模块名与落库归属标签。

const stateKey = "quest"

// 任务状态（与快照内 JSON 一致）。
//
// Go 知识点：这是一组「状态机常量」——显式赋值而非 iota，
// 因为取值同时是线上 JSON 契约（旧快照里躺着的数字），
// 一旦改成 iota 自动编号，历史数据全部错位。

const (
	statusDoing   int32 = 0 // 进行中
	statusDone    int32 = 1 // 已完成（可领奖）
	statusClaimed int32 = 2 // 已领奖
)

// Record 单个任务的状态。
//
// 字段语义（与快照内 JSON 一致）：
//   - Status：0=进行中 / 1=已完成待领 / 2=已领奖，见下方常量；
//   - Progress：当前进度，达到配置 Col3 即完成；超额在 OnEvent 里截断。

var M = module.Define[Data](stateKey, newData).
	WithLogout(onLogout).
	WithEvent(onEvent)

func init() {
	M.HandleRoute(&gamev1.QuestListReq{}, onList)
	module.Route(M, &gamev1.QuestAcceptReq{}, onAccept)
	module.Route(M, &gamev1.QuestClaimReq{}, onClaim)
}

// onLogout 下线时标脏，保证最后一次进度推进随快照落库。
//
// 不做预加载：任务数据随 Ext 懒加载，列表请求时才解码 JSON。
