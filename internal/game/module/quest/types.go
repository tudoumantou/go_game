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

type Record struct {
	Status   int32 `json:"status"`
	Progress int32 `json:"progress"`
}

// Data 是任务模块的私有状态：quest_id -> 进度与领取事实。
//
// 「没接取」用「map 里没有这个键」表达，而不是 Status=-1——
// 少一种状态，领取事实的判定（claimed）就不会有第三态。

type Data struct {
	Quests map[int32]Record `json:"quests"`
}

// newData 新玩家的空状态：map 必须初始化，nil map 写会 panic。

func newData() *Data { return &Data{Quests: map[int32]Record{}} }

// M 任务模块的唯一实例。
