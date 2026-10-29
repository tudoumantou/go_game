// internal/game/module/equip/equip.go —— 装备模块（3101-3106）
//
// 装备实例放在玩家快照 Ext["equip"]：穿戴/卸下只改内存，属性加成随同一次
// 提交生效，Redis CAS 失败时装备状态与加成一起回滚（契约见 docs/R2.1-缓存原子变更契约.md）。
//
// 架构职责：装备的协议面 + 属性加成的维护方之一。穿戴状态是「玩法进度」，
// 因此整包进 Ext 槽位；加成是派生值只进内存，登录时按已穿戴件重算。
// 事件订阅：无（穿戴不产生行为事件；P7 可加 equip_change 驱动任务）。
// 配置表：equip.xml（Col2=等级门槛，Col3=部位，Col4=攻击加成）。
// 懒初始化：玩家从未获得装备时 Ext 槽位为空行，load() 返回空 Data，
// 不会为「空装备列表」预建行——首次穿戴才产生第一行快照数据。

package equip

import (
	"game_study/internal/game/module"
	gamev1 "game_study/pkg/pb/game/v1"
)

// stateKey 同时是 Ext 槽位键与模块名：Ext/SetExt、落库归属、日志标签
// 三处共用同一个字符串，改一处即可全局一致（各玩法模块的同一约定）。

const stateKey = "equip"

// itemDiamond 强化消耗的货币（钻石）。与 shop/skill/quest 共用 1001。

const itemDiamond int32 = 1001

// Entry 装备实例。
//
// 字段语义：
//   - UID：实例唯一 ID（同配置 ID 可持有多件，按 UID 穿戴/卸下）；
//   - EnhanceLevel：强化等级；
//   - Quality：品质（配置派生，预留强化差异）；
//   - Equipped：是否已穿戴；加成只由已穿戴件提供。
//
// Go 知识点：每个字段都带 json tag——快照里存的是 JSON 字符串，
// tag 即「线上契约」。零值语义：EnhanceLevel=0 表示未强化，
// Equipped=false 表示未穿戴，omitempty 一概不加——省略 false/0
// 会让「未强化」和「字段缺失」无法区分。

var M = module.Define[Data](stateKey, newData).WithLogin(onLogin)

func init() {
	M.HandleRoute(&gamev1.EquipListReq{}, onList)
	module.Route(M, &gamev1.EquipWearReq{}, onWear)
	module.Route(M, &gamev1.EquipTakeOffReq{}, onTakeOff)
	module.Route(M, &gamev1.EquipEnhanceReq{}, onEnhance)
}

// onLogin 按已穿戴装备重算属性加成。
//
// 加成是派生值，不落库：每次登录从快照里的装备列表重算，避免重复累加。
// 时序契约：玩家层在 LoginAll 之前统一 ClearBonus，本模块只叠加自己的部分；
// 若不先清空，重连一次就会把 atk 加成翻倍。
