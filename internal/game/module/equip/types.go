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

type Entry struct {
	UID          int64 `json:"uid"`
	EquipID      int32 `json:"equip_id"`
	EnhanceLevel int32 `json:"enhance_level"`
	Quality      int32 `json:"quality"`
	Equipped     bool  `json:"equipped"`
}

// Data 是装备模块的私有状态：玩家全部装备实例。
//
// 用切片而非 map：持有量小（几十件）、顺序即获得顺序，
// 且 UID 查找是线性扫描也够快（findEquip）。

type Data struct {
	Equips []Entry `json:"equips"`
}

// M 装备模块的唯一实例。
