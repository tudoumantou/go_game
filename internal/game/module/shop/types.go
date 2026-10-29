// internal/game/module/shop/shop.go —— 商店模块（6401-6404）
//
// 本周期购买次数放在玩家快照 Ext["shop"]：扣费、发货与次数累加在同一次
// Redis CAS 提交，失败整批回滚（契约见 docs/R2.1-缓存原子变更契约.md）。
//
// 架构职责：商品列表 + 购买。商品定义全部来自 shop_goods.xml，
// 模块只记录「本周期已购次数」。
// Ext 槽位：shop_record（Day + Bought JSON 列）。
// 事件订阅：产出方。购买成功 Emit "buy"（Key=商品 ID），驱动任务。
// 配置表：shop.xml / shop_goods.xml（Col2=道具，Col3=单价，Col4=每份数量，
// Col5=每周期限购份数，0 或负数表示不限）、shop_refresh.xml（col2=daily
// 表示按日刷新）。
// 懒初始化：从未购买返回空 Data；跨天清零发生在**读取路径**上
// （Get 之后按 Day 比较），不需要全服定时任务，也不怕玩家离线几天漏刷。
//
// 写法（v3.6 迁移）：模块声明一行、路由若干行、业务只碰 Ctx.Repo；
// 协议号由消息类型反查，错误码来自统一枚举。

package shop

type Data struct {
	Day    int64            `json:"day"`
	Bought map[string]int32 `json:"bought"`
}

// M 商店模块的唯一实例。
//
// 不做跨天清理——清零逻辑在读取路径上（见 normalize），因此没有登录钩子。
