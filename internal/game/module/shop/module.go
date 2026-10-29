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

import (
	"game_study/internal/game/module"
	gamev1 "game_study/pkg/pb/game/v1"
)

// stateKey 同时是 Ext 槽位键、模块名与落库归属标签。

const stateKey = "shop"

// 商店结算货币是金币（module.ItemGold），道具 ID 定义在 module/itemid.go 一处。

// shopExists 商店配置是否存在（shop 表是商品归属的权威）。
//
// 商品行第一列是 shop_id：归属到不存在的商店说明配置被改坏，
// 这种商品既不展示也不能买——否则玩家能绕过「商店下架」直接购买。

var M = module.Define[Data](stateKey, newData)

func init() {
	M.HandleRoute(&gamev1.ShopListReq{}, onList)
	module.Route(M, &gamev1.ShopBuyReq{}, onBuy)
}

// onList 商店列表（商品 + 本周期已购 / 限购）。
//
// normalize 传今天：跨天时在读取路径清零，列表天然展示新周期。
