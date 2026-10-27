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
	"fmt"
	"time"

	"game_study/internal/common/errcode"
	"game_study/internal/game/module"
	"game_study/pkg/codec"
	"game_study/pkg/gamedata"

	gamev1 "game_study/pkg/pb/game/v1"
	"google.golang.org/protobuf/proto"
)

// stateKey 同时是 Ext 槽位键、模块名与落库归属标签。
const stateKey = "shop"

// itemDiamond 商店结算货币（钻石）。与 quest/skill/activity 共用 1001。
const itemDiamond int32 = 1001

// shopExists 商店配置是否存在（shop 表是商品归属的权威）。
//
// 商品行第一列是 shop_id：归属到不存在的商店说明配置被改坏，
// 这种商品既不展示也不能买——否则玩家能绕过「商店下架」直接购买。
func shopExists(shopID int32) bool {
	_, ok := gamedata.GetShopCfg(int64(shopID))
	return ok
}

// Data 是商店模块的私有状态： "shopID:goodsID" -> 本周期已购次数。
//
// Day 是购买次数所属的业务日；每日刷新的商店在读取时按天清零（shop_refresh.xml）。
//
// Go 知识点：Bought 的键是字符串复合键（"1:1001"）而非嵌套 map——
// 购买记录是稀疏的（几十个商品只买过几个），复合键省掉一层 map
// 且 JSON 序列化后人类可读，便于线上排查。
type Data struct {
	Day    int64            `json:"day"`
	Bought map[string]int32 `json:"bought"`
}

// M 商店模块的唯一实例。
//
// 不做跨天清理——清零逻辑在读取路径上（见 normalize），因此没有登录钩子。
var M = module.Define[Data](stateKey, newData)

func init() {
	M.HandleRoute(&gamev1.ShopListReq{}, onList)
	module.Route(M, &gamev1.ShopBuyReq{}, onBuy)
}

// onList 商店列表（商品 + 本周期已购 / 限购）。
//
// normalize 传今天：跨天时在读取路径清零，列表天然展示新周期。
func onList(c *module.Ctx[Data]) error {
	d, err := c.Repo.Get()
	if err != nil {
		return c.Fail(&gamev1.ShopListResp{Code: int32(errcode.ErrSystem)}, err)
	}
	// 只读路径上的跨天重置不写回：下次读会重算，行为一致（原实现同）。
	normalize(d, businessDay(time.Now()))
	resp := &gamev1.ShopListResp{}
	for _, cfg := range gamedata.ShopGoodsCfgList() {
		shopID := int32(cfg.Col1) // 商品归属的商店
		if !shopExists(shopID) {
			continue // 归属商店已下架/配置错：不展示也不允许购买
		}
		resp.Goods = append(resp.Goods, &gamev1.ShopGoods{
			GoodsId: int32(cfg.Id),
			ItemId:  int32(cfg.Col2),
			Price:   int32(cfg.Col3),
			Count:   int32(cfg.Col4),
			Bought:  d.Bought[goodsKey(shopID, int32(cfg.Id))],
			Limit:   int32(cfg.Col5), // 0 = 不限购（配置缺列时的零值即「不限」）
		})
	}
	return c.Reply(resp)
}

// onBuy 购买：扣费、发货与次数累加同一次快照提交。
//
// 顺序即正确性：先只预检（不改状态）→ 扣费 → 才发货。
// 反过来做就会出现「钱扣了货没到」或「货到了钱没扣」，只能客服补偿。
func onBuy(c *module.Ctx[Data], req *gamev1.ShopBuyReq) error {
	cfg, ok := gamedata.GetShopGoodsCfg(int64(req.GoodsId))
	if !ok {
		return c.FailCode(&gamev1.ShopBuyResp{}, errcode.ErrShopGoodsNotExist)
	}
	count := int64(req.Count)
	// 数量裁剪到 1-999：0/负数按 1，上限防单次请求买穿限购与造成大额扣费。
	if count <= 0 {
		count = 1
	}
	if count > 999 {
		count = 999
	}
	shopID := int32(cfg.Col1)
	if !shopExists(shopID) {
		// 与列表同一条规则：归属商店不存在就不能买（防绕过下架）
		return c.FailCode(&gamev1.ShopBuyResp{}, errcode.ErrShopGoodsNotExist)
	}

	d, err := c.Repo.Get()
	if err != nil {
		return c.Fail(&gamev1.ShopBuyResp{Code: int32(errcode.ErrSystem)}, err)
	}

	// 先跨天归零再读已购次数：否则「昨天买满了今天买不了」。
	// 这里改了内存但失败路径不 Save——下次读会按新的一天重算，行为一致。
	today := businessDay(time.Now())
	normalize(d, today)
	key := goodsKey(shopID, req.GoodsId)

	// 周期限购：必须在扣费之前判。放到扣费之后就会出现
	// 「钱扣了、货被限购挡下」，只能客服补偿。
	//
	// 限购的计量单位是「份」（一份 = Col4 个道具），与 Bought 的口径一致；
	// 0 表示不限购，兼容没配这一列的旧配置。
	if !withinLimit(d.Bought[key], int32(count), int32(cfg.Col5)) {
		return c.FailCode(&gamev1.ShopBuyResp{}, errcode.ErrShopLimitExceeded)
	}

	itemID := int32(cfg.Col2)
	grants := []module.ItemGrant{{ItemID: itemID, Count: int64(cfg.Col4) * count}}
	if err := module.PrecheckGrant(c.State, grants); err != nil {
		return c.FailCode(&gamev1.ShopBuyResp{}, errcode.ErrShopBagFull)
	}
	price := int64(cfg.Col3) * count
	if err := module.Spend(c.State, itemDiamond, price); err != nil {
		return c.FailCode(&gamev1.ShopBuyResp{}, errcode.ErrShopDiamondNotEnough)
	}
	// 预检已过，这里必定成功；仍按错误处理是纵深防御（配置热更等极端情况）。
	changes, grantErr := module.Grant(c.State, grants)
	if grantErr != nil {
		return c.FailCode(&gamev1.ShopBuyResp{}, errcode.ErrShopBagFull)
	}

	d.Bought[key] += int32(count)
	bought := d.Bought[key]
	if err := c.Repo.Save(d); err != nil {
		return c.Fail(&gamev1.ShopBuyResp{Code: int32(errcode.ErrSystem)}, err)
	}

	// 背包增量推送（钻石扣减 + 道具增加），一条通知带两条变更。
	// 协议号走生成常量，不再硬编码 3010。
	notify := &gamev1.BagChangeNotify{Changes: append([]*gamev1.ItemChange{
		{ItemUid: int64(itemDiamond), ItemId: itemDiamond, Count: int32(c.State.GetItemCount(itemDiamond)), Change: 2},
	}, module.ToItemChanges(changes)...)}
	if raw, err := proto.Marshal(notify); err == nil {
		_ = c.Context.Reply(codec.MsgBagChangeNotify, raw)
	}
	// 事件：驱动任务（buy 类）
	if c.Emit != nil {
		c.Emit(module.Event{Type: "buy", Key: fmt.Sprintf("%d", req.GoodsId), Delta: count})
	}
	return c.Reply(&gamev1.ShopBuyResp{Bought: bought})
}

// withinLimit 判断本次购买后是否仍在周期限购内。
//
// limit <= 0 表示不限购：这是「配置没填这一列」的兼容语义——新增列之前
// 的配置全是 0，若把 0 当作「一件都不能买」，热更配置会瞬间让商店停摆。
//
// 计量单位是「份」而非「个」：Bought 记的是购买份数（一份 = Col4 个道具），
// 两者口径必须一致，否则一次买 50 个道具会被算成 50 份。
func withinLimit(bought, want, limit int32) bool {
	return limit <= 0 || bought+want <= limit
}

// goodsKey 组合商店与商品 ID，作为快照内的稳定键。
func goodsKey(shopID, goodsID int32) string {
	return fmt.Sprintf("%d:%d", shopID, goodsID)
}

// dailyRefresh 商店是否按日刷新（配置缺失视为不刷新）。
//
// 缺配置回退 false：刷新是「玩法决策」，配置没说不刷就不刷，
// 比默认刷更保守（不会误清玩家已购次数）。
func dailyRefresh() bool {
	if len(gamedata.ShopRefreshCfgList()) == 0 {
		return false
	}
	return gamedata.ShopRefreshCfgList()[0].Col2 == "daily"
}

// businessDay 北京时间自然日序号。
//
// 与 loginreward/signin 同一口径（UTC+8 固定偏移）：进程跑在
// UTC 容器里也不能改变「今天」的归属，跨天清零才对得上玩家体感。
func businessDay(now time.Time) int64 { return (now.Unix() + 8*60*60) / 86400 }
