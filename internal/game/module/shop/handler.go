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
	"game_study/internal/common/errcode"
	"game_study/internal/game/module"
	"game_study/pkg/codec"
	"game_study/pkg/gamedata"
	gamev1 "game_study/pkg/pb/game/v1"
	"game_study/pkg/timeutil"
	"time"

	"google.golang.org/protobuf/proto"
)

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

func onList(c *module.Ctx[Data]) error {
	resp := &gamev1.ShopListResp{}
	err := c.Repo.View(func(d *Data) error {
		// 跨天归零只影响本次展示，不写回：下次读会重新算一遍，行为一致。
		normalize(d, timeutil.BusinessDay(time.Now()))
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
		return nil
	})
	if err != nil {
		return c.Fail(&gamev1.ShopListResp{Code: int32(errcode.ErrSystem)}, err)
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

	itemID := int32(cfg.Col2)
	grants := []module.ItemGrant{{ItemID: itemID, Count: int64(cfg.Col4) * count}}
	price := int64(cfg.Col3) * count

	resp := &gamev1.ShopBuyResp{}
	var (
		bought  int32              // 购买后的累计份数（回包用）
		changes []module.BagChange // 发货结果（背包推送用）
	)

	err := c.Repo.Mutate(func(d *Data) (bool, error) {
		// 先跨天归零再读已购次数：否则「昨天买满了今天买不了」。
		// 归零本身不算改动——它只是把过期的计数清掉，真正的 changed 是
		// 下面的 Bought 累加；失败路径不写回，下次读会按新的一天重算。
		today := timeutil.BusinessDay(time.Now())
		normalize(d, today)
		key := goodsKey(shopID, req.GoodsId)

		// 周期限购：必须在扣费之前判。放到扣费之后就会出现
		// 「钱扣了、货被限购挡下」，只能客服补偿。
		//
		// 限购的计量单位是「份」（一份 = Col4 个道具），与 Bought 的口径一致；
		// 0 表示不限购，兼容没配这一列的旧配置。
		if !withinLimit(d.Bought[key], int32(count), int32(cfg.Col5)) {
			resp.Code = int32(errcode.ErrShopLimitExceeded)
			return false, nil
		}

		// 预检必须早于扣费：先扣费再发现背包满，就只能客服补偿了。
		if err := module.PrecheckGrant(c.State, grants); err != nil {
			resp.Code = int32(errcode.ErrShopBagFull)
			return false, nil
		}
		if err := module.Spend(c.State, itemDiamond, price); err != nil {
			resp.Code = int32(errcode.ErrShopDiamondNotEnough)
			return false, nil
		}
		// 预检已过，这里必定成功；仍按错误处理是纵深防御（配置热更等极端情况）。
		granted, grantErr := module.Grant(c.State, grants)
		if grantErr != nil {
			resp.Code = int32(errcode.ErrShopBagFull)
			return false, nil
		}

		d.Bought[key] += int32(count)
		bought = d.Bought[key]
		changes = granted
		return true, nil
	})
	if err != nil {
		return c.Fail(&gamev1.ShopBuyResp{Code: int32(errcode.ErrSystem)}, err)
	}
	if resp.Code != 0 {
		return c.Reply(resp)
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

// 跨天判定用 timeutil.BusinessDay：北京自然日序号，全服一份口径。
//
// 原先这里有一份本地 businessDay，注释写着「与 loginreward/signin 同一口径」
// ——靠注释维持的约定等于没有约定：活动模块那份漏了 +8 偏移，
// 于是限购在北京 0 点重置、活动实例等到北京 8 点才换。
