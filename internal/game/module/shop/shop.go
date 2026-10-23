// internal/game/module/shop/shop.go —— 商店模块（6401-6404）
//
// 本周期购买次数放在玩家快照 Ext["shop"]：扣费、发货与次数累加在同一次
// Redis CAS 提交，失败整批回滚（契约见 docs/R2.1-缓存原子变更契约.md）。
//
// 架构职责：商品列表 + 购买。商品定义全部来自 shop_goods.xml，
// 模块只记录「本周期已购次数」。
// Ext 槽位：shop_record（Day + Bought JSON 列）。
// 事件订阅：产出方。购买成功 Emit "buy"（Key=商品 ID），驱动任务。
// 配置表：shop.xml / shop_goods.xml（Col2=道具，Col3=单价，Col4=数量）、
// shop_refresh.xml（col2=daily 表示按日刷新）。
// 懒初始化：从未购买返回空 Data；跨天清零发生在**读取路径**上
// （load 时按 Day 比较），不需要全服定时任务，也不怕玩家离线几天漏刷。
package shop

import (
	"fmt"
	"time"

	"game_study/internal/game/module"
	"game_study/pkg/codec"
	"game_study/pkg/gamedata"
	"game_study/pkg/log"

	gamev1 "game_study/pkg/pb/game/v1"
	"google.golang.org/protobuf/proto"
)

// stateKey 同时是 Ext 槽位键、模块名与落库归属标签。
const stateKey = "shop"

// 协议号（4.9：6401/6402 列表，6403/6404 购买）。
const (
	msgIDListReq  = codec.MsgShopListReq
	msgIDListResp = codec.MsgShopListResp
	msgIDBuyReq   = codec.MsgShopBuyReq
	msgIDBuyResp  = codec.MsgShopBuyResp
)

// 业务码（4.10）：商店段 6401-6404。
const (
	codeOK               int32 = 0
	codeGoodsNotExist    int32 = 6401
	codeDiamondNotEnough int32 = 6402
	codeLimitExceeded    int32 = 6403
	codeBagFull          int32 = 6404
)

// itemDiamond 商店结算货币（钻石）。与 quest/skill/activity 共用 1001。
const itemDiamond int32 = 1001

// defaultShopID 默认商店（P6 单商店：shop.xml 仅一条，P7 支持多商店页签）。
const defaultShopID int32 = 1

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

// Module 商店模块。
type Module struct {
	// Stateful 内嵌 Thin：生命周期与消息注册仍由 Thin 提供。
	*module.Stateful[Data]
	logger *log.Logger
}

// New 创建商店模块。
//
// 嵌入 Thin：解约只借模块七件套与协议注册，业务仍在 onList/onBuy。
// 不做跨天清理——清零逻辑在 load 的读取路径上（见 load 注释），
// 因此没有 OnLogin 钩子。
func New(logger *log.Logger) *Module {
	m := &Module{logger: logger}
	m.Stateful = module.NewStateful(stateKey, newData, module.ThinLogger(logger))
	m.HandleR(msgIDListReq, msgIDListResp, &gamev1.ShopListResp{}, m.onList)
	m.HandleR(msgIDBuyReq, msgIDBuyResp, &gamev1.ShopBuyResp{}, module.Typed(msgIDBuyResp, m.onBuy))
	return m
}

// onList 商店列表（商品 + 本周期已购 / 限购）。
//
// load 传今天：跨天时在读取路径清零，列表天然展示新周期。
func (m *Module) onList(ctx *module.Context, _ uint32, _ []byte) error {
	resp := &gamev1.ShopListResp{Code: codeOK}
	today := businessDay(time.Now())
	if err := m.Read(ctx, func(d *Data) {
		// 只读路径上的跨天重置不写回：下次读会重算，行为一致（原实现同）。
		normalize(d, today)
		for _, cfg := range gamedata.ShopGoodsCfgList() {
			resp.Goods = append(resp.Goods, &gamev1.ShopGoods{
				GoodsId: int32(cfg.Id),
				ItemId:  int32(cfg.Col2),
				Price:   int32(cfg.Col3),
				Count:   int32(cfg.Col4),
				Bought:  d.Bought[goodsKey(defaultShopID, int32(cfg.Id))],
				Limit:   0, // P6 不限购，P7 接 shop.xml 限购列
			})
		}
	}); err != nil {
		return err
	}
	return reply(ctx, msgIDListResp, resp)
}

// onBuy 购买：扣费、发货与次数累加同一次快照提交。
func (m *Module) onBuy(ctx *module.Context, req *gamev1.ShopBuyReq, resp *gamev1.ShopBuyResp) error {
	*resp = gamev1.ShopBuyResp{}

	cfg, ok := gamedata.GetShopGoodsCfg(int64(req.GoodsId))
	if !ok {
		resp.Code = codeGoodsNotExist
		return nil
	}
	count := int64(req.Count)
	// 数量裁剪到 1-999：0/负数按 1，上限防单次请求买穿限购与造成大额扣费。
	if count <= 0 {
		count = 1
	}
	if count > 999 {
		count = 999
	}

	itemID := int32(cfg.Col2)
	gain := int64(cfg.Col4) * count
	today := businessDay(time.Now())

	var (
		applied bool
		changes []module.BagChange
		bought  int32
	)
	err := m.Mutate(ctx, func(d *Data) bool {
		// 顺序即正确性：先只预检（不改状态）→ 扣费 → 才发货。
		// 反过来做就会出现「钱扣了货没到」或「货到了钱没扣」，只能客服补偿。
		grants := []module.ItemGrant{{ItemID: itemID, Count: gain}}
		if err := module.PrecheckGrant(ctx.State, grants); err != nil {
			resp.Code = codeBagFull
			return false
		}
		price := int64(cfg.Col3) * count
		if err := module.Spend(ctx.State, itemDiamond, price); err != nil {
			resp.Code = codeDiamondNotEnough
			return false
		}
		// 预检已过，这里必定成功；仍按错误处理是纵深防御（配置热更等极端情况）。
		c, grantErr := module.Grant(ctx.State, grants)
		if grantErr != nil {
			resp.Code = codeBagFull
			return false
		}
		changes = c

		normalize(d, today)
		key := goodsKey(defaultShopID, req.GoodsId)
		d.Bought[key] += int32(count)
		bought = d.Bought[key]
		applied = true
		return true
	})
	if err != nil {
		return err
	}
	if !applied {
		return nil
	}

	resp.Code = codeOK
	resp.Bought = bought
	// 背包增量推送（钻石扣减 + 道具增加），一条通知带两条变更。
	notify := &gamev1.BagChangeNotify{Changes: append([]*gamev1.ItemChange{
		{ItemUid: int64(itemDiamond), ItemId: itemDiamond, Count: int32(ctx.State.GetItemCount(itemDiamond)), Change: 2},
	}, module.ToItemChanges(changes)...)}
	if raw, err := proto.Marshal(notify); err == nil {
		_ = ctx.Reply(3010, raw)
	}
	// 事件：驱动任务（buy 类）
	if ctx.Emit != nil {
		ctx.Emit(module.Event{Type: "buy", Key: fmt.Sprintf("%d", req.GoodsId), Delta: count})
	}
	return nil
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

// reply 序列化 proto 并经 ctx.Reply 下发。
func reply(ctx *module.Context, msgID uint32, msg proto.Message) error {
	body, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	return ctx.Reply(msgID, body)
}
