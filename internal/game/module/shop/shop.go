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
	"encoding/json"
	"fmt"
	"time"

	"game_study/internal/game/module"
	"game_study/pkg/codec"
	"game_study/pkg/gamedata"
	"game_study/pkg/log"
	"game_study/pkg/model"
	"game_study/pkg/writebehind"

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

// init 注册持久化：shop_record 主键为 (player_id)。
func init() {
	writebehind.RegisterFlat[model.ShopRecord](stateKey, model.ShopRecordFlat, validate)
}

// validate 落库前校验：次数非负。
//
// 空键也拒绝：键格式由 goodsKey 生成，空键说明状态被外部改坏。
func validate(row *model.ShopRecord) error {
	d, err := decode(row)
	if err != nil {
		return err
	}
	for key, n := range d.Bought {
		if key == "" || n < 0 {
			return fmt.Errorf("invalid shop record %q = %d", key, n)
		}
	}
	return nil
}

// Module 商店模块。
type Module struct {
	*module.Thin
	logger *log.Logger
}

// New 创建商店模块。
//
// 嵌入 Thin：解约只借模块七件套与协议注册，业务仍在 onList/onBuy。
// 不做跨天清理——清零逻辑在 load 的读取路径上（见 load 注释），
// 因此没有 OnLogin 钩子。
func New(logger *log.Logger) *Module {
	m := &Module{logger: logger}
	m.Thin = module.NewThin(stateKey, module.ThinLogger(logger))
	m.HandleR(msgIDListReq, msgIDListResp, &gamev1.ShopListResp{}, m.onList)
	m.HandleR(msgIDBuyReq, msgIDBuyResp, &gamev1.ShopBuyResp{}, m.onBuy)
	return m
}

// onList 商店列表（商品 + 本周期已购 / 限购）。
//
// load 传今天：跨天时在读取路径清零，列表天然展示新周期。
func (m *Module) onList(ctx *module.Context, _ uint32, _ []byte) error {
	resp := &gamev1.ShopListResp{Code: codeOK}
	d, err := load(ctx.State, businessDay(time.Now()))
	if err != nil {
		return err
	}
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
	return reply(ctx, msgIDListResp, resp)
}

// onBuy 购买：扣费、发货与次数累加同一次快照提交。
func (m *Module) onBuy(ctx *module.Context, _ uint32, body []byte) error {
	var req gamev1.ShopBuyReq
	if err := proto.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("decode ShopBuyReq: %w", err)
	}
	resp := &gamev1.ShopBuyResp{}

	cfg, ok := gamedata.GetShopGoodsCfg(int64(req.GoodsId))
	if !ok {
		resp.Code = codeGoodsNotExist
		return reply(ctx, msgIDBuyResp, resp)
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
	// 顺序即正确性：先只预检（不改状态）→ 扣费 → 才发货。
	// 反过来做就会出现「钱扣了货没到」或「货到了钱没扣」，只能客服补偿。
	grants := []module.ItemGrant{{ItemID: itemID, Count: gain}}
	if err := module.PrecheckGrant(ctx.State, grants); err != nil {
		resp.Code = codeBagFull
		return reply(ctx, msgIDBuyResp, resp)
	}

	price := int64(cfg.Col3) * count
	if err := module.Spend(ctx.State, itemDiamond, price); err != nil {
		resp.Code = codeDiamondNotEnough
		return reply(ctx, msgIDBuyResp, resp)
	}

	// 预检已过，这里必定成功；仍按错误处理是纵深防御（配置热更等极端情况）。
	changes, grantErr := module.Grant(ctx.State, grants)
	if grantErr != nil {
		resp.Code = codeBagFull
		return reply(ctx, msgIDBuyResp, resp)
	}

	// 购买次数在扣费/发货之后才累加，但同属一份快照：CAS 失败时扣费与次数一起回滚。
	// load 传入今天，跨天时会在读取路径上清零历史次数（见 load 注释）。
	today := businessDay(time.Now())
	d, err := load(ctx.State, today)
	if err != nil {
		return err
	}
	d.Day = today
	key := goodsKey(defaultShopID, req.GoodsId)
	d.Bought[key] += int32(count)
	if err := save(ctx.State, ctx.State.PlayerID(), d); err != nil {
		return err
	}

	resp.Code = codeOK
	resp.Bought = d.Bought[key]
	if err := reply(ctx, msgIDBuyResp, resp); err != nil {
		return err
	}
	// 背包增量推送（钻石扣减 + 道具增加）
	// 一条通知带两条变更：扣减 Change=2、增加 Change=1，客户端一次刷新面板。
	notify := &gamev1.BagChangeNotify{Changes: append([]*gamev1.ItemChange{
		// 扣减用 Change=2（与增加区分，客户端据此播扣款动效）
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

// load 读取购买次数；从未购买返回空结构。
//
// 每日刷新的商店（shop_refresh.xml 的 col2=daily）在跨天时清零购买次数：
// 清零发生在读取路径上，不需要全服定时任务，也不会因为玩家离线几天而漏刷。
// 代价是每次读都要比一次 Day——换来的是「零定时任务」的简单性。
func load(state writebehind.ExtState, today int64) (*Data, error) {
	row, err := writebehind.LoadExt[model.ShopRecord](state, stateKey)
	if err != nil {
		return nil, err
	}
	if row == nil {
		return &Data{Day: today, Bought: map[string]int32{}}, nil
	}
	d, err := decode(row)
	if err != nil {
		return nil, err
	}
	if dailyRefresh() && d.Day != today {
		// 跨天：旧次数作废，重置 Day 让本次购买从 0 计起。
		d.Bought = map[string]int32{}
		d.Day = today
	}
	return d, nil
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

// decode 把表行 JSON 解成业务视图；保证 map 非 nil。
func decode(row *model.ShopRecord) (*Data, error) {
	d := &Data{Bought: map[string]int32{}}
	if row.Bought != "" {
		if err := json.Unmarshal([]byte(row.Bought), &d.Bought); err != nil {
			return nil, fmt.Errorf("decode shop state: %w", err)
		}
	}
	if d.Bought == nil {
		d.Bought = map[string]int32{}
	}
	return d, nil
}

// save 把购买次数编码回 Ext 槽位；落库调度由 Write-Behind 负责。
func save(state writebehind.ExtState, playerID int64, d *Data) error {
	raw, err := json.Marshal(d.Bought)
	if err != nil {
		return fmt.Errorf("encode shop state: %w", err)
	}
	return writebehind.SaveExt(state, stateKey, &model.ShopRecord{
		PlayerId: playerID,
		Day:      d.Day,
		Bought:   string(raw),
	})
}

// reply 序列化 proto 并经 ctx.Reply 下发。
func reply(ctx *module.Context, msgID uint32, msg proto.Message) error {
	body, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	return ctx.Reply(msgID, body)
}
