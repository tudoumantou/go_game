// internal/game/module/shop/shop.go —— 商店模块（6401-6404）
//
// 本周期购买次数放在玩家快照 Ext["shop"]：扣费、发货与次数累加在同一次
// Redis CAS 提交，失败整批回滚（契约见 docs/R2.1-缓存原子变更契约.md）。
package shop

import (
	"context"
	"encoding/json"
	"fmt"

	"game_study/internal/game/module"
	"game_study/pkg/gamedata"
	"game_study/pkg/log"
	"game_study/pkg/model"
	"game_study/pkg/writebehind"

	gamev1 "game_study/pkg/pb/game/v1"
	"google.golang.org/protobuf/proto"
)

const stateKey = "shop"

// 协议号（4.9：6401/6402 列表，6403/6404 购买）。
const (
	msgIDListReq  uint32 = 6401
	msgIDListResp uint32 = 6402
	msgIDBuyReq   uint32 = 6403
	msgIDBuyResp  uint32 = 6404
)

// 业务码（4.10）。
const (
	codeOK               int32 = 0
	codeGoodsNotExist    int32 = 6401
	codeDiamondNotEnough int32 = 6402
	codeLimitExceeded    int32 = 6403
)

const itemDiamond int32 = 1001

// defaultShopID 默认商店（P6 单商店：shop.xml 仅一条，P7 支持多商店页签）。
const defaultShopID int32 = 1

// Data 是商店模块的私有状态： "shopID:goodsID" -> 本周期已购次数。
type Data struct {
	Bought map[string]int32 `json:"bought"`
}

// init 注册持久化：shop_record 主键为 (player_id)。
func init() {
	writebehind.RegisterFlat[model.ShopRecord](stateKey, model.ShopRecordFlat, validate)
}

// validate 落库前校验：次数非负。
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
	logger *log.Logger
}

// New 创建商店模块。
func New(logger *log.Logger) *Module { return &Module{logger: logger} }

// Name 实现 module.Module。
func (m *Module) Name() string { return stateKey }

// MsgIDs 实现 module.Module。
func (m *Module) MsgIDs() []uint32 { return []uint32{msgIDListReq, msgIDBuyReq} }

// Init 实现 module.Module。
func (m *Module) Init(*module.Context) error { return nil }

// OnMessage 实现 module.Module。
func (m *Module) OnMessage(ctx *module.Context, msgID uint32, body []byte) error {
	switch msgID {
	case msgIDListReq:
		return m.onList(ctx)
	case msgIDBuyReq:
		return m.onBuy(ctx, body)
	default:
		return fmt.Errorf("shop: unexpected msg %d", msgID)
	}
}

// OnSave 实现 module.Module：次数随快照提交。
func (m *Module) OnSave(context.Context) error { return nil }

// OnLogin 实现 module.Module。
func (m *Module) OnLogin(*module.Context) error { return nil }

// OnLogout 实现 module.Module。
func (m *Module) OnLogout(*module.Context) error { return nil }

// onList 商店列表（商品 + 本周期已购 / 限购）。
func (m *Module) onList(ctx *module.Context) error {
	resp := &gamev1.ShopListResp{Code: codeOK}
	d, err := load(ctx.State)
	if err != nil {
		return err
	}
	for _, cfg := range gamedata.ShopGoodsCfgList {
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
func (m *Module) onBuy(ctx *module.Context, body []byte) error {
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
	if count <= 0 {
		count = 1
	}
	if count > 999 {
		count = 999
	}

	price := int64(cfg.Col3) * count
	if ctx.State.GetItemCount(itemDiamond) < price {
		resp.Code = codeDiamondNotEnough
		return reply(ctx, msgIDBuyResp, resp)
	}
	if !ctx.State.RemoveItem(itemDiamond, price) {
		resp.Code = codeDiamondNotEnough
		return reply(ctx, msgIDBuyResp, resp)
	}

	itemID := int32(cfg.Col2)
	gain := int64(cfg.Col4) * count
	ctx.State.AddItem(itemID, gain)
	ctx.State.MarkDirty("bag")

	d, err := load(ctx.State)
	if err != nil {
		return err
	}
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
	notify := &gamev1.BagChangeNotify{Changes: []*gamev1.ItemChange{
		{ItemUid: int64(itemDiamond), ItemId: itemDiamond, Count: int32(ctx.State.GetItemCount(itemDiamond)), Change: 2},
		{ItemUid: int64(itemID), ItemId: itemID, Count: int32(ctx.State.GetItemCount(itemID)), Change: 1},
	}}
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
func load(state writebehind.ExtState) (*Data, error) {
	row, err := writebehind.LoadExt[model.ShopRecord](state, stateKey)
	if err != nil {
		return nil, err
	}
	if row == nil {
		return &Data{Bought: map[string]int32{}}, nil
	}
	return decode(row)
}

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

func save(state writebehind.ExtState, playerID int64, d *Data) error {
	raw, err := json.Marshal(d.Bought)
	if err != nil {
		return fmt.Errorf("encode shop state: %w", err)
	}
	return writebehind.SaveExt(state, stateKey, &model.ShopRecord{
		PlayerId: playerID,
		Bought:   string(raw),
	})
}

func reply(ctx *module.Context, msgID uint32, msg proto.Message) error {
	body, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	return ctx.Reply(msgID, body)
}
