// internal/game/module/shop/shop.go —— 商店模块（6401-6404）
package shop

import (
	"context"
	"fmt"

	"game_study/internal/game/module"
	"game_study/pkg/db"
	"game_study/pkg/gamedata"
	"game_study/pkg/log"

	gamev1 "game_study/pkg/pb/game/v1"
	"google.golang.org/protobuf/proto"
)

// 协议号（4.9：6401/6402 列表，6403/6404 购买）。
const (
	msgIDListReq uint32 = 6401
	msgIDListResp uint32 = 6402
	msgIDBuyReq  uint32 = 6403
	msgIDBuyResp uint32 = 6404
)

// 业务码（4.10）。
const (
	codeOK          int32 = 0
	codeGoodsNotExist int32 = 6401
	codeDiamondNotEnough int32 = 6402
	codeLimitExceeded int32 = 6403
)

// 货币道具 ID（钻石）。
const itemDiamond int32 = 1001

// defaultShopID 默认商店（P6 单商店：shop.xml 仅一条，P7 支持多商店页签）。
const defaultShopID int32 = 1

// Repo 商店仓储。
type Repo struct {
	db *db.DB
}

// NewRepo 创建仓储。
func NewRepo(database *db.DB) *Repo { return &Repo{db: database} }

// BoughtMap 查询玩家在指定商店各商品已购次数。
func (r *Repo) BoughtMap(ctx context.Context, playerID int64, shopID int32) (map[int32]int32, error) {
	const q = `SELECT goods_id, buy_count FROM shop_record WHERE player_id = ? AND shop_id = ?`
	rows, err := r.db.QueryContext(ctx, q, playerID, shopID)
	if err != nil {
		return nil, fmt.Errorf("list shop_record: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := make(map[int32]int32)
	for rows.Next() {
		var goodsID, count int32
		if err := rows.Scan(&goodsID, &count); err != nil {
			return nil, err
		}
		out[goodsID] = count
	}
	return out, rows.Err()
}

// IncrBuy 增加已购次数（uk_player_goods 幂等累加）。
func (r *Repo) IncrBuy(ctx context.Context, playerID int64, shopID, goodsID, delta int32) error {
	const q = `INSERT INTO shop_record (player_id, shop_id, goods_id, buy_count) VALUES (?, ?, ?, ?)
	           ON DUPLICATE KEY UPDATE buy_count = buy_count + VALUES(buy_count)`
	if _, err := r.db.ExecContext(ctx, q, playerID, shopID, goodsID, delta); err != nil {
		return fmt.Errorf("incr shop_record: %w", err)
	}
	return nil
}

// Module 商店模块。
type Module struct {
	repo   *Repo
	logger *log.Logger
}

// New 创建商店模块。
func New(repo *Repo, logger *log.Logger) *Module { return &Module{repo: repo, logger: logger} }

// Name 实现 module.Module。
func (m *Module) Name() string { return "shop" }

// MsgIDs 实现 module.Module。
func (m *Module) MsgIDs() []uint32 { return []uint32{msgIDListReq, msgIDBuyReq} }

// Init 实现 module.Module。
func (m *Module) Init(ctx *module.Context) error { return nil }

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

// OnSave 实现 module.Module。
func (m *Module) OnSave(ctx context.Context) error { return nil }

// OnLogin 实现 module.Module。
func (m *Module) OnLogin(ctx *module.Context) error { return nil }

// OnLogout 实现 module.Module。
func (m *Module) OnLogout(ctx *module.Context) error { return nil }

// onList 商店列表（商品 + 本周期已购 / 限购）。
func (m *Module) onList(ctx *module.Context) error {
	resp := &gamev1.ShopListResp{Code: codeOK}
	bought := map[int32]int32{}
	if m.repo != nil {
		var err error
		bought, err = m.repo.BoughtMap(context.Background(), ctx.State.PlayerID(), defaultShopID)
		if err != nil {
			return err
		}
	}
	for _, cfg := range gamedata.ShopGoodsCfgList {
		resp.Goods = append(resp.Goods, &gamev1.ShopGoods{
			GoodsId: int32(cfg.Id),
			ItemId:  int32(cfg.Col2),
			Price:   int32(cfg.Col3),
			Count:   int32(cfg.Col4),
			Bought:  bought[int32(cfg.Id)],
			Limit:   0, // P6 不限购，P7 接 shop.xml 限购列
		})
	}
	return m.reply(ctx, msgIDListResp, resp)
}

// onBuy 购买（钻石扣减 → 发道具 → 已购 +1）。
func (m *Module) onBuy(ctx *module.Context, body []byte) error {
	var req gamev1.ShopBuyReq
	if err := proto.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("decode ShopBuyReq: %w", err)
	}
	resp := &gamev1.ShopBuyResp{}

	cfg, ok := gamedata.GetShopGoodsCfg(int64(req.GoodsId))
	if !ok {
		resp.Code = codeGoodsNotExist
		return m.reply(ctx, msgIDBuyResp, resp)
	}
	count := int64(req.Count)
	if count <= 0 {
		count = 1
	}
	// 单次购买上限，防溢出（配置数量 * 次数）
	if count > 999 {
		count = 999
	}

	price := int64(cfg.Col3) * count
	if ctx.State.GetItemCount(itemDiamond) < price {
		resp.Code = codeDiamondNotEnough
		return m.reply(ctx, msgIDBuyResp, resp)
	}
	if !ctx.State.RemoveItem(itemDiamond, price) {
		resp.Code = codeDiamondNotEnough
		return m.reply(ctx, msgIDBuyResp, resp)
	}

	itemID := int32(cfg.Col2)
	gain := int64(cfg.Col4) * count
	ctx.State.AddItem(itemID, gain)
	ctx.State.MarkDirty(m.Name())
	ctx.State.MarkDirty("bag")

	bought := int32(count)
	if m.repo != nil {
		// 落库失败不回滚内存（Write-Behind 语义：内存为真，落库重试）
		if err := m.repo.IncrBuy(context.Background(), ctx.State.PlayerID(), defaultShopID, req.GoodsId, int32(count)); err != nil {
			m.logger.Warn("shop record incr failed", "player_id", ctx.State.PlayerID(),
				"goods_id", req.GoodsId, "err", err)
		}
		if cur, err := m.repo.BoughtMap(context.Background(), ctx.State.PlayerID(), defaultShopID); err == nil {
			bought = cur[req.GoodsId]
		}
	}

	resp.Code = codeOK
	resp.Bought = bought
	if err := m.reply(ctx, msgIDBuyResp, resp); err != nil {
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

func (m *Module) reply(ctx *module.Context, msgID uint32, msg proto.Message) error {
	body, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	return ctx.Reply(msgID, body)
}
