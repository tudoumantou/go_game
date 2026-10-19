// internal/game/module/bag/bag.go —— 背包模块（3001 使用道具 / 3003 列表）
package bag

import (
	"context"
	"fmt"

	"game_study/internal/game/module"
	"game_study/pkg/gamedata"
	"game_study/pkg/log"

	gamev1 "game_study/pkg/pb/game/v1"
	"google.golang.org/protobuf/proto"
)

// 协议号（4.9：3001/3002 使用，3003/3004 列表，3010 推送）。
const (
	msgIDUseItemReq  uint32 = 3001
	msgIDUseItemResp uint32 = 3002
	msgIDBagListReq  uint32 = 3003
	msgIDBagListResp uint32 = 3004
	msgIDBagChange   uint32 = 3010
)

// 业务码（4.10）。
const (
	codeOK            int32 = 0
	codeItemNotExist  int32 = 3001
	codeItemNotEnough int32 = 3002
	codeItemNotUsable int32 = 3004
)

// Module 背包模块。
type Module struct {
	logger *log.Logger
}

// New 创建背包模块。
func New(logger *log.Logger) *Module {
	return &Module{logger: logger}
}

// Name 实现 module.Module。
func (m *Module) Name() string { return "bag" }

// MsgIDs 实现 module.Module。
// Rollbackable 背包列表只读；使用道具会改资产，按不可回滚处理。
func (*Module) Rollbackable(msgID uint32) bool { return msgID == msgIDBagListReq }

func (m *Module) MsgIDs() []uint32 { return []uint32{msgIDUseItemReq, msgIDBagListReq} }

// Init 实现 module.Module。
func (m *Module) Init(ctx *module.Context) error { return nil }

// OnMessage 实现 module.Module。
func (m *Module) OnMessage(ctx *module.Context, msgID uint32, body []byte) error {
	switch msgID {
	case msgIDBagListReq:
		return m.onList(ctx)
	case msgIDUseItemReq:
		return m.onUse(ctx, body)
	default:
		return fmt.Errorf("bag: unexpected msg %d", msgID)
	}
}

// OnSave 实现 module.Module。
func (m *Module) OnSave(ctx context.Context) error { return nil }

// OnLogin 实现 module.Module。
func (m *Module) OnLogin(ctx *module.Context) error {
	m.logger.Debug("bag login", "player_id", ctx.State.PlayerID())
	return nil
}

// OnLogout 实现 module.Module。
func (m *Module) OnLogout(ctx *module.Context) error {
	ctx.State.MarkDirty(m.Name())
	return nil
}

// onList 背包列表（P6 堆叠模式：item_uid 用 item_id 占位）。
func (m *Module) onList(ctx *module.Context) error {
	items := ctx.State.Items()
	resp := &gamev1.BagListResp{Code: codeOK, Items: make([]*gamev1.ItemEntry, 0, len(items))}
	for itemID, count := range items {
		if count <= 0 {
			continue
		}
		resp.Items = append(resp.Items, &gamev1.ItemEntry{
			ItemUid: int64(itemID),
			ItemId:  itemID,
			Count:   int32(count),
		})
	}
	body, err := proto.Marshal(resp)
	if err != nil {
		return err
	}
	return ctx.Reply(msgIDBagListResp, body)
}

// onUse 使用道具。
func (m *Module) onUse(ctx *module.Context, body []byte) error {
	var req gamev1.UseItemReq
	if err := proto.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("decode UseItemReq: %w", err)
	}
	itemID := int32(req.ItemUid)
	count := req.Count
	if count <= 0 {
		count = 1
	}

	resp := &gamev1.UseItemResp{}
	cfg, ok := gamedata.GetItemCfg(int64(itemID))
	if !ok {
		resp.Code = codeItemNotExist
		return m.replyUse(ctx, resp)
	}
	// 配置约定：Col3 = 单次可使用数量上限，<=0 表示不可主动使用（P7 接 item_type.xml）
	if cfg.Col3 <= 0 {
		resp.Code = codeItemNotUsable
		return m.replyUse(ctx, resp)
	}
	if int64(count) > cfg.Col3 {
		count = int32(cfg.Col3)
	}

	have := ctx.State.GetItemCount(itemID)
	if have < int64(count) {
		resp.Code = codeItemNotEnough
		return m.replyUse(ctx, resp)
	}
	if !ctx.State.RemoveItem(itemID, int64(count)) {
		resp.Code = codeItemNotEnough
		return m.replyUse(ctx, resp)
	}
	ctx.State.MarkDirty(m.Name())
	// 事件：驱动任务/成就（use_item，Key=道具 ID）
	if ctx.Emit != nil {
		ctx.Emit(module.Event{Type: "use_item", Key: fmt.Sprintf("%d", itemID), Delta: int64(count)})
	}

	resp.Code = codeOK
	resp.RemainCount = int32(ctx.State.GetItemCount(itemID))
	if err := m.replyUse(ctx, resp); err != nil {
		return err
	}
	// 增量推送
	notify := &gamev1.BagChangeNotify{Changes: []*gamev1.ItemChange{{
		ItemUid: int64(itemID),
		ItemId:  itemID,
		Count:   resp.RemainCount,
		Change:  2,
	}}}
	if raw, err := proto.Marshal(notify); err == nil {
		_ = ctx.Reply(msgIDBagChange, raw)
	}
	return nil
}

func (m *Module) replyUse(ctx *module.Context, resp *gamev1.UseItemResp) error {
	body, err := proto.Marshal(resp)
	if err != nil {
		return err
	}
	return ctx.Reply(msgIDUseItemResp, body)
}
