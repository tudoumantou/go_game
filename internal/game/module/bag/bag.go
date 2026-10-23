// internal/game/module/bag/bag.go —— 背包模块（3001 使用道具 / 3003 列表）
//
// 架构职责：玩家道具资产的协议面。持有量本身是玩家级资产（PlayerState.Items
// 管理的 map[int32]int64，随玩家快照 CAS 提交），本模块只负责协议校验、
// 配置规则（可否使用/单次上限）与下行推送。
//
// Ext 槽位：无独立槽位——道具是「资产」不是「玩法进度」，与经验/金币一样
// 随玩家主快照提交，保证扣道具与加奖励在同一事务语义里。
// 事件订阅：产出方。使用道具时 Emit "use_item"，驱动任务/活动计数。
// 配置表：item.xml（Col2=堆叠上限，Col3=单次可使用数量）。
// 懒初始化：资产零值即合法（没有道具 = 空 map），无需「首次操作建行」。
package bag

import (
	"fmt"

	"game_study/internal/game/module"
	"game_study/pkg/codec"
	"game_study/pkg/gamedata"
	"game_study/pkg/log"

	gamev1 "game_study/pkg/pb/game/v1"
	"github.com/samber/lo"
	"google.golang.org/protobuf/proto"
)

// 协议号（4.9：3001/3002 使用，3003/3004 列表，3010 推送）。
//
// Go 知识点：const 块把相关常量分组，未指定类型的整型常量（这里显式 int）
// 在传给 uint32 参数时需匹配；全部取自 pkg/codec 的单一来源，避免各模块
// 各写一份魔数导致协议号漂移。
const (
	msgIDUseItemReq  = codec.MsgUseItemReq
	msgIDUseItemResp = codec.MsgUseItemResp
	msgIDBagListReq  = codec.MsgBagListReq
	msgIDBagListResp = codec.MsgBagListResp
	msgIDBagChange   = codec.MsgBagChangeNotify
)

// 业务码（4.10）。
//
// 4.10 的分配规则：每个模块一段号段（背包 3001-3004），客户端按号段
// 直接定位是哪个玩法拒绝的请求，不需要解析消息体。
const (
	codeOK            int32 = 0
	codeItemNotExist  int32 = 3001
	codeItemNotEnough int32 = 3002
	codeItemNotUsable int32 = 3004
)

// Module 背包模块。
//
// 无玩家级字段：logger 之外不持有任何状态，道具数据全部来自 Context.State。
type Module struct {
	*module.Thin

	logger *log.Logger
}

// New 创建背包模块。
//
// 嵌入 Thin：接口七件套由基座提供；两个处理器经 module.Typed 拿到解包后的
// 请求与待填的响应。Discardable 由本类型覆盖（见下），因为背包的写操作
// 不能像只读模块那样「失败即丢弃」。
func New(logger *log.Logger) *Module {
	m := &Module{logger: logger}
	m.Thin = module.NewThin("bag",
		module.ThinLogger(logger),
		module.ThinLogin(m.onLogin),
		module.ThinLogout(m.onLogout),
	)
	m.HandleR(msgIDBagListReq, msgIDBagListResp, &gamev1.BagListResp{},
		module.Typed(msgIDBagListResp, m.onList))
	m.HandleR(msgIDUseItemReq, msgIDUseItemResp, &gamev1.UseItemResp{},
		module.Typed(msgIDUseItemResp, m.onUse))
	return m
}

// Discardable 覆盖 Thin 的宽松默认：Thin 把「已注册协议」一律声明为可丢弃，
// 但「使用道具」会改资产——提交失败时不能丢，必须保留待提交快照补提，
// 否则会出现「道具扣了、奖励没发」且无从恢复。
func (*Module) Discardable(msgID uint32) bool { return msgID == msgIDBagListReq }

// onLogin 只记一条调试日志（背包不在登录时做任何预热）。
func (m *Module) onLogin(ctx *module.Context) error {
	m.logger.Debug("bag login", "player_id", ctx.State.PlayerID())
	return nil
}

// onLogout 下线标脏：让 flusher 走一次常规 flush 周期。
func (m *Module) onLogout(ctx *module.Context) error {
	ctx.State.MarkDirty("bag")
	return nil
}

// onList 背包列表（P6 堆叠模式：item_uid 用 item_id 占位）。
//
// Go 知识点：items 是 map[int32]int64，for range 遍历 map 的顺序是
// **随机**的（Go 运行时故意打乱），所以每次拉列表的道具顺序可能不同。
// 这里能接受是因为协议不承诺顺序、客户端自行排序；若哪天下发顺序
// 变成契约，必须先排序再遍历。
func (m *Module) onList(ctx *module.Context, _ *gamev1.BagListReq, resp *gamev1.BagListResp) error {
	items := ctx.State.Items()
	// FilterMap：一次遍历完成「跳过脏数据 + 转协议结构」，比先 make 再
	// append 少两行，且「哪些条目会被下发」集中在一个闭包里。
	entries := lo.FilterMap(lo.Entries(items), func(e lo.Entry[int32, int64], _ int) (*gamev1.ItemEntry, bool) {
		if e.Value <= 0 {
			return nil, false // 防御：扣减路径理论不留 0，脏数据不展示
		}
		return &gamev1.ItemEntry{
			ItemUid: int64(e.Key),
			ItemId:  e.Key,
			Count:   int32(e.Value),
		}, true
	})
	*resp = gamev1.BagListResp{Code: codeOK, Items: entries}
	return nil
}

// onUse 使用道具。
func (m *Module) onUse(ctx *module.Context, req *gamev1.UseItemReq, resp *gamev1.UseItemResp) error {
	itemID := int32(req.ItemUid)
	count := req.Count
	// 未带数量按 1 个处理；堆叠模式下 ItemUid 即 item_id。
	if count <= 0 {
		count = 1
	}

	cfg, ok := gamedata.GetItemCfg(int64(itemID))
	if !ok {
		resp.Code = codeItemNotExist
		return nil
	}
	// 配置约定：Col3 = 单次可使用数量上限，<=0 表示不可主动使用（P7 接 item_type.xml）
	if cfg.Col3 <= 0 {
		resp.Code = codeItemNotUsable
		return nil
	}
	// 请求数量超过单次上限时截断，而不是报错：与客户端的批量使用体验一致。
	if int64(count) > cfg.Col3 {
		count = int32(cfg.Col3)
	}

	have := ctx.State.GetItemCount(itemID)
	if have < int64(count) {
		resp.Code = codeItemNotEnough
		return nil
	}
	// RemoveItem 自带原子性：数量不足返回 false 且不做部分扣减；
	// Actor 单线程下上面的 have 检查已足够，这次是纵深防御。
	if !ctx.State.RemoveItem(itemID, int64(count)) {
		resp.Code = codeItemNotEnough
		return nil
	}
	ctx.State.MarkDirty(m.Name())
	// 事件：驱动任务/成就（use_item，Key=道具 ID）
	// Emit 是可选依赖（nil = 无订阅方），必须先判空——
	// 函数类型的零值是 nil，直接调用会 panic。
	if ctx.Emit != nil {
		ctx.Emit(module.Event{Type: "use_item", Key: fmt.Sprintf("%d", itemID), Delta: int64(count)})
	}

	resp.Code = codeOK
	resp.RemainCount = int32(ctx.State.GetItemCount(itemID))
	if err := m.replyUse(ctx, resp); err != nil {
		return err
	}
	// 增量推送
	// Change=2 表示扣减（协议约定 1=增加、2=减少），与商店/发奖路径对称。
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

// replyUse 序列化并回包使用道具响应。
func (m *Module) replyUse(ctx *module.Context, resp *gamev1.UseItemResp) error {
	body, err := proto.Marshal(resp)
	if err != nil {
		return err
	}
	return ctx.Reply(msgIDUseItemResp, body)
}
