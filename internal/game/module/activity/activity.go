// internal/game/module/activity/activity.go —— 活动模块（6501-6504）
//
// 活动进度与领取位图放在玩家快照 Ext["activity"]：领奖与发放在同一次 Redis CAS
// 提交，Redis 失败时两者一起回滚（契约见 docs/R2.1-缓存原子变更契约.md）。
package activity

import (
	"context"
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

const stateKey = "activity"

// 协议号（4.9：6501/6502 列表，6503/6504 领奖）。
const (
	msgIDListReq   = codec.MsgActivityListReq
	msgIDListResp  = codec.MsgActivityListResp
	msgIDClaimReq  = codec.MsgActivityClaimReq
	msgIDClaimResp = codec.MsgActivityClaimResp
)

// 业务码（4.10）。
const (
	codeOK               int32 = 0
	codeActivityNotExist int32 = 6501
	codeNotFinished      int32 = 6502
	codeAlreadyClaimed   int32 = 6503
	codeBagFull          int32 = 6504
	codeNotOpen          int32 = 6505 // 活动不在开放期（下发的窗口已过期）
)

// 奖励道具（P6 固定：钻石 1001）。
const itemDiamond int32 = 1001

// Record 单个活动的进度与领取位图。
type Record struct {
	Progress   int32 `json:"progress"`
	RewardMask int64 `json:"reward_mask"`
}

// Data 是活动模块的私有状态：activity_id -> 进度与已领位图。
type Data struct {
	Records map[int32]Record `json:"records"`
}

// init 注册持久化：activity_record 主键为 (player_id)。
func init() {
	writebehind.RegisterFlat[model.ActivityRecord](stateKey, model.ActivityRecordFlat, validate)
}

// validate 落库前校验：进度非负、活动 ID 合法。
func validate(row *model.ActivityRecord) error {
	d, err := decode(row)
	if err != nil {
		return err
	}
	for id, rec := range d.Records {
		if id <= 0 || rec.Progress < 0 {
			return fmt.Errorf("invalid activity %d progress %d", id, rec.Progress)
		}
	}
	return nil
}

// Module 活动模块。
type Module struct {
	logger *log.Logger
}

// New 创建活动模块。
func New(logger *log.Logger) *Module { return &Module{logger: logger} }

// Name 实现 module.Module。
func (m *Module) Name() string { return stateKey }

// MsgIDs 实现 module.Module。
func (m *Module) MsgIDs() []uint32 { return []uint32{msgIDListReq, msgIDClaimReq} }

// Init 实现 module.Module。
func (m *Module) Init(*module.Context) error { return nil }

// OnMessage 实现 module.Module。
func (m *Module) OnMessage(ctx *module.Context, msgID uint32, body []byte) error {
	switch msgID {
	case msgIDListReq:
		return m.onList(ctx)
	case msgIDClaimReq:
		return m.onClaim(ctx, body)
	default:
		return fmt.Errorf("activity: unexpected msg %d", msgID)
	}
}

// OnSave 实现 module.Module：状态随快照提交。
func (m *Module) OnSave(context.Context) error { return nil }

// OnLogin 登录类活动进度 +1，纯内存改动，随本次快照提交。
func (m *Module) OnLogin(ctx *module.Context) error {
	d, err := load(ctx.State)
	if err != nil {
		return nil // 状态损坏不阻断登录，后续消息会再次报错
	}
	changed := false
	for _, cfg := range gamedata.ActivityCfgList() {
		if cfg.Col2 != "login" {
			continue
		}
		id := int32(cfg.Id)
		rec := d.Records[id]
		if rec.Progress >= int32(cfg.Col3) {
			continue
		}
		rec.Progress++
		d.Records[id] = rec
		changed = true
	}
	if changed {
		if err := save(ctx.State, ctx.State.PlayerID(), d); err != nil {
			m.warn("activity login progress failed", ctx, err)
		}
	}
	return nil
}

// OnLogout 实现 module.Module。
func (m *Module) OnLogout(*module.Context) error { return nil }

// OnEvent 实现 module.EventAware：把玩家行为事件累加为活动进度。
func (m *Module) OnEvent(ctx *module.Context, evt module.Event) {
	d, err := load(ctx.State)
	if err != nil {
		return
	}
	changed := false
	for _, cfg := range gamedata.ActivityCfgList() {
		if cfg.Col2 != evt.Type {
			continue
		}
		id := int32(cfg.Id)
		rec := d.Records[id]
		next := rec.Progress + int32(evt.Delta)
		if next > int32(cfg.Col3) {
			next = int32(cfg.Col3)
		}
		if next == rec.Progress {
			continue
		}
		rec.Progress = next
		d.Records[id] = rec
		changed = true
	}
	if changed {
		if err := save(ctx.State, ctx.State.PlayerID(), d); err != nil {
			m.warn("activity progress update failed", ctx, err)
		}
	}
}

// onList 活动列表（配置 + 进度 + 是否已领）。
func (m *Module) onList(ctx *module.Context) error {
	resp := &gamev1.ActivityListResp{Code: codeOK}
	d, err := load(ctx.State)
	if err != nil {
		return err
	}
	for _, cfg := range gamedata.ActivityCfgList() {
		// 下发的窗口已过期就不展示：客户端仍会定时刷新，不需要服务端推「活动结束」。
		if !openNow(int32(cfg.Id), time.Now().Unix()) {
			continue
		}
		entry := &gamev1.ActivityEntry{
			ActivityId: int32(cfg.Id),
			Name:       cfg.Col1,
			Type:       cfg.Col2,
			Target:     int32(cfg.Col3),
		}
		if rec, ok := d.Records[int32(cfg.Id)]; ok {
			entry.Progress = rec.Progress
			entry.Claimed = rec.RewardMask&1 != 0
		}
		resp.Activities = append(resp.Activities, entry)
	}
	return reply(ctx, msgIDListResp, resp)
}

// onClaim 领奖：进度达标 + 位图防重复，领奖事实与奖励同一次快照提交。
func (m *Module) onClaim(ctx *module.Context, body []byte) error {
	var req gamev1.ActivityClaimReq
	if err := proto.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("decode ActivityClaimReq: %w", err)
	}
	resp := &gamev1.ActivityClaimResp{}
	cfg, ok := gamedata.GetActivityCfg(int64(req.ActivityId))
	if !ok {
		resp.Code = codeActivityNotExist
		return reply(ctx, msgIDClaimResp, resp)
	}
	// 领取同样校验开放期：不能依赖客户端「恰好没收到结束通知」。
	if !openNow(req.ActivityId, time.Now().Unix()) {
		resp.Code = codeNotOpen
		return reply(ctx, msgIDClaimResp, resp)
	}
	d, err := load(ctx.State)
	if err != nil {
		return err
	}
	rec := d.Records[req.ActivityId]
	if rec.Progress < int32(cfg.Col3) {
		resp.Code = codeNotFinished
		return reply(ctx, msgIDClaimResp, resp)
	}
	if rec.RewardMask&1 != 0 {
		resp.Code = codeAlreadyClaimed
		return reply(ctx, msgIDClaimResp, resp)
	}

	// 发奖：target * 20 钻石；先预检容量，满包时不发也不置领取位
	gain := int64(cfg.Col3) * 20
	if !ctx.State.CanHold([]module.ItemGrant{{ItemID: itemDiamond, Count: gain}}) {
		resp.Code = codeBagFull
		return reply(ctx, msgIDClaimResp, resp)
	}
	ctx.State.AddItem(itemDiamond, gain)
	ctx.State.MarkDirty("bag")
	rec.RewardMask |= 1
	d.Records[req.ActivityId] = rec
	if err := save(ctx.State, ctx.State.PlayerID(), d); err != nil {
		return err
	}

	resp.Code = codeOK
	if raw, err := proto.Marshal(&gamev1.BagChangeNotify{Changes: []*gamev1.ItemChange{{
		ItemUid: int64(itemDiamond),
		ItemId:  itemDiamond,
		Count:   int32(ctx.State.GetItemCount(itemDiamond)),
		Change:  1,
	}}}); err == nil {
		resp.Rewards = raw
	}
	return reply(ctx, msgIDClaimResp, resp)
}

// load 读取活动状态；从未参与返回空结构。
func load(state writebehind.ExtState) (*Data, error) {
	row, err := writebehind.LoadExt[model.ActivityRecord](state, stateKey)
	if err != nil {
		return nil, err
	}
	if row == nil {
		return &Data{Records: map[int32]Record{}}, nil
	}
	return decode(row)
}

func decode(row *model.ActivityRecord) (*Data, error) {
	d := &Data{Records: map[int32]Record{}}
	if row.Records != "" {
		if err := json.Unmarshal([]byte(row.Records), &d.Records); err != nil {
			return nil, fmt.Errorf("decode activity state: %w", err)
		}
	}
	if d.Records == nil {
		d.Records = map[int32]Record{}
	}
	return d, nil
}

func save(state writebehind.ExtState, playerID int64, d *Data) error {
	raw, err := json.Marshal(d.Records)
	if err != nil {
		return fmt.Errorf("encode activity state: %w", err)
	}
	return writebehind.SaveExt(state, stateKey, &model.ActivityRecord{
		PlayerId: playerID,
		Records:  string(raw),
	})
}

func reply(ctx *module.Context, msgID uint32, msg proto.Message) error {
	body, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	return ctx.Reply(msgID, body)
}

func (m *Module) warn(msg string, ctx *module.Context, err error) {
	if m.logger != nil {
		m.logger.Warn(msg, "player_id", ctx.State.PlayerID(), "err", err)
	}
}
