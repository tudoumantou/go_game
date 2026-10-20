// internal/game/module/equip/equip.go —— 装备模块（3101-3106）
//
// 装备实例放在玩家快照 Ext["equip"]：穿戴/卸下只改内存，属性加成随同一次
// 提交生效，Redis CAS 失败时装备状态与加成一起回滚（契约见 docs/R2.1-缓存原子变更契约.md）。
package equip

import (
	"context"
	"encoding/json"
	"fmt"

	"game_study/internal/game/module"
	"game_study/pkg/codec"
	"game_study/pkg/gamedata"
	"game_study/pkg/log"
	"game_study/pkg/model"
	"game_study/pkg/writebehind"

	gamev1 "game_study/pkg/pb/game/v1"
	"google.golang.org/protobuf/proto"
)

const stateKey = "equip"

// 协议号（4.9：3101/3102 列表，3103/3104 穿戴，3105/3106 脱下）。
const (
	msgIDListReq     = codec.MsgEquipListReq
	msgIDListResp    = codec.MsgEquipListResp
	msgIDWearReq     = codec.MsgEquipWearReq
	msgIDWearResp    = codec.MsgEquipWearResp
	msgIDTakeOffReq  = codec.MsgEquipTakeOffReq
	msgIDTakeOffResp = codec.MsgEquipTakeOffResp
)

// 业务码（4.10）。
const (
	codeOK           int32 = 0
	codeNotExist     int32 = 3101
	codeLevelLow     int32 = 3102
	codeSlotConflict int32 = 3103
	codeNotEquipped  int32 = 3104
)

// Entry 装备实例。
//
// 字段语义：
//   - UID：实例唯一 ID（同配置 ID 可持有多件，按 UID 穿戴/卸下）；
//   - EnhanceLevel：强化等级；
//   - Quality：品质（配置派生，预留强化差异）；
//   - Equipped：是否已穿戴；加成只由已穿戴件提供。
type Entry struct {
	UID          int64 `json:"uid"`
	EquipID      int32 `json:"equip_id"`
	EnhanceLevel int32 `json:"enhance_level"`
	Quality      int32 `json:"quality"`
	Equipped     bool  `json:"equipped"`
}

// Data 是装备模块的私有状态：玩家全部装备实例。
type Data struct {
	Equips []Entry `json:"equips"`
}

// init 注册持久化：player_equip 主键为 (player_id)。
func init() {
	writebehind.RegisterFlat[model.PlayerEquip](stateKey, model.PlayerEquipFlat, validate)
}

// validate 落库前校验：UID 唯一、强化等级非负。
func validate(row *model.PlayerEquip) error {
	d, err := decode(row)
	if err != nil {
		return err
	}
	seen := make(map[int64]bool, len(d.Equips))
	for _, e := range d.Equips {
		if e.UID <= 0 || e.EquipID <= 0 || e.EnhanceLevel < 0 {
			return fmt.Errorf("invalid equip %+v", e)
		}
		if seen[e.UID] {
			return fmt.Errorf("duplicate equip uid %d", e.UID)
		}
		seen[e.UID] = true
	}
	return nil
}

// Module 装备模块。
type Module struct {
	logger *log.Logger
}

// New 创建装备模块。
func New(logger *log.Logger) *Module { return &Module{logger: logger} }

// Name 实现 module.Module。
func (m *Module) Name() string { return stateKey }

// MsgIDs 实现 module.Module。
func (m *Module) MsgIDs() []uint32 { return []uint32{msgIDListReq, msgIDWearReq, msgIDTakeOffReq} }

// Init 实现 module.Module。
func (m *Module) Init(*module.Context) error { return nil }

// OnMessage 实现 module.Module。
func (m *Module) OnMessage(ctx *module.Context, msgID uint32, body []byte) error {
	switch msgID {
	case msgIDListReq:
		return m.onList(ctx)
	case msgIDWearReq:
		return m.onWear(ctx, body)
	case msgIDTakeOffReq:
		return m.onTakeOff(ctx, body)
	default:
		return fmt.Errorf("equip: unexpected msg %d", msgID)
	}
}

// OnSave 实现 module.Module：装备随快照提交。
func (m *Module) OnSave(context.Context) error { return nil }

// OnLogin 按已穿戴装备重算属性加成。
//
// 加成是派生值，不落库：每次登录从快照里的装备列表重算，避免重复累加。
func (m *Module) OnLogin(ctx *module.Context) error {
	d, err := load(ctx.State)
	if err != nil {
		return err
	}
	// 不清空加成：玩家层已在 LoginAll 前统一清空，这里只叠加自己的部分。
	for _, e := range d.Equips {
		if !e.Equipped {
			continue
		}
		if cfg, ok := gamedata.GetEquipCfg(int64(e.EquipID)); ok {
			ctx.State.AddBonus("atk", cfg.Col4)
		}
	}
	return nil
}

// OnLogout 实现 module.Module。
func (m *Module) OnLogout(*module.Context) error { return nil }

// onList 装备列表：按快照顺序原样下发（顺序即持有顺序，不做排序）。
func (m *Module) onList(ctx *module.Context) error {
	resp := &gamev1.EquipListResp{Code: codeOK}
	d, err := load(ctx.State)
	if err != nil {
		return err
	}
	for _, e := range d.Equips {
		resp.Equips = append(resp.Equips, &gamev1.EquipEntry{
			Uid:          e.UID,
			EquipId:      e.EquipID,
			EnhanceLevel: e.EnhanceLevel,
			Quality:      e.Quality,
			Equipped:     e.Equipped,
		})
	}
	return reply(ctx, msgIDListResp, resp)
}

// onWear 穿戴：同部位冲突检查、等级校验，状态与加成同一次快照提交。
func (m *Module) onWear(ctx *module.Context, body []byte) error {
	var req gamev1.EquipWearReq
	if err := proto.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("decode EquipWearReq: %w", err)
	}
	resp := &gamev1.EquipWearResp{}
	d, err := load(ctx.State)
	if err != nil {
		return err
	}
	idx := findEquip(d, req.Uid)
	if idx < 0 {
		resp.Code = codeNotExist
		return reply(ctx, msgIDWearResp, resp)
	}
	e := d.Equips[idx]
	// 已穿戴再穿：幂等成功，直接回当前属性，不重复叠加 atk 加成。
	if e.Equipped {
		resp.Code = codeOK
		resp.Attrs = attrPairs(ctx)
		return reply(ctx, msgIDWearResp, resp)
	}
	cfg, ok := gamedata.GetEquipCfg(int64(e.EquipID))
	if !ok {
		resp.Code = codeNotExist
		return reply(ctx, msgIDWearResp, resp)
	}
	if ctx.State.Profile().Level < int32(cfg.Col2) {
		resp.Code = codeLevelLow
		return reply(ctx, msgIDWearResp, resp)
	}
	// 同部位冲突检查（Col3=部位）
	sameSlot := sameSlotEquipIDs(cfg.Col3)
	for _, other := range d.Equips {
		if !other.Equipped || other.UID == e.UID {
			continue
		}
		for _, id := range sameSlot {
			if other.EquipID == id {
				resp.Code = codeSlotConflict
				return reply(ctx, msgIDWearResp, resp)
			}
		}
	}

	e.Equipped = true
	d.Equips[idx] = e
	if err := save(ctx.State, ctx.State.PlayerID(), d); err != nil {
		return err
	}
	ctx.State.AddBonus("atk", cfg.Col4)
	ctx.State.MarkDirty(stateKey)

	resp.Code = codeOK
	resp.Attrs = attrPairs(ctx)
	return reply(ctx, msgIDWearResp, resp)
}

// onTakeOff 卸下：装备仍在列表里，只置 equipped=false 并扣回加成。
func (m *Module) onTakeOff(ctx *module.Context, body []byte) error {
	var req gamev1.EquipTakeOffReq
	if err := proto.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("decode EquipTakeOffReq: %w", err)
	}
	resp := &gamev1.EquipTakeOffResp{}
	d, err := load(ctx.State)
	if err != nil {
		return err
	}
	idx := findEquip(d, req.Uid)
	if idx < 0 {
		resp.Code = codeNotExist
		return reply(ctx, msgIDTakeOffResp, resp)
	}
	e := d.Equips[idx]
	if !e.Equipped {
		resp.Code = codeNotEquipped
		return reply(ctx, msgIDTakeOffResp, resp)
	}
	cfg, ok := gamedata.GetEquipCfg(int64(e.EquipID))
	if !ok {
		resp.Code = codeNotExist
		return reply(ctx, msgIDTakeOffResp, resp)
	}

	e.Equipped = false
	d.Equips[idx] = e
	if err := save(ctx.State, ctx.State.PlayerID(), d); err != nil {
		return err
	}
	ctx.State.AddBonus("atk", -cfg.Col4)
	ctx.State.MarkDirty(stateKey)

	resp.Code = codeOK
	resp.Attrs = attrPairs(ctx)
	return reply(ctx, msgIDTakeOffResp, resp)
}

// findEquip 按实例 UID 找下标；找不到返回 -1。
func findEquip(d *Data, uid int64) int {
	for i, e := range d.Equips {
		if e.UID == uid {
			return i
		}
	}
	return -1
}

// sameSlotEquipIDs 返回同部位的全部装备配置 ID。
func sameSlotEquipIDs(slot string) []int32 {
	var out []int32
	for _, cfg := range gamedata.EquipCfgList() {
		if cfg.Col3 == slot {
			out = append(out, int32(cfg.Id))
		}
	}
	return out
}

// load 读取装备状态；从未获得装备返回空结构。
func load(state writebehind.ExtState) (*Data, error) {
	row, err := writebehind.LoadExt[model.PlayerEquip](state, stateKey)
	if err != nil {
		return nil, err
	}
	if row == nil {
		return &Data{Equips: []Entry{}}, nil
	}
	return decode(row)
}

// decode 把表行 JSON 解成业务视图；保证切片非 nil。
func decode(row *model.PlayerEquip) (*Data, error) {
	d := &Data{Equips: []Entry{}}
	if row.Equips != "" {
		if err := json.Unmarshal([]byte(row.Equips), &d.Equips); err != nil {
			return nil, fmt.Errorf("decode equip state: %w", err)
		}
	}
	if d.Equips == nil {
		d.Equips = []Entry{}
	}
	return d, nil
}

func save(state writebehind.ExtState, playerID int64, d *Data) error {
	raw, err := json.Marshal(d.Equips)
	if err != nil {
		return fmt.Errorf("encode equip state: %w", err)
	}
	return writebehind.SaveExt(state, stateKey, &model.PlayerEquip{
		PlayerId: playerID,
		Equips:   string(raw),
	})
}

// attrPairs 把五维属性打包成协议对（AttrId 约定见 profile 模块 1-5）。
func attrPairs(ctx *module.Context) []*gamev1.AttrPair {
	return []*gamev1.AttrPair{
		{AttrId: 1, Value: ctx.State.GetAttr("hp")},
		{AttrId: 2, Value: ctx.State.GetAttr("mp")},
		{AttrId: 3, Value: ctx.State.GetAttr("atk")},
		{AttrId: 4, Value: ctx.State.GetAttr("def")},
		{AttrId: 5, Value: ctx.State.GetAttr("spd")},
	}
}

// reply 序列化 proto 并经 ctx.Reply 下发。
func reply(ctx *module.Context, msgID uint32, msg proto.Message) error {
	body, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	return ctx.Reply(msgID, body)
}
