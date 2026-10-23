// internal/game/module/equip/equip.go —— 装备模块（3101-3106）
//
// 装备实例放在玩家快照 Ext["equip"]：穿戴/卸下只改内存，属性加成随同一次
// 提交生效，Redis CAS 失败时装备状态与加成一起回滚（契约见 docs/R2.1-缓存原子变更契约.md）。
//
// 架构职责：装备的协议面 + 属性加成的维护方之一。穿戴状态是「玩法进度」，
// 因此整包进 Ext 槽位；加成是派生值只进内存，登录时按已穿戴件重算。
// 事件订阅：无（穿戴不产生行为事件；P7 可加 equip_change 驱动任务）。
// 配置表：equip.xml（Col2=等级门槛，Col3=部位，Col4=攻击加成）。
// 懒初始化：玩家从未获得装备时 Ext 槽位为空行，load() 返回空 Data，
// 不会为「空装备列表」预建行——首次穿戴才产生第一行快照数据。
package equip

import (
	"game_study/internal/game/module"
	"game_study/pkg/codec"
	"game_study/pkg/gamedata"
	"game_study/pkg/log"

	gamev1 "game_study/pkg/pb/game/v1"
	"google.golang.org/protobuf/proto"
)

// stateKey 同时是 Ext 槽位键与模块名：Ext/SetExt、落库归属、日志标签
// 三处共用同一个字符串，改一处即可全局一致（各玩法模块的同一约定）。
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

// 业务码（4.10）：装备段 3101-3104。
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
//
// Go 知识点：每个字段都带 json tag——快照里存的是 JSON 字符串，
// tag 即「线上契约」。零值语义：EnhanceLevel=0 表示未强化，
// Equipped=false 表示未穿戴，omitempty 一概不加——省略 false/0
// 会让「未强化」和「字段缺失」无法区分。
type Entry struct {
	UID          int64 `json:"uid"`
	EquipID      int32 `json:"equip_id"`
	EnhanceLevel int32 `json:"enhance_level"`
	Quality      int32 `json:"quality"`
	Equipped     bool  `json:"equipped"`
}

// Data 是装备模块的私有状态：玩家全部装备实例。
//
// 用切片而非 map：持有量小（几十件）、顺序即获得顺序，
// 且 UID 查找是线性扫描也够快（findEquip）。
type Data struct {
	Equips []Entry `json:"equips"`
}

// Module 装备模块。
type Module struct {
	// Stateful 内嵌 Thin：生命周期与消息注册仍由 Thin 提供。
	*module.Stateful[Data]
	logger *log.Logger
}

// New 创建装备模块。
func New(logger *log.Logger) *Module {
	m := &Module{logger: logger}
	m.Stateful = module.NewStateful(stateKey, newData,
		module.ThinLogger(logger),
		module.ThinLogin(m.onLogin),
	)
	m.HandleR(msgIDListReq, msgIDListResp, &gamev1.EquipListResp{}, m.onList)
	m.HandleR(msgIDWearReq, msgIDWearResp, &gamev1.EquipWearResp{}, module.Typed(msgIDWearResp, m.onWear))
	m.HandleR(msgIDTakeOffReq, msgIDTakeOffResp, &gamev1.EquipTakeOffResp{}, module.Typed(msgIDTakeOffResp, m.onTakeOff))
	return m
}

// onLogin 按已穿戴装备重算属性加成（经 ThinLogin 挂接）。
//
// 加成是派生值，不落库：每次登录从快照里的装备列表重算，避免重复累加。
// 时序契约：玩家层在 LoginAll 之前统一 ClearBonus，本模块只叠加自己的部分；
// 若不先清空，重连一次就会把 atk 加成翻倍。
func (m *Module) onLogin(ctx *module.Context) error {
	return m.Read(ctx, func(d *Data) {
		// 不清空加成：玩家层已在 LoginAll 前统一清空，这里只叠加自己的部分。
		for _, e := range d.Equips {
			if !e.Equipped {
				continue
			}
			if cfg, ok := gamedata.GetEquipCfg(int64(e.EquipID)); ok {
				ctx.State.AddBonus("atk", cfg.Col4)
			}
		}
	})
}

// onList 装备列表：按快照顺序原样下发（顺序即持有顺序，不做排序）。
func (m *Module) onList(ctx *module.Context, _ uint32, _ []byte) error {
	resp := &gamev1.EquipListResp{Code: codeOK}
	if err := m.Read(ctx, func(d *Data) {
		for _, e := range d.Equips {
			resp.Equips = append(resp.Equips, &gamev1.EquipEntry{
				Uid:          e.UID,
				EquipId:      e.EquipID,
				EnhanceLevel: e.EnhanceLevel,
				Quality:      e.Quality,
				Equipped:     e.Equipped,
			})
		}
	}); err != nil {
		return err
	}
	return reply(ctx, msgIDListResp, resp)
}

// onWear 穿戴：同部位冲突检查、等级校验，状态与加成同一次快照提交。
func (m *Module) onWear(ctx *module.Context, req *gamev1.EquipWearReq, resp *gamev1.EquipWearResp) error {
	*resp = gamev1.EquipWearResp{}
	var bonus int64
	err := m.Mutate(ctx, func(d *Data) bool {
		idx := findEquip(d, req.Uid)
		if idx < 0 {
			resp.Code = codeNotExist
			return false
		}
		e := d.Equips[idx]
		// 已穿戴再穿：幂等成功，直接回当前属性，不重复叠加 atk 加成。
		if e.Equipped {
			resp.Code = codeOK
			resp.Attrs = attrPairs(ctx)
			return false
		}
		cfg, ok := gamedata.GetEquipCfg(int64(e.EquipID))
		if !ok {
			resp.Code = codeNotExist
			return false
		}
		if ctx.State.Profile().Level < int32(cfg.Col2) {
			resp.Code = codeLevelLow
			return false
		}
		// 同部位冲突检查（Col3=部位）：反查同部位配置 ID 集合，一次遍历。
		sameSlot := sameSlotEquipIDs(cfg.Col3)
		for _, other := range d.Equips {
			if !other.Equipped || other.UID == e.UID {
				continue
			}
			for _, id := range sameSlot {
				if other.EquipID == id {
					resp.Code = codeSlotConflict
					return false
				}
			}
		}

		e.Equipped = true
		d.Equips[idx] = e
		bonus = cfg.Col4
		return true
	})
	if err != nil {
		return err
	}
	if bonus != 0 {
		// 加成只改内存，与 Equipped 在同一条消息的快照 CAS 里一起生效：
		// CAS 失败则属性与状态一起回滚，不会「状态说穿了、属性没加」。
		ctx.State.AddBonus("atk", bonus)
	}
	if resp.Attrs == nil {
		resp.Attrs = attrPairs(ctx)
	}
	return nil
}

// onTakeOff 卸下：装备仍在列表里，只置 equipped=false 并扣回加成。
//
// 卸下不删除实例——「拥有」与「穿戴」是两个事实，分别由列表存在性
// 与 Equipped 字段表达。
func (m *Module) onTakeOff(ctx *module.Context, req *gamev1.EquipTakeOffReq, resp *gamev1.EquipTakeOffResp) error {
	*resp = gamev1.EquipTakeOffResp{}
	var bonus int64
	err := m.Mutate(ctx, func(d *Data) bool {
		idx := findEquip(d, req.Uid)
		if idx < 0 {
			resp.Code = codeNotExist
			return false
		}
		e := d.Equips[idx]
		if !e.Equipped {
			resp.Code = codeNotEquipped
			return false
		}
		cfg, ok := gamedata.GetEquipCfg(int64(e.EquipID))
		if !ok {
			resp.Code = codeNotExist
			return false
		}
		e.Equipped = false
		d.Equips[idx] = e
		bonus = -cfg.Col4
		return true
	})
	if err != nil {
		return err
	}
	if bonus != 0 {
		// 取负扣回：与穿戴时的 +Col4 精确对冲，登录重算前不会残留。
		ctx.State.AddBonus("atk", bonus)
	}
	if resp.Attrs == nil {
		resp.Attrs = attrPairs(ctx)
	}
	return nil
}

// findEquip 按实例 UID 找下标；找不到返回 -1。
//
// 返回下标而非 *Entry：调用方要改写后写回切片（d.Equips[idx] = e），
// 直接给指针会绕过「整份 Data 重新序列化」的提交路径。
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

// attrPairs 把五维属性打包成协议对（AttrId 约定见 profile 模块 1-5）。
//
// 取 GetAttr 而不是基础值：装备/伙伴的 bonus 已经加在最终值里，
// 客户端据此刷新面板，不需要知道加成来自哪个模块。
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
