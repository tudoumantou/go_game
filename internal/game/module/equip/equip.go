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

// init 注册持久化：player_equip 主键为 (player_id)。
//
// Go 知识点：init 是包级副作用入口，import 本包即执行；
// RegisterFlat 是泛型函数[T any]，把「表行类型 T + FlatOps + 校验函数」
// 登记进 writebehind 全局注册表——类型参数让同一份注册逻辑复用于
// 所有玩法表，无需反射或代码生成。
func init() {
	writebehind.RegisterFlat[model.PlayerEquip](stateKey, model.PlayerEquipFlat, validate)
}

// validate 落库前校验：UID 唯一、强化等级非负。
//
// 这是快照落库前的最后一道闸：校验失败会让整批写回失败（宁可丢一批
// 也不写坏数据），因此只查「结构性不变量」，不查业务规则。
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
	*module.Thin
	logger *log.Logger
}

// New 创建装备模块。
func New(logger *log.Logger) *Module {
	m := &Module{logger: logger}
	m.Thin = module.NewThin(stateKey,
		module.ThinLogger(logger),
		module.ThinLogin(m.onLogin),
	)
	m.Handle(msgIDListReq, m.onList)
	m.Handle(msgIDWearReq, m.onWear)
	m.Handle(msgIDTakeOffReq, m.onTakeOff)
	return m
}

// onLogin 按已穿戴装备重算属性加成（经 ThinLogin 挂接）。
//
// 加成是派生值，不落库：每次登录从快照里的装备列表重算，避免重复累加。
// 时序契约：玩家层在 LoginAll 之前统一 ClearBonus，本模块只叠加自己的部分；
// 若不先清空，重连一次就会把 atk 加成翻倍。
func (m *Module) onLogin(ctx *module.Context) error {
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

// onList 装备列表：按快照顺序原样下发（顺序即持有顺序，不做排序）。
func (m *Module) onList(ctx *module.Context, _ uint32, _ []byte) error {
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
func (m *Module) onWear(ctx *module.Context, _ uint32, body []byte) error {
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
	// 反查「同部位全部配置 ID」而不是逐件读对方配置：装备表很小，
	// 一次遍历建集合，比 N 次查配置 + 比较更直观。
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
	// save 与 AddBonus 都只改内存，真正生效是同一条消息结束时的快照 CAS：
	// CAS 失败则 Equipped 与加成一起回滚，不会「状态说穿了、属性没加」。
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
//
// 卸下不删除实例——「拥有」与「穿戴」是两个事实，分别由列表存在性
// 与 Equipped 字段表达。
func (m *Module) onTakeOff(ctx *module.Context, _ uint32, body []byte) error {
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
	// 加成取负扣回：与穿戴时的 +Col4 精确对冲，登录重算前不会残留。
	ctx.State.AddBonus("atk", -cfg.Col4)
	ctx.State.MarkDirty(stateKey)

	resp.Code = codeOK
	resp.Attrs = attrPairs(ctx)
	return reply(ctx, msgIDTakeOffResp, resp)
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

// load 读取装备状态；从未获得装备返回空结构。
//
// 懒初始化落点：row == nil 时返回空 Data 而不写库——
// 「没有装备」不需要一行记录，第一次穿戴才产生行。
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
//
// nil 切片与空切片的区别：JSON 里 null 解出来是 nil，直接 append 虽也能用，
// 但序列化回库会变成 null 而非 "[]"，diff 与对账时不直观，所以统一归一。
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

// save 把装备列表编码回 Ext 槽位；落库调度由 Write-Behind 负责。
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
