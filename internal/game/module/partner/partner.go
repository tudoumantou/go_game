// Package partner 伙伴（R6）：激活 → 升星 → 上阵/下阵。
//
// 状态整体进玩家快照（Ext["partner"]），表是 player_id 单行：
// 激活/升星要「扣金币 + 改状态」同成同败，走快照 CAS 天然满足；
// 拆成多行实体表反而要自己维护事务。
//
// 架构职责：伙伴收集度 + 上阵编成 + 攻击加成维护。
// Ext 槽位：player_partner.Partners 单列 JSON（stars + field）。
// 事件订阅：无。
// 配置表：partner.xml（Col1=名称，Col2=品质，Col3=每星攻击加成）、
// partner_star.xml（col1=伙伴，col2=当前星级，col3=升下一星消耗）。
// 懒初始化：未激活任何伙伴时 Ext 为空，load 返回空 Data；
// 「上阵位」上限 fieldLimit 是玩法常量而非配置。
//
// 属性加成只由**已上阵**伙伴提供，且只在「登录 / 上阵 / 下阵 / 升星」四个时点
// 精确增减一次，不在每次读列表时重算——避免反复累加（R6.3）。
package partner

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

const (
	// stateKey 同时是 Ext 槽位键与落库归属的模块名。
	stateKey = "partner"

	msgIDListReq      = codec.MsgPartnerListReq
	msgIDListResp     = codec.MsgPartnerListResp
	msgIDActivateReq  = codec.MsgPartnerActivateReq
	msgIDActivateResp = codec.MsgPartnerActivateResp
	msgIDStarUpReq    = codec.MsgPartnerStarUpReq
	msgIDStarUpResp   = codec.MsgPartnerStarUpResp
	msgIDFieldReq     = codec.MsgPartnerFieldReq
	msgIDFieldResp    = codec.MsgPartnerFieldResp

	codeOK        int32 = 0
	codeNotFound  int32 = 6901 // 伙伴配置不存在
	codeOwned     int32 = 6902 // 已拥有
	codeNotOwned  int32 = 6903 // 未拥有
	codeNoCoin    int32 = 6904 // 金币不足
	codeMaxStar   int32 = 6905 // 已满星
	codeFieldFull int32 = 6906 // 上阵位已满

	// fieldLimit 上阵位数。配置里没有这一项，作为玩法常量维护。
	fieldLimit = 3
	// itemCoin 金币道具 ID（激活与升星消耗同一种货币）。
	itemCoin int32 = 1001
)

// Data 伙伴状态：partner_id → 星级；Field 是上阵顺序。
//
// 两个容器分工：Stars 是 map（稀疏、按 ID 随机访问），
// Field 是 slice（稠密、保序、最多 3 个元素）——用对容器比用对算法重要。
type Data struct {
	Stars map[int32]int32 `json:"stars"`
	Field []int32         `json:"field"`
}

// init 注册持久化：表主键恰为 (player_id)，FlatOps 直接复用。
func init() {
	writebehind.RegisterFlat[model.PlayerPartner](stateKey, model.PlayerPartnerFlat, validate)
}

// validate 落库前校验：只做可解码性检查，业务不变量（上阵合法）在 decode 里。
func validate(row *model.PlayerPartner) error {
	_, err := decode(row)
	return err
}

// decode 把表行 JSON 解成业务视图，并拦住坏快照：
// 上阵位超限、上阵未拥有、重复上阵都会让登录给出错误加成且无法自愈，
// 宁可整批落库失败，也不把坏状态放进出战计算。
//
// Go 知识点：这是「可信边界」模式——Ext 数据可能来自旧版本、
// 手工改库或传输损坏，decode 是第一个接触点，把不变量检查
// 集中在读取路径，比在每个使用点防御更可靠。
func decode(row *model.PlayerPartner) (*Data, error) {
	d := &Data{Stars: map[int32]int32{}, Field: []int32{}}
	if row == nil || row.Partners == "" {
		return d, nil
	}
	if err := json.Unmarshal([]byte(row.Partners), d); err != nil {
		return nil, fmt.Errorf("decode partner state: %w", err)
	}
	if d.Stars == nil {
		d.Stars = map[int32]int32{}
	}
	// 快照可能被旧版本或手工改坏：上阵位超限、上阵未拥有的伙伴都要在这里拦住，
	// 否则登录会给出错误加成且无法自愈。
	if len(d.Field) > fieldLimit {
		return nil, fmt.Errorf("partner field has %d entries, limit %d", len(d.Field), fieldLimit)
	}
	seen := map[int32]bool{}
	for _, id := range d.Field {
		if d.Stars[id] <= 0 {
			return nil, fmt.Errorf("partner %d on field but not owned", id)
		}
		if seen[id] {
			return nil, fmt.Errorf("partner %d duplicated on field", id)
		}
		seen[id] = true
	}
	return d, nil
}

// load 读取伙伴状态；从未激活返回空结构（Stars 空 map、Field 空切片）。
func load(state writebehind.ExtState) (*Data, error) {
	row, err := writebehind.LoadExt[model.PlayerPartner](state, stateKey)
	if err != nil {
		return nil, err
	}
	return decode(row)
}

// save 把星级与上阵序列化回 Ext 槽位；落库由 Write-Behind 负责。
func save(state writebehind.ExtState, playerID int64, d *Data) error {
	raw, err := json.Marshal(d)
	if err != nil {
		return fmt.Errorf("encode partner state: %w", err)
	}
	return writebehind.SaveExt(state, stateKey, &model.PlayerPartner{
		PlayerId: playerID,
		Partners: string(raw),
	})
}

// Module 伙伴模块。
type Module struct {
	*module.Thin
	logger *log.Logger
}

// New 创建模块。
func New(logger *log.Logger) *Module {
	m := &Module{logger: logger}
	m.Thin = module.NewThin(stateKey,
		module.ThinLogger(logger),
		module.ThinLogin(m.onLogin),
	)
	m.HandleR(msgIDListReq, msgIDListResp, &gamev1.PartnerListResp{}, m.onList)
	m.HandleR(msgIDActivateReq, msgIDActivateResp, &gamev1.PartnerActivateResp{}, m.onActivate)
	m.HandleR(msgIDStarUpReq, msgIDStarUpResp, &gamev1.PartnerStarUpResp{}, m.onStarUp)
	m.HandleR(msgIDFieldReq, msgIDFieldResp, &gamev1.PartnerFieldResp{}, m.onField)
	return m
}

// onLogin 重新叠加已上阵伙伴的加成。
//
// 玩家层在 LoginAll 之前已 ClearBonus，所以这里是「从零叠加」，不会与上次登录累加。
// 状态损坏时吞掉错误继续登录：加成可以下次写操作时修复，
// 但把玩家拦在登录外的影响更大（可用性优先）。
func (m *Module) onLogin(ctx *module.Context) error {
	d, err := load(ctx.State)
	if err != nil {
		return nil // 状态损坏不阻断登录；下次写操作会再次报错
	}
	for _, id := range d.Field {
		if bonus := atkBonus(id, d.Stars[id]); bonus != 0 {
			ctx.State.AddBonus("atk", bonus)
		}
	}
	return nil
}

// onList 伙伴列表：全部配置都下发，未拥有（星级 0）不带加成字段。
//
// 与任务/副本同构：列表 = 配置全集 JOIN 玩家进度，未解锁项也展示
// （星级 0），客户端可渲染「可激活」入口。
func (m *Module) onList(ctx *module.Context, _ uint32, _ []byte) error {
	d, err := load(ctx.State)
	if err != nil {
		return err
	}
	resp := &gamev1.PartnerListResp{Code: codeOK, FieldLimit: fieldLimit}
	for _, cfg := range gamedata.PartnerCfgList() {
		id := int32(cfg.Id)
		star := d.Stars[id]
		entry := &gamev1.PartnerEntry{
			PartnerId: id,
			Name:      cfg.Col1,
			Quality:   int32(cfg.Col2),
			Star:      star,
		}
		if star > 0 {
			entry.OnField = contains(d.Field, id)
			entry.AtkBonus = atkBonus(id, star)
		}
		resp.Partners = append(resp.Partners, entry)
	}
	return reply(ctx, msgIDListResp, resp)
}

// onActivate 激活：消耗配置里的 col3 金币，状态置为 1 星（不上阵）。
//
// 扣费与置星都在内存：CAS 失败一起回滚，不会「扣了钱没激活」。
func (m *Module) onActivate(ctx *module.Context, _ uint32, body []byte) error {
	var req gamev1.PartnerActivateReq
	if err := proto.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("decode PartnerActivateReq: %w", err)
	}
	resp := &gamev1.PartnerActivateResp{}

	cfg, ok := gamedata.GetPartnerCfg(int64(req.PartnerId))
	if !ok {
		resp.Code = codeNotFound
		return reply(ctx, msgIDActivateResp, resp)
	}
	d, err := load(ctx.State)
	if err != nil {
		return err
	}
	if d.Stars[req.PartnerId] > 0 {
		resp.Code = codeOwned
		return reply(ctx, msgIDActivateResp, resp)
	}
	if err := module.Spend(ctx.State, itemCoin, cfg.Col3); err != nil {
		resp.Code = codeNoCoin
		return reply(ctx, msgIDActivateResp, resp)
	}

	d.Stars[req.PartnerId] = 1
	if err := save(ctx.State, ctx.State.PlayerID(), d); err != nil {
		return err
	}
	return reply(ctx, msgIDActivateResp, resp)
}

// onStarUp 升星：按 partner_star 的「当前星级 → 下一星」消耗金币。
//
// 已上阵的伙伴升星要同步改加成：先记旧加成，扣费置新星后
// 加「新星加成 - 旧星加成」的差值——净变化只发生一次，
// 比「先减后加」少一次中间态。
func (m *Module) onStarUp(ctx *module.Context, _ uint32, body []byte) error {
	var req gamev1.PartnerStarUpReq
	if err := proto.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("decode PartnerStarUpReq: %w", err)
	}
	resp := &gamev1.PartnerStarUpResp{}

	d, err := load(ctx.State)
	if err != nil {
		return err
	}
	star := d.Stars[req.PartnerId]
	if star <= 0 {
		resp.Code = codeNotOwned
		return reply(ctx, msgIDStarUpResp, resp)
	}
	cost, ok := starCost(req.PartnerId, star)
	if !ok {
		// 配置里没有「当前星 → 下一星」行 = 已满星
		resp.Code = codeMaxStar
		return reply(ctx, msgIDStarUpResp, resp)
	}
	// 扣费放在读旧加成之前：Spend 失败时不改状态，后面的逻辑就不会跑到。
	if err := module.Spend(ctx.State, itemCoin, cost); err != nil {
		resp.Code = codeNoCoin
		return reply(ctx, msgIDStarUpResp, resp)
	}

	wasOnField := contains(d.Field, req.PartnerId)
	oldBonus := atkBonus(req.PartnerId, star)

	d.Stars[req.PartnerId] = star + 1
	if err := save(ctx.State, ctx.State.PlayerID(), d); err != nil {
		return err
	}

	// 已上阵时属性要跟着变：先减旧星的加成，再加新星的，净变化只发生一次。
	if wasOnField {
		ctx.State.AddBonus("atk", atkBonus(req.PartnerId, star+1)-oldBonus)
	}
	resp.Star = star + 1
	return reply(ctx, msgIDStarUpResp, resp)
}

// onField 上阵/下阵；幂等，重复请求不重复加/减属性。
//
// Go 知识点：switch 不带表达式（switch true 的省略写法）+ case 布尔条件，
// 把「请求态 × 当前态」的四种组合平铺成四个分支，比 if-else 嵌套
// 更能表达「状态机」语义。两个「已是目标态」的分支故意空转——
// 幂等成功但不做任何内存改动。
func (m *Module) onField(ctx *module.Context, _ uint32, body []byte) error {
	var req gamev1.PartnerFieldReq
	if err := proto.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("decode PartnerFieldReq: %w", err)
	}
	resp := &gamev1.PartnerFieldResp{}
	d, err := load(ctx.State)
	if err != nil {
		return err
	}
	star := d.Stars[req.PartnerId]
	if star <= 0 {
		resp.Code = codeNotOwned
		return reply(ctx, msgIDFieldResp, resp)
	}

	onField := contains(d.Field, req.PartnerId)
	switch {
	case req.On && onField:
		// 已上阵：幂等成功，不重复加加成。
	case req.On && !onField:
		if len(d.Field) >= fieldLimit {
			resp.Code = codeFieldFull
			return reply(ctx, msgIDFieldResp, resp)
		}
		d.Field = append(d.Field, req.PartnerId)
		ctx.State.AddBonus("atk", atkBonus(req.PartnerId, star))
	case !req.On && onField:
		d.Field = remove(d.Field, req.PartnerId)
		ctx.State.AddBonus("atk", -atkBonus(req.PartnerId, star))
	case !req.On && !onField:
		// 已下阵：幂等成功。
	}

	if err := save(ctx.State, ctx.State.PlayerID(), d); err != nil {
		return err
	}
	resp.AtkBonus = totalBonus(d)
	return reply(ctx, msgIDFieldResp, resp)
}

// atkBonus 一个伙伴当前星级提供的攻击加成：配置基础值 × 星级。
func atkBonus(partnerID, star int32) int64 {
	if star <= 0 {
		return 0
	}
	cfg, ok := gamedata.GetPartnerCfg(int64(partnerID))
	if !ok {
		return 0
	}
	return cfg.Col3 * int64(star)
}

// totalBonus 已上阵伙伴的加成合计。
func totalBonus(d *Data) int64 {
	var sum int64
	for _, id := range d.Field {
		sum += atkBonus(id, d.Stars[id])
	}
	return sum
}

// starCost 当前星级升下一星的消耗（partner_star：col1 伙伴、col2 当前星级、col3 消耗）。
func starCost(partnerID, star int32) (int64, bool) {
	for _, cfg := range gamedata.PartnerStarCfgList() {
		if int32(cfg.Col1) == partnerID && int32(cfg.Col2) == star {
			return cfg.Col3, true
		}
	}
	return 0, false
}

// contains 列表成员判断（上阵位很小，线性扫描足够）。
func contains(list []int32, v int32) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
}

// remove 原地过滤掉指定成员并返回同一底层数组的新切片（调用方立即覆盖原字段）。
//
// Go 知识点：out := list[:0] 是经典的「原地过滤」——复用同一底层数组，
// 零分配。坑在于返回的切片与入参共享数组：调用方若还持有旧切片
// （比如另一处 defer 里读它），会看到被覆写的数据。这里调用方
// 立即 d.Field = remove(...) 覆盖原字段，没有第二个引用，所以安全。
func remove(list []int32, v int32) []int32 {
	out := list[:0]
	for _, x := range list {
		if x != v {
			out = append(out, x)
		}
	}
	return out
}

func reply(ctx *module.Context, msgID uint32, msg proto.Message) error {
	raw, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal partner resp: %w", err)
	}
	return ctx.Reply(msgID, raw)
}
