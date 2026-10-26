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
// 懒初始化：未激活任何伙伴时 Ext 为空，Get 返回空 Data；
// 「上阵位」上限 fieldLimit 是玩法常量而非配置。
//
// 属性加成只由**已上阵**伙伴提供，且只在「登录 / 上阵 / 下阵 / 升星」四个时点
// 精确增减一次，不在每次读列表时重算——避免反复累加（R6.3）。
//
// 写法（v3.6 迁移）：模块声明一行（Define）、路由若干行、业务函数只碰
// Ctx.Repo；协议号由消息类型反查，错误码来自统一枚举。
package partner

import (
	"game_study/internal/common/errcode"
	"game_study/internal/game/module"
	"game_study/pkg/gamedata"

	gamev1 "game_study/pkg/pb/game/v1"
)

const (
	// stateKey 同时是 Ext 槽位键与落库归属的模块名。
	stateKey = "partner"

	// fieldLimit 上阵位数。配置里没有这一项，作为玩法常量维护。
	fieldLimit = 3
	// itemCoin 金币道具 ID（激活与升星消耗同一种货币）。
	itemCoin int32 = 1001
)

// Data 伙伴状态：partner_id → 星级；Field 是上阵顺序。
//
// 两个容器分工：Stars 是 map（稀疏、按 ID 随机访问），
// Field 是 slice（稠密、保序、最多 3 个元素）——用对容器比用对算法重要。
//
// json tag 是**落库契约**（整份序列化进 player_partner.partners 一列），
// 改 tag 等于让旧数据读不出来。
type Data struct {
	Stars map[int32]int32 `json:"stars"`
	Field []int32         `json:"field"`
}

// M 是伙伴模块的唯一实例。
//
// 没有 New()：日志、状态编解码、分发、生命周期都由 Define 兜底。
// 登录钩子重算加成——玩家层在 LoginAll 之前已 ClearBonus，所以是「从零叠加」。
var M = module.Define[Data](stateKey, newData).WithLogin(onLogin)

func init() {
	// 列表请求无参数：语义完全由「谁在发」决定。
	M.HandleRoute(&gamev1.PartnerListReq{}, onList)
	// 其余三条都要请求体（伙伴 id / 上阵开关）。
	module.Route(M, &gamev1.PartnerActivateReq{}, onActivate)
	module.Route(M, &gamev1.PartnerStarUpReq{}, onStarUp)
	module.Route(M, &gamev1.PartnerFieldReq{}, onField)
}

// onLogin 重新叠加已上阵伙伴的加成。
//
// 状态损坏时吞掉错误继续登录：加成可以下次写操作时修复，
// 但把玩家拦在登录外的影响更大（可用性优先）。
func onLogin(c *module.Ctx[Data]) error {
	d, err := c.Repo.Get()
	if err != nil {
		return nil
	}
	for _, id := range d.Field {
		if bonus := atkBonus(id, d.Stars[id]); bonus != 0 {
			c.State.AddBonus("atk", bonus)
		}
	}
	return nil
}

// onList 伙伴列表：全部配置都下发，未拥有（星级 0）不带加成字段。
//
// 与任务/副本同构：列表 = 配置全集 JOIN 玩家进度，未解锁项也展示
// （星级 0），客户端可渲染「可激活」入口。
func onList(c *module.Ctx[Data]) error {
	d, err := c.Repo.Get()
	if err != nil {
		return c.Fail(&gamev1.PartnerListResp{Code: int32(errcode.ErrSystem)}, err)
	}
	resp := &gamev1.PartnerListResp{FieldLimit: fieldLimit}
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
	return c.Reply(resp)
}

// onActivate 激活：消耗配置里的 col3 金币，状态置为 1 星（不上阵）。
//
// 扣费与置星都在内存：任何一步失败都不 Save，不会「扣了钱没激活」。
func onActivate(c *module.Ctx[Data], req *gamev1.PartnerActivateReq) error {
	cfg, ok := gamedata.GetPartnerCfg(int64(req.PartnerId))
	if !ok {
		return c.Reply(&gamev1.PartnerActivateResp{Code: int32(errcode.ErrPartnerNotFound)})
	}
	d, err := c.Repo.Get()
	if err != nil {
		return c.Fail(&gamev1.PartnerActivateResp{Code: int32(errcode.ErrSystem)}, err)
	}
	if d.Stars[req.PartnerId] > 0 {
		return c.Reply(&gamev1.PartnerActivateResp{Code: int32(errcode.ErrPartnerOwned)})
	}
	// 扣费与置星在同一份内存状态里：提交失败一起回滚。
	if err := module.Spend(c.State, itemCoin, cfg.Col3); err != nil {
		return c.Reply(&gamev1.PartnerActivateResp{Code: int32(errcode.ErrPartnerNoCoin)})
	}
	d.Stars[req.PartnerId] = 1
	if err := c.Repo.Save(d); err != nil {
		return c.Fail(&gamev1.PartnerActivateResp{Code: int32(errcode.ErrSystem)}, err)
	}
	return c.Reply(&gamev1.PartnerActivateResp{})
}

// onStarUp 升星：按 partner_star 的「当前星级 → 下一星」消耗金币。
//
// 已上阵的伙伴升星要同步改加成：先记旧加成，扣费置新星后
// 加「新星加成 - 旧星加成」的差值——净变化只发生一次，
// 比「先减后加」少一次中间态。
func onStarUp(c *module.Ctx[Data], req *gamev1.PartnerStarUpReq) error {
	d, err := c.Repo.Get()
	if err != nil {
		return c.Fail(&gamev1.PartnerStarUpResp{Code: int32(errcode.ErrSystem)}, err)
	}
	star := d.Stars[req.PartnerId]
	if star <= 0 {
		return c.Reply(&gamev1.PartnerStarUpResp{Code: int32(errcode.ErrPartnerNotOwned)})
	}
	cost, ok := starCost(req.PartnerId, star)
	if !ok {
		// 配置里没有「当前星 → 下一星」行 = 已满星
		return c.Reply(&gamev1.PartnerStarUpResp{Code: int32(errcode.ErrPartnerMaxStar)})
	}
	// 扣费放在读旧加成之前：Spend 失败时不改状态，后面的逻辑就不会跑到。
	if err := module.Spend(c.State, itemCoin, cost); err != nil {
		return c.Reply(&gamev1.PartnerStarUpResp{Code: int32(errcode.ErrPartnerNoCoin)})
	}
	wasOnField := contains(d.Field, req.PartnerId)
	oldBonus := atkBonus(req.PartnerId, star)
	d.Stars[req.PartnerId] = star + 1
	if err := c.Repo.Save(d); err != nil {
		return c.Fail(&gamev1.PartnerStarUpResp{Code: int32(errcode.ErrSystem)}, err)
	}
	resp := &gamev1.PartnerStarUpResp{Star: star + 1}
	// 已上阵时属性要跟着变：只加「新星 - 旧星」的差值，净变化发生一次。
	if wasOnField {
		c.State.AddBonus("atk", atkBonus(req.PartnerId, resp.Star)-oldBonus)
	}
	return c.Reply(resp)
}

// onField 上阵/下阵；幂等，重复请求不重复加/减属性。
//
// Go 知识点：switch 不带表达式（switch true 的省略写法）+ case 布尔条件，
// 把「请求态 × 当前态」的四种组合平铺成四个分支，比 if-else 嵌套
// 更能表达「状态机」语义。两个「已是目标态」的分支直接回包——幂等成功
// 但不改状态、不 Save。
func onField(c *module.Ctx[Data], req *gamev1.PartnerFieldReq) error {
	d, err := c.Repo.Get()
	if err != nil {
		return c.Fail(&gamev1.PartnerFieldResp{Code: int32(errcode.ErrSystem)}, err)
	}
	star := d.Stars[req.PartnerId]
	if star <= 0 {
		return c.Reply(&gamev1.PartnerFieldResp{Code: int32(errcode.ErrPartnerNotOwned)})
	}
	on := contains(d.Field, req.PartnerId)
	switch {
	case req.On && on:
		// 已上阵：幂等成功，不重复加加成，也不产生提交。
		return c.Reply(&gamev1.PartnerFieldResp{AtkBonus: totalBonus(d)})
	case req.On && !on:
		if len(d.Field) >= fieldLimit {
			return c.Reply(&gamev1.PartnerFieldResp{Code: int32(errcode.ErrPartnerFieldFull)})
		}
		d.Field = append(d.Field, req.PartnerId)
		c.State.AddBonus("atk", atkBonus(req.PartnerId, star))
	case !req.On && on:
		d.Field = remove(d.Field, req.PartnerId)
		c.State.AddBonus("atk", -atkBonus(req.PartnerId, star))
	case !req.On && !on:
		// 已下阵：幂等成功，无改动。
		return c.Reply(&gamev1.PartnerFieldResp{AtkBonus: totalBonus(d)})
	}
	if err := c.Repo.Save(d); err != nil {
		return c.Fail(&gamev1.PartnerFieldResp{Code: int32(errcode.ErrSystem)}, err)
	}
	return c.Reply(&gamev1.PartnerFieldResp{AtkBonus: totalBonus(d)})
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
