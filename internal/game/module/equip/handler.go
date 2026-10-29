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
	"game_study/internal/common/errcode"
	"game_study/internal/game/module"
	"game_study/pkg/gamedata"
	gamev1 "game_study/pkg/pb/game/v1"
)

func onLogin(c *module.Ctx[Data]) error {
	// 只读遍历：这里只往 State 加属性加成，不修改模块状态（View 不会被写回）。
	return c.Repo.View(func(d *Data) error {
		// 不清空加成：玩家层已在 LoginAll 前统一清空，这里只叠加自己的部分。
		for _, e := range d.Equips {
			if !e.Equipped {
				continue
			}
			if b := equipBonus(e.EquipID, e.EnhanceLevel); b != 0 {
				c.State.AddBonus("atk", b)
			}
		}
		return nil
	})
}

// onList 装备列表：按快照顺序原样下发（顺序即持有顺序，不做排序）。

func onList(c *module.Ctx[Data]) error {
	resp := &gamev1.EquipListResp{}
	err := c.Repo.View(func(d *Data) error {
		for _, e := range d.Equips {
			resp.Equips = append(resp.Equips, &gamev1.EquipEntry{
				Uid:          e.UID,
				EquipId:      e.EquipID,
				EnhanceLevel: e.EnhanceLevel,
				Quality:      e.Quality,
				Equipped:     e.Equipped,
			})
		}
		return nil
	})
	if err != nil {
		return c.Fail(&gamev1.EquipListResp{Code: int32(errcode.ErrSystem)}, err)
	}
	return c.Reply(resp)
}

// onWear 穿戴：同部位冲突检查、等级校验，状态与加成同一次快照提交。
// onWear 穿戴：同部位冲突检查、等级校验，状态与加成同一次快照提交。

func onWear(c *module.Ctx[Data], req *gamev1.EquipWearReq) error {
	resp := &gamev1.EquipWearResp{}
	// bonus 只在真正穿上时非零；已穿戴的幂等路径不该重复叠加。
	var bonus int64

	err := c.Repo.Mutate(func(d *Data) (bool, error) {
		idx := findEquip(d, req.Uid)
		if idx < 0 {
			resp.Code = int32(errcode.ErrEquipNotExist)
			return false, nil
		}
		e := d.Equips[idx]
		// 已穿戴再穿：幂等成功，直接回当前属性，不重复叠加 atk 加成。
		if e.Equipped {
			return false, nil
		}
		cfg, ok := gamedata.GetEquipCfg(int64(e.EquipID))
		if !ok {
			resp.Code = int32(errcode.ErrEquipNotExist)
			return false, nil
		}
		if c.State.Profile().Level < int32(cfg.Col2) {
			resp.Code = int32(errcode.ErrEquipLevelLow)
			return false, nil
		}
		// 同部位冲突检查（Col3=部位）：反查同部位配置 ID 集合，一次遍历。
		sameSlot := sameSlotEquipIDs(cfg.Col3)
		for _, other := range d.Equips {
			if !other.Equipped || other.UID == e.UID {
				continue
			}
			for _, id := range sameSlot {
				if other.EquipID == id {
					resp.Code = int32(errcode.ErrEquipSlotConflict)
					return false, nil
				}
			}
		}

		e.Equipped = true
		d.Equips[idx] = e
		bonus = equipBonus(e.EquipID, e.EnhanceLevel)
		return true, nil
	})
	if err != nil {
		return c.Fail(&gamev1.EquipWearResp{Code: int32(errcode.ErrSystem)}, err)
	}
	// 加成只改内存，与 Equipped 在同一条消息的快照 CAS 里一起生效：
	// CAS 失败则属性与状态一起回滚，不会「状态说穿了、属性没加」。
	// 放在 Mutate 之后：写回失败时不该已经把加成算进面板。
	if bonus != 0 {
		c.State.AddBonus("atk", bonus)
	}
	resp.Attrs = attrPairs(c.State)
	return c.Reply(resp)
}

// onTakeOff 卸下：装备仍在列表里，只置 equipped=false 并扣回加成。
//
// 卸下不删除实例——「拥有」与「穿戴」是两个事实，分别由列表存在性
// 与 Equipped 字段表达。

func onTakeOff(c *module.Ctx[Data], req *gamev1.EquipTakeOffReq) error {
	resp := &gamev1.EquipTakeOffResp{}
	// 取下时扣回的加成（与穿戴时加的精确对冲）。
	var bonus int64

	err := c.Repo.Mutate(func(d *Data) (bool, error) {
		idx := findEquip(d, req.Uid)
		if idx < 0 {
			resp.Code = int32(errcode.ErrEquipNotExist)
			return false, nil
		}
		e := d.Equips[idx]
		if !e.Equipped {
			resp.Code = int32(errcode.ErrEquipNotEquipped)
			return false, nil
		}
		e.Equipped = false
		d.Equips[idx] = e
		bonus = equipBonus(e.EquipID, e.EnhanceLevel)
		return true, nil
	})
	if err != nil {
		return c.Fail(&gamev1.EquipTakeOffResp{Code: int32(errcode.ErrSystem)}, err)
	}
	// 取负扣回：与穿戴时加的 equipBonus 精确对冲，登录重算前不会残留。
	// 与穿戴同理放在 Mutate 之后：写回失败时不该已经改面板。
	if bonus != 0 {
		c.State.AddBonus("atk", -bonus)
	}
	resp.Attrs = attrPairs(c.State)
	return c.Reply(resp)
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

func attrPairs(st module.PlayerState) []*gamev1.AttrPair {
	return []*gamev1.AttrPair{
		{AttrId: 1, Value: st.GetAttr("hp")},
		{AttrId: 2, Value: st.GetAttr("mp")},
		{AttrId: 3, Value: st.GetAttr("atk")},
		{AttrId: 4, Value: st.GetAttr("def")},
		{AttrId: 5, Value: st.GetAttr("spd")},
	}
}

// equipBonus 一件装备提供的攻击加成 = 配置基础值 + 强化加成。
//
// 四处必须用同一个函数（登录重算 / 穿戴 / 卸下 / 强化的增量）：
// 只要有一处算法不同，就会出现「穿上加了基础值、卸下扣了基础+强化」这类
// 不对称，属性越用越偏，而且只在反复穿脱后才看得出来。

func equipBonus(equipID, enhanceLevel int32) int64 {
	cfg, ok := gamedata.GetEquipCfg(int64(equipID))
	if !ok {
		return 0
	}
	total := cfg.Col4
	if enhanceLevel > 0 {
		if ec, ok := gamedata.GetEquipEnhanceCfg(int64(enhanceLevel)); ok {
			total += ec.Col3
		}
	}
	return total
}

// onEnhance 强化一件装备：校验 → 扣材料 → 等级 +1 → 已穿戴则补加成。
//
// 顺序即正确性：先校验（存在 / 未满级）→ 再扣费 → 最后改状态。反过来做
// 就会出现「扣了材料但等级没涨」或「等级涨了但没扣材料」，两种都要客服补偿。
//
// 上限用「配置缺失」判定而不是硬编码常量：运营往 equip_enhance.xml 加一行
// 就能放开一级，不需要改代码发版。

func onEnhance(c *module.Ctx[Data], req *gamev1.EquipEnhanceReq) error {
	resp := &gamev1.EquipEnhanceResp{}
	var (
		next   int32 // 强化后的等级（回包用）
		bonusD int64 // 已穿戴装备的加成增量
	)

	err := c.Repo.Mutate(func(d *Data) (bool, error) {
		idx := findEquip(d, req.Uid)
		if idx < 0 {
			resp.Code = int32(errcode.ErrEquipNotExist)
			return false, nil
		}
		e := d.Equips[idx]
		next = e.EnhanceLevel + 1
		cfg, ok := gamedata.GetEquipEnhanceCfg(int64(next))
		if !ok {
			resp.Code = int32(errcode.ErrEquipMaxEnhance)
			return false, nil
		}
		if err := module.Spend(c.State, itemDiamond, cfg.Col2); err != nil {
			resp.Code = int32(errcode.ErrEquipNoMaterial)
			return false, nil
		}

		before := equipBonus(e.EquipID, e.EnhanceLevel)
		e.EnhanceLevel = next
		d.Equips[idx] = e
		// 只有已穿戴的装备影响面板；未穿戴的强化在穿上的那一刻按 equipBonus
		// 自然生效，不需要在这里补。
		if e.Equipped {
			bonusD = equipBonus(e.EquipID, next) - before
		}
		return true, nil
	})
	if err != nil {
		return c.Fail(&gamev1.EquipEnhanceResp{Code: int32(errcode.ErrSystem)}, err)
	}
	if bonusD != 0 {
		c.State.AddBonus("atk", bonusD)
	}
	resp.EnhanceLevel = next
	resp.Attrs = attrPairs(c.State)
	return c.Reply(resp)
}
