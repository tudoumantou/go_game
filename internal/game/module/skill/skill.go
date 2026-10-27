// internal/game/module/skill/skill.go —— 技能模块（3201-3206）
//
// 已学技能与等级放在玩家快照 Ext["skill"]：学习/升级与扣材料在同一次
// Redis CAS 提交，失败整批回滚。
//
// 写法（v3.6 迁移）：模块声明一行、路由若干行、业务只碰 Ctx.Repo；
// 协议号由消息类型反查，错误码来自统一枚举。
package skill

import (
	"game_study/internal/common/errcode"
	"game_study/internal/game/module"
	"game_study/pkg/gamedata"

	gamev1 "game_study/pkg/pb/game/v1"
)

// stateKey 同时是 Ext 槽位键、模块名与落库归属标签。
const stateKey = "skill"

// 升级消耗道具（钻石）。
//
// 与 quest/shop/activity 共用同一个道具 ID 1001——这是全服统一的
// 「钻石」约定，各模块分别声明常量而不是抽公共包，避免模块间耦合。
const upgradeItemID int32 = 1001

// Data 是技能模块的私有状态：skill_id -> 等级。
//
// map 的值即等级，隐去「未学习」态：键不存在就是没学过，
// 与任务模块同一设计（减少状态数量）。
type Data struct {
	Levels map[int32]int32 `json:"levels"`
}

// M 技能模块的唯一实例。
var M = module.Define[Data](stateKey, newData).WithLogout(onLogout)

func init() {
	M.HandleRoute(&gamev1.SkillListReq{}, onList)
	module.Route(M, &gamev1.SkillLearnReq{}, onLearn)
	module.Route(M, &gamev1.SkillUpgradeReq{}, onUpgrade)
}

// onLogout 下线时标脏，确保最后一次学习的技能随快照落库。
func onLogout(c *module.Ctx[Data]) error {
	c.State.MarkDirty(stateKey)
	return nil
}

// onList 已学技能列表：按 skill_id 升序返回（技能数量很小，用手写排序避免引入 sort 依赖），
// 配置缺失时名字留空——不让单个技能配置把整个列表请求拖失败。
//
// Go 知识点：map 的 range 顺序随机，所以必须先把 key 收集成切片再排序；
// 这里用手写冒泡而非 sort.Ints——N 通常 <20，省一个 import，
// 也顺带在注释里写明了「为什么不用标准库」。
func onList(c *module.Ctx[Data]) error {
	d, err := c.Repo.Get()
	if err != nil {
		return c.Fail(&gamev1.SkillListResp{Code: int32(errcode.ErrSystem)}, err)
	}
	resp := &gamev1.SkillListResp{}
	ids := make([]int32, 0, len(d.Levels))
	for id := range d.Levels {
		ids = append(ids, id)
	}
	// 稳定顺序，客户端列表不抖动（N 通常 <20，冒泡足够）。
	for i := 0; i < len(ids); i++ {
		for j := i + 1; j < len(ids); j++ {
			if ids[j] < ids[i] {
				ids[i], ids[j] = ids[j], ids[i]
			}
		}
	}
	for _, id := range ids {
		name := ""
		if cfg, ok := gamedata.GetSkillCfg(int64(id)); ok {
			name = cfg.Col1
		}
		resp.Skills = append(resp.Skills, &gamev1.SkillEntry{
			SkillId: id,
			Name:    name,
			Level:   d.Levels[id],
		})
	}
	return c.Reply(resp)
}

// onLearn 学习技能：Col2 是学习等级门槛，未达到直接拒绝。
//
// 「已学」用键存在判定：学习即置 1 级，与升级路径共用同一张表。
func onLearn(c *module.Ctx[Data], req *gamev1.SkillLearnReq) error {
	cfg, ok := gamedata.GetSkillCfg(int64(req.SkillId))
	if !ok {
		return c.FailCode(&gamev1.SkillLearnResp{}, errcode.ErrSkillNotExist)
	}
	// 等级门槛是只读判断，放在读状态之前，省一次解码。
	if c.State.Profile().Level < int32(cfg.Col2) {
		return c.FailCode(&gamev1.SkillLearnResp{}, errcode.ErrSkillLevelLow)
	}
	d, err := c.Repo.Get()
	if err != nil {
		return c.Fail(&gamev1.SkillLearnResp{Code: int32(errcode.ErrSystem)}, err)
	}
	if _, learned := d.Levels[req.SkillId]; learned {
		return c.FailCode(&gamev1.SkillLearnResp{}, errcode.ErrSkillLearned)
	}
	d.Levels[req.SkillId] = 1
	if err := c.Repo.Save(d); err != nil {
		return c.Fail(&gamev1.SkillLearnResp{Code: int32(errcode.ErrSystem)}, err)
	}
	return c.Reply(&gamev1.SkillLearnResp{})
}

// onUpgrade 升级：扣材料与等级变更在同一次快照提交。
//
// 消耗 = Col3 × 当前等级（线性增长，Col3=0 表示免费）；
// 扣费（RemoveItem）与升级（Levels+1）都在内存里，CAS 失败一起回滚，
// 不会「扣了钻石没升级」。
func onUpgrade(c *module.Ctx[Data], req *gamev1.SkillUpgradeReq) error {
	if _, ok := gamedata.GetSkillCfg(int64(req.SkillId)); !ok {
		return c.FailCode(&gamev1.SkillUpgradeResp{}, errcode.ErrSkillNotExist)
	}
	d, err := c.Repo.Get()
	if err != nil {
		return c.Fail(&gamev1.SkillUpgradeResp{Code: int32(errcode.ErrSystem)}, err)
	}
	level, learned := d.Levels[req.SkillId]
	if !learned {
		return c.FailCode(&gamev1.SkillUpgradeResp{}, errcode.ErrSkillNotLearned)
	}
	// 消耗读 skill_upgrade 表（Col1=当前等级，Col2=升到下一级的消耗）。
	// 查不到该等级的行即已满级——旧实现是 skill.Col3 * level，没有上限，
	// 技能可以无限升级；运营也没有任何可调的地方（那张表纯摆设）。
	cost, up := skillUpgradeCost(level)
	if !up {
		return c.FailCode(&gamev1.SkillUpgradeResp{}, errcode.ErrSkillMaxLevel)
	}
	if err := module.Spend(c.State, upgradeItemID, cost); err != nil {
		return c.FailCode(&gamev1.SkillUpgradeResp{}, errcode.ErrSkillNoResource)
	}
	next := level + 1
	d.Levels[req.SkillId] = next
	if err := c.Repo.Save(d); err != nil {
		return c.Fail(&gamev1.SkillUpgradeResp{Code: int32(errcode.ErrSystem)}, err)
	}
	return c.Reply(&gamev1.SkillUpgradeResp{Level: next})
}

// skillUpgradeCost 返回「从 level 升到 level+1」的消耗；没有该行表示已满级。
//
// 表结构：Col1 = 当前等级，Col2 = 消耗。用「当前等级」而不是「目标等级」，
// 是因为满级判定需要「查不到」这个信号——最高等级没有对应的行。
func skillUpgradeCost(level int32) (int64, bool) {
	for _, row := range gamedata.SkillUpgradeCfgList() {
		if int32(row.Col1) == level {
			return row.Col2, true
		}
	}
	return 0, false
}
