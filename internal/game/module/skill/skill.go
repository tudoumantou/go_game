// internal/game/module/skill/skill.go —— 技能模块（3201-3206）
//
// 技能等级放在玩家快照 Ext["skill"]：学习/升级只改内存，与消耗的材料一起提交，
// Redis CAS 失败时整批回滚（契约见 docs/R2.1-缓存原子变更契约.md）。
//
// 架构职责：已学技能及其等级的记录。技能定义（名字/门槛/消耗）全部来自
// skill.xml，模块不存任何配置副本。
// 事件订阅：无（技能不监听行为事件；P7 可加「释放技能」计数任务）。
// 配置表：skill.xml（Col1=名称，Col2=学习等级门槛，Col3=每级消耗钻石）。
// 懒初始化：从未学习返回空 Levels map，首次学习才建键；
// 列表是「已学技能」而非配置全集——与任务/副本不同，这里刻意
// 只展示玩家拥有的，避免几十个未解锁技能刷屏。
package skill

import (
	"game_study/internal/game/module"
	"game_study/pkg/codec"
	"game_study/pkg/gamedata"
	"game_study/pkg/log"

	gamev1 "game_study/pkg/pb/game/v1"
	"google.golang.org/protobuf/proto"
)

// stateKey 同时是 Ext 槽位键、模块名与落库归属标签。
const stateKey = "skill"

// 协议号（4.9：3201/3202 列表，3203/3204 学习，3205/3206 升级）。
const (
	msgIDListReq     = codec.MsgSkillListReq
	msgIDListResp    = codec.MsgSkillListResp
	msgIDLearnReq    = codec.MsgSkillLearnReq
	msgIDLearnResp   = codec.MsgSkillLearnResp
	msgIDUpgradeReq  = codec.MsgSkillUpgradeReq
	msgIDUpgradeResp = codec.MsgSkillUpgradeResp
)

// 业务码（4.10）：技能段 3201-3205。
const (
	codeOK         int32 = 0
	codeNotExist   int32 = 3201
	codeLevelLow   int32 = 3202
	codeLearned    int32 = 3203
	codeNotLearned int32 = 3204
	codeNoResource int32 = 3205
	codeMaxLevel   int32 = 3206 // 已达最高等级
)

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

// Module 技能模块。
type Module struct {
	// Stateful 内嵌 Thin：生命周期与消息注册仍由 Thin 提供。
	*module.Stateful[Data]
	logger *log.Logger
}

// New 创建技能模块。
func New(logger *log.Logger) *Module {
	m := &Module{logger: logger}
	m.Stateful = module.NewStateful(stateKey, newData,
		module.ThinLogger(logger),
		module.ThinLogout(m.onLogout),
	)
	m.HandleR(msgIDListReq, msgIDListResp, &gamev1.SkillListResp{}, m.onList)
	m.HandleR(msgIDLearnReq, msgIDLearnResp, &gamev1.SkillLearnResp{}, module.Typed(msgIDLearnResp, m.onLearn))
	m.HandleR(msgIDUpgradeReq, msgIDUpgradeResp, &gamev1.SkillUpgradeResp{}, module.Typed(msgIDUpgradeResp, m.onUpgrade))
	return m
}

// onLogout 下线时标脏，确保最后一次学习的技能随快照落库。
func (m *Module) onLogout(ctx *module.Context) error {
	ctx.State.MarkDirty(m.Name())
	return nil
}

// onList 已学技能列表：按 skill_id 升序返回（技能数量很小，用手写排序避免引入 sort 依赖），
// 配置缺失时名字留空——不让单个技能配置把整个列表请求拖失败。
//
// Go 知识点：map 的 range 顺序随机，所以必须先把 key 收集成切片再排序；
// 这里用手写冒泡而非 sort.Ints——N 通常 <20，省一个 import，
// 也顺带在注释里写明了「为什么不用标准库」。
func (m *Module) onList(ctx *module.Context, _ uint32, _ []byte) error {
	resp := &gamev1.SkillListResp{Code: codeOK}
	if err := m.Read(ctx, func(d *Data) {
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
	}); err != nil {
		return err
	}
	return reply(ctx, msgIDListResp, resp)
}

// onLearn 学习技能：Col2 是学习等级门槛，未达到直接拒绝。
//
// 「已学」用键存在判定：学习即置 1 级，与升级路径共用同一张表。
func (m *Module) onLearn(ctx *module.Context, req *gamev1.SkillLearnReq, resp *gamev1.SkillLearnResp) error {
	*resp = gamev1.SkillLearnResp{}
	cfg, ok := gamedata.GetSkillCfg(int64(req.SkillId))
	if !ok {
		resp.Code = codeNotExist
		return nil
	}
	// 等级门槛是只读判断，放在闭包外，避免无改动也走一次解码。
	if ctx.State.Profile().Level < int32(cfg.Col2) {
		resp.Code = codeLevelLow
		return nil
	}
	return m.Mutate(ctx, func(d *Data) bool {
		if _, learned := d.Levels[req.SkillId]; learned {
			resp.Code = codeLearned
			return false
		}
		d.Levels[req.SkillId] = 1
		resp.Code = codeOK
		return true
	})
}

// onUpgrade 升级：扣材料与等级变更在同一次快照提交。
//
// 消耗 = Col3 × 当前等级（线性增长，Col3=0 表示免费）；
// 扣费（RemoveItem）与升级（Levels+1）都在内存里，CAS 失败一起回滚，
// 不会「扣了钻石没升级」。
func (m *Module) onUpgrade(ctx *module.Context, req *gamev1.SkillUpgradeReq, resp *gamev1.SkillUpgradeResp) error {
	*resp = gamev1.SkillUpgradeResp{}
	if _, ok := gamedata.GetSkillCfg(int64(req.SkillId)); !ok {
		resp.Code = codeNotExist
		return nil
	}
	return m.Mutate(ctx, func(d *Data) bool {
		level, learned := d.Levels[req.SkillId]
		if !learned {
			resp.Code = codeNotLearned
			return false
		}
		// 消耗读 skill_upgrade 表（Col1=当前等级，Col2=升到下一级的消耗）。
		// 查不到该等级的行即已满级——旧实现是 skill.Col3 * level，没有上限，
		// 技能可以无限升级；运营也没有任何可调的地方（那张表纯摆设）。
		cost, up := skillUpgradeCost(level)
		if !up {
			resp.Code = codeMaxLevel
			return false
		}
		if err := module.Spend(ctx.State, upgradeItemID, cost); err != nil {
			resp.Code = codeNoResource
			return false
		}
		next := level + 1
		d.Levels[req.SkillId] = next
		resp.Code = codeOK
		resp.Level = next
		return true
	})
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

// reply 序列化 proto 并经 ctx.Reply 下发。
func reply(ctx *module.Context, msgID uint32, msg proto.Message) error {
	body, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	return ctx.Reply(msgID, body)
}
