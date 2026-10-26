// internal/game/module/copy/copy.go —— 副本模块（6301-6304）
//
// 副本进度（星级 / 通关次数 / 首通）放在玩家快照 Ext["copy"]：结算、首通奖励
// 与经验在同一次 Redis CAS 提交，失败整批回滚（契约见 docs/R2.1-缓存原子变更契约.md）。
//
// 架构职责：副本进度记录 + 结算发奖。关卡列表由配置派生（CopyCfgList），
// 模块只保存「每个副本的玩家进度」，不为没打过的副本建行。
// 事件订阅：产出方。结算成功 Emit "copy_pass"（Key=副本 ID），驱动任务/活动。
// 配置表：copy.xml（Col1=名称，Col2=等级门槛）、copy_drop.xml（首通掉落）。
// 懒初始化：进度按需写入——首次通关某副本才在 Records map 里出现该键，
// 列表请求只是「配置 × 已有记录」的即时 join。
//
// 写法（v3.6 迁移）：模块声明一行、路由若干行、业务只碰 Ctx.Repo；
// 协议号由消息类型反查，错误码来自统一枚举。
package copy

import (
	"fmt"

	"game_study/internal/common/errcode"
	"game_study/internal/game/module"
	"game_study/pkg/gamedata"

	gamev1 "game_study/pkg/pb/game/v1"
	"google.golang.org/protobuf/proto"
)

// stateKey 同时是 Ext 槽位键、模块名与落库归属标签。
const stateKey = "copy"

// Record 单个副本的通关记录。
//
// 字段语义：
//   - Star：历史最佳星级（0-3），只升不降；
//   - PassCount：累计通关次数；
//   - FirstPass：首通奖励是否已发放——一旦置位永不清零，是首通幂等的唯一依据。
//
// Go 知识点：map[int32]Record 是「值类型 map」——读出来的是副本，
// 改字段后必须整值写回（d.Records[id] = rec），就地改 rec.Star 不生效。
type Record struct {
	Star      int32 `json:"star"`
	PassCount int32 `json:"pass_count"`
	FirstPass bool  `json:"first_pass"`
}

// Data 是副本模块的私有状态：copy_id -> 通关记录。
//
// 为什么用 map 而不是每个副本一行表：副本数量随配置增长、行数不可控，
// 且进度是玩家私有的低频读数据，整体一个 JSON 列足够。
type Data struct {
	Records map[int32]Record `json:"records"`
}

// M 副本模块的唯一实例。
//
// 登录不做事：进度在 Ext 里，列表/结算时按需 decode——数据量小，
// 懒加载比登录时预解码更划算。
var M = module.Define[Data](stateKey, newData)

func init() {
	M.HandleRoute(&gamev1.CopyListReq{}, onList)
	module.Route(M, &gamev1.CopySettleReq{}, onSettle)
}

// onList 副本列表（等级不足仍下发，结算时强校验）。
//
// 列表 = 配置全集 LEFT JOIN 玩家进度：没打过的副本也在列表里
// （BestStar=0/PassCount=0），这样客户端不需要再拉配置表。
func onList(c *module.Ctx[Data]) error {
	d, err := c.Repo.Get()
	if err != nil {
		return c.Fail(&gamev1.CopyListResp{Code: int32(errcode.ErrSystem)}, err)
	}
	resp := &gamev1.CopyListResp{}
	for _, cfg := range gamedata.CopyCfgList() {
		entry := &gamev1.CopyEntry{
			CopyId:   int32(cfg.Id),
			Name:     cfg.Col1,
			MinLevel: int32(cfg.Col2),
		}
		if rec, ok := d.Records[int32(cfg.Id)]; ok {
			entry.BestStar = rec.Star
			entry.PassCount = rec.PassCount
		}
		resp.Copies = append(resp.Copies, entry)
	}
	return c.Reply(resp)
}

// onSettle 通关结算：星级取历史最佳，首通发放掉落并写经验。
// 结算事实、首通奖励与经验都在同一条消息内，随同一份快照提交。
func onSettle(c *module.Ctx[Data], req *gamev1.CopySettleReq) error {
	cfg, ok := gamedata.GetCopyCfg(int64(req.CopyId))
	if !ok {
		return c.Reply(&gamev1.CopySettleResp{Code: int32(errcode.ErrCopyNotFound)})
	}
	if c.State.Profile().Level < int32(cfg.Col2) {
		return c.Reply(&gamev1.CopySettleResp{Code: int32(errcode.ErrCopyLevelNotEnough)})
	}
	star := req.Star
	// 客户端上报的星级裁剪到 1-3：0/负数按 1 星，超高按 3 星，防止脏数据落库。
	if star < 1 {
		star = 1
	}
	if star > 3 {
		star = 3
	}

	d, err := c.Repo.Get()
	if err != nil {
		return c.Fail(&gamev1.CopySettleResp{Code: int32(errcode.ErrSystem)}, err)
	}

	// rec 是值副本；FirstPass 先取反再置位——本次是否首通必须在改写之前判定。
	rec := d.Records[req.CopyId]
	firstPass := !rec.FirstPass
	if rec.Star < star {
		rec.Star = star // 历史最佳只升不降
	}
	rec.PassCount++
	rec.FirstPass = true

	// 首通发奖：按 copy_drop.xml 中 col1 = copy_id 的行发道具。
	// 满包/超限：整个结算拒绝（进度也不前进），玩家清包后重新结算。
	var changes []*gamev1.ItemChange
	if firstPass {
		grants := make([]module.ItemGrant, 0, 4)
		for _, drop := range gamedata.CopyDropCfgList() {
			if drop.Col1 != cfg.Id {
				continue
			}
			grants = append(grants, module.ItemGrant{ItemID: int32(drop.Col2), Count: drop.Col3})
		}
		bagChanges, grantErr := module.Grant(c.State, grants)
		if grantErr != nil {
			return c.Reply(&gamev1.CopySettleResp{Code: int32(errcode.ErrCopyBagFull)})
		}
		changes = module.ToItemChanges(bagChanges)
	}
	d.Records[req.CopyId] = rec
	if err := c.Repo.Save(d); err != nil {
		return c.Fail(&gamev1.CopySettleResp{Code: int32(errcode.ErrSystem)}, err)
	}

	// 通关经验：等级门槛 * 50（门槛缺失时给保底 50）。
	expGain := cfg.Col2 * 50
	if cfg.Col2 <= 0 {
		expGain = 50
	}
	resp := &gamev1.CopySettleResp{FirstPass: firstPass, ExpGain: expGain}
	if len(changes) > 0 {
		if raw, err := proto.Marshal(&gamev1.BagChangeNotify{Changes: changes}); err == nil {
			resp.Rewards = raw
		}
	}
	// AddExp 是可选注入：nil 时跳过（测试/无等级系统环境）。
	if c.AddExp != nil {
		c.AddExp(expGain)
	}
	// 事件：驱动任务/活动（copy_pass）
	if c.Emit != nil {
		c.Emit(module.Event{Type: "copy_pass", Key: fmt.Sprintf("%d", cfg.Id), Delta: 1})
	}
	return c.Reply(resp)
}
