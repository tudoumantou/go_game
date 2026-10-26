// internal/game/module/quest/quest.go —— 任务模块（7101-7106）
//
// 状态（进度 + 领取事实）整体放在玩家快照的 Ext["quest"] 里：领奖时改内存、
// 与道具经验一起提交，Redis CAS 失败则整批回滚（契约见 docs/R2.1-缓存原子变更契约.md）。
// 不再直接写 MySQL 业务事实。
//
// 架构职责：任务进度推进 + 领奖。任务列表由 quest.xml 配置派生，
// 模块只存「接取/进度/领取」三态，不存任务定义。
// 事件订阅：消费方（EventAware）。监听 kill/use_item/copy_pass 等事件，
// 按配置 Col2 匹配后推进进度。
// 配置表：quest.xml（Col1=名称，Col2=监听事件类型，Col3=目标次数）。
// 懒初始化：未接取的任务在 Records map 里不存在，接取时才建键；
// 列表请求把「配置全集」与「已有记录」join 后下发。
package quest

import (
	"game_study/internal/common/errcode"
	"game_study/internal/game/module"
	"game_study/pkg/gamedata"

	gamev1 "game_study/pkg/pb/game/v1"
	"google.golang.org/protobuf/proto"
)

// stateKey 同时是 Ext 槽位键、模块名与落库归属标签。
const stateKey = "quest"

// 任务状态（与快照内 JSON 一致）。
//
// Go 知识点：这是一组「状态机常量」——显式赋值而非 iota，
// 因为取值同时是线上 JSON 契约（旧快照里躺着的数字），
// 一旦改成 iota 自动编号，历史数据全部错位。
const (
	statusDoing   int32 = 0 // 进行中
	statusDone    int32 = 1 // 已完成（可领奖）
	statusClaimed int32 = 2 // 已领奖
)

// Record 单个任务的状态。
//
// 字段语义（与快照内 JSON 一致）：
//   - Status：0=进行中 / 1=已完成待领 / 2=已领奖，见下方常量；
//   - Progress：当前进度，达到配置 Col3 即完成；超额在 OnEvent 里截断。
type Record struct {
	Status   int32 `json:"status"`
	Progress int32 `json:"progress"`
}

// Data 是任务模块的私有状态：quest_id -> 进度与领取事实。
//
// 「没接取」用「map 里没有这个键」表达，而不是 Status=-1——
// 少一种状态，领取事实的判定（claimed）就不会有第三态。
type Data struct {
	Quests map[int32]Record `json:"quests"`
}

// newData 新玩家的空状态：map 必须初始化，nil map 写会 panic。
func newData() *Data { return &Data{Quests: map[int32]Record{}} }

// M 任务模块的唯一实例。
var M = module.Define[Data](stateKey, newData).
	WithLogout(onLogout).
	WithEvent(onEvent)

func init() {
	M.HandleRoute(&gamev1.QuestListReq{}, onList)
	module.Route(M, &gamev1.QuestAcceptReq{}, onAccept)
	module.Route(M, &gamev1.QuestClaimReq{}, onClaim)
}

// onLogout 下线时标脏，保证最后一次进度推进随快照落库。
//
// 不做预加载：任务数据随 Ext 懒加载，列表请求时才解码 JSON。
func onLogout(c *module.Ctx[Data]) error {
	c.State.MarkDirty(stateKey)
	return nil
}

// onEvent 推进匹配类型的任务进度（经 ThinOnEvent 挂接）。
//
// 一次事件只解析一次状态、只写回一次；进度与状态都在内存里改，
// 快照提交失败时随整条消息回滚。
//
// Go 知识点：EventAware 是可选接口，Registry 用类型断言探测
// （见 module.go DispatchEvent）——本模块实现它即「订阅全部事件」，
// 事件过滤在函数体内按 cfg.Col2 做。
func onEvent(c *module.Ctx[Data], evt module.Event) {
	// 一次事件只解析一次状态、只写回一次；没推进任何任务就不写回，
	// 也就不产生一次无意义的提交。
	d, err := c.Repo.Get()
	if err != nil {
		c.Log().Warn("quest state load failed", "player_id", c.State.PlayerID(), "err", err)
		return
	}
	changed := false
	// 配置约定：Col1=任务名，Col2=监听的事件类型，Col3=目标次数。
	for _, cfg := range gamedata.QuestCfgList() {
		if cfg.Col2 != evt.Type {
			continue
		}
		id := int32(cfg.Id)
		rec, ok := d.Quests[id]
		if !ok || rec.Status != statusDoing {
			continue // 未接取或已完成的任务不推进
		}
		target := int32(cfg.Col3)
		next := rec.Progress + int32(evt.Delta)
		// 到达目标即置完成态并截断：超额计数没有意义，
		// 截断还能防止 int32 被恶意大 Delta 冲爆。
		if next >= target {
			next = target
			rec.Status = statusDone
		}
		rec.Progress = next
		d.Quests[id] = rec
		changed = true
	}
	if changed {
		if err := c.Repo.Save(d); err != nil {
			// 状态损坏只告警不抛错：OnEvent 没有返回值，
			// 不能让一个坏任务把整条业务消息的响应搞失败。
			c.Log().Warn("quest progress save failed", "player_id", c.State.PlayerID(), "err", err)
		}
	}
}

// onList 任务列表（未接/进行中/可领奖/已领奖四态）。
//
// 注意内部 status（0/1/2）与协议 status（0-3）是两套编码：
// 内部没有「未接」状态（用键不存在表达），协议需要显式 0，
// 所以这里做一次映射，而不是把内部状态直接下发。
func onList(c *module.Ctx[Data]) error {
	d, err := c.Repo.Get()
	if err != nil {
		return c.Fail(&gamev1.QuestListResp{Code: int32(errcode.ErrSystem)}, err)
	}
	resp := &gamev1.QuestListResp{}
	for _, cfg := range gamedata.QuestCfgList() {
		entry := &gamev1.QuestEntry{
			QuestId: int32(cfg.Id),
			Name:    cfg.Col1,
			Target:  int32(cfg.Col3),
			Status:  0, // 未接
		}
		if rec, ok := d.Quests[int32(cfg.Id)]; ok {
			entry.Progress = rec.Progress
			switch rec.Status {
			case statusClaimed:
				entry.Status = 3
			case statusDone:
				entry.Status = 2
			default:
				if rec.Progress >= int32(cfg.Col3) {
					entry.Status = 2
				} else {
					entry.Status = 1
				}
			}
		}
		resp.Quests = append(resp.Quests, entry)
	}
	return c.Reply(resp)
}

// onAccept 接取任务。
//
// 已领奖的任务允许重新接取（日常任务的设计前提）：Status==claimed
// 时覆盖为 doing 并清零进度，实现「每日刷新」而不需要删键。
func onAccept(c *module.Ctx[Data], req *gamev1.QuestAcceptReq) error {
	if _, ok := gamedata.GetQuestCfg(int64(req.QuestId)); !ok {
		return c.Reply(&gamev1.QuestAcceptResp{Code: int32(errcode.ErrQuestNotExist)})
	}
	d, err := c.Repo.Get()
	if err != nil {
		return c.Fail(&gamev1.QuestAcceptResp{Code: int32(errcode.ErrSystem)}, err)
	}
	if rec, ok := d.Quests[req.QuestId]; ok && rec.Status != statusClaimed {
		// 已在进行中：不改状态，也不产生提交。
		return c.Reply(&gamev1.QuestAcceptResp{Code: int32(errcode.ErrQuestAccepted)})
	}
	d.Quests[req.QuestId] = Record{Status: statusDoing}
	if err := c.Repo.Save(d); err != nil {
		return c.Fail(&gamev1.QuestAcceptResp{Code: int32(errcode.ErrSystem)}, err)
	}
	return c.Reply(&gamev1.QuestAcceptResp{})
}

// onClaim 领奖（经验 + 道具）。
//
// 领取事实与奖励在同一次快照提交：已领状态写进 Ext，道具进 Items，经验进 Profile，
// Redis 提交失败时三者一起回滚，不会出现「状态已领、奖励没到账」。
func onClaim(c *module.Ctx[Data], req *gamev1.QuestClaimReq) error {
	cfg, ok := gamedata.GetQuestCfg(int64(req.QuestId))
	if !ok {
		return c.Reply(&gamev1.QuestClaimResp{Code: int32(errcode.ErrQuestNotExist)})
	}
	d, err := c.Repo.Get()
	if err != nil {
		return c.Fail(&gamev1.QuestClaimResp{Code: int32(errcode.ErrSystem)}, err)
	}
	rec, ok := d.Quests[req.QuestId]
	if !ok {
		return c.Reply(&gamev1.QuestClaimResp{Code: int32(errcode.ErrQuestUnfinished)})
	}
	if rec.Status == statusClaimed {
		return c.Reply(&gamev1.QuestClaimResp{Code: int32(errcode.ErrQuestClaimed)})
	}
	if rec.Progress < int32(cfg.Col3) {
		return c.Reply(&gamev1.QuestClaimResp{Code: int32(errcode.ErrQuestUnfinished)})
	}

	// 发奖：经验 + 钻石（配置约定 target*100 经验、target*10 钻石）。
	// 满包/超限时整个领取被拒，不会出现「状态已置领、道具没发」。
	grants := []module.ItemGrant{{ItemID: 1001, Count: int64(cfg.Col3) * 10}}
	bagChanges, grantErr := module.Grant(c.State, grants)
	if grantErr != nil {
		return c.Reply(&gamev1.QuestClaimResp{Code: int32(errcode.ErrQuestBagFull)})
	}
	expGain := int64(cfg.Col3) * 100
	rec.Status = statusClaimed
	d.Quests[req.QuestId] = rec
	if err := c.Repo.Save(d); err != nil {
		return c.Fail(&gamev1.QuestClaimResp{Code: int32(errcode.ErrSystem)}, err)
	}

	resp := &gamev1.QuestClaimResp{ExpGain: expGain}
	if raw, err := proto.Marshal(&gamev1.BagChangeNotify{Changes: module.ToItemChanges(bagChanges)}); err == nil {
		resp.Rewards = raw
	}
	// 发经验（Actor 内部会推送等级变化）；仍在同一条消息内，随同一份快照提交。
	if c.AddExp != nil && expGain > 0 {
		c.AddExp(expGain)
	}
	return c.Reply(resp)
}
