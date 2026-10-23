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

// stateKey 同时是 Ext 槽位键、模块名与落库归属标签。
const stateKey = "quest"

// 协议号（4.9：7101/7102 列表，7103/7104 接取，7105/7106 领奖）。
const (
	msgIDListReq    = codec.MsgQuestListReq
	msgIDListResp   = codec.MsgQuestListResp
	msgIDAcceptReq  = codec.MsgQuestAcceptReq
	msgIDAcceptResp = codec.MsgQuestAcceptResp
	msgIDClaimReq   = codec.MsgQuestClaimReq
	msgIDClaimResp  = codec.MsgQuestClaimResp
)

// 业务码（4.10）：任务段 7101-7105。
const (
	codeOK              int32 = 0
	codeQuestNotExist   int32 = 7101
	codeQuestAccepted   int32 = 7102
	codeQuestUnfinished int32 = 7103
	codeQuestClaimed    int32 = 7104
	codeBagFull         int32 = 7105
)

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

// init 注册持久化：player_quest 主键为 (player_id)，FlatPersister 直接接入。
func init() {
	writebehind.RegisterFlat[model.PlayerQuest](stateKey, model.PlayerQuestFlat, validate)
}

// validate 落库前校验状态取值与进度范围，坏数据整批回滚。
//
// Status 上界是 statusClaimed：超范围说明快照被改坏，
// 宁可这批写回失败，也不把非法状态带进数据库。
func validate(row *model.PlayerQuest) error {
	d, err := decode(row)
	if err != nil {
		return err
	}
	for id, rec := range d.Quests {
		if id <= 0 {
			return fmt.Errorf("invalid quest id %d", id)
		}
		if rec.Status < statusDoing || rec.Status > statusClaimed {
			return fmt.Errorf("invalid quest status %d", rec.Status)
		}
		if rec.Progress < 0 {
			return fmt.Errorf("invalid quest progress %d", rec.Progress)
		}
	}
	return nil
}

// Module 任务模块。
type Module struct {
	*module.Thin
	logger *log.Logger
}

// New 创建任务模块。
func New(logger *log.Logger) *Module {
	m := &Module{logger: logger}
	m.Thin = module.NewThin(stateKey,
		module.ThinLogger(logger),
		module.ThinLogout(m.onLogout),
		module.ThinOnEvent(m.onEvent),
	)
	m.HandleR(msgIDListReq, msgIDListResp, &gamev1.QuestListResp{}, m.onList)
	m.HandleR(msgIDAcceptReq, msgIDAcceptResp, &gamev1.QuestAcceptResp{}, module.Typed(msgIDAcceptResp, m.onAccept))
	m.HandleR(msgIDClaimReq, msgIDClaimResp, &gamev1.QuestClaimResp{}, module.Typed(msgIDClaimResp, m.onClaim))
	return m
}

// onLogout 下线时标脏，保证最后一次进度推进随快照落库。
//
// 不做预加载：任务数据随 Ext 懒加载，列表请求时才解码 JSON。
func (m *Module) onLogout(ctx *module.Context) error {
	ctx.State.MarkDirty(stateKey)
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
func (m *Module) onEvent(ctx *module.Context, evt module.Event) {
	d, err := load(ctx.State)
	if err != nil {
		// 状态损坏只告警不抛错：OnEvent 没有返回值，
		// 不能让一个坏任务把整条业务消息的响应搞失败。
		m.warn("quest state load failed", ctx, err)
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
	if !changed {
		return
	}
	if err := save(ctx.State, ctx.State.PlayerID(), d); err != nil {
		m.warn("quest progress save failed", ctx, err)
	}
}

// onList 任务列表（未接/进行中/可领奖/已领奖四态）。
//
// 注意内部 status（0/1/2）与协议 status（0-3）是两套编码：
// 内部没有「未接」状态（用键不存在表达），协议需要显式 0，
// 所以这里做一次映射，而不是把内部状态直接下发。
func (m *Module) onList(ctx *module.Context, _ uint32, _ []byte) error {
	resp := &gamev1.QuestListResp{Code: codeOK}
	d, err := load(ctx.State)
	if err != nil {
		return err
	}
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
	return reply(ctx, msgIDListResp, resp)
}

// onAccept 接取任务。
//
// 已领奖的任务允许重新接取（日常任务的设计前提）：Status==claimed
// 时覆盖为 doing 并清零进度，实现「每日刷新」而不需要删键。
func (m *Module) onAccept(ctx *module.Context, req *gamev1.QuestAcceptReq, resp *gamev1.QuestAcceptResp) error {
	*resp = gamev1.QuestAcceptResp{}
	if _, ok := gamedata.GetQuestCfg(int64(req.QuestId)); !ok {
		resp.Code = codeQuestNotExist
		return nil
	}
	d, err := load(ctx.State)
	if err != nil {
		return err
	}
	if rec, ok := d.Quests[req.QuestId]; ok && rec.Status != statusClaimed {
		resp.Code = codeQuestAccepted
		return nil
	}
	d.Quests[req.QuestId] = Record{Status: statusDoing}
	if err := save(ctx.State, ctx.State.PlayerID(), d); err != nil {
		return err
	}
	resp.Code = codeOK
	return nil
}

// onClaim 领奖（经验 + 道具）。
//
// 领取事实与奖励在同一次快照提交：已领状态写进 Ext，道具进 Items，经验进 Profile，
// Redis 提交失败时三者一起回滚，不会出现「状态已领、奖励没到账」。
func (m *Module) onClaim(ctx *module.Context, req *gamev1.QuestClaimReq, resp *gamev1.QuestClaimResp) error {
	*resp = gamev1.QuestClaimResp{}
	cfg, ok := gamedata.GetQuestCfg(int64(req.QuestId))
	if !ok {
		resp.Code = codeQuestNotExist
		return nil
	}
	d, err := load(ctx.State)
	if err != nil {
		return err
	}
	rec, ok := d.Quests[req.QuestId]
	if !ok {
		resp.Code = codeQuestUnfinished
		return nil
	}
	if rec.Status == statusClaimed {
		resp.Code = codeQuestClaimed
		return nil
	}
	if rec.Progress < int32(cfg.Col3) {
		resp.Code = codeQuestUnfinished
		return nil
	}

	// 发奖：经验 + 钻石（配置约定 target*100 经验、target*10 钻石）
	expGain := int64(cfg.Col3) * 100
	grants := []module.ItemGrant{{ItemID: 1001, Count: int64(cfg.Col3) * 10}}
	// 满包/超限时整个领取被拒，不会出现「状态已置领、道具没发」。
	changes, grantErr := module.Grant(ctx.State, grants)
	if grantErr != nil {
		resp.Code = codeBagFull
		return nil
	}

	rec.Status = statusClaimed
	d.Quests[req.QuestId] = rec
	if err := save(ctx.State, ctx.State.PlayerID(), d); err != nil {
		return err
	}

	resp.Code = codeOK
	resp.ExpGain = expGain
	if raw, err := proto.Marshal(&gamev1.BagChangeNotify{Changes: module.ToItemChanges(changes)}); err == nil {
		resp.Rewards = raw
	}
	// 发经验（Actor 内部会推送等级变化）；仍在同一条消息内，随同一份快照提交。
	if ctx.AddExp != nil {
		ctx.AddExp(expGain)
	}
	// 回包由 module.Typed 统一完成：这里再 reply 一次会让客户端收到两条响应，
	// 多出来的那条留到下一次请求时被当成新响应读走。
	return nil
}

// load 读取任务状态；从未玩过任务时返回空结构（不可为 nil，避免 map 写入 panic）。
func load(state writebehind.ExtState) (*Data, error) {
	row, err := writebehind.LoadExt[model.PlayerQuest](state, stateKey)
	if err != nil {
		return nil, err
	}
	if row == nil {
		return &Data{Quests: map[int32]Record{}}, nil
	}
	return decode(row)
}

// decode 把表行的 JSON 列解成业务视图；坏 JSON 直接报错，由落库校验整批回滚。
func decode(row *model.PlayerQuest) (*Data, error) {
	d := &Data{Quests: map[int32]Record{}}
	if row.Quests != "" {
		if err := json.Unmarshal([]byte(row.Quests), &d.Quests); err != nil {
			return nil, fmt.Errorf("decode quest state: %w", err)
		}
	}
	if d.Quests == nil {
		d.Quests = map[int32]Record{}
	}
	return d, nil
}

// save 把状态编码回快照槽位；落库由 Write-Behind 负责。
func save(state writebehind.ExtState, playerID int64, d *Data) error {
	raw, err := json.Marshal(d.Quests)
	if err != nil {
		return fmt.Errorf("encode quest state: %w", err)
	}
	return writebehind.SaveExt(state, stateKey, &model.PlayerQuest{
		PlayerId: playerID,
		Quests:   string(raw),
	})
}

// reply 序列化 proto 并经 ctx.Reply 下发。
func reply(ctx *module.Context, msgID uint32, msg proto.Message) error {
	body, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	return ctx.Reply(msgID, body)
}

// warn 统一告警入口（logger 可为 nil）。
//
// logger 判空是依赖注入的常见形态：测试传 nil 也不 panic，
// 生产由组装层注入真实 logger。
func (m *Module) warn(msg string, ctx *module.Context, err error) {
	if m.logger != nil {
		m.logger.Warn(msg, "player_id", ctx.State.PlayerID(), "err", err)
	}
}
