// internal/game/module/quest/quest.go —— 任务模块（7101-7106）
//
// 状态（进度 + 领取事实）整体放在玩家快照的 Ext["quest"] 里：领奖时改内存、
// 与道具经验一起提交，Redis CAS 失败则整批回滚（契约见 docs/R2.1-缓存原子变更契约.md）。
// 不再直接写 MySQL 业务事实。
package quest

import (
	"context"
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

// 业务码（4.10）。
const (
	codeOK              int32 = 0
	codeQuestNotExist   int32 = 7101
	codeQuestAccepted   int32 = 7102
	codeQuestUnfinished int32 = 7103
	codeQuestClaimed    int32 = 7104
	codeBagFull         int32 = 7105
)

// 任务状态（与快照内 JSON 一致）。
const (
	statusDoing   int32 = 0 // 进行中
	statusDone    int32 = 1 // 已完成（可领奖）
	statusClaimed int32 = 2 // 已领奖
)

// Record 单个任务的状态。
type Record struct {
	Status   int32 `json:"status"`
	Progress int32 `json:"progress"`
}

// Data 是任务模块的私有状态：quest_id -> 进度与领取事实。
type Data struct {
	Quests map[int32]Record `json:"quests"`
}

// init 注册持久化：player_quest 主键为 (player_id)，FlatPersister 直接接入。
func init() {
	writebehind.RegisterFlat[model.PlayerQuest](stateKey, model.PlayerQuestFlat, validate)
}

// validate 落库前校验状态取值与进度范围，坏数据整批回滚。
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
	logger *log.Logger
}

// New 创建任务模块。
func New(logger *log.Logger) *Module { return &Module{logger: logger} }

// Name 实现 module.Module。
func (m *Module) Name() string { return stateKey }

// MsgIDs 实现 module.Module。
func (m *Module) MsgIDs() []uint32 {
	return []uint32{msgIDListReq, msgIDAcceptReq, msgIDClaimReq}
}

// Init 实现 module.Module。
func (m *Module) Init(ctx *module.Context) error { return nil }

// OnMessage 实现 module.Module。
func (m *Module) OnMessage(ctx *module.Context, msgID uint32, body []byte) error {
	switch msgID {
	case msgIDListReq:
		return m.onList(ctx)
	case msgIDAcceptReq:
		return m.onAccept(ctx, body)
	case msgIDClaimReq:
		return m.onClaim(ctx, body)
	default:
		return fmt.Errorf("quest: unexpected msg %d", msgID)
	}
}

// OnSave 实现 module.Module：状态随快照提交，无需额外落库。
func (m *Module) OnSave(context.Context) error { return nil }

// OnLogin 实现 module.Module。
func (m *Module) OnLogin(*module.Context) error { return nil }

// OnLogout 实现 module.Module。
func (m *Module) OnLogout(ctx *module.Context) error {
	ctx.State.MarkDirty(stateKey)
	return nil
}

// OnEvent 实现 module.EventAware：推进匹配类型的任务进度。
//
// 一次事件只解析一次状态、只写回一次；进度与状态都在内存里改，
// 快照提交失败时随整条消息回滚。
func (m *Module) OnEvent(ctx *module.Context, evt module.Event) {
	d, err := load(ctx.State)
	if err != nil {
		m.warn("quest state load failed", ctx, err)
		return
	}
	changed := false
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
func (m *Module) onList(ctx *module.Context) error {
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
func (m *Module) onAccept(ctx *module.Context, body []byte) error {
	var req gamev1.QuestAcceptReq
	if err := proto.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("decode QuestAcceptReq: %w", err)
	}
	resp := &gamev1.QuestAcceptResp{}
	if _, ok := gamedata.GetQuestCfg(int64(req.QuestId)); !ok {
		resp.Code = codeQuestNotExist
		return reply(ctx, msgIDAcceptResp, resp)
	}
	d, err := load(ctx.State)
	if err != nil {
		return err
	}
	if rec, ok := d.Quests[req.QuestId]; ok && rec.Status != statusClaimed {
		resp.Code = codeQuestAccepted
		return reply(ctx, msgIDAcceptResp, resp)
	}
	d.Quests[req.QuestId] = Record{Status: statusDoing}
	if err := save(ctx.State, ctx.State.PlayerID(), d); err != nil {
		return err
	}
	resp.Code = codeOK
	return reply(ctx, msgIDAcceptResp, resp)
}

// onClaim 领奖（经验 + 道具）。
//
// 领取事实与奖励在同一次快照提交：已领状态写进 Ext，道具进 Items，经验进 Profile，
// Redis 提交失败时三者一起回滚，不会出现「状态已领、奖励没到账」。
func (m *Module) onClaim(ctx *module.Context, body []byte) error {
	var req gamev1.QuestClaimReq
	if err := proto.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("decode QuestClaimReq: %w", err)
	}
	resp := &gamev1.QuestClaimResp{}
	cfg, ok := gamedata.GetQuestCfg(int64(req.QuestId))
	if !ok {
		resp.Code = codeQuestNotExist
		return reply(ctx, msgIDClaimResp, resp)
	}
	d, err := load(ctx.State)
	if err != nil {
		return err
	}
	rec, ok := d.Quests[req.QuestId]
	if !ok {
		resp.Code = codeQuestUnfinished
		return reply(ctx, msgIDClaimResp, resp)
	}
	if rec.Status == statusClaimed {
		resp.Code = codeQuestClaimed
		return reply(ctx, msgIDClaimResp, resp)
	}
	if rec.Progress < int32(cfg.Col3) {
		resp.Code = codeQuestUnfinished
		return reply(ctx, msgIDClaimResp, resp)
	}

	// 发奖：经验 + 钻石（配置约定 target*100 经验、target*10 钻石）
	expGain := int64(cfg.Col3) * 100
	itemCount := int64(cfg.Col3) * 10
	grants := []module.ItemGrant{{ItemID: 1001, Count: itemCount}}
	if !ctx.State.CanHold(grants) {
		resp.Code = codeBagFull
		return reply(ctx, msgIDClaimResp, resp)
	}
	ctx.State.AddItem(1001, itemCount)
	ctx.State.MarkDirty("bag")

	rec.Status = statusClaimed
	d.Quests[req.QuestId] = rec
	if err := save(ctx.State, ctx.State.PlayerID(), d); err != nil {
		return err
	}

	resp.Code = codeOK
	resp.ExpGain = expGain
	if raw, err := proto.Marshal(&gamev1.BagChangeNotify{Changes: []*gamev1.ItemChange{{
		ItemUid: 1001,
		ItemId:  1001,
		Count:   int32(ctx.State.GetItemCount(1001)),
		Change:  1,
	}}}); err == nil {
		resp.Rewards = raw
	}
	if err := reply(ctx, msgIDClaimResp, resp); err != nil {
		return err
	}
	// 发经验（Actor 内部会推送等级变化）；仍在同一条消息内，随同一份快照提交。
	if ctx.AddExp != nil {
		ctx.AddExp(expGain)
	}
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

func reply(ctx *module.Context, msgID uint32, msg proto.Message) error {
	body, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	return ctx.Reply(msgID, body)
}

func (m *Module) warn(msg string, ctx *module.Context, err error) {
	if m.logger != nil {
		m.logger.Warn(msg, "player_id", ctx.State.PlayerID(), "err", err)
	}
}
