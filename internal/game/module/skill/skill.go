// internal/game/module/skill/skill.go —— 技能模块（3201-3206）
//
// 技能等级放在玩家快照 Ext["skill"]：学习/升级只改内存，与消耗的材料一起提交，
// Redis CAS 失败时整批回滚（契约见 docs/R2.1-缓存原子变更契约.md）。
package skill

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

// 业务码（4.10）。
const (
	codeOK         int32 = 0
	codeNotExist   int32 = 3201
	codeLevelLow   int32 = 3202
	codeLearned    int32 = 3203
	codeNotLearned int32 = 3204
	codeNoResource int32 = 3205
)

// 升级消耗道具（钻石）。
const upgradeItemID int32 = 1001

// Data 是技能模块的私有状态：skill_id -> 等级。
type Data struct {
	Levels map[int32]int32 `json:"levels"`
}

// init 注册持久化：player_skill 主键为 (player_id)。
func init() {
	writebehind.RegisterFlat[model.PlayerSkill](stateKey, model.PlayerSkillFlat, validate)
}

// validate 落库前校验：等级必须为正，技能 ID 合法。
func validate(row *model.PlayerSkill) error {
	d, err := decode(row)
	if err != nil {
		return err
	}
	for id, level := range d.Levels {
		if id <= 0 || level <= 0 {
			return fmt.Errorf("invalid skill %d level %d", id, level)
		}
	}
	return nil
}

// Module 技能模块。
type Module struct {
	logger *log.Logger
}

// New 创建技能模块。
func New(logger *log.Logger) *Module { return &Module{logger: logger} }

// Name 实现 module.Module。
func (m *Module) Name() string { return stateKey }

// MsgIDs 实现 module.Module。
func (m *Module) MsgIDs() []uint32 { return []uint32{msgIDListReq, msgIDLearnReq, msgIDUpgradeReq} }

// Init 实现 module.Module。
func (m *Module) Init(*module.Context) error { return nil }

// OnMessage 实现 module.Module。
func (m *Module) OnMessage(ctx *module.Context, msgID uint32, body []byte) error {
	switch msgID {
	case msgIDListReq:
		return m.onList(ctx)
	case msgIDLearnReq:
		return m.onLearn(ctx, body)
	case msgIDUpgradeReq:
		return m.onUpgrade(ctx, body)
	default:
		return fmt.Errorf("skill: unexpected msg %d", msgID)
	}
}

// OnSave 实现 module.Module：等级随快照提交。
func (m *Module) OnSave(context.Context) error { return nil }

// OnLogin 实现 module.Module。
func (m *Module) OnLogin(*module.Context) error { return nil }

// OnLogout 实现 module.Module。
func (m *Module) OnLogout(ctx *module.Context) error {
	ctx.State.MarkDirty(stateKey)
	return nil
}

func (m *Module) onList(ctx *module.Context) error {
	resp := &gamev1.SkillListResp{Code: codeOK}
	d, err := load(ctx.State)
	if err != nil {
		return err
	}
	ids := make([]int32, 0, len(d.Levels))
	for id := range d.Levels {
		ids = append(ids, id)
	}
	// 稳定顺序，客户端列表不抖动。
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
	return reply(ctx, msgIDListResp, resp)
}

func (m *Module) onLearn(ctx *module.Context, body []byte) error {
	var req gamev1.SkillLearnReq
	if err := proto.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("decode SkillLearnReq: %w", err)
	}
	resp := &gamev1.SkillLearnResp{}
	cfg, ok := gamedata.GetSkillCfg(int64(req.SkillId))
	if !ok {
		resp.Code = codeNotExist
		return reply(ctx, msgIDLearnResp, resp)
	}
	if ctx.State.Profile().Level < int32(cfg.Col2) {
		resp.Code = codeLevelLow
		return reply(ctx, msgIDLearnResp, resp)
	}
	d, err := load(ctx.State)
	if err != nil {
		return err
	}
	if _, learned := d.Levels[req.SkillId]; learned {
		resp.Code = codeLearned
		return reply(ctx, msgIDLearnResp, resp)
	}
	d.Levels[req.SkillId] = 1
	if err := save(ctx.State, ctx.State.PlayerID(), d); err != nil {
		return err
	}
	resp.Code = codeOK
	return reply(ctx, msgIDLearnResp, resp)
}

// onUpgrade 升级：扣材料与等级变更在同一次快照提交。
func (m *Module) onUpgrade(ctx *module.Context, body []byte) error {
	var req gamev1.SkillUpgradeReq
	if err := proto.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("decode SkillUpgradeReq: %w", err)
	}
	resp := &gamev1.SkillUpgradeResp{}
	cfg, ok := gamedata.GetSkillCfg(int64(req.SkillId))
	if !ok {
		resp.Code = codeNotExist
		return reply(ctx, msgIDUpgradeResp, resp)
	}
	d, err := load(ctx.State)
	if err != nil {
		return err
	}
	level, learned := d.Levels[req.SkillId]
	if !learned {
		resp.Code = codeNotLearned
		return reply(ctx, msgIDUpgradeResp, resp)
	}
	// 升级消耗：Col3 * 当前等级 钻石（Col3=0 表示免费）
	cost := int64(cfg.Col3) * int64(level)
	if cost > 0 {
		if ctx.State.GetItemCount(upgradeItemID) < cost {
			resp.Code = codeNoResource
			return reply(ctx, msgIDUpgradeResp, resp)
		}
		ctx.State.RemoveItem(upgradeItemID, cost)
		ctx.State.MarkDirty("bag")
	}
	next := level + 1
	d.Levels[req.SkillId] = next
	if err := save(ctx.State, ctx.State.PlayerID(), d); err != nil {
		return err
	}
	resp.Code = codeOK
	resp.Level = next
	return reply(ctx, msgIDUpgradeResp, resp)
}

// load 读取技能状态；从未学习过返回空结构。
func load(state writebehind.ExtState) (*Data, error) {
	row, err := writebehind.LoadExt[model.PlayerSkill](state, stateKey)
	if err != nil {
		return nil, err
	}
	if row == nil {
		return &Data{Levels: map[int32]int32{}}, nil
	}
	return decode(row)
}

func decode(row *model.PlayerSkill) (*Data, error) {
	d := &Data{Levels: map[int32]int32{}}
	if row.Skills != "" {
		if err := json.Unmarshal([]byte(row.Skills), &d.Levels); err != nil {
			return nil, fmt.Errorf("decode skill state: %w", err)
		}
	}
	if d.Levels == nil {
		d.Levels = map[int32]int32{}
	}
	return d, nil
}

func save(state writebehind.ExtState, playerID int64, d *Data) error {
	raw, err := json.Marshal(d.Levels)
	if err != nil {
		return fmt.Errorf("encode skill state: %w", err)
	}
	return writebehind.SaveExt(state, stateKey, &model.PlayerSkill{
		PlayerId: playerID,
		Skills:   string(raw),
	})
}

func reply(ctx *module.Context, msgID uint32, msg proto.Message) error {
	body, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	return ctx.Reply(msgID, body)
}
