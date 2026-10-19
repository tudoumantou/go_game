// internal/game/module/copy/copy.go —— 副本模块（6301-6304）
//
// 副本进度（星级 / 通关次数 / 首通）放在玩家快照 Ext["copy"]：结算、首通奖励
// 与经验在同一次 Redis CAS 提交，失败整批回滚（契约见 docs/R2.1-缓存原子变更契约.md）。
package copy

import (
	"context"
	"encoding/json"
	"fmt"

	"game_study/internal/game/module"
	"game_study/pkg/gamedata"
	"game_study/pkg/log"
	"game_study/pkg/model"
	"game_study/pkg/writebehind"

	gamev1 "game_study/pkg/pb/game/v1"
	"google.golang.org/protobuf/proto"
)

const stateKey = "copy"

// 协议号（4.9：6301/6302 列表，6303/6304 结算）。
const (
	msgIDListReq    uint32 = 6301
	msgIDListResp   uint32 = 6302
	msgIDSettleReq  uint32 = 6303
	msgIDSettleResp uint32 = 6304
)

// 业务码（4.10）。
const (
	codeOK             int32 = 0
	codeCopyNotExist   int32 = 6301
	codeLevelNotEnough int32 = 6302
)

// Record 单个副本的通关记录。
type Record struct {
	Star      int32 `json:"star"`
	PassCount int32 `json:"pass_count"`
	FirstPass bool  `json:"first_pass"`
}

// Data 是副本模块的私有状态：copy_id -> 通关记录。
type Data struct {
	Records map[int32]Record `json:"records"`
}

// init 注册持久化：copy_record 主键为 (player_id)。
func init() {
	writebehind.RegisterFlat[model.CopyRecord](stateKey, model.CopyRecordFlat, validate)
}

// validate 落库前校验：星级 0-3、次数非负。
func validate(row *model.CopyRecord) error {
	d, err := decode(row)
	if err != nil {
		return err
	}
	for id, rec := range d.Records {
		if id <= 0 || rec.Star < 0 || rec.Star > 3 || rec.PassCount < 0 {
			return fmt.Errorf("invalid copy %d record %+v", id, rec)
		}
	}
	return nil
}

// Module 副本模块。
type Module struct {
	logger *log.Logger
}

// New 创建副本模块。
func New(logger *log.Logger) *Module { return &Module{logger: logger} }

// Name 实现 module.Module。
func (m *Module) Name() string { return stateKey }

// MsgIDs 实现 module.Module。
func (m *Module) MsgIDs() []uint32 { return []uint32{msgIDListReq, msgIDSettleReq} }

// Init 实现 module.Module。
func (m *Module) Init(*module.Context) error { return nil }

// OnMessage 实现 module.Module。
func (m *Module) OnMessage(ctx *module.Context, msgID uint32, body []byte) error {
	switch msgID {
	case msgIDListReq:
		return m.onList(ctx)
	case msgIDSettleReq:
		return m.onSettle(ctx, body)
	default:
		return fmt.Errorf("copy: unexpected msg %d", msgID)
	}
}

// OnSave 实现 module.Module：状态随快照提交。
func (m *Module) OnSave(context.Context) error { return nil }

// OnLogin 实现 module.Module。
func (m *Module) OnLogin(*module.Context) error { return nil }

// OnLogout 实现 module.Module。
func (m *Module) OnLogout(*module.Context) error { return nil }

// onList 副本列表（等级不足仍下发，结算时强校验）。
func (m *Module) onList(ctx *module.Context) error {
	resp := &gamev1.CopyListResp{Code: codeOK}
	d, err := load(ctx.State)
	if err != nil {
		return err
	}
	for _, cfg := range gamedata.CopyCfgList {
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
	return reply(ctx, msgIDListResp, resp)
}

// onSettle 通关结算：星级取历史最佳，首通发放掉落并写经验。
// 结算事实、首通奖励与经验都在同一条消息内，随同一份快照提交。
func (m *Module) onSettle(ctx *module.Context, body []byte) error {
	var req gamev1.CopySettleReq
	if err := proto.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("decode CopySettleReq: %w", err)
	}
	resp := &gamev1.CopySettleResp{}

	cfg, ok := gamedata.GetCopyCfg(int64(req.CopyId))
	if !ok {
		resp.Code = codeCopyNotExist
		return reply(ctx, msgIDSettleResp, resp)
	}
	if ctx.State.Profile().Level < int32(cfg.Col2) {
		resp.Code = codeLevelNotEnough
		return reply(ctx, msgIDSettleResp, resp)
	}
	star := req.Star
	if star < 1 {
		star = 1
	}
	if star > 3 {
		star = 3
	}

	d, err := load(ctx.State)
	if err != nil {
		return err
	}
	rec := d.Records[req.CopyId]
	firstPass := !rec.FirstPass
	if rec.Star < star {
		rec.Star = star
	}
	rec.PassCount++
	rec.FirstPass = true
	d.Records[req.CopyId] = rec

	resp.Code = codeOK
	resp.FirstPass = firstPass

	// 通关经验：等级门槛 * 50
	expGain := cfg.Col2 * 50
	if cfg.Col2 <= 0 {
		expGain = 50
	}
	resp.ExpGain = expGain

	// 首通发奖：按 copy_drop.xml 中 col1 = copy_id 的行发道具
	var changes []*gamev1.ItemChange
	if firstPass {
		for _, drop := range gamedata.CopyDropCfgList {
			if drop.Col1 != cfg.Id {
				continue
			}
			itemID := int32(drop.Col2)
			count := drop.Col3
			ctx.State.AddItem(itemID, count)
			changes = append(changes, &gamev1.ItemChange{
				ItemUid: int64(itemID),
				ItemId:  itemID,
				Count:   int32(ctx.State.GetItemCount(itemID)),
				Change:  1,
			})
		}
		if len(changes) > 0 {
			ctx.State.MarkDirty("bag")
			if raw, err := proto.Marshal(&gamev1.BagChangeNotify{Changes: changes}); err == nil {
				resp.Rewards = raw
			}
		}
	}

	if err := save(ctx.State, ctx.State.PlayerID(), d); err != nil {
		return err
	}
	if err := reply(ctx, msgIDSettleResp, resp); err != nil {
		return err
	}
	if ctx.AddExp != nil {
		ctx.AddExp(expGain)
	}
	// 事件：驱动任务/活动（kill 语义的副本通关）
	if ctx.Emit != nil {
		ctx.Emit(module.Event{Type: "copy_pass", Key: fmt.Sprintf("%d", cfg.Id), Delta: 1})
	}
	return nil
}

// load 读取副本状态；从未通关返回空结构。
func load(state writebehind.ExtState) (*Data, error) {
	row, err := writebehind.LoadExt[model.CopyRecord](state, stateKey)
	if err != nil {
		return nil, err
	}
	if row == nil {
		return &Data{Records: map[int32]Record{}}, nil
	}
	return decode(row)
}

func decode(row *model.CopyRecord) (*Data, error) {
	d := &Data{Records: map[int32]Record{}}
	if row.Records != "" {
		if err := json.Unmarshal([]byte(row.Records), &d.Records); err != nil {
			return nil, fmt.Errorf("decode copy state: %w", err)
		}
	}
	if d.Records == nil {
		d.Records = map[int32]Record{}
	}
	return d, nil
}

func save(state writebehind.ExtState, playerID int64, d *Data) error {
	raw, err := json.Marshal(d.Records)
	if err != nil {
		return fmt.Errorf("encode copy state: %w", err)
	}
	return writebehind.SaveExt(state, stateKey, &model.CopyRecord{
		PlayerId: playerID,
		Records:  string(raw),
	})
}

func reply(ctx *module.Context, msgID uint32, msg proto.Message) error {
	body, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	return ctx.Reply(msgID, body)
}
