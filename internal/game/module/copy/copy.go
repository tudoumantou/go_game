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
package copy

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

// stateKey 同时是 Ext 槽位键、模块名与落库归属标签。
const stateKey = "copy"

// 协议号（4.9：6301/6302 列表，6303/6304 结算）。
const (
	msgIDListReq    = codec.MsgCopyListReq
	msgIDListResp   = codec.MsgCopyListResp
	msgIDSettleReq  = codec.MsgCopySettleReq
	msgIDSettleResp = codec.MsgCopySettleResp
)

// 业务码（4.10）：副本段 6301-6303。
const (
	codeOK             int32 = 0
	codeCopyNotExist   int32 = 6301
	codeLevelNotEnough int32 = 6302
	codeBagFull        int32 = 6303
)

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

// init 注册持久化：copy_record 主键为 (player_id)。
func init() {
	writebehind.RegisterFlat[model.CopyRecord](stateKey, model.CopyRecordFlat, validate)
}

// validate 落库前校验：星级 0-3、次数非负。
//
// 星级上界 3 是协议约定（面板只画 3 颗星）；写死在校验里而非配置，
// 是因为它同时约束客户端展示与服务端存储。
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
//
// 不做任何事：进度在 Ext 里，列表/结算时按需 load；
// 登录不预加载是为了少一次 JSON 解码（数据量小，懒加载更划算）。
func (m *Module) OnLogin(*module.Context) error { return nil }

// OnLogout 实现 module.Module。
func (m *Module) OnLogout(*module.Context) error { return nil }

// onList 副本列表（等级不足仍下发，结算时强校验）。
//
// 列表 = 配置全集 LEFT JOIN 玩家进度：没打过的副本也在列表里
// （BestStar=0/PassCount=0），这样客户端不需要再拉配置表。
func (m *Module) onList(ctx *module.Context) error {
	resp := &gamev1.CopyListResp{Code: codeOK}
	d, err := load(ctx.State)
	if err != nil {
		return err
	}
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
	// 客户端上报的星级裁剪到 1-3：0/负数按 1 星，超高按 3 星，防止脏数据落库。
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
	// rec 是值副本；FirstPass 先取反再置位——本次是否首通必须在改写之前判定，
	// 否则置位后永远判不出首通。
	rec := d.Records[req.CopyId]
	firstPass := !rec.FirstPass
	if rec.Star < star {
		rec.Star = star // 历史最佳只升不降
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

	// 首通发奖：按 copy_drop.xml 中 col1 = copy_id 的行发道具。
	// 先收集再预检容量，避免「发到一半满包」留下部分到账。
	var changes []*gamev1.ItemChange
	if firstPass {
		grants := make([]module.ItemGrant, 0, 4)
		for _, drop := range gamedata.CopyDropCfgList() {
			if drop.Col1 != cfg.Id {
				continue
			}
			grants = append(grants, module.ItemGrant{ItemID: int32(drop.Col2), Count: drop.Col3})
		}
		if !ctx.State.CanHold(grants) {
			// 满包：整个结算拒绝（进度也不前进），玩家清包后重新结算——
			// 副本结算可重复打，所以这里不需要「部分成功」。
			resp.Code = codeBagFull
			return reply(ctx, msgIDSettleResp, resp)
		}
		for _, g := range grants {
			ctx.State.AddItem(g.ItemID, g.Count)
			changes = append(changes, &gamev1.ItemChange{
				ItemUid: int64(g.ItemID),
				ItemId:  g.ItemID,
				Count:   int32(ctx.State.GetItemCount(g.ItemID)),
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
	// AddExp 是可选注入：nil 时跳过（测试/无等级系统环境）。
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
//
// 懒初始化：row == nil 返回空 map 而不落库，首通时才产生记录。
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

// decode 把表行 JSON 解成业务视图；保证 map 非 nil。
//
// map 非 nil 是硬约束：onSettle 会 d.Records[id] = rec 写入，
// nil map 写入直接 panic（读不会）。
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

// save 把进度编码回 Ext 槽位；落库调度由 Write-Behind 负责。
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

// reply 序列化 proto 并经 ctx.Reply 下发。
func reply(ctx *module.Context, msgID uint32, msg proto.Message) error {
	body, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	return ctx.Reply(msgID, body)
}
