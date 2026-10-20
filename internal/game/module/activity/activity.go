// internal/game/module/activity/activity.go —— 活动模块（6501-6504）
//
// 活动进度与领取位图放在玩家快照 Ext["activity"]：领奖与发放在同一次 Redis CAS
// 提交，Redis 失败时两者一起回滚（契约见 docs/R2.1-缓存原子变更契约.md）。
package activity

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"game_study/internal/game/module"
	"game_study/pkg/activity"
	"game_study/pkg/codec"
	"game_study/pkg/gamedata"
	"game_study/pkg/log"
	"game_study/pkg/model"
	pkgmq "game_study/pkg/mq"
	"game_study/pkg/writebehind"

	gamev1 "game_study/pkg/pb/game/v1"
	"google.golang.org/protobuf/proto"
)

const stateKey = "activity"

// 协议号（4.9：6501/6502 列表，6503/6504 领奖）。
const (
	msgIDListReq   = codec.MsgActivityListReq
	msgIDListResp  = codec.MsgActivityListResp
	msgIDClaimReq  = codec.MsgActivityClaimReq
	msgIDClaimResp = codec.MsgActivityClaimResp
)

// 业务码（4.10）。
const (
	codeOK               int32 = 0
	codeActivityNotExist int32 = 6501
	codeNotFinished      int32 = 6502
	codeAlreadyClaimed   int32 = 6503
	codeBagFull          int32 = 6504
	codeNotOpen          int32 = 6505 // 活动不在开放期（下发的窗口已过期）
)

// 奖励道具（P6 固定：钻石 1001）。
const itemDiamond int32 = 1001

// Record 单个活动的进度与领取位图。
//
// 字段语义：
//   - Progress：当前进度，由 OnEvent 累加并在配置 Col3 目标处截断；
//   - RewardMask：领取位图，bit0=第一档奖励已领；多档奖励时按位扩展。
type Record struct {
	Progress   int32 `json:"progress"`
	RewardMask int64 `json:"reward_mask"`
}

// Data 是活动模块的私有状态：activity_id -> 进度与已领位图。
type Data struct {
	Records map[int32]Record `json:"records"`
}

// init 注册持久化：activity_record 主键为 (player_id)。
func init() {
	writebehind.RegisterFlat[model.ActivityRecord](stateKey, model.ActivityRecordFlat, validate)
}

// validate 落库前校验：进度非负、活动 ID 合法。
func validate(row *model.ActivityRecord) error {
	d, err := decode(row)
	if err != nil {
		return err
	}
	for id, rec := range d.Records {
		if id <= 0 || rec.Progress < 0 {
			return fmt.Errorf("invalid activity %d progress %d", id, rec.Progress)
		}
	}
	return nil
}

// Module 活动模块。
type Module struct {
	logger   *log.Logger
	producer ProgressProducer // 全服里程碑贡献上报；nil 表示不上报（单机/测试）
}

// ProgressProducer 贡献上报能力（*pkgmq.Producer 实现）。
type ProgressProducer interface {
	// ProduceEnvelope 投递一条全服活动贡献消息；失败由调用方降级为日志。
	ProduceEnvelope(ctx context.Context, env *pkgmq.Envelope) error
}

// New 创建活动模块。
func New(logger *log.Logger, producer ProgressProducer) *Module {
	m := &Module{logger: logger}
	if producer != nil {
		m.producer = producer
	}
	return m
}

// Name 实现 module.Module。
func (m *Module) Name() string { return stateKey }

// MsgIDs 实现 module.Module。
func (m *Module) MsgIDs() []uint32 { return []uint32{msgIDListReq, msgIDClaimReq} }

// Init 实现 module.Module。
func (m *Module) Init(*module.Context) error { return nil }

// OnMessage 实现 module.Module。
func (m *Module) OnMessage(ctx *module.Context, msgID uint32, body []byte) error {
	switch msgID {
	case msgIDListReq:
		return m.onList(ctx)
	case msgIDClaimReq:
		return m.onClaim(ctx, body)
	default:
		return fmt.Errorf("activity: unexpected msg %d", msgID)
	}
}

// OnSave 实现 module.Module：状态随快照提交。
func (m *Module) OnSave(context.Context) error { return nil }

// OnLogin 登录类活动进度 +1，纯内存改动，随本次快照提交。
func (m *Module) OnLogin(ctx *module.Context) error {
	d, err := load(ctx.State)
	if err != nil {
		return nil // 状态损坏不阻断登录，后续消息会再次报错
	}
	changed := false
	for _, cfg := range gamedata.ActivityCfgList() {
		if cfg.Col2 != "login" {
			continue
		}
		id := int32(cfg.Id)
		rec := d.Records[id]
		if rec.Progress >= int32(cfg.Col3) {
			continue
		}
		rec.Progress++
		d.Records[id] = rec
		changed = true
	}
	if changed {
		if err := save(ctx.State, ctx.State.PlayerID(), d); err != nil {
			m.warn("activity login progress failed", ctx, err)
		}
	}
	return nil
}

// OnLogout 实现 module.Module。
func (m *Module) OnLogout(*module.Context) error { return nil }

// OnEvent 实现 module.EventAware：把玩家行为事件累加为活动进度。
func (m *Module) OnEvent(ctx *module.Context, evt module.Event) {
	d, err := load(ctx.State)
	if err != nil {
		return
	}
	changed := false
	var reports [][2]int32 // 全服贡献上报：(activity_id, progress)
	for _, cfg := range gamedata.ActivityCfgList() {
		if cfg.Col2 != evt.Type {
			continue
		}
		id := int32(cfg.Id)
		rec := d.Records[id]
		next := rec.Progress + int32(evt.Delta)
		if next > int32(cfg.Col3) {
			next = int32(cfg.Col3)
		}
		if next == rec.Progress {
			continue
		}
		rec.Progress = next
		d.Records[id] = rec
		changed = true
		if cfg.Col2 == "server_progress" {
			// 全服活动：进度变化后上报新值（绝对值，中心服按差值累加）。
			reports = append(reports, [2]int32{id, next})
		}
	}
	if changed {
		if err := save(ctx.State, ctx.State.PlayerID(), d); err != nil {
			m.warn("activity progress update failed", ctx, err)
		}
	}
	for _, r := range reports {
		m.reportProgress(ctx, r[0], r[1])
	}
}

// reportProgress 上报全服活动贡献（R7.3）。
//
// 上报绝对值而不是增量：重投相同值在中心服得到差值 0，天然幂等。
// 上报失败只记日志——玩家本地进度已保存，下一次事件会带上更大的进度继续推进。
func (m *Module) reportProgress(ctx *module.Context, activityID, progress int32) {
	if m.producer == nil || progress <= 0 {
		return
	}
	now := time.Now().Unix()
	instanceID := instanceIDFor(activityID, now)
	msg := pkgmq.ActivityProgressMsg{
		InstanceID: instanceID,
		ActivityID: activityID,
		PlayerID:   ctx.State.PlayerID(),
		Progress:   progress,
	}
	env, err := pkgmq.NewEnvelope(fmt.Sprintf("%s:%d", instanceID, ctx.State.PlayerID()),
		pkgmq.TopicActivityProgress, instanceID, msg)
	if err != nil {
		return
	}
	if err := m.producer.ProduceEnvelope(context.Background(), env); err != nil {
		m.logger.Debug("report activity progress failed", "activity_id", activityID, "err", err)
	}
}

// instanceIDFor 当前活动实例：优先用中心服下发的，没有就按周期自算。
func instanceIDFor(activityID int32, now int64) string {
	if w, ok := DefaultRegistry().Get(activityID); ok {
		return w.InstanceID
	}
	if cfg, ok := gamedata.GetActivityPeriodCfg(int64(activityID)); ok {
		_, _, period := activity.Window(int32(cfg.Col1), now)
		return fmt.Sprintf("%d-%d", activityID, period)
	}
	return fmt.Sprintf("%d-%d", activityID, now/86400)
}

// onList 活动列表（配置 + 进度 + 是否已领）。
func (m *Module) onList(ctx *module.Context) error {
	resp := &gamev1.ActivityListResp{Code: codeOK}
	d, err := load(ctx.State)
	if err != nil {
		return err
	}
	for _, cfg := range gamedata.ActivityCfgList() {
		// 下发的窗口已过期就不展示：客户端仍会定时刷新，不需要服务端推「活动结束」。
		if !openNow(int32(cfg.Id), time.Now().Unix()) {
			continue
		}
		entry := &gamev1.ActivityEntry{
			ActivityId: int32(cfg.Id),
			Name:       cfg.Col1,
			Type:       cfg.Col2,
			Target:     int32(cfg.Col3),
		}
		if rec, ok := d.Records[int32(cfg.Id)]; ok {
			entry.Progress = rec.Progress
			entry.Claimed = rec.RewardMask&1 != 0
		}
		resp.Activities = append(resp.Activities, entry)
	}
	return reply(ctx, msgIDListResp, resp)
}

// onClaim 领奖：进度达标 + 位图防重复，领奖事实与奖励同一次快照提交。
func (m *Module) onClaim(ctx *module.Context, body []byte) error {
	var req gamev1.ActivityClaimReq
	if err := proto.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("decode ActivityClaimReq: %w", err)
	}
	resp := &gamev1.ActivityClaimResp{}
	cfg, ok := gamedata.GetActivityCfg(int64(req.ActivityId))
	if !ok {
		resp.Code = codeActivityNotExist
		return reply(ctx, msgIDClaimResp, resp)
	}
	// 领取同样校验开放期：不能依赖客户端「恰好没收到结束通知」。
	if !openNow(req.ActivityId, time.Now().Unix()) {
		resp.Code = codeNotOpen
		return reply(ctx, msgIDClaimResp, resp)
	}
	d, err := load(ctx.State)
	if err != nil {
		return err
	}
	rec := d.Records[req.ActivityId]
	if rec.Progress < int32(cfg.Col3) {
		resp.Code = codeNotFinished
		return reply(ctx, msgIDClaimResp, resp)
	}
	if rec.RewardMask&1 != 0 {
		resp.Code = codeAlreadyClaimed
		return reply(ctx, msgIDClaimResp, resp)
	}

	// 发奖：target * 20 钻石；先预检容量，满包时不发也不置领取位
	gain := int64(cfg.Col3) * 20
	if !ctx.State.CanHold([]module.ItemGrant{{ItemID: itemDiamond, Count: gain}}) {
		resp.Code = codeBagFull
		return reply(ctx, msgIDClaimResp, resp)
	}
	ctx.State.AddItem(itemDiamond, gain)
	ctx.State.MarkDirty("bag")
	rec.RewardMask |= 1
	d.Records[req.ActivityId] = rec
	if err := save(ctx.State, ctx.State.PlayerID(), d); err != nil {
		return err
	}

	resp.Code = codeOK
	if raw, err := proto.Marshal(&gamev1.BagChangeNotify{Changes: []*gamev1.ItemChange{{
		ItemUid: int64(itemDiamond),
		ItemId:  itemDiamond,
		Count:   int32(ctx.State.GetItemCount(itemDiamond)),
		Change:  1,
	}}}); err == nil {
		resp.Rewards = raw
	}
	return reply(ctx, msgIDClaimResp, resp)
}

// load 读取活动状态；从未参与返回空结构。
func load(state writebehind.ExtState) (*Data, error) {
	row, err := writebehind.LoadExt[model.ActivityRecord](state, stateKey)
	if err != nil {
		return nil, err
	}
	if row == nil {
		return &Data{Records: map[int32]Record{}}, nil
	}
	return decode(row)
}

// decode 把表行 JSON 解成业务视图；保证 map 非 nil。
func decode(row *model.ActivityRecord) (*Data, error) {
	d := &Data{Records: map[int32]Record{}}
	if row.Records != "" {
		if err := json.Unmarshal([]byte(row.Records), &d.Records); err != nil {
			return nil, fmt.Errorf("decode activity state: %w", err)
		}
	}
	if d.Records == nil {
		d.Records = map[int32]Record{}
	}
	return d, nil
}

// save 把进度与位图编码回 Ext 槽位；落库由 Write-Behind 负责。
func save(state writebehind.ExtState, playerID int64, d *Data) error {
	raw, err := json.Marshal(d.Records)
	if err != nil {
		return fmt.Errorf("encode activity state: %w", err)
	}
	return writebehind.SaveExt(state, stateKey, &model.ActivityRecord{
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

// warn 统一告警入口（logger 可为 nil）。
func (m *Module) warn(msg string, ctx *module.Context, err error) {
	if m.logger != nil {
		m.logger.Warn(msg, "player_id", ctx.State.PlayerID(), "err", err)
	}
}
