// Package signin 实现按北京时间自然周（周一至周日）循环的七日签到。
//
// 状态只有「已领取天数 + 已领取奖励汇总」，持久化由 FlatPersister 一行注册，
// 不再有独立的仓储/持久化/日历文件。周切换的清零属于周期回调模块的职责，
// 本模块不持有时钟锚点、不做定时重置；回调模块上线时调用 Reset 即可。
package signin

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"time"

	"game_study/internal/game/module"
	"game_study/pkg/codec"
	"game_study/pkg/gamedata"
	"game_study/pkg/log"
	"game_study/pkg/model"
	gamev1 "game_study/pkg/pb/game/v1"
	"game_study/pkg/writebehind"
	"google.golang.org/protobuf/proto"
)

const (
	// stateKey 同时是 Ext 槽位键与落库归属的模块名。
	stateKey = "signin"

	MsgInfoReq   = codec.MsgSigninInfoReq
	MsgInfoResp  = codec.MsgSigninInfoResp
	MsgClaimReq  = codec.MsgSigninClaimReq
	MsgClaimResp = codec.MsgSigninClaimResp

	codeSystem   int32 = 1
	codeConfig   int32 = 6601
	codeBagLimit int32 = 6602
	codeBagFull  int32 = 6603

	weekDays int32 = 7
)

// businessZone 固定 UTC+8：进程或容器时区不同不该改变「今天周几」。
var businessZone = time.FixedZone("Asia/Shanghai", 8*60*60)

// Data 是签到的业务视图：本周已领取天数（1=周一…7=周日）与已领取奖励汇总。
type Data struct {
	Days    []int32
	Rewards map[int32]int64
}

// init 注册持久化：表主键恰为 (player_id)，生成的 FlatOps 直接复用。
func init() {
	writebehind.RegisterFlat[model.PlayerSignin](stateKey, model.PlayerSigninFlat, validate)
}

// validate 落库前校验：天数在 1-7，奖励数量为正，坏数据整批回滚。
func validate(row *model.PlayerSignin) error {
	d, err := decode(row)
	if err != nil {
		return err
	}
	for _, day := range d.Days {
		if day < 1 || day > weekDays {
			return fmt.Errorf("invalid signin day %d", day)
		}
	}
	for id, count := range d.Rewards {
		if id <= 0 || count <= 0 {
			return fmt.Errorf("invalid signin reward %d x %d", id, count)
		}
	}
	return nil
}

// Reset 清空签到状态，供周期回调模块在周切换时调用；本模块自身不触发重置。
func Reset(state writebehind.ExtState) { state.SetExt(stateKey, nil) }

type Module struct {
	logger *log.Logger
	now    func() time.Time
}

// New 的可选时钟用于验证周几边界；生产默认使用服务器时间。
func New(logger *log.Logger, clock ...func() time.Time) *Module {
	m := &Module{logger: logger, now: time.Now}
	if len(clock) > 0 && clock[0] != nil {
		m.now = clock[0]
	}
	return m
}

func (*Module) Name() string     { return stateKey }
func (*Module) MsgIDs() []uint32 { return []uint32{MsgInfoReq, MsgClaimReq} }

// Rollbackable 签到请求只改内存且可幂等重放，写缓存失败时可以丢弃本次改动。
func (*Module) Rollbackable(msgID uint32) bool { return msgID == MsgInfoReq || msgID == MsgClaimReq }

// CacheFailureReply 缓存不可用时返回业务错误码，客户端无需处理通用错误通知。
func (*Module) CacheFailureReply(msgID uint32) (uint32, []byte, bool) {
	var out proto.Message
	var id uint32
	switch msgID {
	case MsgInfoReq:
		id, out = MsgInfoResp, &gamev1.SigninInfoResp{Code: codeSystem}
	case MsgClaimReq:
		id, out = MsgClaimResp, &gamev1.SigninClaimResp{Code: codeSystem}
	default:
		return 0, nil, false
	}
	raw, err := proto.Marshal(out)
	if err != nil {
		return 0, nil, false
	}
	return id, raw, true
}

// Init 启动期校验七天奖励配置齐全，配错不等到玩家点领取才暴露。
func (*Module) Init(*module.Context) error {
	for day := int32(1); day <= weekDays; day++ {
		if _, err := rewardsForDay(day); err != nil {
			return err
		}
	}
	return nil
}

func (*Module) OnSave(context.Context) error   { return nil }
func (*Module) OnLogin(*module.Context) error  { return nil }
func (*Module) OnLogout(*module.Context) error { return nil }

func (m *Module) OnMessage(ctx *module.Context, msgID uint32, body []byte) error {
	var req proto.Message
	switch msgID {
	case MsgInfoReq:
		req = &gamev1.SigninInfoReq{}
	case MsgClaimReq:
		req = &gamev1.SigninClaimReq{}
	default:
		return fmt.Errorf("signin: unexpected message %d", msgID)
	}
	if err := proto.Unmarshal(body, req); err != nil {
		return m.failure(ctx, msgID, err)
	}
	if ctx.State == nil || !ctx.CacheEnabled {
		return m.failure(ctx, msgID, fmt.Errorf("signin state unavailable"))
	}
	// 每个请求只采样一次，防止处理途中跨周零点。
	today := weekdayOf(m.now())
	d, err := load(ctx.State)
	if err != nil {
		return m.failure(ctx, msgID, err)
	}
	if msgID == MsgInfoReq {
		return reply(ctx, MsgInfoResp, &gamev1.SigninInfoResp{
			Today:          today,
			ClaimedToday:   d.has(today),
			ClaimedDays:    d.Days,
			ClaimedRewards: d.Rewards,
		})
	}
	return m.claim(ctx, d, today)
}

// claim 领取今天对应的奖励；已领过只回状态，不重复发奖。
func (m *Module) claim(ctx *module.Context, d *Data, today int32) error {
	grants, err := rewardsForDay(today)
	if err != nil {
		return reply(ctx, MsgClaimResp, &gamev1.SigninClaimResp{Code: codeConfig, Day: today, ClaimedDays: d.Days, ClaimedRewards: d.Rewards})
	}
	if d.has(today) {
		return reply(ctx, MsgClaimResp, &gamev1.SigninClaimResp{
			Already: true, Day: today, Rewards: protoRewards(grants),
			ClaimedDays: d.Days, ClaimedRewards: d.Rewards,
		})
	}
	for _, g := range grants {
		current := ctx.State.GetItemCount(g.ItemID)
		if current < 0 || g.Count > math.MaxInt32-current {
			return reply(ctx, MsgClaimResp, &gamev1.SigninClaimResp{Code: codeBagLimit, Day: today, ClaimedDays: d.Days, ClaimedRewards: d.Rewards})
		}
	}
	if !ctx.State.CanHold(grants) {
		return reply(ctx, MsgClaimResp, &gamev1.SigninClaimResp{Code: codeBagFull, Day: today, ClaimedDays: d.Days, ClaimedRewards: d.Rewards})
	}
	d.add(today, grants)
	if err := save(ctx.State, ctx.State.PlayerID(), d); err != nil {
		return m.failure(ctx, MsgClaimReq, err)
	}
	for _, g := range grants {
		ctx.State.AddItem(g.ItemID, g.Count)
	}
	ctx.State.MarkDirty("bag")
	if err := reply(ctx, MsgClaimResp, &gamev1.SigninClaimResp{
		Day: today, Rewards: protoRewards(grants),
		ClaimedDays: d.Days, ClaimedRewards: d.Rewards,
	}); err != nil {
		return err
	}
	changes := make([]*gamev1.ItemChange, 0, len(grants))
	for _, g := range grants {
		changes = append(changes, &gamev1.ItemChange{ItemUid: int64(g.ItemID), ItemId: g.ItemID, Count: int32(ctx.State.GetItemCount(g.ItemID)), Change: 1})
	}
	return reply(ctx, 3010, &gamev1.BagChangeNotify{Changes: changes})
}

func (m *Module) failure(ctx *module.Context, msgID uint32, err error) error {
	if m.logger != nil {
		m.logger.Warn("signin request failed", "player_id", ctx.State.PlayerID(), "msg_id", msgID, "err", err)
	}
	if msgID == MsgInfoReq {
		return reply(ctx, MsgInfoResp, &gamev1.SigninInfoResp{Code: codeSystem})
	}
	return reply(ctx, MsgClaimResp, &gamev1.SigninClaimResp{Code: codeSystem})
}

// has 今天是否已领取。
func (d *Data) has(day int32) bool {
	for _, v := range d.Days {
		if v == day {
			return true
		}
	}
	return false
}

// add 登记领取事实：天数保持升序落库，奖励按道具累加。
func (d *Data) add(day int32, grants []module.ItemGrant) {
	if d.Rewards == nil {
		d.Rewards = make(map[int32]int64, len(grants))
	}
	if !d.has(day) {
		d.Days = append(d.Days, day)
		sort.Slice(d.Days, func(i, j int) bool { return d.Days[i] < d.Days[j] })
	}
	for _, g := range grants {
		d.Rewards[g.ItemID] += g.Count
	}
}

// weekdayOf 返回北京时间下的周几：1=周一…7=周日。
func weekdayOf(now time.Time) int32 {
	wd := now.In(businessZone).Weekday()
	if wd == time.Sunday {
		return 7
	}
	return int32(wd)
}

// load 读取状态；从未签到过返回空结构，天数初始化为空切片以便落库成 "[]"。
func load(state writebehind.ExtState) (*Data, error) {
	row, err := writebehind.LoadExt[model.PlayerSignin](state, stateKey)
	if err != nil {
		return nil, err
	}
	if row == nil {
		return &Data{Days: []int32{}, Rewards: map[int32]int64{}}, nil
	}
	return decode(row)
}

// decode 把表行的两个 JSON 列解成业务视图。
func decode(row *model.PlayerSignin) (*Data, error) {
	d := &Data{Days: []int32{}, Rewards: map[int32]int64{}}
	if row.ClaimedDays != "" {
		if err := json.Unmarshal([]byte(row.ClaimedDays), &d.Days); err != nil {
			return nil, fmt.Errorf("decode signin days: %w", err)
		}
	}
	if row.ClaimedRewards != "" {
		if err := json.Unmarshal([]byte(row.ClaimedRewards), &d.Rewards); err != nil {
			return nil, fmt.Errorf("decode signin rewards: %w", err)
		}
	}
	return d, nil
}

// save 把业务视图编码成表行写回 Ext 槽位，落库调度由框架负责。
func save(state writebehind.ExtState, playerID int64, d *Data) error {
	days, err := json.Marshal(d.Days)
	if err != nil {
		return fmt.Errorf("encode signin days: %w", err)
	}
	rewards, err := json.Marshal(d.Rewards)
	if err != nil {
		return fmt.Errorf("encode signin rewards: %w", err)
	}
	return writebehind.SaveExt(state, stateKey, &model.PlayerSignin{
		PlayerId:       playerID,
		ClaimedDays:    string(days),
		ClaimedRewards: string(rewards),
	})
}

func reply(ctx *module.Context, id uint32, msg proto.Message) error {
	raw, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	return ctx.Reply(id, raw)
}

func protoRewards(grants []module.ItemGrant) []*gamev1.SigninReward {
	out := make([]*gamev1.SigninReward, 0, len(grants))
	for _, g := range grants {
		out = append(out, &gamev1.SigninReward{ItemId: g.ItemID, Count: g.Count})
	}
	return out
}

// rewardsForDay 汇总配置里某一天的奖励（同一天可配多种道具）。
func rewardsForDay(day int32) ([]module.ItemGrant, error) {
	counts := make(map[int32]int64)
	for _, cfg := range gamedata.SigninRewardCfgList() {
		if cfg.Day != int64(day) {
			continue
		}
		if cfg.ItemId <= 0 || cfg.ItemId > math.MaxInt32 || cfg.Count <= 0 || cfg.Count > math.MaxInt32 {
			return nil, fmt.Errorf("invalid signin reward config %d", cfg.Id)
		}
		if _, ok := gamedata.GetItemCfg(cfg.ItemId); !ok {
			return nil, fmt.Errorf("signin item %d not found", cfg.ItemId)
		}
		id := int32(cfg.ItemId)
		if cfg.Count > math.MaxInt32-counts[id] {
			return nil, fmt.Errorf("signin reward count overflow")
		}
		counts[id] += cfg.Count
	}
	if len(counts) == 0 {
		return nil, fmt.Errorf("signin day %d has no reward", day)
	}
	out := make([]module.ItemGrant, 0, len(counts))
	for id, count := range counts {
		out = append(out, module.ItemGrant{ItemID: id, Count: count})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ItemID < out[j].ItemID })
	return out, nil
}
