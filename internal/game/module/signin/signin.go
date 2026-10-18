// Package signin 实现角色独立、北京时间零点切换的七个自然日签到。
package signin

import (
	"context"
	"fmt"
	"math"
	"sort"
	"time"

	"game_study/internal/game/module"
	"game_study/pkg/gamedata"
	"game_study/pkg/log"
	gamev1 "game_study/pkg/pb/game/v1"
	"google.golang.org/protobuf/proto"
)

const (
	MsgInfoReq   uint32 = 6601
	MsgInfoResp  uint32 = 6602
	MsgClaimReq  uint32 = 6603
	MsgClaimResp uint32 = 6604
	codeSystem   int32  = 1
	codeConfig   int32  = 6601
	codeBagLimit int32  = 6602
)

type Module struct {
	repo   *Repo
	logger *log.Logger
	now    func() time.Time
}

// New 的可选时钟用于验证跨日边界；生产默认使用服务器时间。
func New(repo *Repo, logger *log.Logger, clock ...func() time.Time) *Module {
	m := &Module{repo: repo, logger: logger, now: time.Now}
	if len(clock) > 0 && clock[0] != nil {
		m.now = clock[0]
	}
	return m
}

func (*Module) Name() string     { return "signin" }
func (*Module) MsgIDs() []uint32 { return []uint32{MsgInfoReq, MsgClaimReq} }
func (*Module) Init(*module.Context) error {
	for day := int32(1); day <= 7; day++ {
		if _, err := rewardsForDay(day, gamedata.SigninRewardCfgList); err != nil {
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
	state, ok := ctx.State.(module.SigninState)
	if m.repo == nil || !ok || !ctx.CacheEnabled {
		return m.failure(ctx, msgID, fmt.Errorf("signin repository unavailable"))
	}
	today := businessDay(m.now()) // 每个请求只采样一次，防止处理途中跨零点。
	v, err := m.repo.Load(state, today)
	if err != nil {
		return m.failure(ctx, msgID, err)
	}
	if msgID == MsgInfoReq {
		return reply(ctx, MsgInfoResp, &gamev1.SigninInfoResp{State: render(v, today)})
	}
	if stored, exists := v.Claims[today]; exists {
		return reply(ctx, MsgClaimResp, &gamev1.SigninClaimResp{Already: true, Rewards: protoRewards(stored), State: render(v, today)})
	}
	if v.Day < 1 || v.Day > 7 {
		return m.failure(ctx, msgID, fmt.Errorf("server business date precedes signin anchor"))
	}
	grants, err := rewardsForDay(v.Day, gamedata.SigninRewardCfgList)
	if err != nil {
		return reply(ctx, MsgClaimResp, &gamev1.SigninClaimResp{Code: codeConfig, State: render(v, today)})
	}
	for _, g := range grants {
		if ctx.State.GetItemCount(g.ItemID) < 0 || g.Count > math.MaxInt32-ctx.State.GetItemCount(g.ItemID) {
			return reply(ctx, MsgClaimResp, &gamev1.SigninClaimResp{Code: codeBagLimit, State: render(v, today)})
		}
	}
	grants, already, err := m.repo.Claim(state, today, grants)
	if err != nil {
		return m.failure(ctx, msgID, err)
	}
	if !already {
		for _, g := range grants {
			ctx.State.AddItem(g.ItemID, g.Count)
		}
		ctx.State.MarkDirty("bag")
	}
	v.Claims[today] = grants
	if err := reply(ctx, MsgClaimResp, &gamev1.SigninClaimResp{Already: already, Rewards: protoRewards(grants), State: render(v, today)}); err != nil {
		return err
	}
	if !already {
		changes := make([]*gamev1.ItemChange, 0, len(grants))
		for _, g := range grants {
			changes = append(changes, &gamev1.ItemChange{ItemUid: int64(g.ItemID), ItemId: g.ItemID, Count: int32(ctx.State.GetItemCount(g.ItemID)), Change: 1})
		}
		return reply(ctx, 3010, &gamev1.BagChangeNotify{Changes: changes})
	}
	return nil
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

func render(v *view, today int64) *gamev1.SigninState {
	_, signed := v.Claims[today]
	s := &gamev1.SigninState{Today: v.Day, SignedToday: signed, CycleStart: dayTime(v.Start).Unix(), CycleEnd: dayTime(v.Start + 7).Unix(), NextResetAt: dayTime(today + 1).Unix()}
	for day := int32(1); day <= 7; day++ {
		date := v.Start + int64(day) - 1
		grants, claimed := v.Claims[date]
		if !claimed {
			grants, _ = rewardsForDay(day, gamedata.SigninRewardCfgList)
		}
		s.Days = append(s.Days, &gamev1.SigninDay{Day: day, Rewards: protoRewards(grants), Claimed: claimed, Missed: date < today && !claimed})
	}
	return s
}

func rewardsForDay(day int32, configs []gamedata.SigninRewardCfg) ([]module.ItemGrant, error) {
	counts := make(map[int32]int64)
	for _, cfg := range configs {
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
