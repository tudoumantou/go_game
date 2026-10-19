// Package rank 客户端榜单查询（R8.1）。
//
// 游戏服**不持有**榜单数据：查询代理到中心服 Redis ZSet，一次往返拿到
// Top-N + 我的名次 + 周边（中心服 QueryRank）。这样多游戏服看到的榜单天然一致，
// 不需要在节点间同步。
//
// 中心服不可用时回 6701「榜单暂不可用」，而不是回空榜——空榜会让玩家
// 误以为自己没有成绩，也比「稍后重试」更难排查。
package rank

import (
	"context"
	"fmt"
	"time"

	"game_study/internal/game/module"
	"game_study/pkg/codec"
	"game_study/pkg/log"

	gamev1 "game_study/pkg/pb/game/v1"
	"google.golang.org/protobuf/proto"
)

const (
	msgIDListReq  = codec.MsgRankListReq
	msgIDListResp = codec.MsgRankListResp

	codeOK          int32 = 0
	codeBadType     int32 = 6701
	codeUnavailable int32 = 6702

	defaultRankType = "arena_score"
	defaultTopN     = int32(50)
	maxRankTypeLen  = 32
	queryTimeout    = 3 * time.Second
)

// Entry 榜单条目（与中心服协议解耦的中性结构，便于单测注入假实现）。
type Entry struct {
	PlayerID int64
	Score    int64
	Rank     int64
}

// Result 一次查询结果。
type Result struct {
	Season    string
	Entries   []Entry
	Around    []Entry
	OnBoard   bool
	MyRank    int64
	MyScore   int64
	UpdatedAt int64
}

// Querier 榜单查询能力（游戏服由中心服客户端实现；测试注入假实现）。
type Querier interface {
	Query(ctx context.Context, rankType string, playerID int64, topN int32) (*Result, error)
}

// Module 榜单模块。
type Module struct {
	q      Querier
	logger *log.Logger
}

// New 创建模块；q 为 nil 时所有查询回「暂不可用」（未配置中心服的单机环境）。
func New(q Querier, logger *log.Logger) *Module {
	return &Module{q: q, logger: logger}
}

func (*Module) Name() string { return "rank" }

func (*Module) MsgIDs() []uint32 { return []uint32{msgIDListReq} }

func (*Module) Init(*module.Context) error { return nil }

// 榜单是只读查询，不持有玩家状态，也没有需要落库的聚合数据。
func (*Module) OnSave(context.Context) error   { return nil }
func (*Module) OnLogin(*module.Context) error  { return nil }
func (*Module) OnLogout(*module.Context) error { return nil }

func (m *Module) OnMessage(ctx *module.Context, msgID uint32, body []byte) error {
	if msgID != msgIDListReq {
		return fmt.Errorf("rank: unexpected msg %d", msgID)
	}
	return m.onList(ctx, body)
}

func (m *Module) onList(ctx *module.Context, body []byte) error {
	var req gamev1.RankListReq
	if err := proto.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("decode RankListReq: %w", err)
	}

	rankType := req.GetRankType()
	if rankType == "" {
		rankType = defaultRankType
	}
	if len(rankType) > maxRankTypeLen {
		return m.reply(ctx, &gamev1.RankListResp{Code: codeBadType})
	}
	if m.q == nil {
		return m.reply(ctx, &gamev1.RankListResp{Code: codeUnavailable, RankType: rankType})
	}

	qctx, cancel := context.WithTimeout(context.Background(), queryTimeout)
	defer cancel()
	res, err := m.q.Query(qctx, rankType, ctx.State.PlayerID(), defaultTopN)
	if err != nil {
		// 中心服断连/超时：明确告诉客户端稍后重试，不伪造空榜。
		m.logger.Debug("rank query failed", "rank_type", rankType, "err", err)
		return m.reply(ctx, &gamev1.RankListResp{Code: codeUnavailable, RankType: rankType})
	}

	resp := &gamev1.RankListResp{
		Code:      codeOK,
		RankType:  rankType,
		Season:    res.Season,
		Entries:   toProto(res.Entries),
		Around:    toProto(res.Around),
		OnBoard:   res.OnBoard,
		MyRank:    res.MyRank,
		MyScore:   res.MyScore,
		UpdatedAt: res.UpdatedAt,
	}
	return m.reply(ctx, resp)
}

func toProto(entries []Entry) []*gamev1.RankEntry {
	if len(entries) == 0 {
		return nil
	}
	out := make([]*gamev1.RankEntry, 0, len(entries))
	for _, e := range entries {
		// Name 留空：游戏服不持有其他玩家昵称，由客户端按 player_id 补齐。
		out = append(out, &gamev1.RankEntry{
			PlayerId: e.PlayerID,
			Score:    e.Score,
			Rank:     e.Rank,
		})
	}
	return out
}

func (m *Module) reply(ctx *module.Context, resp *gamev1.RankListResp) error {
	raw, err := proto.Marshal(resp)
	if err != nil {
		return fmt.Errorf("marshal RankListResp: %w", err)
	}
	return ctx.Reply(msgIDListResp, raw)
}
