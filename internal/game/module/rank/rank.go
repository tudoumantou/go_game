// Package rank 客户端榜单查询（R8.1）。
//
// 游戏服**不持有**榜单数据：查询代理到中心服 Redis ZSet，一次往返拿到
// Top-N + 我的名次 + 周边（中心服 QueryRank）。这样多游戏服看到的榜单天然一致，
// 不需要在节点间同步。
//
// 中心服不可用时回 6702「榜单暂不可用」，而不是回空榜——空榜会让玩家
// 误以为自己没有成绩，也比「稍后重试」更难排查。
//
// 架构职责：只读查询模块——把 6801 榜单请求翻译成一次中心服 QueryRank，
// 再把结果翻成本模块的中性结构 → proto 回包。
//
// Ext 槽位：无（不持有任何玩家快照状态）。
// 事件订阅：无（不响应业务事件）。
// 配置表关系：无（榜单类型是字符串键，默认 "arena_score"）。
// 懒初始化：不适用（纯代理，无本地行概念）。
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

	// 业务码（0 成功；6701 输入非法；6702 中心服暂不可用——
	// 刻意区别于 0 + 空 entries：让客户端能提示「稍后重试」）
	codeOK          int32 = 0
	codeBadType     int32 = 6701
	codeUnavailable int32 = 6702

	// defaultRankType 客户端不传榜单类型时的兜底（竞技场积分榜）
	defaultRankType = "arena_score"
	// defaultTopN Top-N 大小（我的名次与周边由中心服另算，不占 Top-N 配额）
	defaultTopN = int32(50)
	// maxRankTypeLen 榜单类型字符串长度上限：rankType 会进 Redis key，
	// 预检超长输入既是 key 规范也是防滥用
	maxRankTypeLen = 32
	// queryTimeout 中心服查询超时：榜单是旁路页面，3s 等不到就明确
	// 告诉用户不可用，占着玩家 Actor 的 OnMessage 更久没有意义
	queryTimeout = 3 * time.Second
)

// Entry 榜单条目（与中心服协议解耦的中性结构，便于单测注入假实现）。
type Entry struct {
	// PlayerID 上榜玩家 id（昵称不随榜下发，客户端按 id 补齐）
	PlayerID int64
	// Score 分数（ZSet 的 score，中心服语义）
	Score int64
	// Rank 名次（从 1 开始，由中心服计算）
	Rank int64
}

// Result 一次查询结果。
type Result struct {
	// Season 赛季标识（客户端展示「第 N 赛季」，中心服当前值）
	Season string
	// Entries Top-N 榜单
	Entries []Entry
	// Around 我的周边（名次前后若干条，玩家未上榜时为空）
	Around []Entry
	// OnBoard 我是否在 Top-N 内（在榜则无需再看 Around）
	OnBoard bool
	// MyRank 我的名次（未上榜为 0）
	MyRank int64
	// MyScore 我的分数（未上榜为 0）
	MyScore int64
	// UpdatedAt 榜单数据更新时间（Unix 秒，客户端展示「更新于」）
	UpdatedAt int64
}

// Querier 榜单查询能力（游戏服由中心服客户端实现；测试注入假实现）。
//
// 单方法接口 + 中性 Result：模块不 import 中心服 proto，
// 中心服协议变更只影响组装层的实现，不影响本模块与它的测试。
type Querier interface {
	Query(ctx context.Context, rankType string, playerID int64, topN int32) (*Result, error)
}

// Module 榜单模块。
type Module struct {
	// q 中心服查询代理；nil 表示未配置中心服（单机环境），全部查询回 6702
	q      Querier
	logger *log.Logger
}

// New 创建模块；q 为 nil 时所有查询回「暂不可用」（未配置中心服的单机环境）。
func New(q Querier, logger *log.Logger) *Module {
	return &Module{q: q, logger: logger}
}

// 以下方法用匿名接收者 (*Module)：它们不触碰 m 的任何字段，
// 匿名接收者是 Go 惯用写法——向读者声明「与实例状态无关」，
// 同时避免 m 声明了不用的编译告警之外的噪音。
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
	// 长度预检在打到中心服之前：超长 rankType 既是无效输入也是潜在滥用
	// （会变成 Redis key 的一部分），本地挡掉最便宜
	if len(rankType) > maxRankTypeLen {
		return m.reply(ctx, &gamev1.RankListResp{Code: codeBadType})
	}
	if m.q == nil {
		return m.reply(ctx, &gamev1.RankListResp{Code: codeUnavailable, RankType: rankType})
	}

	// WithTimeout 给中心服 RPC 加超时；cancel 必须 defer——
	// Go  context 契约：WithTimeout 创建的 ctx 不 cancel 会一直占用
	// 内部计时器直到超时才释放，忘 cancel 是经典的 context 泄漏。
	// 用 Background 而非请求 ctx：榜单查询是受理后的代理动作，
	// 请求 ctx 在回包后即取消，会误杀查询。
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

// toProto 中性结构 → proto 条目。
//
// len==0 返回 nil 而非空切片：proto3 序列化下 nil 不出现在 wire 上，
// 省几字节；客户端判空用 len()，nil 与空语义等价。
// make(..., 0, len) 预分配避免 append 增长复制。
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

// reply 序列化并回包（marshal 失败用 %w 包装，保留错误链）。
func (m *Module) reply(ctx *module.Context, resp *gamev1.RankListResp) error {
	raw, err := proto.Marshal(resp)
	if err != nil {
		return fmt.Errorf("marshal RankListResp: %w", err)
	}
	return ctx.Reply(msgIDListResp, raw)
}
