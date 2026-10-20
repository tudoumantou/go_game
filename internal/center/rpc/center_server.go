// Package rpc 中心服 gRPC 服务（CenterService.Report，设计文档 9.2 / 4.6）
//
// 协议骨架：Report 是 bidi 流——每个游戏服/跨服节点一条，上行 Register/
// Heartbeat/Match/Relay/Rank/Feature，下行 CenterCommand（GroupUpdate/
// MatchResult/RelayPush/Kick）。
//
// 协作关系：本包只依赖 registry（节点目录），排行榜/统计能力以接口
// （RankQuerier/RankSearcher/StatsQuerier）由 server 侧注入——避免 rpc 包
// 反向依赖 Redis；匹配/中转的业务处理经 Handler 接口回调 server，
// 协议层与业务层解耦，两边可独立单测。
package rpc

import (
	"context"
	"fmt"
	"io"
	"sync"

	"game_study/internal/center/registry"
	"game_study/pkg/log"

	internalv2 "game_study/pkg/pb/internalv2"
)

// Handler 业务处理器（匹配/中转发往 server 侧）。
//
// 接口注入而非直接引用 server 包：打破 rpc ↔ server 的循环依赖
// （server 要创建 rpc.Server，rpc 又要调 server 的逻辑）。
// 方法只收「已解析的最小信息」（nodeID + 请求体），不暴露流对象。
type Handler interface {
	// OnMatchRequest 处理匹配请求（可能立即回 MatchResult）。
	OnMatchRequest(nodeID string, req *internalv2.MatchRequest)
	// OnRelayRequest 处理中转请求。
	OnRelayRequest(nodeID string, req *internalv2.RelayRequest)
	// OnRegister 节点注册（返回需要回给节点的初始指令，如 GroupUpdate）。
	OnRegister(node *registry.Node) []*internalv2.CenterCommand
}

// Server CenterService 实现。
//
// 嵌入 UnimplementedCenterServiceServer：proto 生成的占位实现，
// 保证未来 pb 加新方法时本服务自动满足接口（返回 Unimplemented 而非
// 编译失败）。三个查询接口在启动后经 Set* 注入——构造时还不知道
// Redis 是否可用（见 center.Server.Start 的顺序）。
type Server struct {
	internalv2.UnimplementedCenterServiceServer

	logger *log.Logger
	reg    *registry.Registry
	h      Handler

	// ranking / stats 由 server 侧注入（避免本包依赖 Redis）
	// 三者初始为 nil：查询方法对 nil 做显式降级（返回 Code=1），
	// 而不是 panic——未配 Redis 的部署下 GM 查询应该得到「不可用」而非崩溃
	rankQuery  RankQuerier
	rankSearch RankSearcher
	statsQuery StatsQuerier
}

// RankQuerier 排行榜查询（server 注入）。
type RankQuerier interface {
	TopN(rankType, season string, n int32) []*internalv2.RankEntry
	Around(rankType, season string, playerID int64, n int32) []*internalv2.RankEntry
}

// RankSearcher 玩家侧组合查询（*rank.Service 实现，server 注入）。
//
// 单独一个接口而不是扩 RankQuerier：GM 查询与玩家查询的返回形状不同，
// 拆开后现有实现与测试不受影响。
//
// 这是接口隔离原则的实践：注入方只需要「玩家一屏查询」一个方法时，
// 不必实现 TopN/Around 的完整面，mock 也更小。
type RankSearcher interface {
	Query(rankType, season string, playerID int64, topN, aroundN int32) *internalv2.QueryRankResp
}

// StatsQuerier 统计查询（server 注入）。
type StatsQuerier interface {
	Query(groupID, feature string, from, to int64) []*internalv2.FeatureReport
}

// NewServer 创建 CenterService。
// 此时查询接口全部为 nil，由 server 在 Start 内按 Redis 可用性注入——
// 分两步是因为 Redis 连接可能失败，而 rpc.Server 的注册必须先于监听。
func NewServer(reg *registry.Registry, h Handler, logger *log.Logger) *Server {
	return &Server{reg: reg, h: h, logger: logger}
}

// SetRankQuerier 注入排行榜查询实现。
// Set* 系列在 Start 内、监听启动前调用：启动后注入会有数据竞争
// （gRPC handler 已在并发读这些字段）；当前用法下单 goroutine 赋值，
// 靠启动顺序保证安全，未加锁。
func (s *Server) SetRankQuerier(q RankQuerier) { s.rankQuery = q }

// SetRankSearcher 注入玩家侧榜单查询实现。
func (s *Server) SetRankSearcher(q RankSearcher) { s.rankSearch = q }

// SetStatsQuerier 注入统计查询实现。
func (s *Server) SetStatsQuerier(q StatsQuerier) { s.statsQuery = q }

// Report 实现 bidi 流：一个节点一条。
//
// 生命周期：首帧 Register → 注册 + 起下行泵 → 回执初始指令 → 循环收帧
// 分发 → 流断开摘除。任何一步出错都返回，由 defer 保证 Unregister。
//
// 两个 goroutine 的分工：Report 本体只收上行；writeLoop 单独写下行——
// gRPC 的 stream.Send 不允许并发调用，单写 goroutine 是协议硬性要求。
func (s *Server) Report(stream internalv2.CenterService_ReportServer) error {
	// 首帧必须是 Register（4.6：连接后第一条，必发）
	// 先注册再接客：没有 Register 就没有 nodeID，后续所有帧无法归属
	first, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("recv register: %w", err)
	}
	reg := first.GetRegister()
	if reg == nil || reg.NodeId == "" {
		return fmt.Errorf("first frame must be NodeReport{register}")
	}

	nodeType := reg.NodeType
	if nodeType == "" {
		nodeType = registry.TypeGame
	}
	node := &registry.Node{
		ID:      reg.NodeId,
		Type:    nodeType,
		Addr:    reg.Addr,
		GroupID: reg.GroupId,
		StartAt: reg.StartAt,
	}

	sess := &session{
		nodeID: reg.NodeId,
		// 512 缓冲：下行指令（MatchResult/RelayPush）突发时给写泵余量，
		// 满了走丢弃策略（见 session.Send）而不是阻塞业务 goroutine
		sendCh: make(chan *internalv2.CenterCommand, 512),
		done:   make(chan struct{}),
	}
	s.reg.Register(node, sess)
	// defer 配对 Register：流断开（含 panic 后的栈展开）必经摘除，
	// 不会留下「注册表有节点、流已死」的僵尸
	defer s.reg.Unregister(sess.nodeID)

	// 下行泵（gRPC 流不允许并发 Send）
	// 错误经 writeErr 传回主循环：写失败（流已断）要让收帧循环也退出，
	// 否则它会永远阻塞在 Recv 上等一个不会再来的人
	writeErr := make(chan error, 1)
	go func() { writeErr <- sess.writeLoop(stream) }()

	// 注册回执：GroupUpdate + 可能的其它初始指令
	// 回执走 sendCh 而不是直接 stream.Send——发送权在 writeLoop，
	// 这里并发 Send 会踩 gRPC 的「不允许并发写」红线
	for _, cmd := range s.h.OnRegister(node) {
		_ = sess.Send(cmd)
	}

	for {
		rep, err := stream.Recv()
		if err != nil {
			if err != io.EOF && stream.Context().Err() == nil {
				s.logger.Warn("center report recv error", "node", sess.nodeID, "err", err)
			}
			sess.close()
			return nil
		}

		switch body := rep.Body.(type) {
		case *internalv2.NodeReport_Heartbeat:
			hb := body.Heartbeat
			if !s.reg.Heartbeat(hb.GetNodeId(), int(hb.GetPlayerCount()), int(hb.GetCpuLoad()),
				int(hb.GetMemLoad()), hb.GetMsgQps(), hb.GetVersion()) {
				// 未注册（中心服重启过）→ 要求补发 Register
				// 只告警不踢：节点下一拍心跳会补发，协议自愈；
				// 这里踢断反而制造不必要的重连风暴
				s.logger.Warn("heartbeat from unknown node", "node", hb.GetNodeId())
			}

		case *internalv2.NodeReport_Match:
			if m := body.Match; m != nil {
				s.h.OnMatchRequest(sess.nodeID, m)
			}

		case *internalv2.NodeReport_Relay:
			if r := body.Relay; r != nil {
				s.h.OnRelayRequest(sess.nodeID, r)
			}

		case *internalv2.NodeReport_Rank:
			// 排行榜写入由 server 侧处理（本包不引 Redis）
			// 类型断言而不是直接持有 RankWriter：没注入时（无 Redis）
			// 上报静默丢弃，rpc 层不需要知道「为什么没有 rank」
			if r := body.Rank; r != nil && s.rankQuery != nil {
				if w, ok := s.rankQuery.(RankWriter); ok {
					w.Write(r)
				}
			}

		case *internalv2.NodeReport_Feature:
			if f := body.Feature; f != nil && s.statsQuery != nil {
				if w, ok := s.statsQuery.(StatsWriter); ok {
					w.Write(f)
				}
			}

		case *internalv2.NodeReport_Register:
			// 重连后补发 Register：更新信息但不重建会话
			// 复用现有 session（sendCh/done 不重建）：流一直是同一条，
			// 重建会话会让注册表里的 Sender 指向已关闭的 channel
			if r := body.Register; r != nil {
				s.reg.Register(&registry.Node{
					ID:      r.NodeId,
					Type:    defaultType(r.NodeType),
					Addr:    r.Addr,
					GroupID: r.GroupId,
					StartAt: r.StartAt,
				}, sess)
			}
		}

		// 非阻塞探写泵错误：default 分支让「没有错误」时零等待划过；
		// 有错误说明流已断，立刻返回让 defer 走摘除流程
		select {
		case werr := <-writeErr:
			return werr
		default:
		}
	}
}

// RankWriter 排行榜写入能力（可选扩展）。
//
// 「可选扩展」的实现手法：定义窄接口 + 对已注入的宽接口做类型断言。
// rank.Service 实现它，但 rpc 包的主注入点是 RankQuerier（查询面）——
// 同一实现同时满足两个接口，各取所需，互不知道对方存在。
type RankWriter interface {
	Write(rep *internalv2.RankReport)
}

// StatsWriter 统计写入能力（可选扩展）。
type StatsWriter interface {
	Write(rep *internalv2.FeatureReport)
}

// QueryGlobalRank 实现 GM 查询。
// 服务端流：逐条 Send 而不是攒够一次发——榜单可能很大（上限 100），
// 流式让 GM 工具能边收边渲染；AroundPlayerId 非 0 时语义切换为周边查询。
func (s *Server) QueryGlobalRank(req *internalv2.QueryRankReq, stream internalv2.CenterService_QueryGlobalRankServer) error {
	if s.rankQuery == nil {
		return fmt.Errorf("rank not available")
	}
	var list []*internalv2.RankEntry
	if req.GetAroundPlayerId() != 0 {
		list = s.rankQuery.Around(req.GetRankType(), req.GetSeason(), req.GetAroundPlayerId(), req.GetTopN())
	} else {
		list = s.rankQuery.TopN(req.GetRankType(), req.GetSeason(), req.GetTopN())
	}
	for _, e := range list {
		if err := stream.Send(e); err != nil {
			return err
		}
	}
	return nil
}

// QueryRank 实现玩家榜单一屏查询（游戏服代理客户端请求）。
//
// 与 QueryGlobalRank 的区别：一次返回 Top-N + 我的名次 + 周边，客户端只需一个往返。
//
// 降级返回 Code=1 而非 gRPC error：玩家端对 error 的通用处理是弹
// 「网络错误」，而这里是「榜单服务没开」——用业务码区分，
// 客户端可以展示「排行榜维护中」。aroundN 传 0 让 service 用默认 11。
func (s *Server) QueryRank(_ context.Context, req *internalv2.QueryRankReq) (*internalv2.QueryRankResp, error) {
	if s.rankSearch == nil {
		return &internalv2.QueryRankResp{Code: 1}, nil
	}
	resp := s.rankSearch.Query(req.GetRankType(), req.GetSeason(), req.GetAroundPlayerId(), req.GetTopN(), 0)
	if resp == nil {
		return &internalv2.QueryRankResp{Code: 1}, nil
	}
	return resp, nil
}

// QueryFeatureStats 实现 GM 查询。
func (s *Server) QueryFeatureStats(_ context.Context, req *internalv2.QueryStatsReq) (*internalv2.QueryStatsResp, error) {
	if s.statsQuery == nil {
		return &internalv2.QueryStatsResp{Code: 1}, nil
	}
	return &internalv2.QueryStatsResp{
		Code:    0,
		Reports: s.statsQuery.Query(req.GetGroupId(), req.GetFeature(), req.GetFromTs(), req.GetToTs()),
	}, nil
}

// session 一条 Report 流的下行通道（实现 registry.Sender）。
//
// 三件套：sendCh（有界队列，背压时丢弃）、done（关闭广播）、
// once（保证 close(done) 只一次——重复 close channel 会 panic）。
type session struct {
	nodeID string
	sendCh chan *internalv2.CenterCommand
	done   chan struct{}
	once   sync.Once
}

// NodeID 实现 registry.Sender。
func (s *session) NodeID() string { return s.nodeID }

// SendCommand 实现 registry.Sender（类型断言为 CenterCommand）。
func (s *session) SendCommand(cmd any) error {
	c, ok := cmd.(*internalv2.CenterCommand)
	if !ok {
		return fmt.Errorf("session.SendCommand: want *CenterCommand, got %T", cmd)
	}
	return s.Send(c)
}

// Send 投递一条指令（非阻塞，队列满则丢弃并报错）。
//
// select 三分支的优先级设计：sendCh 可写 → 投递；done 已关 → 报
// 「会话已结束」；都走不通 → 报「队列满」。注意 Go 的 select 在多个
// case 就绪时随机选一个——所以「已关闭 + 队列恰好有坑」时可能投进
// 一条没人收的指令：writeLoop 退出后 sendCh 很快被 GC，无泄漏，
// 语义上等价于丢弃，可接受。
func (s *session) Send(cmd *internalv2.CenterCommand) error {
	select {
	case s.sendCh <- cmd:
		return nil
	case <-s.done:
		return fmt.Errorf("session %s closed", s.nodeID)
	default:
		return fmt.Errorf("session %s send queue full", s.nodeID)
	}
}

func (s *session) close() { s.once.Do(func() { close(s.done) }) }

// writeLoop 单独 goroutine 写流。
//
// 退出条件三选一：流 context 取消（客户端断开/服务端停）、done 关闭
// （Report 主循环结束）、Send 失败（写入错误）。返回的错误经
// Report 里的 writeErr 通道传回主循环，触发整条流的收尾。
func (s *session) writeLoop(stream internalv2.CenterService_ReportServer) error {
	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case <-s.done:
			return nil
		case cmd := <-s.sendCh:
			if err := stream.Send(cmd); err != nil {
				return fmt.Errorf("send to %s: %w", s.nodeID, err)
			}
		}
	}
}

// defaultType 节点类型缺省值（注册帧未填 node_type 时按 game 处理）。
// 历史协议兼容：早期节点不上报类型，默认 game 保持旧行为不变；
// 跨服节点显式上报 cross，不会落到这个默认分支。
func defaultType(t string) string {
	if t == "" {
		return registry.TypeGame
	}
	return t
}
