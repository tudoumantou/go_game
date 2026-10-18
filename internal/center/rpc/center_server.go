// Package rpc 中心服 gRPC 服务（CenterService.Report，设计文档 9.2 / 4.6）
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
type Handler interface {
	// OnMatchRequest 处理匹配请求（可能立即回 MatchResult）。
	OnMatchRequest(nodeID string, req *internalv2.MatchRequest)
	// OnRelayRequest 处理中转请求。
	OnRelayRequest(nodeID string, req *internalv2.RelayRequest)
	// OnRegister 节点注册（返回需要回给节点的初始指令，如 GroupUpdate）。
	OnRegister(node *registry.Node) []*internalv2.CenterCommand
}

// Server CenterService 实现。
type Server struct {
	internalv2.UnimplementedCenterServiceServer

	logger *log.Logger
	reg    *registry.Registry
	h      Handler

	// ranking / stats 由 server 侧注入（避免本包依赖 Redis）
	rankQuery  RankQuerier
	statsQuery StatsQuerier
}

// RankQuerier 排行榜查询（server 注入）。
type RankQuerier interface {
	TopN(rankType, season string, n int32) []*internalv2.RankEntry
	Around(rankType, season string, playerID int64, n int32) []*internalv2.RankEntry
}

// StatsQuerier 统计查询（server 注入）。
type StatsQuerier interface {
	Query(groupID, feature string, from, to int64) []*internalv2.FeatureReport
}

// NewServer 创建 CenterService。
func NewServer(reg *registry.Registry, h Handler, logger *log.Logger) *Server {
	return &Server{reg: reg, h: h, logger: logger}
}

// SetRankQuerier 注入排行榜查询实现。
func (s *Server) SetRankQuerier(q RankQuerier) { s.rankQuery = q }

// SetStatsQuerier 注入统计查询实现。
func (s *Server) SetStatsQuerier(q StatsQuerier) { s.statsQuery = q }

// Report 实现 bidi 流：一个节点一条。
func (s *Server) Report(stream internalv2.CenterService_ReportServer) error {
	// 首帧必须是 Register（4.6：连接后第一条，必发）
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
		sendCh: make(chan *internalv2.CenterCommand, 512),
		done:   make(chan struct{}),
	}
	s.reg.Register(node, sess)
	defer s.reg.Unregister(sess.nodeID)

	// 下行泵（gRPC 流不允许并发 Send）
	writeErr := make(chan error, 1)
	go func() { writeErr <- sess.writeLoop(stream) }()

	// 注册回执：GroupUpdate + 可能的其它初始指令
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

		select {
		case werr := <-writeErr:
			return werr
		default:
		}
	}
}

// RankWriter 排行榜写入能力（可选扩展）。
type RankWriter interface {
	Write(rep *internalv2.RankReport)
}

// StatsWriter 统计写入能力（可选扩展）。
type StatsWriter interface {
	Write(rep *internalv2.FeatureReport)
}

// QueryGlobalRank 实现 GM 查询。
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

func defaultType(t string) string {
	if t == "" {
		return registry.TypeGame
	}
	return t
}
