// internal/game/center/client.go —— 游戏服 → 中心服的 Report 流（设计文档 9.2）
//
// 一条 Report 流既是上报通道（注册/心跳/匹配/中转/统计），也是指令通道
// （MatchResult / RelayPush / GroupUpdate / KickNode 下发）。
//
// 断流后指数退避重连（1s→2s→4s…→30s 封顶），重连后补发 Register + 全量状态（9.2）。
package center

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"game_study/pkg/log"

	internalv2 "game_study/pkg/pb/internalv2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// 重连退避参数（9.2）。
const (
	minBackoff = 1 * time.Second
	maxBackoff = 30 * time.Second
)

// Handler 中心服下行指令处理器。
type Handler interface {
	// OnMatchResult 匹配成功（分配了跨服房间）。
	OnMatchResult(res *internalv2.MatchResult)
	// OnRelayPush 收到其它节点的中转消息。
	OnRelayPush(push *internalv2.RelayPush)
	// OnGroupUpdate 分组变更（组内游戏服/跨服地址变化）。
	OnGroupUpdate(g *internalv2.GroupUpdate)
	// OnKick 中心服要求本节点重连（滚动重启）。
	OnKick(k *internalv2.KickNode)
	// OnConfigPush 配置热更新。
	OnConfigPush(c *internalv2.ConfigPush)
}

// Client 中心服客户端。
type Client struct {
	selfID   string
	nodeType string
	addr     string
	groupID  string
	h        Handler
	logger   *log.Logger

	mu     sync.RWMutex
	stream internalv2.CenterService_ReportClient
	conn   *grpc.ClientConn

	ready     chan struct{}
	readyOnce sync.Once
	stopCh    chan struct{}
	stopOnce  sync.Once

	// 心跳数据（由业务侧更新）
	playerNum atomic.Int32
	qps       atomic.Int64
	version   string
}

// NewClient 创建客户端。
func NewClient(selfID, addr string, h Handler, logger *log.Logger) *Client {
	return &Client{
		selfID:   selfID,
		nodeType: "game",
		addr:     addr,
		h:        h,
		logger:   logger,
		ready:    make(chan struct{}),
		stopCh:   make(chan struct{}),
		version:  "1.0.0",
	}
}

// SetNodeType 设置节点类型（game / cross）。
func (c *Client) SetNodeType(t string) { c.nodeType = t }

// SetGroup 设置分组（空表示由中心服分配）。
func (c *Client) SetGroup(g string) { c.groupID = g }

// SetPlayerNum 更新在线玩家数（心跳上报）。
func (c *Client) SetPlayerNum(n int) { c.playerNum.Store(int32(n)) }

// SetQPS 更新消息 QPS。
func (c *Client) SetQPS(q int64) { c.qps.Store(q) }

// Start 建流并常驻重连（阻塞至 ctx 取消或 Stop）。
func (c *Client) Start(ctx context.Context) {
	backoff := minBackoff
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.stopCh:
			return
		default:
		}

		err := c.runOnce(ctx)
		if err != nil && ctx.Err() == nil {
			c.logger.Warn("center report stream broken",
				"center", c.addr, "backoff", backoff.String(), "err", err)
		}

		select {
		case <-ctx.Done():
			return
		case <-c.stopCh:
			return
		case <-time.After(backoff):
		}
		if backoff < maxBackoff {
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}
}

// runOnce 建一次流并跑到断开。
func (c *Client) runOnce(ctx context.Context) error {
	conn, err := grpc.NewClient(c.addr,
		grpc.WithTransportCredentials(insecureCreds()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(4<<20)),
	)
	if err != nil {
		return fmt.Errorf("dial center %s: %w", c.addr, err)
	}
	defer func() { _ = conn.Close() }()

	client := internalv2.NewCenterServiceClient(conn)
	stream, err := client.Report(ctx)
	if err != nil {
		return fmt.Errorf("open report stream: %w", err)
	}

	// 首帧必须 Register（9.2）
	if err := stream.Send(&internalv2.NodeReport{
		Body: &internalv2.NodeReport_Register{Register: &internalv2.NodeRegister{
			NodeId:   c.selfID,
			NodeType: c.nodeType,
			Addr:     c.addr,
			GroupId:  c.groupID,
			StartAt:  time.Now().Unix(),
		}},
	}); err != nil {
		return fmt.Errorf("send register: %w", err)
	}

	c.mu.Lock()
	c.stream = stream
	c.conn = conn
	c.mu.Unlock()

	c.readyOnce.Do(func() { close(c.ready) })
	c.logger.Info("center report stream established", "center", c.addr, "node", c.selfID)

	// 心跳 + 下行接收
	hbDone := make(chan struct{})
	go func() { c.heartbeatLoop(ctx, stream); close(hbDone) }()

	for {
		cmd, err := stream.Recv()
		if err != nil {
			c.mu.Lock()
			if c.stream == stream {
				c.stream = nil
			}
			c.mu.Unlock()
			return err
		}
		c.dispatch(cmd)
	}
}

// heartbeatLoop 周期心跳（间隔取服务端期望值的一半，容忍一次丢失）。
func (c *Client) heartbeatLoop(ctx context.Context, stream internalv2.CenterService_ReportClient) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.stopCh:
			return
		case <-ticker.C:
			rep := &internalv2.NodeReport{
				Body: &internalv2.NodeReport_Heartbeat{Heartbeat: &internalv2.NodeHeartbeat{
					NodeId:      c.selfID,
					PlayerCount: c.playerNum.Load(),
					MsgQps:      c.qps.Load(),
					Version:     c.version,
				}},
			}
			if err := stream.Send(rep); err != nil {
				return
			}
		}
	}
}

// dispatch 分发下行指令。
func (c *Client) dispatch(cmd *internalv2.CenterCommand) {
	if cmd == nil || c.h == nil {
		return
	}
	switch body := cmd.Body.(type) {
	case *internalv2.CenterCommand_MatchResult:
		if body.MatchResult != nil {
			c.h.OnMatchResult(body.MatchResult)
		}
	case *internalv2.CenterCommand_RelayPush:
		if body.RelayPush != nil {
			c.h.OnRelayPush(body.RelayPush)
		}
	case *internalv2.CenterCommand_Group:
		if body.Group != nil {
			c.h.OnGroupUpdate(body.Group)
		}
	case *internalv2.CenterCommand_Kick:
		if body.Kick != nil {
			c.h.OnKick(body.Kick)
		}
	case *internalv2.CenterCommand_Config:
		if body.Config != nil {
			c.h.OnConfigPush(body.Config)
		}
	}
}

// Send 上报一条消息（流未就绪返回错误，调用方决定是否降级）。
func (c *Client) Send(rep *internalv2.NodeReport) error {
	c.mu.RLock()
	stream := c.stream
	c.mu.RUnlock()
	if stream == nil {
		return fmt.Errorf("center stream not ready")
	}
	return stream.Send(rep)
}

// SendMatch 上报匹配请求。
func (c *Client) SendMatch(playerID int64, mode string, elo int32, snapshot []byte) error {
	return c.Send(&internalv2.NodeReport{
		Body: &internalv2.NodeReport_Match{Match: &internalv2.MatchRequest{
			NodeId:   c.selfID,
			PlayerId: playerID,
			Mode:     mode,
			Snapshot: snapshot,
			Elo:      elo,
		}},
	})
}

// SendRelay 上报中转请求。
func (c *Client) SendRelay(relayID, toNode string, broadcast bool, groupID string, payload []byte) error {
	return c.Send(&internalv2.NodeReport{
		Body: &internalv2.NodeReport_Relay{Relay: &internalv2.RelayRequest{
			RelayId:   relayID,
			FromNode:  c.selfID,
			ToNode:    toNode,
			Broadcast: broadcast,
			GroupId:   groupID,
			Payload:   payload,
		}},
	})
}

// SendRank 上报排行分数；bizID 非空时中心服按它去重（同一次结算重投不重复加分）。
func (c *Client) SendRank(rankType string, playerID, score int64, season, bizID string) error {
	return c.Send(&internalv2.NodeReport{
		Body: &internalv2.NodeReport_Rank{Rank: &internalv2.RankReport{
			NodeId:   c.selfID,
			RankType: rankType,
			PlayerId: playerID,
			Score:    score,
			Season:   season,
			BizId:    bizID,
		}},
	})
}

// QueryRank 查询榜单（Top-N + 我的名次 + 周边）。
//
// 未与中心服建连时返回错误，调用方回「榜单稍后重试」，而不是空榜
// （空榜会让玩家误以为自己没有成绩）。
func (c *Client) QueryRank(ctx context.Context, rankType string, playerID int64, topN int32) (*internalv2.QueryRankResp, error) {
	c.mu.RLock()
	conn := c.conn
	c.mu.RUnlock()
	if conn == nil {
		return nil, fmt.Errorf("center not connected")
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	return internalv2.NewCenterServiceClient(conn).QueryRank(ctx, &internalv2.QueryRankReq{
		RankType:       rankType,
		TopN:           topN,
		AroundPlayerId: playerID,
	})
}

// SendFeature 上报功能统计。
func (c *Client) SendFeature(groupID, feature string, metrics map[string]int64) error {
	return c.Send(&internalv2.NodeReport{
		Body: &internalv2.NodeReport_Feature{Feature: &internalv2.FeatureReport{
			NodeId:  c.selfID,
			GroupId: groupID,
			Feature: feature,
			Metrics: metrics,
			Ts:      time.Now().Unix(),
		}},
	})
}

// Ready 流是否已建立过。
func (c *Client) Ready() bool {
	select {
	case <-c.ready:
		return true
	default:
		return false
	}
}

// Stop 停止重连并关闭流。
func (c *Client) Stop() {
	c.stopOnce.Do(func() { close(c.stopCh) })
	c.mu.Lock()
	conn := c.conn
	c.stream = nil
	c.mu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}
}

// insecureCreds 内网明文（同机房；生产可换 mTLS）。
func insecureCreds() credentials.TransportCredentials {
	return insecure.NewCredentials()
}
