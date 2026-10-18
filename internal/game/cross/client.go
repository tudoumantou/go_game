// internal/game/cross/client.go —— 游戏服 → 跨服节点的 CrossService 流管理（设计文档 10.2）
//
// 一条流 = 一个（游戏服, 跨服节点）对。断流后指数退避重连（10.2）。
// 流的用途：
//   - 上行：送玩家战斗快照、转发客户端战斗操作与结算上报；
//   - 下行：收房间就绪、对手战斗消息、兜底结算结果。
package cross

import (
	"context"
	"fmt"
	"sync"
	"time"

	"game_study/pkg/log"

	internalv2 "game_study/pkg/pb/internalv2"
	"google.golang.org/grpc"
)

// Backoff 重连退避参数。
const (
	minBackoff = 500 * time.Millisecond
	maxBackoff = 30 * time.Second
)

// Handler 下行帧处理器（由游戏服组装层实现：把帧转成对玩家的推送）。
type Handler interface {
	// OnCreateResp 建房响应（回填请求方 pending）。
	OnCreateResp(resp *internalv2.RoomCreateResp)
	// OnRoomReady 房间就绪（可开始转发战斗消息）。
	OnRoomReady(roomID uint64)
	// OnBattleSync 收到对手战斗消息。
	OnBattleSync(msg *internalv2.BattleMsg)
	// OnBattleResult 收到兜底结算（超时平局等）。
	OnBattleResult(res *internalv2.BattleResult)
}

// Client 到单个跨服节点的连接。
type Client struct {
	nodeAddr string // 跨服节点 gRPC 地址（etcd:// 或 host:port）
	selfID   string // 本游戏服节点 ID（BIND 帧用）
	h        Handler
	logger   *log.Logger

	mu     sync.RWMutex
	stream internalv2.CrossService_ChannelClient
	ready  chan struct{} // 首次 BIND 成功时关闭

	stopCh   chan struct{}
	stopOnce sync.Once
}

// NewClient 创建到跨服节点的客户端（不立即连接，Start 时才连）。
func NewClient(selfID, nodeAddr string, h Handler, logger *log.Logger) *Client {
	return &Client{
		nodeAddr: nodeAddr,
		selfID:   selfID,
		h:        h,
		logger:   logger,
		ready:    make(chan struct{}),
		stopCh:   make(chan struct{}),
	}
}

// Start 建立流并常驻重连（阻塞，直到 ctx 取消或 Stop）。
func (c *Client) Start(ctx context.Context, conn *grpc.ClientConn) {
	backoff := minBackoff
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.stopCh:
			return
		default:
		}

		err := c.runOnce(ctx, conn)
		if err != nil {
			c.logger.Warn("cross stream broken, reconnecting",
				"cross", c.nodeAddr, "backoff", backoff.String(), "err", err)
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
func (c *Client) runOnce(ctx context.Context, conn *grpc.ClientConn) error {
	client := internalv2.NewCrossServiceClient(conn)
	stream, err := client.Channel(ctx)
	if err != nil {
		return fmt.Errorf("open channel: %w", err)
	}

	// 首帧必须是 BIND（CrossServer 据此精确回推结算）
	if err := stream.Send(&internalv2.CrossFrame{
		Data: &internalv2.CrossFrame_Ctrl{Ctrl: &internalv2.CrossCtrl{
			Type:   internalv2.CrossCtrl_BIND,
			NodeId: c.selfID,
		}},
	}); err != nil {
		return fmt.Errorf("send bind: %w", err)
	}

	c.mu.Lock()
	c.stream = stream
	c.mu.Unlock()

	c.markReady()
	c.logger.Info("cross stream established", "cross", c.nodeAddr, "self", c.selfID)

	// 下行循环
	for {
		frame, err := stream.Recv()
		if err != nil {
			c.mu.Lock()
			if c.stream == stream {
				c.stream = nil
			}
			c.mu.Unlock()
			return err
		}
		c.dispatch(frame)
	}
}

// dispatch 分发下行帧。
func (c *Client) dispatch(frame *internalv2.CrossFrame) {
	switch data := frame.Data.(type) {
	case *internalv2.CrossFrame_Ctrl:
		if data.Ctrl != nil && data.Ctrl.Type == internalv2.CrossCtrl_ROOM_READY {
			c.h.OnRoomReady(data.Ctrl.RoomId)
		}
	case *internalv2.CrossFrame_Battle:
		if data.Battle != nil {
			c.h.OnBattleSync(data.Battle)
		}
	case *internalv2.CrossFrame_Result:
		if data.Result != nil {
			c.h.OnBattleResult(data.Result)
		}
	case *internalv2.CrossFrame_CreateResp:
		if data.CreateResp != nil {
			c.h.OnCreateResp(data.CreateResp)
		}
	}
}

// Send 上行一帧（流未就绪返回错误，调用方决定是否重试）。
func (c *Client) Send(frame *internalv2.CrossFrame) error {
	c.mu.RLock()
	stream := c.stream
	c.mu.RUnlock()
	if stream == nil {
		return fmt.Errorf("cross stream %s not ready", c.nodeAddr)
	}
	return stream.Send(frame)
}

// SendSnapshot 送玩家战斗快照（入房）。
func (c *Client) SendSnapshot(roomID uint64, playerID int64, token string, snapshot []byte) error {
	return c.Send(&internalv2.CrossFrame{
		Data: &internalv2.CrossFrame_Snapshot{Snapshot: &internalv2.PlayerSnapshot{
			RoomId:      roomID,
			BattleToken: token,
			PlayerId:    playerID,
			Snapshot:    snapshot,
		}},
	})
}

// SendBattle 转发客户端战斗操作。
func (c *Client) SendBattle(roomID uint64, playerID int64, msgID uint32, body []byte) error {
	return c.Send(&internalv2.CrossFrame{
		Data: &internalv2.CrossFrame_Battle{Battle: &internalv2.BattleMsg{
			RoomId:   roomID,
			PlayerId: playerID,
			MsgId:    msgID,
			Body:     body,
		}},
	})
}

// SendResult 转发结算上报。
func (c *Client) SendResult(res *internalv2.BattleResult) error {
	return c.Send(&internalv2.CrossFrame{
		Data: &internalv2.CrossFrame_Result{Result: res},
	})
}

// Ready 流是否已就绪。
func (c *Client) Ready() bool {
	select {
	case <-c.ready:
		return true
	default:
		return false
	}
}

// WaitReady 等待流就绪或超时。
func (c *Client) WaitReady(ctx context.Context) error {
	select {
	case <-c.ready:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Stop 停止重连。
func (c *Client) Stop() {
	c.stopOnce.Do(func() { close(c.stopCh) })
}

// markReady 只关闭一次 ready。
func (c *Client) markReady() {
	c.mu.Lock()
	defer c.mu.Unlock()
	select {
	case <-c.ready:
	default:
		close(c.ready)
	}
}
