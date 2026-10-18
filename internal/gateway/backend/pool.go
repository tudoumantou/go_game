// internal/gateway/backend/pool.go —— 网关到游戏服的 Channel 池
// 设计约束（4.5）：每个客户端会话 = 一条 GameService.Channel stream，
// 响应沿同流回写保证有序；因此池按 session_id 索引，而非按 game 节点复用。
package backend

import (
	"context"
	"fmt"
	"sync"

	"game_study/pkg/log"
	"game_study/pkg/metrics"
	rpcpkg "game_study/pkg/rpc"

	internalv2 "game_study/pkg/pb/internalv2"
)

// FrameHandler 下行帧回调（游戏服 → 网关 → 客户端）。
type FrameHandler func(sessionID uint64, msgID uint32, body []byte)

// Pool 管理到 game server 的会话级 stream。
type Pool struct {
	clientPool *rpcpkg.ClientPool
	logger     *log.Logger
	onFrame    FrameHandler

	mu      sync.RWMutex
	streams map[uint64]*streamConn // session_id -> stream
}

type streamConn struct {
	client internalv2.GameService_ChannelClient
	cancel context.CancelFunc
	target string
}

// NewPool 创建 Pool。
func NewPool(clientPool *rpcpkg.ClientPool, logger *log.Logger, onFrame FrameHandler) *Pool {
	return &Pool{
		clientPool: clientPool,
		logger:     logger,
		onFrame:    onFrame,
		streams:    make(map[uint64]*streamConn),
	}
}

// GetOrConnect 获取或创建指定会话的 stream（firstFrame 通常为 BIND）。
func (p *Pool) GetOrConnect(ctx context.Context, target string, sessionID uint64, firstFrame *internalv2.StreamFrame) (internalv2.GameService_ChannelClient, error) {
	p.mu.RLock()
	sc, ok := p.streams[sessionID]
	p.mu.RUnlock()
	if ok && sc != nil {
		return sc.client, nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if sc, ok := p.streams[sessionID]; ok && sc != nil {
		return sc.client, nil
	}

	cc, err := p.clientPool.Get(target)
	if err != nil {
		return nil, err
	}
	client := internalv2.NewGameServiceClient(cc)
	streamCtx, cancel := context.WithCancel(context.Background())
	stream, err := client.Channel(streamCtx)
	if err != nil {
		cancel()
		metrics.ChannelError("open")
		return nil, fmt.Errorf("channel %s: %w", target, err)
	}

	if firstFrame != nil {
		if err := stream.Send(firstFrame); err != nil {
			cancel()
			return nil, fmt.Errorf("send bind: %w", err)
		}
	}

	p.streams[sessionID] = &streamConn{client: stream, cancel: cancel, target: target}
	if metrics.Enabled() {
		metrics.ChannelStreams(len(p.streams))
	}
	go p.recvLoop(sessionID, stream)
	return stream, nil
}

// Send 向指定会话的 stream 发送帧。
func (p *Pool) Send(sessionID uint64, frame *internalv2.StreamFrame) error {
	p.mu.RLock()
	sc, ok := p.streams[sessionID]
	p.mu.RUnlock()
	if !ok || sc == nil {
		metrics.ChannelError("send_no_stream")
		return fmt.Errorf("session %d stream not found", sessionID)
	}
	if err := sc.client.Send(frame); err != nil {
		metrics.ChannelError("send")
		return err
	}
	return nil
}

func (p *Pool) recvLoop(sessionID uint64, stream internalv2.GameService_ChannelClient) {
	for {
		frame, err := stream.Recv()
		if err != nil {
			p.logger.Debug("backend stream closed", "session_id", sessionID, "err", err)
			p.remove(sessionID)
			return
		}
		if cp := frame.GetClientPacket(); cp != nil && p.onFrame != nil {
			p.onFrame(cp.SessionId, cp.MsgId, cp.Body)
		}
	}
}

// remove 关闭并移除会话 stream。
func (p *Pool) remove(sessionID uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if sc, ok := p.streams[sessionID]; ok {
		sc.cancel()
		delete(p.streams, sessionID)
	}
}

// CloseSession 主动关闭某会话的 stream（会话断开时调用）。
func (p *Pool) CloseSession(sessionID uint64) {
	p.remove(sessionID)
}

// Close 关闭所有 stream。
func (p *Pool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for id, sc := range p.streams {
		sc.cancel()
		delete(p.streams, id)
	}
	return nil
}
