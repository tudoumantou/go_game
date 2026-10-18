// internal/gateway/backend/pool.go —— 网关到游戏服的 Channel 池
package backend

import (
	"context"
	"fmt"
	"sync"

	"game_study/pkg/log"
	rpcpkg "game_study/pkg/rpc"

	internalv2 "game_study/pkg/pb/internalv2"
)

// Pool 管理到 game server 的 stream。
type Pool struct {
	clientPool *rpcpkg.ClientPool
	logger     *log.Logger

	mu      sync.RWMutex
	streams map[string]*streamConn
}

type streamConn struct {
	client internalv2.GameService_ChannelClient
	cancel context.CancelFunc
}

// NewPool 创建 Pool。
func NewPool(clientPool *rpcpkg.ClientPool, logger *log.Logger) *Pool {
	return &Pool{
		clientPool: clientPool,
		logger:     logger,
		streams:    make(map[string]*streamConn),
	}
}

// GetOrConnect 获取或创建到 target 的 stream。
func (p *Pool) GetOrConnect(ctx context.Context, target string, firstFrame *internalv2.StreamFrame) (internalv2.GameService_ChannelClient, error) {
	p.mu.RLock()
	sc, ok := p.streams[target]
	p.mu.RUnlock()
	if ok && sc != nil {
		return sc.client, nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if sc, ok := p.streams[target]; ok && sc != nil {
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
		return nil, fmt.Errorf("channel %s: %w", target, err)
	}

	if firstFrame != nil {
		if err := stream.Send(firstFrame); err != nil {
			cancel()
			return nil, fmt.Errorf("send bind: %w", err)
		}
	}

	p.streams[target] = &streamConn{client: stream, cancel: cancel}
	// 启动接收 goroutine（P2 阶段只读不处理，P3 把下行包写回客户端）
	go p.recvLoop(target, stream)
	return stream, nil
}

// Send 向已建立的 stream 发送帧。
func (p *Pool) Send(target string, frame *internalv2.StreamFrame) error {
	p.mu.RLock()
	sc, ok := p.streams[target]
	p.mu.RUnlock()
	if !ok || sc == nil {
		return fmt.Errorf("stream %s not connected", target)
	}
	return sc.client.Send(frame)
}

func (p *Pool) recvLoop(target string, stream internalv2.GameService_ChannelClient) {
	for {
		frame, err := stream.Recv()
		if err != nil {
			p.logger.Warn("backend stream recv error", "target", target, "err", err)
			p.remove(target)
			return
		}
		// TODO(P3): 根据 frame 写回对应客户端
		_ = frame
	}
}

func (p *Pool) remove(target string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if sc, ok := p.streams[target]; ok {
		sc.cancel()
		delete(p.streams, target)
	}
}

// Close 关闭所有 stream。
func (p *Pool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for target, sc := range p.streams {
		sc.cancel()
		delete(p.streams, target)
	}
	return nil
}
