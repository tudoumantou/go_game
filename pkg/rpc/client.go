// pkg/rpc/client.go —— gRPC ClientConn 池
package rpc

import (
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

// ClientPool 按目标服务名缓存 ClientConn。
type ClientPool struct {
	mu    sync.RWMutex
	conns map[string]*grpc.ClientConn
	opts  []grpc.DialOption
}

// NewClientPool 创建 ClientPool。
func NewClientPool(resolverBuilder ...grpc.DialOption) *ClientPool {
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                60 * time.Second,
			Timeout:             20 * time.Second,
			PermitWithoutStream: false,
		}),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff: backoff.DefaultConfig,
		}),
	}
	opts = append(opts, resolverBuilder...)
	return &ClientPool{
		conns: make(map[string]*grpc.ClientConn),
		opts:  opts,
	}
}

// Get 获取或创建到 target 的连接。target 支持 etcd:///<service> 或直接 ip:port。
func (p *ClientPool) Get(target string) (*grpc.ClientConn, error) {
	p.mu.RLock()
	cc, ok := p.conns[target]
	p.mu.RUnlock()
	if ok && cc != nil {
		state := cc.GetState()
		if state != connectivity.Shutdown && state != connectivity.TransientFailure {
			return cc, nil
		}
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	// 双重检查
	if cc, ok := p.conns[target]; ok && cc != nil {
		state := cc.GetState()
		if state != connectivity.Shutdown && state != connectivity.TransientFailure {
			return cc, nil
		}
	}

	cc, err := grpc.NewClient(target, p.opts...)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", target, err)
	}
	p.conns[target] = cc
	return cc, nil
}

// Close 关闭所有连接。
func (p *ClientPool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	var lastErr error
	for _, cc := range p.conns {
		if err := cc.Close(); err != nil {
			lastErr = err
		}
	}
	p.conns = make(map[string]*grpc.ClientConn)
	return lastErr
}
