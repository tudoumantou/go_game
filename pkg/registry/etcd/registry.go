// pkg/registry/etcd/registry.go —— 基于 etcd 的服务注册与发现
package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// Node 服务节点信息，与 internal/v2/registry.proto 的 NodeInfo 对齐。
type Node struct {
	ID       string            `json:"id"`
	Service  string            `json:"service"`
	Addr     string            `json:"addr"`
	Version  string            `json:"version"`
	Metadata map[string]string `json:"metadata"`
}

// Registry etcd 注册表。
type Registry struct {
	client *clientv3.Client
}

// New 创建 Registry。
func New(endpoints []string) (*Registry, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("etcd new client: %w", err)
	}
	return &Registry{client: cli}, nil
}

// RegisterWithKeepalive 注册服务节点并开启 lease keepalive，返回取消函数。
func (r *Registry) RegisterWithKeepalive(ctx context.Context, node Node, ttl int64) (context.CancelFunc, error) {
	key := fmt.Sprintf("/services/%s/%s", node.Service, node.ID)
	val, err := json.Marshal(node)
	if err != nil {
		return nil, err
	}

	grant, err := r.client.Grant(ctx, ttl)
	if err != nil {
		return nil, err
	}

	_, err = r.client.Put(ctx, key, string(val), clientv3.WithLease(grant.ID))
	if err != nil {
		return nil, err
	}

	keepCtx, cancel := context.WithCancel(context.Background())
	ch, err := r.client.KeepAlive(keepCtx, grant.ID)
	if err != nil {
		cancel()
		return nil, err
	}

	go func() {
		for {
			select {
			case <-keepCtx.Done():
				return
			case ka, ok := <-ch:
				if !ok {
					return
				}
				_ = ka
			}
		}
	}()

	return func() {
		cancel()
		_, _ = r.client.Delete(ctx, key)
	}, nil
}

// Register 注册服务节点（ttl 秒，无自动续期）。
func (r *Registry) Register(ctx context.Context, node Node, ttl int64) error {
	cancel, err := r.RegisterWithKeepalive(ctx, node, ttl)
	if err != nil {
		return err
	}
	// 同步注册场景不保留 keepalive，由调用方自行管理
	cancel()
	return nil
}

// Deregister 反注册。
func (r *Registry) Deregister(ctx context.Context, node Node) error {
	key := fmt.Sprintf("/services/%s/%s", node.Service, node.ID)
	_, err := r.client.Delete(ctx, key)
	return err
}

// Watch 监听服务实例变化。
func (r *Registry) Watch(ctx context.Context, service string) clientv3.WatchChan {
	key := fmt.Sprintf("/services/%s/", service)
	return r.client.Watch(ctx, key, clientv3.WithPrefix())
}

// List 列出当前服务实例。
func (r *Registry) List(ctx context.Context, service string) ([]Node, error) {
	key := fmt.Sprintf("/services/%s/", service)
	resp, err := r.client.Get(ctx, key, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	var nodes []Node
	for _, kv := range resp.Kvs {
		var n Node
		if err := json.Unmarshal(kv.Value, &n); err != nil {
			continue
		}
		nodes = append(nodes, n)
	}
	return nodes, nil
}

// Close 关闭 etcd 客户端。
func (r *Registry) Close() error {
	return r.client.Close()
}
