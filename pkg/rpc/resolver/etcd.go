// pkg/rpc/resolver/etcd.go —— gRPC etcd:// 解析器
package resolver

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"game_study/pkg/registry/etcd"

	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/resolver"
)

const (
	// Scheme etcd URL scheme。
	Scheme = "etcd"
)

// etcdResolverBuilder 实现 resolver.Builder。
type etcdResolverBuilder struct {
	client *clientv3.Client
}

// NewBuilder 创建 etcd resolver builder。
func NewBuilder(client *clientv3.Client) resolver.Builder {
	return &etcdResolverBuilder{client: client}
}

// Build 实现 resolver.Builder。
func (b *etcdResolverBuilder) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	service := target.Endpoint()
	if service == "" {
		return nil, fmt.Errorf("etcd resolver: empty service name")
	}

	r := &etcdResolver{
		client:  b.client,
		service: service,
		cc:      cc,
	}
	if err := r.start(); err != nil {
		return nil, err
	}
	return r, nil
}

// Scheme 实现 resolver.Builder。
func (b *etcdResolverBuilder) Scheme() string { return Scheme }

// etcdResolver 监听 etcd 服务变化并更新 gRPC 地址。
type etcdResolver struct {
	client  *clientv3.Client
	service string
	cc      resolver.ClientConn
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

func (r *etcdResolver) start() error {
	ctx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel

	// 首次加载
	if err := r.refresh(ctx); err != nil {
		cancel()
		return err
	}

	// watch 变化
	r.wg.Add(1)
	go r.watch(ctx)
	return nil
}

func (r *etcdResolver) refresh(ctx context.Context) error {
	key := fmt.Sprintf("/services/%s/", r.service)
	resp, err := r.client.Get(ctx, key, clientv3.WithPrefix())
	if err != nil {
		return err
	}
	var addrs []resolver.Address
	for _, kv := range resp.Kvs {
		var n etcd.Node
		if err := json.Unmarshal(kv.Value, &n); err != nil {
			continue
		}
		addrs = append(addrs, resolver.Address{
			Addr:       n.Addr,
			ServerName: r.service,
			Attributes: nil,
		})
	}
	state := resolver.State{Addresses: addrs}
	return r.cc.UpdateState(state)
}

func (r *etcdResolver) watch(ctx context.Context) {
	defer r.wg.Done()
	key := fmt.Sprintf("/services/%s/", r.service)
	ch := r.client.Watch(ctx, key, clientv3.WithPrefix())
	for {
		select {
		case <-ctx.Done():
			return
		case wr, ok := <-ch:
			if !ok {
				return
			}
			if wr.Err() != nil {
				continue
			}
			_ = r.refresh(ctx)
		}
	}
}

// ResolveNow 实现 resolver.Resolver。
func (r *etcdResolver) ResolveNow(resolver.ResolveNowOptions) {}

// Close 实现 resolver.Resolver。
func (r *etcdResolver) Close() {
	if r.cancel != nil {
		r.cancel()
	}
	r.wg.Wait()
}

// ParseTarget 保留兼容。
func ParseTarget(target string) (service string, err error) {
	if !strings.HasPrefix(target, Scheme+"://") {
		return "", fmt.Errorf("invalid etcd target %s", target)
	}
	return strings.TrimPrefix(target, Scheme+"://"), nil
}
