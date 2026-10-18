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
// DefaultPrefix 服务注册的默认根前缀（与 registry/etcd 保持一致）。
const DefaultPrefix = "/services"

type etcdResolverBuilder struct {
	client *clientv3.Client
	prefix string
}

// NewBuilder 创建 etcd resolver builder。
//
// prefix 是 etcd key 的根前缀（如 "/services"），实际 key 为 {prefix}/{service}/。
// 传空则用 DefaultPrefix —— 注册侧（registry/etcd）硬编码同一约定，
// 两边不一致会表现为「节点注册了但网关找不到」，排查成本很高。
func NewBuilder(client *clientv3.Client, prefix string) resolver.Builder {
	if prefix == "" {
		prefix = DefaultPrefix
	}
	return &etcdResolverBuilder{client: client, prefix: prefix}
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
		prefix:  b.prefix,
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
	prefix  string
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
	key := fmt.Sprintf("%s/%s/", r.prefix, r.service)
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
	key := fmt.Sprintf("%s/%s/", r.prefix, r.service)
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
