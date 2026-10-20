// pkg/rpc/resolver/etcd.go —— gRPC etcd:// 解析器
//
// 架构职责：实现 gRPC 的 resolver.Builder/resolver.Resolver 接口，把
// etcd 里的 /services/{service}/ 节点列表翻译成 gRPC 的地址列表
// （resolver.State），并在节点上下线时经 UpdateState 推送更新。
// 协作关系：客户端 dial "etcd:///game" 时，gRPC 框架按 scheme 找到本
// builder（经 grpc.WithResolvers 注册），调用 Build 创建 resolver；
// 注册侧由 pkg/registry/etcd 写 key，两侧共享 DefaultPrefix 约定。
// 设计取舍：
//   - 自定义 resolver 而非 DNS：进程扩缩容/漂移要求秒级感知，DNS 有
//     TTL 缓存且记录维护依赖外部；etcd watch 是推模式，变更即时到达。
//   - 全量 refresh 而非增量 diff：地址列表规模=实例数（个位到几十），
//     每次 watch 事件重建整个列表并 UpdateState，实现简单且无 diff bug
//     风险；增量优化的复杂度在这个规模上不划算。
//   - ResolveNow 空实现：etcd watch 已保证实时性，gRPC 的「立即重新
//     解析」请求（如 LB 策略需要时）没有额外信息可加，满足接口即可。
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
	// gRPC target 形如 etcd:///game：scheme 部分由本常量声明，
	// Builder 注册进 grpc 后框架按它路由到本实现。
	Scheme = "etcd"
)

// etcdResolverBuilder 实现 resolver.Builder。
// DefaultPrefix 服务注册的默认根前缀（与 registry/etcd 保持一致）。
// 两侧各自硬编码同一字符串：改一侧必须改另一侧，注释互相点名防漂移；
// 不一致的故障现象是「注册成功但客户端永远解析不到地址」。
const DefaultPrefix = "/services"

// etcdResolverBuilder 构造器：gRPC 每个新 target 调一次 Build 产出一个
// resolver 实例；client 全局共享（etcd 连接池化在调用方手里），
// prefix 固化在 builder 上，同 builder 产出的 resolver 行为一致。
type etcdResolverBuilder struct {
	client *clientv3.Client
	prefix string
}

// NewBuilder 创建 etcd resolver builder。
//
// prefix 是 etcd key 的根前缀（如 "/services"），实际 key 为 {prefix}/{service}/。
// 传空则用 DefaultPrefix —— 注册侧（registry/etcd）硬编码同一约定，
// 两边不一致会表现为「节点注册了但网关找不到」，排查成本很高。
// 用法：grpc.WithResolvers(NewBuilder(client, ""))。
func NewBuilder(client *clientv3.Client, prefix string) resolver.Builder {
	if prefix == "" {
		prefix = DefaultPrefix
	}
	return &etcdResolverBuilder{client: client, prefix: prefix}
}

// Build 实现 resolver.Builder。
// target.Endpoint() 从 "etcd:///game" 中取出服务名 "game"
// （authority 部分为空是 etcd scheme 的正常形态，gRPC 把它视为无 authority）。
// Build 失败（服务名为空/首次 refresh 失败）会让 grpc.NewClient 报错——
// 快速失败优于返回一个永远无地址的「哑」resolver。
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
// 返回 Scheme 常量：gRPC 用它把 etcd:// target 分发到本 builder。
func (b *etcdResolverBuilder) Scheme() string { return Scheme }

// etcdResolver 监听 etcd 服务变化并更新 gRPC 地址。
//
// cc 是 gRPC 的回调通道（UpdateState 推地址）；cancel/wg 管理 watch
// goroutine 生命周期，Close 时收敛——防止「连接已关、resolver 还在跑」的
// 悬垂 goroutine 往废弃的 cc 里写状态。
type etcdResolver struct {
	client  *clientv3.Client
	service string
	prefix  string
	cc      resolver.ClientConn
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

// start 启动 resolver：先 refresh 全量，再起 watch 增量。
func (r *etcdResolver) start() error {
	// context.Background() 派生：resolver 生命周期跟随 resolver 本身，
	// 不挂在任何请求 ctx 上；停止只能走 Close→cancel。
	ctx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel

	// 首次加载
	// 必须先 refresh 再 watch：watch 只覆盖启动后的变化，启动前已存在的
	// 节点只有全量 Get 能看到；跳过这步的后果是「服务明明在跑，新连接的
	// 客户端却解析到零地址」。
	if err := r.refresh(ctx); err != nil {
		// 启动即失败：cancel 掉刚建的 ctx（防泄漏）再上报，
		// 让 Build 失败、grpc.NewClient 报错。
		cancel()
		return err
	}

	// watch 变化
	r.wg.Add(1)
	go r.watch(ctx)
	return nil
}

// refresh 全量拉取当前地址列表并推给 gRPC。
func (r *etcdResolver) refresh(ctx context.Context) error {
	key := fmt.Sprintf("%s/%s/", r.prefix, r.service)
	resp, err := r.client.Get(ctx, key, clientv3.WithPrefix())
	if err != nil {
		return err
	}
	var addrs []resolver.Address
	for _, kv := range resp.Kvs {
		// 单条脏数据跳过：与 registry.List 同样的可用性优先策略，
		// 一个坏节点不该让整个 target 的地址列表空掉。
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
	// 全量替换而非增量 diff：UpdateState 语义是「这是最新完整列表」，
	// 旧地址被隐式移除；规模小（实例数个），全量最简单可靠。
	state := resolver.State{Addresses: addrs}
	return r.cc.UpdateState(state)
}

// watch 增量监听 etcd 变化，每次事件触发一次全量 refresh。
func (r *etcdResolver) watch(ctx context.Context) {
	defer r.wg.Done()
	key := fmt.Sprintf("%s/%s/", r.prefix, r.service)
	ch := r.client.Watch(ctx, key, clientv3.WithPrefix())
	for {
		select {
		case <-ctx.Done():
			return
		case wr, ok := <-ch:
			// ok=false 表示 watch 通道关闭（etcd 断开/ctx 取消），退出 goroutine
			if !ok {
				return
			}
			// 单次事件带错误（如 revision 压缩）不退出：watch 通道自身
			// 会重连，退出反而丢失后续更新；只忽略这个事件等下一个。
			if wr.Err() != nil {
				continue
			}
			// refresh 失败仅忽略：瞬时 etcd 抖动，下个事件会再试；
			// 启动期的首次 refresh 失败已在 start() 里升级为错误。
			_ = r.refresh(ctx)
		}
	}
}

// ResolveNow 实现 resolver.Resolver。
// 空实现：gRPC 在 LB 策略认为地址可能过期时会调用它请求立即重新解析；
// etcd watch 已是推模式，没有额外可做的事。不实现该方法的 Builder
// 无法通过 gRPC 的接口断言，所以必须占位。
func (r *etcdResolver) ResolveNow(resolver.ResolveNowOptions) {}

// Close 实现 resolver.Resolver。
// cancel 停 watch，再 wg.Wait 等 goroutine 真正退出——保证 Close 返回后
// 不再有 goroutine 持有 r.cc（调用方随后可能关闭 ClientConn，
// 悬垂的 UpdateState 会打到已废弃对象上）。
func (r *etcdResolver) Close() {
	if r.cancel != nil {
		r.cancel()
	}
	r.wg.Wait()
}

// ParseTarget 保留兼容。
// 历史遗留的手工解析辅助（早期 target 不经 gRPC 框架分发）；现在
// etcd:// target 的统一解析由 Builder/Endpoint() 承担，本函数仅为
// 旧调用点与测试保留，新代码不要用。
func ParseTarget(target string) (service string, err error) {
	if !strings.HasPrefix(target, Scheme+"://") {
		return "", fmt.Errorf("invalid etcd target %s", target)
	}
	return strings.TrimPrefix(target, Scheme+"://"), nil
}
