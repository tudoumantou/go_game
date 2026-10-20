// pkg/registry/etcd/registry.go —— 基于 etcd 的服务注册与发现
//
// 架构职责：Registry 一个类型覆盖注册（带租约续期）、反注册、Watch、List，
// 对外只暴露 Node 数据结构与 etcd key 格式约定。
// 协作关系：与 pkg/rpc/resolver/etcd.go 共享 /services 前缀与 Node JSON 格式
// （两侧必须同步演进，否则表现为「注册成功但网关找不到」）；
// clientv3.Client 由调用方传入 resolver，Registry 与 resolver 复用同一连接。
// 设计取舍：
//   - Grant lease + Put with lease + KeepAlive 三段式：先拿租约再挂 key，
//     保证 key 从诞生起就带 TTL，不存在「裸 key 忘了挂租约」的中间态
//     （那种 key 会永驻 etcd，进程崩了还被认为在线）。
//   - keepalive goroutine 常驻读取响应通道：KeepAlive 返回的 channel 若不
//     被消费，etcd 客户端内部的发送会阻塞，续约静默停止——这类「不读就
//     死」的通道契约必须在本层封住，不能漏给调用方。
package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// Node 服务节点信息，与 internal/v2/registry.proto 的 NodeInfo 对齐。
//
// 注册 key 形如 /services/{Service}/{ID}；Addr 是 Watch 侧直接拨号的
// host:port；Metadata 放区服/权重等扩展属性，由消费方按需解释。
// ID 由调用方保证进程内唯一（建议 hostname-pid），同 ID 重复注册是覆盖语义
// ——etcd Put 同 key 即覆盖，恰好用于「同进程重启后刷新自身信息」。
type Node struct {
	ID       string            `json:"id"`
	Service  string            `json:"service"`
	Addr     string            `json:"addr"`
	Version  string            `json:"version"`
	Metadata map[string]string `json:"metadata"`
}

// Registry etcd 注册表。
//
// client 由 New 创建并归 Registry 所有，Close 时释放；
// 所有方法线程安全（clientv3.Client 自身并发安全）。
// 注意 Close 之后 Registry 不可再用（clientv3.Client 不支持重连），
// 需要重连时应 New 一个新的 Registry。
type Registry struct {
	client *clientv3.Client
}

// New 创建 Registry。
// DialTimeout 只影响「建立连接」阶段；连接断开后 clientv3 会自动重连，
// 无需调用方干预。返回 error 而非 panic：etcd 未就绪时上层可决定
// 重试或降级（如网关先起监听、后台重试注册），直接 panic 会掐断这种策略。
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
//
// 完整时序：Grant 租约 → Put key 挂租约 → KeepAlive 续约 → 返回 cancel。
// cancel 契约：停止续约并删除 key；调用后节点立即从 etcd 消失（优雅退出用）。
// 调用方需持有 cancel 直到进程退出——提前调用等于提前下线。
func (r *Registry) RegisterWithKeepalive(ctx context.Context, node Node, ttl int64) (context.CancelFunc, error) {
	// key 格式与 resolver 的 DefaultPrefix 必须一致，两边各自硬编码是
	// 已知的耦合点（改一处必须改另一处，有注释互相点名）。
	key := fmt.Sprintf("/services/%s/%s", node.Service, node.ID)
	val, err := json.Marshal(node)
	if err != nil {
		return nil, err
	}

	// ① 租约：ttl 秒不续约，key 自动过期——崩溃检测的核心机制。
	grant, err := r.client.Grant(ctx, ttl)
	if err != nil {
		return nil, err
	}

	// ② 挂租约写 key：key 从诞生即带 TTL，无「裸 key」中间态。
	_, err = r.client.Put(ctx, key, string(val), clientv3.WithLease(grant.ID))
	if err != nil {
		return nil, err
	}

	// keepalive 的生命周期独立于调用方 ctx：注册是进程级承诺，
	// 不随某次请求的取消而中断；停止注册只能走返回的 cancel。
	// 若用调用方 ctx，一次带 ctx 的调用取消就会把节点从 etcd 摘掉——
	// 表现为「服务莫名消失几分钟又回来」的灵异故障。
	keepCtx, cancel := context.WithCancel(context.Background())
	ch, err := r.client.KeepAlive(keepCtx, grant.ID)
	if err != nil {
		// KeepAlive 启动失败必须 cancel 掉刚建的 ctx，否则 keepCtx 泄漏
		// （context.WithCancel 的 cancel 必须被调用，vet 的 lostcancel 也查这个）。
		cancel()
		return nil, err
	}

	// KeepAlive 通道必须被持续读取，否则 etcd 客户端内部会阻塞；
	// 这里只关心通道关闭（续约失败或 cancel），响应值本身不处理。
	// 通道关闭即意味着租约将到期、节点将消失——视为注册失效，
	// 由上层监控/对账发现（更高级的做法是自动重新 Grant，见 Watch 侧降级）。
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

	// cancel 顺序：先停 keepalive 再删 key。反序（先删后停）在理论上
	// 留一个窗口：续约响应还在路上，删掉的 key 不会复活（key 已删），
	// 但先停续约能让租约尽快过期、兜底清理任何遗漏状态，是更稳的顺序。
	return func() {
		cancel()
		_, _ = r.client.Delete(ctx, key)
	}, nil
}

// Register 注册服务节点（ttl 秒，无自动续期）。
// 与 RegisterWithKeepalive 的区别：注册完立即 cancel，key 靠 ttl 自然过期。
// 适用：一次性注册/测试探活；常驻服务必须用 RegisterWithKeepalive。
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
// 与 RegisterWithKeepalive 返回的 cancel 等价（都是 Delete key），
// 区别是该方法不需要持有 cancel 闭包——适用于「注册方与注销方不是同一处代码」
// 的场景（如管理面强制摘节点）。
func (r *Registry) Deregister(ctx context.Context, node Node) error {
	key := fmt.Sprintf("/services/%s/%s", node.Service, node.ID)
	_, err := r.client.Delete(ctx, key)
	return err
}

// Watch 监听服务实例变化。
//
// 返回的 WatchChan 是 etcd 的 channel，调用方需自行 select ctx.Done()
// 决定退出时机；Watch 只覆盖「调用之后」的变化事件，启动瞬间已存在的
// 节点必须先 List 再 Watch（resolver 的 start 正是这么做的，防丢初始地址）。
// WithPrefix 监听 /services/{service}/ 整个子树：上线/下线/信息更新都触发。
func (r *Registry) Watch(ctx context.Context, service string) clientv3.WatchChan {
	key := fmt.Sprintf("/services/%s/", service)
	return r.client.Watch(ctx, key, clientv3.WithPrefix())
}

// List 列出当前服务实例。
// 单条 JSON 解析失败 continue 而非整体报错：一个脏节点（如外部直写 etcd
// 写进非法值）不应让整个服务列表不可用——可用性优先；
// 注册侧保证写入合法，脏数据只可能是外部写入，容忍好于拒绝。
// 无实例时返回 nil 切片（len==0），调用方 range 安全。
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
// 会连带停掉本 Registry 创建的所有 keepalive goroutine（client 内部管理），
// 之后 Registry 与方法均不可用；通常挂在 shutdown 的 STOP 阶段。
func (r *Registry) Close() error {
	return r.client.Close()
}
