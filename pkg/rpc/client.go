// pkg/rpc/client.go —— gRPC ClientConn 池
//
// 架构职责：按 target（etcd:///<service> 或 ip:port）缓存 ClientConn，
// 屏蔽建连参数（凭据/keepalive/backoff）与坏连接重建逻辑。
// 协作关系：Get 返回的 *grpc.ClientConn 由调用方拿去构造业务 stub
// （如 NewGameServiceClient）；地址更新由 resolver 子包经 gRPC 内部
// 回调完成，本池不感知实例上下线，只管连接生命周期。
// 设计取舍：
//   - grpc.NewClient 而非已废弃的 grpc.Dial：NewClient 懒连接
//     （不立即建 TCP），把「能否连上」的判断推迟到首次 RPC，配合
//     resolver 可在无任何实例时不报错（grpc 内部排队/重试）。
//   - RWMutex 而非 Mutex：连接命中是热路径且只读，RLock 允许多 goroutine
//     并发查缓存；只有首次建连/重建才走写锁。
//   - insecure 凭据：集群内网可信、暂未上 mTLS 的阶段性选择；
//     生产硬化应在 service mesh 或 LB 层补双向认证，而非散落在调用点。
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
//
// 契约：Close 后池仍可用（Get 会重新建连），但建议 Close 作为进程收尾
// 最后一步；map 无容量上限——target 数量是服务名级别（个位数），
// 不为假想的海量 target 加 LRU。
type ClientPool struct {
	// mu 读多写少：命中走 RLock，建连走 Lock
	mu sync.RWMutex
	// conns target → 连接；target 数量是服务名级别，无容量上限
	conns map[string]*grpc.ClientConn
	// opts 预构建：每个新 conn 复用同一组参数，避免重复分配
	opts []grpc.DialOption
}

// NewClientPool 创建 ClientPool。
// resolverBuilder 是可变 DialOption：调用方把 etcd resolver 的
// grpc.WithResolvers(...) 传进来，池本身不认识 etcd——依赖倒置，
// 换服务发现方案只需换传入项。
func NewClientPool(resolverBuilder ...grpc.DialOption) *ClientPool {
	opts := []grpc.DialOption{
		// 内网明文传输：TLS 未上线阶段的显式选择（见文件头取舍）。
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		// keepalive 探测：60s 无活动发 ping，20s 无响应判定连接死。
		// PermitWithoutStream=false：没有活跃 RPC 时不发 ping——空闲连接
		// 不耗资源，代价是纯空闲连接可能被中间设备（NAT/LB）静默掐断，
		// 由下次 RPC 的重连机制恢复（本项目调用频，可接受）。
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                60 * time.Second,
			Timeout:             20 * time.Second,
			PermitWithoutStream: false,
		}),
		// 连接退避：默认指数退避（1.6x，上限 120s）——下游 etcd/目标服务
		// 故障时防止疯狂重连把双端打爆；重连由 grpc 内部自动完成。
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
//
// 双检锁（double-checked locking）：RLock 快路径命中即返回；未命中才升级
// 为 Lock，加锁后再查一次——两个 goroutine 同时未命中时，后到者会发现
// 前者已建好 conn，直接复用，避免重复建连。
func (p *ClientPool) Get(target string) (*grpc.ClientConn, error) {
	p.mu.RLock()
	cc, ok := p.conns[target]
	p.mu.RUnlock()
	if ok && cc != nil {
		state := cc.GetState()
		// Shutdown = conn 已关闭不可用；TransientFailure = 持续连接失败中。
		// 两种状态都扔掉重建：新 conn 会重新经 resolver 解析最新地址，
		// 把「endpoint 整体迁移」的恢复能力收回池自身，而不是赌旧 conn
		// 的内部状态机自己好转。
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
		// 坏 conn 直接覆盖：不显式 Close 旧的（grpc 允许泄漏式替换，
		// Close 旧 conn 会中断其上的 in-flight RPC——那些调用方还在用）。
	}

	// NewClient 懒连接：目标无实例时不报错，地址列表由 resolver 异步填充。
	cc, err := grpc.NewClient(target, p.opts...)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", target, err)
	}
	p.conns[target] = cc
	return cc, nil
}

// Close 关闭所有连接。
// lastErr 策略：逐个 Close，记录最后一个错误即可暴露「有失败」；
// 连接池关闭是收尾路径，聚合全部错误（errors.Join）信息量提升有限。
// 最后把 map 重置为空：池回到「刚创建」状态，误用的后续 Get 仍能工作。
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
