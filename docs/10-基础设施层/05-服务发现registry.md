# 05 服务发现 registry

一句话定位：`pkg/registry/etcd` 用「Grant 租约 → Put 挂租约 → KeepAlive 续约」三段式把节点登记进 etcd（崩溃即过期，无需人工清理），`pkg/rpc/resolver` 把同一套 `/services/{service}/` key 翻译成 gRPC 地址列表推给客户端——注册与发现共享前缀约定，两侧注释互相点名防漂移。

## 1. 概述

微服务的「谁在哪」有三个要求：**实时**（扩缩容/宕徙秒级感知）、**自愈**（进程崩了不留下幽灵节点）、**简单**（个位到几十的实例规模，不需要花哨的负载均衡算法）。

本项目的选型与取舍：

- **etcd 而非 DNS**：DNS 有 TTL 缓存且记录维护依赖外部；etcd watch 是推模式，变更即时到达。
- **租约（lease）而非心跳上报**：key 从诞生起就挂 TTL，进程崩了租约到期 key 自动消失；心跳方案需要一个「谁来判定死亡」的权威方，还要处理误判。
- **自定义 gRPC resolver 而非轮询 List**：`etcd:///game` target 由 resolver 翻译成地址列表，节点上下线经 `UpdateState` 推送，客户端零改动。
- **全量 refresh 而非增量 diff**：地址列表规模 = 实例数（个位到几十），每次 watch 事件重建整个列表，实现简单且无 diff bug 风险。
- **不抽象 Registry 接口**（`pkg/registry/doc.go`）：当前只有 etcd 一种实现，提前为「未来接 Consul/Nacos」留接口属于为假想需求增加间接层（YAGNI）；真出现第二实现时再抽不迟。

## 2. 涉及文件

| 路径 | 职责 |
| --- | --- |
| `pkg/registry/doc.go` | 包定位：声明本层解决进程发现问题，刻意不定义 Registry 接口（YAGNI 取舍） |
| `pkg/registry/etcd/registry.go` | etcd 实现：Node 结构、Grant/Put/KeepAlive 三段式注册、Deregister、Watch、List、Close |
| `pkg/rpc/resolver/etcd.go` | gRPC resolver：`etcd://` scheme、Build/start/refresh/watch、UpdateState 推送、Close 收敛 |
| `internal/gateway/server.go` | 网关侧接线：注册自身 + 用 resolver 建到游戏服的 backend 连接池 |
| `internal/game/server_start.go` | 游戏服侧接线：`registerEtcd` 注册自身（Service=game） |
| `configs/*/[service]-dev.yaml` | `etcd.endpoints`/`etcd.lease_ttl`/`rpc.service_prefix` 配置 |

## 3. 核心设计

### 3.1 注册时序：三段式与取消契约

```go
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

	// cancel 顺序：先停 keepalive 再删 key。
	return func() {
		cancel()
		_, _ = r.client.Delete(ctx, key)
	}, nil
}
```

四个不显眼但关键的点：

1. **先 Grant 再 Put with lease**：保证 key 从诞生起就带 TTL，不存在「裸 key 忘了挂租约」的中间态——那种 key 会永驻 etcd，进程崩了还被认为在线。
2. **keepalive ctx 用 `context.Background()` 派生而非调用方 ctx**：注册是进程级承诺，不随某次请求取消而中断。若用调用方 ctx，一次带 ctx 的调用取消就会把节点从 etcd 摘掉——表现为「服务莫名消失几分钟又回来」的灵异故障。
3. **KeepAlive 返回的通道必须被持续消费**：etcd 客户端内部是「发送续约请求等响应」，通道不读会让内部发送阻塞、续约静默停止。这类「不读就死」的通道契约在本层封住，不漏给调用方。
4. **cancel 先停续约再删 key**：反序（先删后停）在理论上留窗口——续约响应还在路上，删掉的 key 不会复活，但先停续约能让租约尽快过期、兜底清理任何遗漏状态。

返回的 `cancel` 即「优雅下线」：调用后节点立即从 etcd 消失。调用方需持有 cancel 直到进程退出——提前调用等于提前下线。

### 3.2 key 约定与 Node 结构

```go
type Node struct {
	ID       string            `json:"id"`
	Service  string            `json:"service"`
	Addr     string            `json:"addr"`
	Version  string            `json:"version"`
	Metadata map[string]string `json:"metadata"`
}
```

- 注册 key 形如 `/services/{Service}/{ID}`；`Addr` 是 Watch 侧直接拨号的 `host:port`；
- ID 由调用方保证进程内唯一（各服务用 `cfg.Server.Name`，如 `game-01`）；同 ID 重复注册是**覆盖语义**——etcd Put 同 key 即覆盖，恰好用于「同进程重启后刷新自身信息」；
- `Metadata` 放区服/权重等扩展属性，由消费方按需解释（当前未用，预留）。

### 3.3 发现侧：自定义 resolver 的「先 refresh 后 watch」

客户端 dial `etcd:///game` 时，gRPC 按 scheme 找到注册的 builder，调用 `Build`：

```go
func (r *etcdResolver) start() error {
	ctx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel

	// 必须先 refresh 再 watch：watch 只覆盖启动后的变化，启动前已存在的
	// 节点只有全量 Get 能看到；跳过这步的后果是「服务明明在跑，新连接的
	// 客户端却解析到零地址」。
	if err := r.refresh(ctx); err != nil {
		cancel()
		return err
	}

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
```

- **先 refresh 后 watch**：watch 只覆盖「调用之后」的变化事件；跳过 refresh 的后果是「服务明明在跑，新连接的客户端却解析到零地址」。启动即失败让 `Build` 报错、`grpc.NewClient` 失败——快速失败优于返回一个永远无地址的「哑」resolver。
- **每次 watch 事件触发一次全量 refresh**：不 diff、不挑事件类型（PUT/DELETE 一视同仁），规模小的情况下这是最可靠的形态。
- **运行期错误分级**：watch 单次事件带错误（如 revision 压缩）不退出（通道自身会重连）；refresh 失败仅忽略（下个事件会再试）；只有**启动期首次** refresh 失败才升级为错误。
- **脏数据跳过而非整体失败**：与 `registry.List` 同样的可用性优先策略——一个外部直写 etcd 的坏节点不该让整个服务的地址列表空掉。

### 3.4 生命周期：Close 的悬垂防护

```go
func (r *etcdResolver) Close() {
	if r.cancel != nil {
		r.cancel()
	}
	r.wg.Wait()
}
```

cancel 停 watch，再 `wg.Wait` 等 goroutine 真正退出——保证 Close 返回后不再有 goroutine 持有 `r.cc`（调用方随后可能关闭 ClientConn，悬垂的 `UpdateState` 会打到已废弃对象上）。这是「连接已关、resolver 还在跑」类泄漏的标准解法。

## 4. 关键代码解析

### 4.1 网关侧完整接线

`internal/gateway/server.go:226` 是注册 + 发现两侧的合流示范：

```go
// etcd
//
// 端点来自 rpc.etcd_endpoints（网关配置里没有顶层 etcd 段）。
// 之前误读 s.cfg.Etcd.Endpoints，导致整个分支被跳过 —— backend 为 nil，
// 会话永远绑不到游戏服，客户端进游戏只能等超时。
// 教训：配置读取路径错误不会编译失败，只会让分支静默跳过——
// 对「分支跳过 = 功能全废」的配置项，日志必须显式告知。
etcdEndpoints := s.cfg.RPC.EtcdEndpoints
if len(etcdEndpoints) == 0 {
	etcdEndpoints = s.cfg.Etcd.Endpoints // 兼容顶层写法
}
if len(etcdEndpoints) > 0 {
	cli, err := clientv3.New(clientv3.Config{Endpoints: etcdEndpoints, DialTimeout: s.cfg.GetDialTimeout()})
	if err != nil {
		return fmt.Errorf("etcd: %w", err)
	}
	s.etcdClient = cli
	s.etcdReg, _ = gameetcd.New(etcdEndpoints)

	// 注册自己到 etcd
	node := gameetcd.Node{
		ID:      s.cfg.Server.Name,
		Service: "gateway",
		Addr:    s.cfg.Server.TCPAddr,
		Version: "1.0.0",
	}
	cancel, err := s.etcdReg.RegisterWithKeepalive(ctx, node, s.cfg.Etcd.LeaseTTL)
	if err != nil {
		return fmt.Errorf("register gateway: %w", err)
	}
	s.etcdCancel = cancel

	// backend pool with etcd resolver
	// resolver 让 gRPC 目标 "etcd:///game" 动态解析为实例列表，
	// 节点上下线由 resolver 推送更新，网关无需重启
	resolverBuilder := grpc.WithResolvers(resolver.NewBuilder(cli, s.cfg.RPC.ServicePrefix))
	clientPool := rpcpkg.NewClientPool(resolverBuilder)
	s.backend = backend.NewPool(clientPool, s.logger, s.deliverToClient)
}
```

三个接线细节：

- **同一个 `clientv3.Client` 双用**：注册（`Registry`）与发现（`resolver.NewBuilder`）复用同一 etcd 连接——注释明说「Registry 与 resolver 复用同一连接」；
- **`s.etcdCancel` 挂到 shutdown**：停机时调 cancel = 立即摘除自身，配合 `shutdown.register_drain`（3s）让网关侧 resolver 先感知再停 RPC，消除滚动发布的失败窗口；
- **注释里的真实故障**：「之前误读 `s.cfg.Etcd.Endpoints`，导致整个分支被跳过——backend 为 nil，会话永远绑不到游戏服」。配置读取路径错误不编译失败、只静默跳过，这类问题的教训直接写进了代码注释。

### 4.2 发现如何被消费：选路与绑定

网关 `bindToGame` 的选路策略（`internal/gateway/server.go:283`）：

> 按 `account_id` 对存活 game 节点取模——同一账号始终落到同一节点，玩家数据（Actor）无需跨节点迁移；节点列表来自 etcd（故障节点已被摘除）。

resolver 保证「列表里只有活着的节点」（租约过期即摘），选路只负责「活节点里挑哪个」。两层职责分离：**发现保证正确性，选路保证均衡**。玩家 Actor 的跨节点投递（一个节点确实持有另一个节点上的玩家时）见 [`01-Actor模型.md`](01-Actor模型.md) 的 P0 跨节点链路。

## 5. 演进记录

- **`a0ed305` P1-P4 最小可运行链路**——registry/etcd 与 resolver 同期落地，三段式注册（Grant→Put→KeepAlive）与「先 refresh 后 watch」从初版即为此形态。
- **`331b5e5` P15 全链路演练：压测工具/故障注入/演练报告 + 修复 8 个真实缺陷**——`drill-suite`（`make drill-suite`）注入 MySQL/Redis/Kafka/etcd 故障并恢复；etcd 侧重点验证「杀进程后节点按期消失、重启后重新注册」的租约自愈路径。
- **`6665059` chore: 修正 10 个撒谎的 doc.go，删除 8 处无归属的空包/空目录**——`pkg/registry/doc.go` 的「不定义接口」声明即此批校准的产物：把「当前只有一种实现所以不抽象」从隐含事实写成显式决策。
- **`175e320` chore(r11): 滚动发布暴露 0.63% 失败窗口**——注册摘除与 resolver 感知之间的时间差导致请求打到已关闭实例；`shutdown.register_drain` 配置项（摘注册后等 3s 再停 RPC）即修复。
- **`0e7d1f0`/`a40f144` docs(comments)**——全量中文注释补充，把 key 前缀的两侧硬编码耦合点、keepalive 通道消费契约、「先 refresh 后 watch」的原因写进源码注释（本文引用的注释即此批成果）。

## 6. 测试情况

| 层次 | 覆盖方式 |
| --- | --- |
| 单元测试 | `pkg/registry/etcd` 与 `pkg/rpc/resolver` **均无单测**——两者的行为强依赖 etcd 服务端语义（租约过期、watch 重连），mock clientv3 等于把被测对象的契约再抄一遍，收益低 |
| 集成测试 | `test/integration/p1_p4_smoke_test.go`：testcontainers 起真实 etcd（`quay.io/coreos/etcd:v3.5.10`），验证「登录 → 网关 → etcd 选游戏服 → 绑定」整链路，含「等待 etcd 注册完成」的同步点 |
| 故障演练 | `make drill-suite`：etcd 故障注入与恢复，验证租约自愈（进程崩 → 节点按期消失） |
| leader_test.go | 中心服选主（etcd 竞态创建 leader key）的专项集成测试 |

这是本项目对「基础设施行为验证」的一贯立场：**强依赖外部系统语义的代码用集成测试守，纯逻辑用单测守**——与 [`02-网络层与编解码.md`](02-网络层与编解码.md) 中 codec 只测帧格式不测 proto 解析是同一种分层思路。

## 7. 小结

服务发现用最少的机制满足三个要求：

1. **租约三段式**（Grant → Put with lease → KeepAlive）：key 从诞生带 TTL，崩溃自愈、优雅下线有显式 cancel；keepalive 通道消费与 ctx 独立性两个「不读就死/一消就瞎」的坑都在本层封住；
2. **resolver 先 refresh 后 watch**：启动不丢初始地址，运行期错误分级（启动期失败才上报），Close 收敛防悬垂；
3. **两侧共享前缀、注释互相点名**：`/services` 在注册侧与 resolver 各自硬编码是已知的耦合点，不一致的故障现象（「注册成功但客户端永远解析不到」）写在两边注释里。

下一篇 [`06-缓存cache与writebehind.md`](06-缓存cache与writebehind.md) 进入数据层，看 Redis 如何成为玩家状态的权威源。
