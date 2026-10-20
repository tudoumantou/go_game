// Package etcd 基于 etcd 的服务注册与发现。
//
// 租约 + KeepAlive 维持节点存活，Watch 感知上下线；网关与游戏服都通过它注册。
//
// 架构职责：把「进程存活」翻译成 etcd 的原语——注册 = Put 一个带租约的
// key；存活 = 定期续约（KeepAlive）；发现 = Watch/List 前缀。
// 协作关系：pkg/rpc/resolver 消费 Watch 结果更新 gRPC 地址列表；
// 各进程 main 在启动尾部调用 RegisterWithKeepalive，关服时调返回的 cancel。
// 设计取舍：
//   - 用租约 TTL 而非进程主动 Deregister 判存活：进程可能被 SIGKILL/宕机，
//     根本来不及反注册；TTL 到期 key 自动消失，崩溃检测不依赖「死者的配合」。
//   - keepalive 用 context.Background()：注册是进程级承诺，不能随某个
//     请求的 ctx 取消而中断（详见 RegisterWithKeepalive 注释）。
//   - key 里存完整 JSON 而非只存 addr：Watch 侧拿到即用，省一次 RPC；
//     代价是节点元数据变更要整值覆盖，对本场景（节点信息小且少变）可接受。
package etcd
