// Package etcd 基于 etcd 的服务注册与发现。
//
// 租约 + KeepAlive 维持节点存活，Watch 感知上下线；网关与游戏服都通过它注册。
package etcd
