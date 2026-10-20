// Package rpc gRPC 基础设施。
//
// 能力：服务端组装与优雅停止（server.go）、客户端连接（client.go）、
// etcd 服务发现 resolver（resolver/etcd.go）。
//
// 架构职责：进程间通信（网关↔游戏↔中心）的唯一通道——服务注册、监听、
// 客户端连接池、地址解析全部收敛在本包及其子包。
// 协作关系：服务端经 rpc.NewServer + GRPCServer() 挂业务 service；
// 客户端经 ClientPool.Get("etcd:///<service>") 拿连接，地址由
// resolver 子包从 etcd 实时解析；registry/etcd 负责节点上下线。
// 设计取舍：
//   - 客户端连接池按 target 缓存 ClientConn：gRPC 的 ClientConn 内部是
//     多路复用 HTTP/2 连接，一个 target 一个 conn 即可服务所有 RPC，
//     池化避免频繁建连；坏连接（Shutdown/TransientFailure）主动重建，
//     不把「半死」的 conn 递给调用方。
//   - 服务端优雅停止必须有界：双向流 RPC 在客户端不主动断开时永不结束，
//     无界 GracefulStop 会把关服卡到被 SIGKILL（见 server.go 注释）。
package rpc
