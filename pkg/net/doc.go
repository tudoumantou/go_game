// Package net 网络层封装：TCP 服务端（网关接入基础），实现见 tcp_server.go。
//
// 架构职责：把 net.Listener 的 accept 循环、连接 goroutine 管理、优雅关闭
// 封装成可复用组件；协议解析（定长/长度前缀、protobuf 编解码）不在本包，
// 由 Handler 实现方负责。
// 协作关系：gateway 进程用它监听客户端 TCP 端口；rpc.Server 走 gRPC 不经过这里；
// shutdown 编排通过 ctx 取消触发 Start 返回，再等 Stop 收敛。
// 设计取舍：
//   - goroutine per connection：每个连接一个 goroutine 做读写循环，
//     代码直观、天然并发；代价是每连接约 2~8KB 初始栈，万级连接可接受，
//     本项目网关按万级设计；若未来上十万级需换 epoll/goroutine 池方案。
//   - 粘包/半包不在本包解决：TCP 是字节流，消息边界由应用协议定义
//     （长度前缀等），Handler 侧用 bufio.Reader 逐条解析；本包只交付
//     net.Conn，保持职责单一。
package net
