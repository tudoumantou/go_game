// Package codec 编解码层：TCP 物理包编解码、MsgID↔proto.Message 注册表、消息 Handler 分发。
//
// 架构位置：位于网关 net 之下、业务之上，是进程收发消息的第一道边界——
//   - packet.go：客户端 ↔ 网关的外层定长包格式 Length|MsgID|Seq|Body（设计文档 4.2），
//     只负责字节流切包，不接触密文解密与消息语义；
//   - protobuf.go：MsgID → proto.Message 工厂注册表，把消息号解析成具体消息对象；
//   - registry.go：MsgID → 业务 Handler 分发表，把反序列化后的消息路由给玩法模块。
//
// 协作关系：net 层读连接 → ReadLength 取长度 → DecodePacket 切包 → crypto 解密得到明文
// → MessageRegistry.Unmarshal 反序列化 → HandlerRegistry.Handle 分发。
// 设计取舍：两张注册表都约定「启动期注册、运行期只读」，读路径因此完全无锁；
// 消息号由 proto 注解声明并生成（msgids_gen.go），一旦下发客户端即不可变更。
package codec
