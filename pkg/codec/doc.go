// Package codec 编解码：定长/变长包协议（Length|MsgID|Seq|Body）、protobuf 消息注册表与反射分发。
// 实现在 packet.go / protobuf.go / registry.go（设计文档 4.2）。
package codec
