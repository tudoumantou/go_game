# 03 TCP连接与会话生命周期

[上一章](02-配置日志错误码与数据库.md) · [课程总览](00-总览与阅读顺序.md)

## 这一章做出什么

接收 TCP 连接、累计读缓存、解析完整帧，并让关闭动作幂等。

## 前置条件与本章文件

已完成基础日志与 codec 的概念准备；第 04 章再加入 Protobuf 与加密。

- [pkg/net/tcp_server.go](../../pkg/net/tcp_server.go)：监听与 Accept。
- [internal/gateway/conn/conn.go](../../internal/gateway/conn/conn.go)：连接读缓存与发送。
- [internal/gateway/conn/manager.go](../../internal/gateway/conn/manager.go)：连接索引。
- [internal/gateway/session/session.go](../../internal/gateway/session/session.go)：会话状态与身份。
- [internal/gateway/session/manager.go](../../internal/gateway/session/manager.go)：连接到会话映射。

## 按顺序实现

先实现 Listener，Accept 后为每条连接创建独立 Conn。慢连接不能占住 Accept 循环，读取与连接回收分别管理。测试结束必须 Close listener，解除阻塞的 Accept。

再实现 Conn 的私有缓存：Read 只增加字节；processReadBuf 只在完整帧齐全时调用 handler。先收到长度前缀，再等待完整剩余部分，解析后继续处理连包。

第三步补关闭状态：Close 可以由读失败、写失败、主动踢人、停止服务同时触发。sync.Once 确保底层关闭和 OnClose 回调只执行一次。ConnManager 和 SessionManager 都要清除对应索引。

第四步将连接与会话分开。Conn 管字节与地址；Session 管握手、Token、身份和加密上下文。重连策略决定 Session 是否跨 Conn 留存，不能只因 YAML 存在 reconnect_ttl 就认定整个重连机制已完成。

教学解析思路见网络专题。初学先用明文 echo 测分包，等 04 再替换 body 为加密 protobuf；不要第一次调试就同时排查 TCP、RSA 和业务协议。

```go
// 教学片段：异步持有消息前取得包体所有权。
ownedBody := append([]byte(nil), packet.Body...)
// 此后可以复用网络读缓存；消费者不再引用原缓存。
// postToActor 的真正实现放在第 07 章。
```


## 核心源码与注释阅读

源码节选：[internal/gateway/conn/conn.go](../../internal/gateway/conn/conn.go)。这是现有包中的片段，依赖同文件类型，不是可单独运行的完整程序。

```go
func (c *Conn) processReadBuf() {
	for {
		if len(c.readBuf) < codec.LengthFieldSize {
			return
		}
		length, err := codec.ReadLength(c.readBuf)
		if err != nil {
			c.logger.Error("invalid length", "err", err)
			c.Close(err)
			return
		}
		if length <= 0 || length > 65535-codec.LengthFieldSize {
			c.logger.Error("invalid packet length", "len", length)
			c.Close(fmt.Errorf("invalid packet length %d", length))
			return
		}
		total := codec.LengthFieldSize + length
		if len(c.readBuf) < total {
			return
		}

		pkt, err := codec.DecodePacket(c.readBuf[codec.LengthFieldSize:total])
		if err != nil {
			c.logger.Error("decode packet", "err", err)
			c.Close(err)
			return
		}

		c.handler.OnPacket(c, pkt)
		if metrics.Enabled() {
			metrics.MsgInc("in", "ok")
			metrics.MsgObserve("in", 0, len(pkt.Body))
		}
		copy(c.readBuf, c.readBuf[total:])
		c.readBuf = c.readBuf[:len(c.readBuf)-total]
	}
}
```

源码节选：[internal/gateway/conn/conn.go](../../internal/gateway/conn/conn.go)。这是现有包中的片段，依赖同文件类型，不是可单独运行的完整程序。

```go
func (c *Conn) Close(err error) error {
	c.closeOnce.Do(func() {
		c.closed.Store(true)
		c.closeErr = err
		close(c.closeCh)
		if c.raw != nil {
			_ = c.raw.Close()
		}
		c.handler.OnClose(c, err)
	})
	return c.closeErr
}
```

## 逐段解释与知识点

processReadBuf 使用两层条件：长度字段齐不齐、整个包齐不齐。两个条件缺一个都无法正确处理 TCP。解析失败应关闭异常连接，不继续拿错位数据猜测下一帧。

当前 writeLoop 只等待 closeCh，真正发送是同步 Send。不要照设计文档把当前代码讲成“读写队列已经完整分离”。后续改进单写者时，需要处理队列满、消息顺序、总字节预算与停止排空。

Close 的 Once 保护一次性动作；atomic.Bool 让其他 goroutine 能观察关闭状态。但“先读 closed=false，再去发送，恰好此时关闭”仍会发生，发送方必须处理底层错误。closeCh 只是通知机制，不会深拷贝任何消息。

## 怎么验证与定位错误

执行 `go test ./pkg/codec`。为练习 Conn 用 net.Pipe 连接两端，分别按单字节、整帧和三帧合并方式写入；中途关闭，确认 handler 回调和 manager 删除恰好一次。不要用 time.Sleep 猜测测试完成，使用带超时的确认 channel。

如果能连端口却没有业务响应，先看分包与握手状态；nc 连通只证明 TCP 建连。

## 独立练习与通过标准

补一组“短头、非法长度、半包关闭、并发 Close”用例。通过标准：没有越界、没有重复回调、没有测试残留 goroutine。

下一章：[第 04 章 Protobuf封包与加密握手](04-Protobuf封包与加密握手.md)。
