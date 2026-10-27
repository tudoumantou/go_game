# 06 gRPC服务发现与网关转发

[上一章](05-游客登录与Token鉴权.md) · [课程总览](00-总览与阅读顺序.md)

## 这一章做出什么

让网关按 etcd 发现 Game，通过会话独立的 gRPC 双向流上下行转发。

## 前置条件与本章文件

先有 TCP/Token 概念；练习中可以用空 StreamHandler，不急着接玩法。

- [pkg/registry/etcd/registry.go](../../pkg/registry/etcd/registry.go)：租约注册、列表和监听。
- [pkg/rpc/resolver/etcd.go](../../pkg/rpc/resolver/etcd.go)：gRPC 解析器。
- [pkg/rpc/client.go](../../pkg/rpc/client.go)：连接复用。
- [internal/gateway/backend/pool.go](../../internal/gateway/backend/pool.go)：会话 stream。
- [internal/game/rpc/game_server.go](../../internal/game/rpc/game_server.go)：首帧 BIND 与转发。
- [api/proto/internalv2/game.proto](../../api/proto/internalv2/game.proto)：内部帧结构。

## 按顺序实现

先定义 StreamFrame：控制帧承载 BIND/UNBIND，业务帧承载 session_id、msg_id、seq、body。内部 protobuf 不与外网密文包混用，Gateway 在加解密边界转换。

再启动一个 GameService.Channel 空服务，要求第一帧 BIND。无 BIND、空 ctrl、非法身份都要有明确错误。把身份绑定到流后，后续消息不要仅因它带 session_id 就信任任意会话。

第三步加入 etcd。当前注册路径是 `/services/<service>/<node>`，不是配置注释中的 `/game_study/game`。先 List 得到当前节点，再 Watch 变化；生产实现要考虑 List/Watch 的 revision 衔接和压缩恢复。

第四步网关把 Token 得到的 account_id 放进 BIND。连接池复用 gRPC ClientConn，但每个客户端会话有自己的 Channel stream。会话断开时关闭该流与索引，不能把整个共享 ClientConn 当作某玩家私有连接关掉。

第五步串下行：module Reply → Game.sendToClient → GameRPC.SendToSession → backend 接收 → Gateway.deliverToClient → Session 加密 → TCP Send。

```bash
# 查询真实注册信息，注意前缀是 /services/。
docker compose -f deploy/docker-compose.yaml exec -T etcd   etcdctl get /services/ --prefix
```


## 核心源码与注释阅读

源码节选：[pkg/registry/etcd/registry.go](../../pkg/registry/etcd/registry.go)。这是现有包中的片段，依赖同文件类型，不是可单独运行的完整程序。

```go
func (r *Registry) RegisterWithKeepalive(ctx context.Context, node Node, ttl int64) (context.CancelFunc, error) {
	key := fmt.Sprintf("/services/%s/%s", node.Service, node.ID)
	val, err := json.Marshal(node)
	if err != nil {
		return nil, err
	}

	grant, err := r.client.Grant(ctx, ttl)
	if err != nil {
		return nil, err
	}

	_, err = r.client.Put(ctx, key, string(val), clientv3.WithLease(grant.ID))
	if err != nil {
		return nil, err
	}

	keepCtx, cancel := context.WithCancel(context.Background())
	ch, err := r.client.KeepAlive(keepCtx, grant.ID)
	if err != nil {
		cancel()
		return nil, err
	}

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

	return func() {
		cancel()
		_, _ = r.client.Delete(ctx, key)
	}, nil
}
```

源码节选：[internal/game/rpc/game_server.go](../../internal/game/rpc/game_server.go)。这是现有包中的片段，依赖同文件类型，不是可单独运行的完整程序。

```go
func (s *GameServer) SendToSession(sessionID uint64, frame *internalv2.StreamFrame) error {
	s.mu.RLock()
	stream, ok := s.streams[sessionID]
	s.mu.RUnlock()
	if !ok {
		return fmt.Errorf("session %d stream not found", sessionID)
	}
	return stream.Send(frame)
}
```

## 逐段解释与知识点

租约带 TTL，KeepAlive 保持活性；取消保活不等于立即处理掉所有流量，删除注册与关闭监听应协同。当前 Register helper 又立即调用 cancel，不能按其注释误认为得到一个持续 TTL 的已注册节点，主路径应看 RegisterWithKeepalive。

SendToSession 查 map 后调用 stream.Send，没有统一的每流发送锁。需要分析是否有多个业务 goroutine 并发写同一 stream；保护 map 的 RWMutex 不自动保护 stream 的发送顺序。

在 Channel 实现中 ctrl 建立的 session 与业务帧 cp.SessionId 是否一致，需要显式校验。网关在可信内网不等于任意内部消息都可以随意带身份。

## 怎么验证与定位错误

使用 TestP1toP4Smoke 或现有机器人验证上下行。故意把注册地址写成 0.0.0.0，观察“etcd 有节点但 Gateway 无法访问”；容器注册应公布对端可达地址。

断开一位玩家的 stream，其他玩家仍应可发请求；移除 Game 节点后，新会话不能一直发到旧节点。这里的课程测试目标不等于当前实现已覆盖所有重连边界。

## 独立练习与通过标准

加入一个只回固定 PlayerInfo 的临时 Handler，完全不访问数据库，证明 gRPC 与网络回传独立可用。通过标准：能区分 TCP 连接、ClientConn、stream、Session 和 Actor。

下一章：[第 07 章 Actor运行时与并发所有权](07-Actor运行时与并发所有权.md)。
