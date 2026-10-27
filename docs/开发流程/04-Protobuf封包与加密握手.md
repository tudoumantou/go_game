# 04 Protobuf封包与加密握手

[上一章](03-TCP连接与会话生命周期.md) · [课程总览](00-总览与阅读顺序.md)

## 这一章做出什么

实现数字消息类型、长度包与会话握手，区分建立密钥和验证账号。

## 前置条件与本章文件

已完成 TCP echo；需要理解字节序、slice 和错误返回。

- [pkg/codec/packet.go](../../pkg/codec/packet.go)：物理帧。
- [api/proto/gate/v1/handshake.proto](../../api/proto/gate/v1/handshake.proto)：握手消息。
- [api/proto/buf.gen.yaml](../../api/proto/buf.gen.yaml)：协议生成。
- [pkg/crypto/handshake.go](../../pkg/crypto/handshake.go)：密钥交换。
- [pkg/crypto/session_cipher.go](../../pkg/crypto/session_cipher.go)：加解密与序号。
- [internal/gateway/router/router.go](../../internal/gateway/router/router.go)：状态机入口。

## 按顺序实现

第一步固定 wire 格式 Length(2)|MsgID(2)|Seq(4)|Body。先测 Encode/Decode 对称，再接 TCP。Length 不包括自己；网络整数用大端；外层 MsgID 是 uint16。

第二步为一个 PlayerInfoReq/Resp 定义 proto，生成 Go 代码。Protobuf 字段号不是数组位置，不随意重用。网络上只有 bytes，需要数字 MsgID 帮你决定反序列化成哪个类型。

第三步加入 ServerHello → ClientKeyExchange → Finished。服务器有长期 RSA 密钥，客户端每次生成随机 pre-master；salt 和 HKDF 让双方派生相同 AES-256 密钥。不要自己用字符串截取当派生算法。

第四步所有已握手业务包用 SessionCipher。当前实现将 MsgID、Seq 和 protobuf 一起放入加密体，外层仍有头；双方封包必须一致。

第五步接状态机：没建密钥不能接收密文业务；建好密钥但没 Token 鉴权，也不能操作角色。错误顺序明确拒绝，不能靠 nil 指针 panic 实现验证。

```bash
# 已有仓库的纯组件回归，不会启动服务。
go test ./pkg/codec ./pkg/crypto -v
# 协议变化才需要生成；已提交的 pb.go 可直接编译。
make gen-proto
```


## 核心源码与注释阅读

源码节选：[pkg/crypto/session_cipher.go](../../pkg/crypto/session_cipher.go)。这是现有包中的片段，依赖同文件类型，不是可单独运行的完整程序。

```go
func (s *SessionCipher) Encrypt(msgID uint16, plaintext []byte) ([]byte, uint32, error) {
	s.mu.Lock()
	seq := s.sendSeq
	s.sendSeq++
	s.mu.Unlock()

	// body = msgID(2B) | seq(4B) | plaintext
	body := make([]byte, 6+len(plaintext))
	binary.BigEndian.PutUint16(body[0:], msgID)
	binary.BigEndian.PutUint32(body[2:], seq)
	copy(body[6:], plaintext)

	ct, err := s.gcm.Encrypt(body)
	if err != nil {
		return nil, 0, err
	}
	return ct, seq, nil
}
```

源码节选：[pkg/crypto/session_cipher.go](../../pkg/crypto/session_cipher.go)。这是现有包中的片段，依赖同文件类型，不是可单独运行的完整程序。

```go
func (s *SessionCipher) checkSeq(seq uint32) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if seq == 0 {
		return fmt.Errorf("seq must be > 0")
	}

	if seq > s.recvSeq {
		// 前进窗口
		diff := seq - s.recvSeq
		if diff >= 64 {
			s.window = 0
		} else {
			s.window <<= diff
		}
		s.window |= 1
		s.recvSeq = seq
		return nil
	}

	// seq <= recvSeq，落在窗口内才接受
	diff := s.recvSeq - seq
	if diff >= 64 {
		return fmt.Errorf("seq %d too old (window base %d)", seq, s.recvSeq-63)
	}
	bit := uint64(1) << diff
	if s.window&bit != 0 {
		return fmt.Errorf("seq %d replay detected", seq)
	}
	s.window |= bit
	return nil
}
```

## 逐段解释与知识点

Encrypt 的锁保护 sendSeq 分配，锁外进行加密。随机 nonce 与单调 Seq 解决不同问题，不能互相替代。业务层若并发发送，还需检查序号分配到实际写出的顺序。

窗口用 uint64 位图标记最近序号。收到更大 Seq 就移动窗口，窗口内重复位拒绝，太旧序号拒绝。uint32 序号耗尽时需要重新建会话或明确定义策略，不能让自然回绕后继续当新连接使用。

当前 RSA 公钥从 ServerHello 接收，缺少独立信任根验证时仍不能证明对端身份。这里是在理解教学协议，不宣称已完成生产传输安全审计。

## 怎么验证与定位错误

密钥两端一致、篡改密文失败、重复包失败、零序号失败、窗口边界通过/拒绝应各有用例。Handshake 与 SessionCipher 单测通过后，再执行机器人握手，避免带数据库排查纯加密问题。

如果 protobuf unmarshal 失败，检查是否忘记解密、内层六字节头是否剥离、MsgID 是否对应消息类型。

## 独立练习与通过标准

画出四种状态：“已连接、已发公钥、密钥就绪、已鉴权”，为每种状态列允许的消息。通过标准：能解释加密成功为什么不等于登录成功。

下一章：[第 05 章 游客登录与Token鉴权](05-游客登录与Token鉴权.md)。
