// pkg/codec/codec_test.go —— codec 包单测
package codec

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	gatev1 "game_study/pkg/pb/gate/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// TestPacketEncodeDecode 覆盖 EncodePacket → ReadLength → DecodePacket 全链路。
//
// 关键断言：Length 字段 == HeaderSize+len(Body)（Length 不含自身，这是
// 协议图里最容易写反的一条）；解码后 MsgID/Seq/Body 逐字段一致。
// 构造说明：Body 用任意字符串冒充「已加密密文」——本层只验证帧格式，
// 不关心内容，加密正确性由 crypto 包的测试覆盖。
func TestPacketEncodeDecode(t *testing.T) {
	p := Packet{MsgID: 1001, Seq: 42, Body: []byte("encrypted-body")}
	buf, err := EncodePacket(p)
	require.NoError(t, err, "encode")
	length, err := ReadLength(buf)
	require.NoError(t, err, "read length")
	require.EqualValues(t, HeaderSize+len(p.Body), length)
	got, err := DecodePacket(buf[LengthFieldSize:])
	require.NoError(t, err, "decode")
	require.False(t, got.MsgID != p.MsgID || got.Seq != p.Seq || !bytes.Equal(got.Body, p.Body), fmt.Sprintf("decode mismatch: %+v", got))
}

// TestMessageRegistry 验证 Create 返回**正确类型**的空消息实例。
//
// 只测工厂路由（msgID → 类型），不测反序列化内容——proto 库的解析
// 正确性由它自己的测试保证，这里重复测没有收益。
// 构造：注册 gatev1.HandshakeReq 后用类型断言验证，避免 reflect.DeepEqual
// 对空消息的误判。
func TestMessageRegistry(t *testing.T) {
	r := NewMessageRegistry()
	r.Register(1001, func() proto.Message { return &gatev1.HandshakeReq{} })

	m, err := r.Create(1001)
	require.NoError(t, err, "create")
	_, ok := m.(*gatev1.HandshakeReq)
	require.True(t, ok, "type mismatch")
}

// TestHandlerRegistry 验证 Handle 按 MsgID 路由到注册的 handler。
//
// 关键断言：handler 被真正调用（called 标志位）且返回值透传（nil）。
// 构造：handler 用闭包捕获 called，避免引入包级变量造成测试间污染。
func TestHandlerRegistry(t *testing.T) {
	r := NewHandlerRegistry()
	called := false
	r.Register(1001, func(ctx context.Context, msg proto.Message) error {
		called = true
		return nil
	})
	err := r.Handle(context.Background(), 1001, &gatev1.HandshakeReq{})
	require.NoError(t, err, fmt.Sprintf("handle: %v", err))
	require.True(t, called, "handler not called")
}

// TestEncodePacketRejectsMsgIDOverWire 超过 2 字节的消息号必须报错。
//
// 内部类型是 uint32，wire 仍是 2 字节：窄化只在这一处。若静默截断，
// 70000 会变成 4464——一个看起来完全合法的号，收端会路由到不相干的
// 协议上，症状是「偶发串包」，查起来比重启还慢。
func TestEncodePacketRejectsMsgIDOverWire(t *testing.T) {
	if _, err := EncodePacket(Packet{MsgID: maxWireMsgID + 1}); err == nil {
		t.Fatal("EncodePacket 接受了超出 wire 范围的 msgID")
	}
	// 边界值本身要能过（65535 是 2 字节能表示的最大值）。
	buf, err := EncodePacket(Packet{MsgID: maxWireMsgID})
	require.NoError(t, err)
	pkt, err := DecodePacket(buf[LengthFieldSize:])
	require.NoError(t, err)
	require.Equal(t, uint32(maxWireMsgID), pkt.MsgID)
}
