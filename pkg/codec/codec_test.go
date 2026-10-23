// pkg/codec/codec_test.go —— codec 包单测
package codec

import (
	"bytes"
	"context"
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
	if got.MsgID != p.MsgID || got.Seq != p.Seq || !bytes.Equal(got.Body, p.Body) {
		t.Fatalf("decode mismatch: %+v", got)
	}
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
	if _, ok := m.(*gatev1.HandshakeReq); !ok {
		t.Fatalf("type mismatch")
	}
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
	if err := r.Handle(context.Background(), 1001, &gatev1.HandshakeReq{}); err != nil {
		t.Fatalf("handle: %v", err)
	}
	require.True(t, called, "handler not called")
}
