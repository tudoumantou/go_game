// pkg/codec/codec_test.go —— codec 包单测
package codec

import (
	"bytes"
	"context"
	"testing"

	gatev1 "game_study/pkg/pb/gate/v1"
	"google.golang.org/protobuf/proto"
)

func TestPacketEncodeDecode(t *testing.T) {
	p := Packet{MsgID: 1001, Seq: 42, Body: []byte("encrypted-body")}
	buf, err := EncodePacket(p)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	length, err := ReadLength(buf)
	if err != nil {
		t.Fatalf("read length: %v", err)
	}
	if length != HeaderSize+len(p.Body) {
		t.Fatalf("length mismatch: %d", length)
	}
	got, err := DecodePacket(buf[LengthFieldSize:])
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.MsgID != p.MsgID || got.Seq != p.Seq || !bytes.Equal(got.Body, p.Body) {
		t.Fatalf("decode mismatch: %+v", got)
	}
}

func TestMessageRegistry(t *testing.T) {
	r := NewMessageRegistry()
	r.Register(1001, func() proto.Message { return &gatev1.HandshakeReq{} })

	m, err := r.Create(1001)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, ok := m.(*gatev1.HandshakeReq); !ok {
		t.Fatalf("type mismatch")
	}
}

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
	if !called {
		t.Fatalf("handler not called")
	}
}

