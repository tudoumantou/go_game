// internal/game/module/proto_test.go —— 类型化处理器：解包/回包/错误通道。
package module

import (
	"errors"
	"testing"

	gamev1 "game_study/pkg/pb/game/v1"
	"google.golang.org/protobuf/proto"
)

// TestTypedDecodesAndReplies 正常路径：解开请求、交给业务、自动回包。
func TestTypedDecodesAndReplies(t *testing.T) {
	var seen int64
	h := Typed(2002, func(_ *Context, req *gamev1.FriendApplyReq, resp *gamev1.FriendApplyResp) error {
		seen = req.PlayerId
		resp.Code = 0
		return nil
	})
	ctx, replies := newThinFixture(nil)

	body, err := proto.Marshal(&gamev1.FriendApplyReq{PlayerId: 42})
	if err != nil {
		t.Fatal(err)
	}
	if err := h(ctx, 2001, body); err != nil {
		t.Fatal(err)
	}
	if seen != 42 {
		t.Fatalf("req not decoded: %d", seen)
	}
	var resp gamev1.FriendApplyResp
	if err := proto.Unmarshal((*replies)[2002], &resp); err != nil {
		t.Fatalf("响应未回包: %v", err)
	}
	if resp.Code != 0 {
		t.Fatalf("code = %d", resp.Code)
	}
}

// TestTypedEmptyBodyIsZeroRequest 空消息体（列表类请求）不报错，req 为零值。
func TestTypedEmptyBodyIsZeroRequest(t *testing.T) {
	called := false
	h := Typed(2002, func(_ *Context, req *gamev1.FriendListReq, _ *gamev1.FriendListResp) error {
		called = req != nil
		return nil
	})
	ctx, replies := newThinFixture(nil)
	if err := h(ctx, 2001, nil); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("空 body 应传入非 nil 的零值 req")
	}
	if _, ok := (*replies)[2002]; !ok {
		t.Fatal("空 body 也应正常回包")
	}
}

// TestTypedDecodeError 解码失败不回包（数据已损坏，给客户端一个空响应没有意义）。
func TestTypedDecodeError(t *testing.T) {
	h := Typed(2002, func(*Context, *gamev1.FriendApplyReq, *gamev1.FriendApplyResp) error {
		t.Fatal("解码失败时不应进入业务")
		return nil
	})
	ctx, replies := newThinFixture(nil)
	err := h(ctx, 2001, []byte{0xff, 0xff})
	if err == nil {
		t.Fatal("损坏的 body 应报错")
	}
	if len(*replies) != 0 {
		t.Fatalf("解码失败不应回包: %v", *replies)
	}
}

// TestTypedBusinessErrorPropagates 业务返回 error（DB 故障）时上抛且不回包。
func TestTypedBusinessErrorPropagates(t *testing.T) {
	boom := errors.New("db down")
	h := Typed(2002, func(*Context, *gamev1.FriendApplyReq, *gamev1.FriendApplyResp) error {
		return boom
	})
	ctx, replies := newThinFixture(nil)
	body, _ := proto.Marshal(&gamev1.FriendApplyReq{PlayerId: 1})
	if err := h(ctx, 2001, body); !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
	if len(*replies) != 0 {
		t.Fatal("业务错误不应自动回包（交给错误通道）")
	}
}
