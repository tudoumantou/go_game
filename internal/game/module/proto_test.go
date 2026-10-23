// internal/game/module/proto_test.go —— 类型化处理器：解包/回包/错误通道。
package module

import (
	"errors"
	"testing"

	"fmt"
	gamev1 "game_study/pkg/pb/game/v1"
	"github.com/stretchr/testify/require"
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
	require.NoError(t, err)
	err = h(ctx, 2001, body)
	require.NoError(t, err)
	require.EqualValues(t, 42, seen)
	var resp gamev1.FriendApplyResp
	err = proto.Unmarshal((*replies)[2002], &resp)
	require.NoError(t, err, fmt.Sprintf("响应未回包: %v", err))
	require.EqualValues(t, 0, resp.Code)
}

// TestTypedEmptyBodyIsZeroRequest 空消息体（列表类请求）不报错，req 为零值。
func TestTypedEmptyBodyIsZeroRequest(t *testing.T) {
	called := false
	h := Typed(2002, func(_ *Context, req *gamev1.FriendListReq, _ *gamev1.FriendListResp) error {
		called = req != nil
		return nil
	})
	ctx, replies := newThinFixture(nil)
	err := h(ctx, 2001, nil)
	require.NoError(t, err)
	require.True(t, called, "空 body 应传入非 nil 的零值 req")
	_, ok := (*replies)[2002]
	require.True(t, ok, "空 body 也应正常回包")
}

// TestTypedDecodeError 解码失败不回包（数据已损坏，给客户端一个空响应没有意义）。
func TestTypedDecodeError(t *testing.T) {
	h := Typed(2002, func(*Context, *gamev1.FriendApplyReq, *gamev1.FriendApplyResp) error {
		t.Fatal("解码失败时不应进入业务")
		return nil
	})
	ctx, replies := newThinFixture(nil)
	err := h(ctx, 2001, []byte{0xff, 0xff})
	require.Error(t, err, "损坏的 body 应报错")
	require.Len(t, *replies, 0)
}

// TestTypedBusinessErrorPropagates 业务返回 error（DB 故障）时上抛且不回包。
func TestTypedBusinessErrorPropagates(t *testing.T) {
	boom := errors.New("db down")
	h := Typed(2002, func(*Context, *gamev1.FriendApplyReq, *gamev1.FriendApplyResp) error {
		return boom
	})
	ctx, replies := newThinFixture(nil)
	body, _ := proto.Marshal(&gamev1.FriendApplyReq{PlayerId: 1})
	err := h(ctx, 2001, body)
	require.False(t, !errors.Is(err, boom), fmt.Sprintf("err = %v", err))
	require.Len(t, *replies, 0, "业务错误不应自动回包（交给错误通道）")
}
