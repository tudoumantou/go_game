// internal/game/module/fail_test.go —— FailCode 的反射置码
//
// 验两件事：带 code 字段的响应能被置上；没有该字段的消息静默跳过（不 panic）。
package module

import (
	"testing"

	"game_study/internal/common/errcode"
	gamev1 "game_study/pkg/pb/game/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestFailCodeSetsBusinessCode(t *testing.T) {
	c := &Context{}
	resp := &gamev1.MailTakeAttachmentResp{}

	require.NoError(t, c.FailCode(resp, errcode.ErrMailNotFound))
	require.EqualValues(t, int32(errcode.ErrMailNotFound), resp.Code)
}

// TestFailCodeSkipsMessageWithoutCode 没有 code 字段的消息不能 panic：
// 参数校验失败也走这条路径，炸在这里会把「回个错误码」变成进程崩溃。
func TestFailCodeSkipsMessageWithoutCode(t *testing.T) {
	c := &Context{}
	msg := &gamev1.BagChangeNotify{}

	require.NotPanics(t, func() {
		require.NoError(t, c.FailCode(msg, errcode.ErrSystem))
	})
}

// TestFailCodeNilMessage nil 消息同样静默返回（防御性，处理器里可能传空）。
func TestFailCodeNilMessage(t *testing.T) {
	c := &Context{}
	var msg proto.Message
	require.NoError(t, c.FailCode(msg, errcode.ErrSystem))
}
