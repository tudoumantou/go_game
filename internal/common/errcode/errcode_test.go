// errcode_test.go —— 错误码包单测：守护 proto/Go/文案表三方一致与 error 提取
package errcode

import (
	"fmt"
	"testing"

	commonv1 "game_study/pkg/pb/common/v1"
	"github.com/stretchr/testify/require"
)

// TestParityWithProto 守护 proto 枚举 ↔ Go 常量 ↔ 文案表三方一致（23.2 单测点）
func TestParityWithProto(t *testing.T) {
	// proto 枚举值集合
	protoVals := map[Code]bool{}
	for _, v := range commonv1.ErrCode_value {
		protoVals[Code(v)] = true
	}
	// Go 常量集合
	goVals := map[Code]bool{}
	for _, v := range []Code{
		OK, ErrParam, ErrTokenExpired, ErrMaintenance, ErrRateLimited, ErrServerBusy,
		ErrAccountBanned, ErrKicked, ErrNoRole, ErrNameUsed, ErrNameIllegal,
		ErrItemNotExist, ErrItemNotEnough, ErrBagFull, ErrItemNotUsable,
		ErrChannelClosed, ErrPeerNotFound, ErrChatTooFast, ErrChatMuted,
		ErrMailNotFound, ErrMailTaken, ErrNoAttachment,
		ErrBattleNotFound, ErrBattleTokenInvalid, ErrResultReported, ErrBattleFull, ErrResultRewardInvalid,
		ErrSigninConfig, ErrSigninRewardLimit, ErrSigninBagFull,
	} {
		goVals[v] = true
	}
	require.EqualValues(t, len(protoVals), len(goVals))
	for v := range goVals {
		require.False(t, !protoVals[v], fmt.Sprintf("Go 常量 %d 不在 proto 枚举中", int32(v)))
	}
	// 文案表覆盖全部常量
	for v := range goVals {
		if v == OK {
			continue
		}
		require.NotEqualValues(t, "", Message(v), fmt.Sprintf("错误码 %d 缺少中文文案", int32(v)))
	}
}

// TestFromError 覆盖三类输入：nil → OK（成功路径不构造 error）、
// *Error → 原样取码（含 Newf 带上下文文案的场景）、其它 error → ErrParam 兜底。
// 兜底策略保证接入层转 Resp.code 时永远不会漏码成 0（0 会被客户端当成功）。
func TestFromError(t *testing.T) {
	got := FromError(nil)
	require.EqualValues(t, OK, got, fmt.Sprintf("nil 应映射 OK，得到 %d", int32(got)))
	got = FromError(New(ErrBagFull))
	require.EqualValues(t, ErrBagFull, got, fmt.Sprintf("应提取 ErrBagFull，得到 %d", int32(got)))
	got = FromError(Newf(ErrItemNotEnough, "缺少 %d 个", 3))
	require.EqualValues(t, ErrItemNotEnough, got, fmt.Sprintf("应提取 ErrItemNotEnough，得到 %d", int32(got)))
}
