package errcode

import (
	"testing"

	commonv1 "game_study/pkg/pb/common/v1"
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
	} {
		goVals[v] = true
	}
	if len(goVals) != len(protoVals) {
		t.Fatalf("Go 常量 %d 个 != proto 枚举 %d 个", len(goVals), len(protoVals))
	}
	for v := range goVals {
		if !protoVals[v] {
			t.Fatalf("Go 常量 %d 不在 proto 枚举中", int32(v))
		}
	}
	// 文案表覆盖全部常量
	for v := range goVals {
		if v == OK {
			continue
		}
		if Message(v) == "" {
			t.Fatalf("错误码 %d 缺少中文文案", int32(v))
		}
	}
}

func TestFromError(t *testing.T) {
	if got := FromError(nil); got != OK {
		t.Fatalf("nil 应映射 OK，得到 %d", int32(got))
	}
	if got := FromError(New(ErrBagFull)); got != ErrBagFull {
		t.Fatalf("应提取 ErrBagFull，得到 %d", int32(got))
	}
	if got := FromError(Newf(ErrItemNotEnough, "缺少 %d 个", 3)); got != ErrItemNotEnough {
		t.Fatalf("应提取 ErrItemNotEnough，得到 %d", int32(got))
	}
}
