// internal/game/module/friend/friend_test.go —— 好友模块单测：自加/非法目标拦截、上限兜底。
//
// repo 传 nil：这些用例只验证「不进数据库」的模块层校验分支。
package friend

import (
	"encoding/json"
	"fmt"
	"testing"

	"game_study/internal/game/module"
	"game_study/pkg/log"

	gamev1 "game_study/pkg/pb/game/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// fakeState 只实现好友模块用到的部分。
type fakeState struct{ id int64 }

func (f *fakeState) PlayerID() int64                 { return f.id }
func (f *fakeState) Profile() module.Profile         { return module.Profile{} }
func (f *fakeState) ExpMax() int64                   { return 0 }
func (f *fakeState) GetAttr(string) int64            { return 0 }
func (f *fakeState) SetAttr(string, int64)           {}
func (f *fakeState) AddBonus(string, int64)          {}
func (f *fakeState) ClearBonus()                     {}
func (f *fakeState) GetItemCount(int32) int64        { return 0 }
func (f *fakeState) Items() map[int32]int64          { return nil }
func (f *fakeState) AddItem(int32, int64)            {}
func (f *fakeState) RemoveItem(int32, int64) bool    { return true }
func (f *fakeState) MarkDirty(string)                {}
func (f *fakeState) SetSceneID(int32)                {}
func (f *fakeState) Ext(string) json.RawMessage      { return nil }
func (f *fakeState) SetExt(string, json.RawMessage)  {}
func (f *fakeState) CanHold([]module.ItemGrant) bool { return true }
func (f *fakeState) ClaimBiz(string) bool            { return true }
func (f *fakeState) HasBiz(string) bool              { return false }

func newCtx(id int64) (*module.Context, *[]byte) {
	var got []byte
	return &module.Context{
		State: &fakeState{id: id},
		Send: func(_ uint32, body []byte) error {
			got = body
			return nil
		},
	}, &got
}

func decodeApply(t *testing.T, raw []byte) *gamev1.FriendApplyResp {
	t.Helper()
	require.NotNil(t, raw, "no reply captured")
	var resp gamev1.FriendApplyResp
	err := proto.Unmarshal(raw, &resp)
	require.NoError(t, err)
	return &resp
}

// TestApplySelfRejected 加自己不发 SQL（repo 为 nil 也能走到校验分支）。
func TestApplySelfRejected(t *testing.T) {
	m := New(nil, nil, log.New("test"))
	ctx, raw := newCtx(7)

	body, _ := proto.Marshal(&gamev1.FriendApplyReq{PlayerId: 7})
	err := m.OnMessage(ctx, msgIDApplyReq, body)
	require.NoError(t, err)
	resp := decodeApply(t, *raw)
	require.EqualValues(t, codeSelf, resp.Code, fmt.Sprintf("code = %d, want %d", resp.Code, codeSelf))
}

// TestApplyInvalidTarget 非法目标直接拒绝，不触碰数据库。
func TestApplyInvalidTarget(t *testing.T) {
	m := New(nil, nil, log.New("test"))
	ctx, raw := newCtx(7)

	body, _ := proto.Marshal(&gamev1.FriendApplyReq{PlayerId: 0})
	err := m.OnMessage(ctx, msgIDApplyReq, body)
	require.NoError(t, err)
	resp := decodeApply(t, *raw)
	require.EqualValues(t, codeTargetNotFound, resp.Code, fmt.Sprintf("code = %d, want %d", resp.Code, codeTargetNotFound))
}

// TestLimitsHaveDefaults 配置缺失时上限有兜底，不返回 0（0 会让所有人立刻「好友已满」）。
func TestLimitsHaveDefaults(t *testing.T) {
	require.False(t, friendLimit() <= 0, fmt.Sprintf("friend limit = %d", friendLimit()))
	require.False(t, applyLimit() <= 0, fmt.Sprintf("apply limit = %d", applyLimit()))
}
