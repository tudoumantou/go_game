package rank

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"game_study/internal/common/errcode"
	"game_study/internal/game/module"
	"game_study/pkg/codec"
	"game_study/pkg/log"

	gamev1 "game_study/pkg/pb/game/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// fakeState 只实现榜单模块用到的部分（PlayerID）。
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

type fakeQuerier struct {
	res     *Result
	err     error
	gotType string
	gotPID  int64
}

func (f *fakeQuerier) Query(_ context.Context, rankType string, playerID int64, _ int32) (*Result, error) {
	f.gotType, f.gotPID = rankType, playerID
	return f.res, f.err
}

// newCtx 组装最小模块上下文，捕获回包。
func newCtx(t *testing.T, id int64) (*module.Context, *[]byte, *uint32) {
	t.Helper()
	var got []byte
	var gotID uint32
	ctx := &module.Context{
		State: &fakeState{id: id},
		Send: func(msgID uint32, body []byte) error {
			gotID, got = msgID, body
			return nil
		},
	}
	return ctx, &got, &gotID
}

func decodeResp(t *testing.T, raw []byte) *gamev1.RankListResp {
	t.Helper()
	require.NotNil(t, raw, "no reply captured")
	var resp gamev1.RankListResp
	err := proto.Unmarshal(raw, &resp)
	require.NoError(t, err, fmt.Sprintf("unmarshal: %v", err))
	return &resp
}

func TestListReturnsTopAndMine(t *testing.T) {
	q := &fakeQuerier{res: &Result{
		Season:    "s1",
		Entries:   []Entry{{PlayerID: 1, Score: 300, Rank: 1}, {PlayerID: 2, Score: 200, Rank: 2}},
		Around:    []Entry{{PlayerID: 2, Score: 200, Rank: 2}},
		OnBoard:   true,
		MyRank:    2,
		MyScore:   200,
		UpdatedAt: 1700000000,
	}}
	m := New(q, log.New("test"))
	ctx, raw, msgID := newCtx(t, 2)

	body, _ := proto.Marshal(&gamev1.RankListReq{RankType: "arena_score"})
	err := m.OnMessage(ctx, codec.MsgRankListReq, body)
	require.NoError(t, err)
	require.EqualValues(t, codec.MsgRankListResp, *msgID)
	resp := decodeResp(t, *raw)
	require.False(t, resp.Code != int32(errcode.OK) || len(resp.Entries) != 2 || resp.MyRank != 2 || !resp.OnBoard, fmt.Sprintf("unexpected resp: %+v", resp))
	require.EqualValues(t, 1700000000, resp.UpdatedAt, fmt.Sprintf("meta lost: %+v", resp))
	require.EqualValues(t, "s1", resp.Season)
	require.EqualValues(t, 2, q.gotPID, fmt.Sprintf("querier got pid=%d type=%q", q.gotPID, q.gotType))
	require.EqualValues(t, "arena_score", q.gotType)
}

func TestListDefaultsRankType(t *testing.T) {
	q := &fakeQuerier{res: &Result{Season: "s1"}}
	m := New(q, log.New("test"))
	ctx, raw, _ := newCtx(t, 7)

	body, _ := proto.Marshal(&gamev1.RankListReq{})
	err := m.OnMessage(ctx, codec.MsgRankListReq, body)
	require.NoError(t, err)
	require.EqualValues(t, defaultRankType, q.gotType)
	resp := decodeResp(t, *raw)
	require.EqualValues(t, defaultRankType, resp.RankType, fmt.Sprintf("resp rank type = %q", resp.RankType))
}

// TestListUnavailable 中心服不可用时不伪造空榜（R8.1）。
func TestListUnavailable(t *testing.T) {
	cases := []struct {
		name string
		q    Querier
	}{
		{"no querier", nil},
		{"query error", &fakeQuerier{err: errors.New("center not connected")}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			m := New(c.q, log.New("test"))
			ctx, raw, _ := newCtx(t, 1)
			body, _ := proto.Marshal(&gamev1.RankListReq{RankType: "arena_score"})
			err := m.OnMessage(ctx, codec.MsgRankListReq, body)
			require.NoError(t, err)
			resp := decodeResp(t, *raw)
			require.EqualValues(t, int32(errcode.ErrRankUnavailable), resp.Code)
			require.Len(t, resp.Entries, 0, "unavailable must not return fake entries")
		})
	}
}

func TestListRejectsLongRankType(t *testing.T) {
	m := New(&fakeQuerier{}, log.New("test"))
	ctx, raw, _ := newCtx(t, 1)
	long := make([]byte, maxRankTypeLen+1)
	for i := range long {
		long[i] = 'a'
	}
	body, _ := proto.Marshal(&gamev1.RankListReq{RankType: string(long)})
	err := m.OnMessage(ctx, codec.MsgRankListReq, body)
	require.NoError(t, err)
	resp := decodeResp(t, *raw)
	require.EqualValues(t, int32(errcode.ErrRankBadType), resp.Code, fmt.Sprintf("code = %d, want %d", resp.Code, int32(errcode.ErrRankBadType)))
}
