// internal/game/module/quest/quest_test.go —— 任务模块单测：接取→事件推进→领奖→幂等。
//
// 事件类型与目标次数直接取自配置首行，XML 变更时用例自动跟随，不写死数值。
package quest

import (
	"encoding/json"
	"fmt"
	"game_study/internal/common/errcode"
	"testing"

	"game_study/internal/game/module"
	"game_study/pkg/codec"
	"game_study/pkg/gamedata"
	gamev1 "game_study/pkg/pb/game/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// fakeState 是 module.PlayerState 的内存实现，只保留任务模块用到的能力。
type fakeState struct {
	id    int64
	items map[int32]int64
	ext   map[string]json.RawMessage
	biz   map[string]bool
	exp   int64
}

func newFakeState(id int64) *fakeState {
	return &fakeState{id: id, items: map[int32]int64{}, ext: map[string]json.RawMessage{}, biz: map[string]bool{}}
}

func (f *fakeState) PlayerID() int64               { return f.id }
func (f *fakeState) Profile() module.Profile       { return module.Profile{Exp: f.exp} }
func (f *fakeState) ExpMax() int64                 { return 0 }
func (f *fakeState) GetAttr(string) int64          { return 0 }
func (f *fakeState) SetAttr(string, int64)         {}
func (f *fakeState) AddBonus(string, int64)        {}
func (f *fakeState) ClearBonus()                   {}
func (f *fakeState) GetItemCount(id int32) int64   { return f.items[id] }
func (f *fakeState) Items() map[int32]int64        { return f.items }
func (f *fakeState) AddItem(id int32, count int64) { f.items[id] += count }
func (f *fakeState) RemoveItem(id int32, count int64) bool {
	if f.items[id] < count {
		return false
	}
	f.items[id] -= count
	return true
}
func (f *fakeState) MarkDirty(string)                {}
func (f *fakeState) SetSceneID(int32)                {}
func (f *fakeState) Ext(name string) json.RawMessage { return f.ext[name] }
func (f *fakeState) SetExt(name string, raw json.RawMessage) {
	f.ext[name] = raw
}
func (f *fakeState) CanHold([]module.ItemGrant) bool { return true }

func (f *fakeState) ClaimBiz(key string) bool {
	if f.biz[key] {
		return false
	}
	f.biz[key] = true
	return true
}
func (f *fakeState) HasBiz(key string) bool { return f.biz[key] }

// newCtx 组装上下文；返回按消息号索引的下行报文，便于断言业务码。
func newCtx(st *fakeState) (*module.Context, *map[uint32][]byte) {
	replies := map[uint32][]byte{}
	return &module.Context{
		State:        st,
		CacheEnabled: true,
		Send: func(id uint32, body []byte) error {
			replies[id] = append([]byte(nil), body...)
			return nil
		},
		AddExp: func(n int64) (bool, int32) {
			st.exp += n
			return false, 1
		},
	}, &replies
}

func firstQuest(t *testing.T) gamedata.QuestCfg {
	t.Helper()
	require.NotEmpty(t, gamedata.QuestCfgList(), "quest config is empty")
	return gamedata.QuestCfgList()[0]
}

// TestAcceptProgressClaim 接取 → 事件推进 → 领奖，状态与奖励都落在快照里。
func TestAcceptProgressClaim(t *testing.T) {
	cfg := firstQuest(t)
	st := newFakeState(1)
	ctx, replies := newCtx(st)
	m := M

	acceptBody, _ := proto.Marshal(&gamev1.QuestAcceptReq{QuestId: int32(cfg.Id)})
	err := m.OnMessage(ctx, codec.MsgQuestAcceptReq, acceptBody)
	require.NoError(t, err)
	// 事件推进（类型取自配置，增量给足）
	m.OnEvent(ctx, module.Event{Type: cfg.Col2, Delta: int64(cfg.Col3)})

	d, err := m.State(ctx)
	require.NoError(t, err)
	rec := d.Quests[int32(cfg.Id)]
	require.EqualValues(t, statusDone, rec.Status, fmt.Sprintf("progress not advanced: %+v", rec))
	require.EqualValues(t, int32(cfg.Col3), rec.Progress)

	claimBody, _ := proto.Marshal(&gamev1.QuestClaimReq{QuestId: int32(cfg.Id)})
	err = m.OnMessage(ctx, codec.MsgQuestClaimReq, claimBody)
	require.NoError(t, err)
	require.EqualValues(t, int64(cfg.Col3)*10, st.GetItemCount(1001))
	require.EqualValues(t, int64(cfg.Col3)*100, st.exp)
	var claimResp gamev1.QuestClaimResp
	require.NoError(t, proto.Unmarshal((*replies)[codec.MsgQuestClaimResp], &claimResp))
	require.EqualValues(t, int32(errcode.OK), claimResp.Code, "领奖成功必须回 0")
	d, _ = m.State(ctx)
	require.EqualValues(t, statusClaimed, d.Quests[int32(cfg.Id)].Status, "claim state not persisted in snapshot")
}

// TestClaimIdempotent 重复领奖只返回已领，不再发奖。
func TestClaimIdempotent(t *testing.T) {
	cfg := firstQuest(t)
	st := newFakeState(1)
	ctx, replies := newCtx(st)
	m := M

	body, _ := proto.Marshal(&gamev1.QuestClaimReq{QuestId: int32(cfg.Id)})
	d := &Data{Quests: map[int32]Record{int32(cfg.Id): {Status: statusDone, Progress: int32(cfg.Col3)}}}
	require.NoError(t, m.Mutate(ctx, func(dst *Data) bool { *dst = *d; return true }))
	for i := 0; i < 2; i++ {
		err := m.OnMessage(ctx, codec.MsgQuestClaimReq, body)
		require.NoError(t, err)
	}
	require.EqualValues(t, int64(cfg.Col3)*10, st.GetItemCount(1001))
	var resp gamev1.QuestClaimResp
	require.NoError(t, proto.Unmarshal((*replies)[codec.MsgQuestClaimResp], &resp))
	require.EqualValues(t, int32(errcode.ErrQuestClaimed), resp.Code, "重复领奖必须回 7104，不能被成功码覆盖")
}

// TestClaimUnfinished 进度不足时不发奖、不改状态。
func TestClaimUnfinished(t *testing.T) {
	cfg := firstQuest(t)
	st := newFakeState(1)
	ctx, replies := newCtx(st)
	m := M
	d := &Data{Quests: map[int32]Record{int32(cfg.Id): {Status: statusDoing}}}
	require.NoError(t, m.Mutate(ctx, func(dst *Data) bool { *dst = *d; return true }))
	body, _ := proto.Marshal(&gamev1.QuestClaimReq{QuestId: int32(cfg.Id)})
	err := m.OnMessage(ctx, codec.MsgQuestClaimReq, body)
	require.NoError(t, err)
	require.EqualValues(t, 0, st.GetItemCount(1001), "unfinished quest must not grant")
	var resp gamev1.QuestClaimResp
	require.NoError(t, proto.Unmarshal((*replies)[codec.MsgQuestClaimResp], &resp))
	require.EqualValues(t, int32(errcode.ErrQuestUnfinished), resp.Code, "未完成必须回 7103，不能被成功码覆盖")
	back, _ := m.State(ctx)
	require.EqualValues(t, statusDoing, back.Quests[int32(cfg.Id)].Status, "status changed on failed claim")
}

// TestSnapshotShape Ext 槽位存的是业务结构（Data），不是表行。
//
// 这是 R2.6 的关键约定：业务结构 ↔ 表行的转换在 persist.go 里，
// 快照里只出现 Data —— 否则业务改字段时还得同步改「快照长什么样」。
func TestSnapshotShape(t *testing.T) {
	st := newFakeState(9)
	ctx, _ := newCtx(st)
	m := M

	d := &Data{Quests: map[int32]Record{101: {Status: statusClaimed, Progress: 5}}}
	require.NoError(t, m.Mutate(ctx, func(dst *Data) bool { *dst = *d; return true }))

	var decoded Data
	require.NoError(t, json.Unmarshal(st.Ext(stateKey), &decoded), "Ext 必须是 Data 结构")
	require.EqualValues(t, statusClaimed, decoded.Quests[101].Status)
	require.EqualValues(t, 5, decoded.Quests[101].Progress)
}
