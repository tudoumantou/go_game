package partner

import (
	"encoding/json"
	"fmt"
	"testing"

	"game_study/internal/game/module"
	"game_study/pkg/gamedata"
	"game_study/pkg/log"
	"game_study/pkg/model"

	gamev1 "game_study/pkg/pb/game/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// fakeState 带真实物品与加成状态的替身：伙伴的扣费与属性变化都要能断言。
type fakeState struct {
	id    int64
	items map[int32]int64
	bonus map[string]int64
	ext   map[string]json.RawMessage
}

func newFakeState(id int64, coin int64) *fakeState {
	return &fakeState{
		id:    id,
		items: map[int32]int64{itemCoin: coin},
		bonus: map[string]int64{},
		ext:   map[string]json.RawMessage{},
	}
}

func (f *fakeState) PlayerID() int64                  { return f.id }
func (f *fakeState) Profile() module.Profile          { return module.Profile{Level: 1} }
func (f *fakeState) ExpMax() int64                    { return 0 }
func (f *fakeState) GetAttr(string) int64             { return 0 }
func (f *fakeState) SetAttr(string, int64)            {}
func (f *fakeState) AddBonus(key string, delta int64) { f.bonus[key] += delta }
func (f *fakeState) ClearBonus()                      { f.bonus = map[string]int64{} }
func (f *fakeState) GetItemCount(id int32) int64      { return f.items[id] }
func (f *fakeState) Items() map[int32]int64           { return f.items }
func (f *fakeState) AddItem(id int32, n int64)        { f.items[id] += n }
func (f *fakeState) RemoveItem(id int32, n int64) bool {
	if f.items[id] < n {
		return false
	}
	f.items[id] -= n
	return true
}
func (f *fakeState) MarkDirty(string)                {}
func (f *fakeState) SetSceneID(int32)                {}
func (f *fakeState) Ext(name string) json.RawMessage { return f.ext[name] }
func (f *fakeState) SetExt(name string, raw json.RawMessage) {
	f.ext[name] = raw
}
func (f *fakeState) CanHold([]module.ItemGrant) bool { return true }
func (f *fakeState) ClaimBiz(string) bool            { return true }
func (f *fakeState) HasBiz(string) bool              { return false }

func newCtx(st *fakeState) (*module.Context, *[]byte) {
	var got []byte
	return &module.Context{
		State: st,
		Send: func(_ uint32, body []byte) error {
			got = body
			return nil
		},
	}, &got
}

func firstPartner(t *testing.T) gamedata.PartnerCfg {
	t.Helper()
	list := gamedata.PartnerCfgList()
	require.NotEmpty(t, list, "partner config is empty")
	return list[0]
}

func TestActivateAndStarUp(t *testing.T) {
	cfg := firstPartner(t)
	st := newFakeState(1, 10_000)
	ctx, raw := newCtx(st)
	m := New(log.New("test"))

	// 激活：扣配置里的 col3，星级 1。
	body, _ := proto.Marshal(&gamev1.PartnerActivateReq{PartnerId: int32(cfg.Id)})
	err := m.OnMessage(ctx, msgIDActivateReq, body)
	require.NoError(t, err)
	var act gamev1.PartnerActivateResp
	_ = proto.Unmarshal(*raw, &act)
	require.EqualValues(t, codeOK, act.Code)
	got := st.items[itemCoin]
	require.EqualValues(t, 10_000-cfg.Col3, got, fmt.Sprintf("coin = %d, want %d", got, 10_000-cfg.Col3))

	// 重复激活：拒绝且不扣钱。
	err = m.OnMessage(ctx, msgIDActivateReq, body)
	require.NoError(t, err)
	_ = proto.Unmarshal(*raw, &act)
	require.EqualValues(t, codeOwned, act.Code)
	got = st.items[itemCoin]
	require.EqualValues(t, 10_000-cfg.Col3, got, fmt.Sprintf("duplicate activate must not charge: %d", got))

	// 升星：1→2 消耗 partner_star 里 (当前星级=1) 的 col3。
	cost, ok := starCost(int32(cfg.Id), 1)
	require.True(t, ok, "missing star cost config")
	starBody, _ := proto.Marshal(&gamev1.PartnerStarUpReq{PartnerId: int32(cfg.Id)})
	err = m.OnMessage(ctx, msgIDStarUpReq, starBody)
	require.NoError(t, err)
	var up gamev1.PartnerStarUpResp
	_ = proto.Unmarshal(*raw, &up)
	require.EqualValues(t, codeOK, up.Code, fmt.Sprintf("star up code=%d star=%d", up.Code, up.Star))
	require.EqualValues(t, 2, up.Star)
	got = st.items[itemCoin]
	require.EqualValues(t, 10_000-cfg.Col3-cost, got, fmt.Sprintf("coin after star up = %d", got))
}

func TestFieldBonusAppliedOnce(t *testing.T) {
	cfg := firstPartner(t)
	st := newFakeState(1, 10_000)
	ctx, raw := newCtx(st)
	m := New(log.New("test"))

	// 先激活。
	body, _ := proto.Marshal(&gamev1.PartnerActivateReq{PartnerId: int32(cfg.Id)})
	_ = m.OnMessage(ctx, msgIDActivateReq, body)

	// 上阵：加成 = col3 × 1 星。
	onBody, _ := proto.Marshal(&gamev1.PartnerFieldReq{PartnerId: int32(cfg.Id), On: true})
	err := m.OnMessage(ctx, msgIDFieldReq, onBody)
	require.NoError(t, err)
	var fr gamev1.PartnerFieldResp
	_ = proto.Unmarshal(*raw, &fr)
	require.EqualValues(t, cfg.Col3, fr.AtkBonus)
	require.EqualValues(t, cfg.Col3, st.bonus["atk"])

	// 重复上阵：幂等，不能二次累加（R6.3）。
	_ = m.OnMessage(ctx, msgIDFieldReq, onBody)
	for i := 0; i < 3; i++ {
		_ = m.OnMessage(ctx, msgIDFieldReq, onBody)
	}
	require.EqualValues(t, cfg.Col3, st.bonus["atk"])

	// 下阵：加成归零。
	offBody, _ := proto.Marshal(&gamev1.PartnerFieldReq{PartnerId: int32(cfg.Id), On: false})
	_ = m.OnMessage(ctx, msgIDFieldReq, offBody)
	require.EqualValues(t, 0, st.bonus["atk"])
}

// TestOnLoginRestoresBonus 登录时按快照重算加成，且不依赖上次会话的 bonus。
func TestOnLoginRestoresBonus(t *testing.T) {
	cfg := firstPartner(t)
	st := newFakeState(1, 0)
	ctx, _ := newCtx(st)
	m := New(log.New("test"))

	d := &Data{Stars: map[int32]int32{int32(cfg.Id): 2}, Field: []int32{int32(cfg.Id)}}
	err := save(st, 1, d)
	require.NoError(t, err)
	// 模拟残留：上次会话的加成还在。
	st.bonus["atk"] = 9999
	st.ClearBonus() // 玩家层在 LoginAll 前统一清空

	err = m.OnLogin(ctx)
	require.NoError(t, err)
	want := cfg.Col3 * 2
	require.EqualValues(t, want, st.bonus["atk"])
}

// TestDecodeRejectsBrokenState 上阵位超限/上阵未拥有都要在解码时拦住。
func TestDecodeRejectsBrokenState(t *testing.T) {
	cases := []string{
		`{"stars":{},"field":[1,2,3,4]}`,  // 超上阵位
		`{"stars":{},"field":[1]}`,        // 上阵但未拥有
		`{"stars":{"1":1},"field":[1,1]}`, // 重复上阵
	}
	for _, raw := range cases {
		_, err := decode(&model.PlayerPartner{Partners: raw})
		require.Error(t, err, fmt.Sprintf("decode must reject %s", raw))
	}
}
