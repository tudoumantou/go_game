// internal/game/module/profile/profile_test.go —— 玩家信息模块单测
//
// 这是纯读模块：自己不持有状态，只把 PlayerState 的字段和 GetAttr 的
// 最终值打包下发。所以测试重点不在分支，而在「取的是最终值还是基础值」
// 和「字段有没有漏/错位」——属性面板错位一个 AttrId，客户端会把它当
// 另一个属性渲染。
package profile

import (
	"testing"

	"game_study/internal/game/module"
	"game_study/internal/game/module/moduletest"
	"game_study/pkg/log"
	gamev1 "game_study/pkg/pb/game/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func newTestModule() (*Module, *moduletest.State, *moduletest.Recorder, *module.Context) {
	st := moduletest.NewState(1001)
	rec := moduletest.NewRecorder()
	m := New(log.New("profile-test", log.WithLevel("error")))
	return m, st, rec, moduletest.NewContext(st, rec)
}

func playerInfo(t *testing.T, m *Module, ctx *module.Context, rec *moduletest.Recorder) *gamev1.PlayerInfoResp {
	t.Helper()
	raw, err := moduletest.Call(m, ctx, rec, msgIDPlayerInfoReq, msgIDPlayerInfoResp, &gamev1.PlayerInfoReq{})
	require.NoError(t, err)
	require.NotNil(t, raw, "玩家信息没有回包")
	resp := &gamev1.PlayerInfoResp{}
	require.NoError(t, proto.Unmarshal(raw, resp))
	require.Equal(t, 1, rec.Count(msgIDPlayerInfoResp), "一次请求只能回一个包")
	return resp
}

// TestPlayerInfoCarriesProfileFields 基础字段逐个透传，不能错位。
func TestPlayerInfoCarriesProfileFields(t *testing.T) {
	m, st, rec, ctx := newTestModule()
	st.SetProfile(module.Profile{Name: "阿甲", Level: 7, Exp: 123, Coin: 4567})

	resp := playerInfo(t, m, ctx, rec)
	require.EqualValues(t, 0, resp.Code)
	require.EqualValues(t, 1001, resp.RoleId, "角色 ID 取会话身份")
	require.Equal(t, "阿甲", resp.Name)
	require.EqualValues(t, 7, resp.Level)
	require.EqualValues(t, 123, resp.Exp)
	require.EqualValues(t, 4567, resp.Coin)
}

// TestPlayerInfoAttrsUseFinalValue 属性下发的是最终值（基础 + 加成），
// 不是 Profile 里的基础值——否则装备/伙伴的加成在面板上永远看不到。
func TestPlayerInfoAttrsUseFinalValue(t *testing.T) {
	m, st, rec, ctx := newTestModule()
	st.SetAttr("hp", 100)
	st.SetAttr("mp", 50)
	st.SetAttr("atk", 10)
	st.SetAttr("def", 5)
	st.SetAttr("spd", 3)
	// 模拟装备/伙伴模块在 OnLogin 里加的派生加成
	st.AddBonus("atk", 15)
	st.AddBonus("def", 5)

	resp := playerInfo(t, m, ctx, rec)
	require.Len(t, resp.Attrs, 5)

	// AttrId 顺序是协议契约（客户端按位渲染），必须严格 1-5
	got := map[int32]int64{}
	for i, p := range resp.Attrs {
		require.EqualValues(t, int32(i+1), p.AttrId, "AttrId 必须按 1-5 顺序下发")
		got[p.AttrId] = p.Value
	}
	require.EqualValues(t, 100, got[attrHP])
	require.EqualValues(t, 50, got[attrMP])
	require.EqualValues(t, 25, got[attrATK], "10 基础 + 15 加成")
	require.EqualValues(t, 10, got[attrDEF], "5 基础 + 5 加成")
	require.EqualValues(t, 3, got[attrSPD])
}

// TestPlayerInfoAttrsZeroWhenEmpty 新号没有属性时下发 0，而不是缺项。
//
// 客户端按 AttrId 建面板，少一项会渲染成上一个属性的值或空白；
// 「0」至少语义明确。
func TestPlayerInfoAttrsZeroWhenEmpty(t *testing.T) {
	m, _, rec, ctx := newTestModule()

	resp := playerInfo(t, m, ctx, rec)
	require.Len(t, resp.Attrs, 5, "五个属性必须齐全，不能因为值为 0 就省略")
	for _, p := range resp.Attrs {
		require.EqualValues(t, 0, p.Value)
	}
}

// TestLogoutMarksDirty 下线标脏（提示 flusher 关注本轮）。
func TestLogoutMarksDirty(t *testing.T) {
	m, st, _, ctx := newTestModule()
	require.NoError(t, m.OnLogout(ctx))
	require.Contains(t, st.DirtyModules(), "profile")
}

// TestOnLoginIsNoop 纯读模块不该在登录时改任何状态。
func TestOnLoginIsNoop(t *testing.T) {
	m, st, rec, ctx := newTestModule()
	require.NoError(t, m.OnLogin(ctx))
	require.Empty(t, st.DirtyModules(), "登录不该产生提交")
	require.Zero(t, rec.Total(), "登录不该下发任何包")
}
