// internal/game/player/state_dirty_test.go —— 写路径标脏的测试锁（R2.5）
//
// 这是「删掉全量 JSON 比对」的安全网。框架现在用 State.changed 判断
// 「本次消息是否需要提交」：任何一个写方法漏调 touch，改动就不会落库——
// 是静默丢数据，线上极难发现。
//
// 所以每新增一个修改持久化字段的方法，都必须在这里加一条用例。
package player

import (
	"encoding/json"
	"testing"

	"game_study/internal/game/module"

	"github.com/stretchr/testify/require"
)

// TestWriteMethodsMarkChanged 每个写方法调用后必然置 changed。
func TestWriteMethodsMarkChanged(t *testing.T) {
	// AddExp 依赖等级配置表（gamedata 经 go:embed 在包初始化时加载）；
	// 即便不升级，「改了 exp 字段」也要求置脏，用例不依赖升级路径。
	cases := []struct {
		name string
		act  func(*State)
	}{
		{"SetExt", func(s *State) { s.SetExt("demo", json.RawMessage(`{}`)) }},
		{"SetProfile", func(s *State) { s.SetProfile(module.Profile{Name: "x", Level: 2}) }},
		{"SetSceneID", func(s *State) { s.SetSceneID(1001) }},
		{"SetAttr", func(s *State) { s.SetAttr("hp", 123) }},
		{"SetAttrStruct", func(s *State) { s.SetAttrStruct(Attr{Hp: 500}) }},
		{"AddItem", func(s *State) { s.AddItem(1001, 1) }},
		{"RemoveItem", func(s *State) { s.AddItem(1001, 1); s.RemoveItem(1001, 1) }},
		{"MarkDirty", func(s *State) { s.MarkDirty("quest") }},
		{"AddExp", func(s *State) { s.AddExp(10) }},
		{"RecalcAttrs", func(s *State) { s.RecalcAttrs() }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := NewState(1)
			s.ClearChanged() // 构造后归零，确保是「调用本身」置的位
			require.False(t, s.HasChanged())

			tc.act(s)

			require.True(t, s.HasChanged(), "%s 修改了状态却没有标脏——会导致改动不落库", tc.name)
		})
	}
}

// TestClearChangedOnlyResetsFlag 清理改动标记不能顺手清掉落库用的脏明细。
//
// dirty/dirtyItems 是 writebehind 与增量落库的依据，生命周期跨消息；
// changed 只是「本次改没改」的瞬时判断，两者混用会让道具增量写丢。
func TestClearChangedOnlyResetsFlag(t *testing.T) {
	s := NewState(1)
	s.AddItem(1001, 5)
	s.SetExt("signin", json.RawMessage(`{"d":1}`))

	require.True(t, s.HasChanged())
	s.ClearChanged()

	require.False(t, s.HasChanged(), "changed 应被清零")
	require.True(t, s.IsDirty("bag"), "模块脏标记必须保留")
	require.EqualValues(t, 5, s.DirtyItems()[1001], "道具增量必须保留")
	require.NotEmpty(t, s.Ext("signin"), "Ext 内容不受影响")
}

// TestSeedDoesNotMarkChanged 预加载灌数据不应产生提交（否则每次登录都白写一次 Redis）。
func TestSeedDoesNotMarkChanged(t *testing.T) {
	s := NewState(1)
	s.ClearChanged()
	s.SeedItem(1001, 10)
	require.False(t, s.HasChanged(), "SeedItem 是加载路径，不应触发提交")
}
