// internal/game/player/state_dirty_test.go —— 写路径标脏的测试锁（R2.5）
//
// 这是「删掉全量 JSON 比对」的安全网。框架现在用 State.changed 判断
// 「本次消息是否需要提交」：任何一个写方法漏调 touch，改动就不会落库——
// 是静默丢数据，线上极难发现。
//
// 所以每新增一个修改持久化字段的方法，都必须在这里加一条用例。
package player

import (
	"context"
	"encoding/json"
	"testing"

	"game_study/internal/game/module"
	"game_study/pkg/db"

	"github.com/DATA-DOG/go-sqlmock"
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

// TestDirtyModulesTrackAndClear 模块级脏标记：置位、排序、清理。
//
// 这组标记决定落库写哪几张 Ext 表，与 DirtyFields（服务 Redis 副本）是
// 两套独立状态：副本同步成功不能顺带清掉落库标记，否则模块状态永远不入库。
func TestDirtyModulesTrackAndClear(t *testing.T) {
	s := NewState(1)
	require.Nil(t, s.DirtyModules(), "初始无脏模块")

	s.SetExt("signin", json.RawMessage(`{"days":[1]}`))
	s.SetExt("quest", json.RawMessage(`{"quests":{}}`))
	require.Equal(t, []string{"quest", "signin"}, s.DirtyModules(), "应按名字升序")

	// 副本同步清字段级标记，不应影响模块级
	s.ClearDirtyFields()
	require.Equal(t, []string{"quest", "signin"}, s.DirtyModules(),
		"ClearDirtyFields 只服务副本同步，不能清模块标记")

	s.ClearDirtyModules()
	require.Nil(t, s.DirtyModules(), "落库成功后应清空")

	// Ext 仍在（数据没删），只是标记清了
	require.NotEmpty(t, s.Ext("signin"))
}

// TestSaveSkipsModulesThatDidNotChange 只改基础字段时，不应产生任何模块表写入。
//
// 用「Ext 里有内容、但没有脏标记」模拟「从库加载进来的模块状态」：
// 它已经在库里了，这一轮没有变化，再写一遍纯属浪费。
// 若实现退回全量写，会因为模块未注册而报错（no persister registered），
// 这条测试就是那道闸门。
func TestSaveSkipsModulesThatDidNotChange(t *testing.T) {
	raw, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer raw.Close()

	repo := NewRepository(&db.DB{DB: raw})
	state := NewState(7)
	state.SetProfile(module.Profile{Name: "n", Level: 2, Exp: 3, Coin: 4, SceneID: 5})
	// 模拟冷启动加载的模块状态：有数据，但不脏
	state.SetExt("zz_loaded_mod", json.RawMessage(`{"v":1}`))
	state.ClearDirtyModules()
	require.Nil(t, state.DirtyModules())

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE player SET").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO player_attr").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	require.NoError(t, repo.Save(context.Background(), state), "只改基础字段应成功")
	require.NoError(t, mock.ExpectationsWereMet())
}
