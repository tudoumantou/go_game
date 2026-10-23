// internal/game/player/state_field_test.go —— FieldSource 实现：按字段序列化。
package player

import (
	"encoding/json"
	"testing"

	"game_study/internal/game/module"
	"github.com/stretchr/testify/require"
)

// TestStateFieldMapsNames 字段名到内部结构的映射。
//
// 这套名字是 Redis Hash 的 field 名，改一个就等于改存储格式，
// 所以用测试把映射钉死。
func TestStateFieldMapsNames(t *testing.T) {
	s := NewState(9)
	s.AddItem(1001, 5)
	s.SetExt("signin", json.RawMessage(`{"days":[1]}`))

	raw, err := s.Field("items")
	require.NoError(t, err)
	require.Equal(t, `{"1001":5}`, string(raw))

	raw, err = s.Field("ext:signin")
	require.NoError(t, err)
	require.Equal(t, `{"days":[1]}`, string(raw), "Ext 直接引用原字节，不二次序列化")

	// 未持有的模块：nil 表示「没有内容」，提交时跳过该字段。
	raw, err = s.Field("ext:quest")
	require.NoError(t, err)
	require.Nil(t, raw)

	// 字段名拼错要让它在提交时报错，而不是静默丢数据。
	_, err = s.Field("nope")
	require.Error(t, err)
}

// TestStateFieldsIncludesExt 整份写入的字段清单：四个基础字段 + 已持有模块。
func TestStateFieldsIncludesExt(t *testing.T) {
	s := NewState(9)
	s.SetExt("signin", json.RawMessage(`{}`))
	s.SetExt("quest", json.RawMessage(`{}`))

	fields := s.Fields()
	require.Len(t, fields, 6)
	for _, base := range []string{"profile", "attr", "items", "grants"} {
		require.Contains(t, fields, base)
	}
	require.Contains(t, fields, "ext:signin")
	require.Contains(t, fields, "ext:quest")
}

// TestStateFieldProfileRoundTrip profile/attr 与 snapshot 同源同格式。
func TestStateFieldProfileRoundTrip(t *testing.T) {
	s := NewState(9)
	s.SetProfile(module.Profile{Name: "hero", Level: 60})

	raw, err := s.Field("profile")
	require.NoError(t, err)
	var got module.Profile
	require.NoError(t, json.Unmarshal(raw, &got))
	require.Equal(t, "hero", got.Name)
	require.Equal(t, int32(60), got.Level)

	// 与 snapshot() 里的 profile 序列化结果一致（否则提交与落库格式会分叉）。
	snap, _ := json.Marshal(s.snapshot().Profile)
	require.JSONEq(t, string(snap), string(raw))
}
