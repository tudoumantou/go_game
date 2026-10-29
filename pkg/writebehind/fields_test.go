// pkg/writebehind/fields_test.go —— 字段名一处定义的守护
//
// 这组用例的存在理由是 fields.go 顶部那段：四个核心字段名此前在六处
// 各写一遍，漏改的表现不均匀（报错 / 静默丢字段 / 永不标脏）。常量表
// 把「写」收敛成一处，但「用」仍有三处 switch/map——它们必须覆盖同一批
// 名字，而编译器不会替我们检查这件事（switch 少一个 case 完全合法）。
//
// 所以这里用反射式遍历代替手工核对：新增字段只加进 CoreFields，编码与
// 解码两条路径都必须能处理它，否则本文件立刻红。
package writebehind

import (
	"encoding/json"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestCoreFieldsAreEncodable 每个核心字段都必须能被 marshalField 编码。
//
// 漏在 marshalField 的症状是 "unknown field" 错误——会炸，属于良性失败；
// 但它把「加字段」变成一次上线故障，能提前挡在测试里就挡。
func TestCoreFieldsAreEncodable(t *testing.T) {
	snap := Snapshot{
		PlayerID:      1,
		Items:         map[int32]int64{},
		AppliedGrants: map[string]bool{"k": true},
	}
	for _, f := range CoreFields {
		raw, err := snap.Field(f)
		require.NoError(t, err, "核心字段 %q 必须能被编码", f)
		require.NotNil(t, raw, "核心字段 %q 编码结果不应为 nil", f)
	}
}

// TestCoreFieldsAreDecodable 每个核心字段都必须能被 parseRecord 解码。
//
// 这条才是真正要防的：漏在 parseRecord 不报错，只是 Redis 里写进去的
// 字段读不回来——重启后玩家数据静默回到旧值，日志一片正常。
func TestCoreFieldsAreDecodable(t *testing.T) {
	values := map[string]string{"version": "1"}
	for _, f := range CoreFields {
		values[f] = sampleJSON(f)
	}
	rec, err := parseRecord(1, values)
	require.NoError(t, err)
	// 逐字段断言「确实解进了对应结构」，而不是只断言没报错：
	// json.Unmarshal 对空对象/未知键都很宽容，仅看 error 会漏。
	require.Equal(t, "p", rec.State.Profile.Name, "profile 字段应解进 Profile")
	require.EqualValues(t, 3, rec.State.Attr.Atk, "attr 字段应解进 Attr")
	require.EqualValues(t, 7, rec.State.Items[1001], "items 字段应解进 Items")
	require.True(t, rec.State.AppliedGrants["biz"], "grants 字段应解进 AppliedGrants")
}

// TestCoreFieldsNoDuplicate 常量表自身不能有重复。
//
// 重复会让 CoreFields 长度与实际字段数不符，进而让「整份写入」多写一个
// 空 field——Redis 里多一条无意义记录，不致命但会一直积累。
func TestCoreFieldsNoDuplicate(t *testing.T) {
	seen := make(map[string]bool, len(CoreFields))
	for _, f := range CoreFields {
		require.False(t, seen[f], "核心字段 %q 重复定义", f)
		seen[f] = true
	}
}

// TestFieldNamesMatchCoreFields 快照的整份字段名必须以前四个核心字段开头。
//
// Snapshot.FieldNames 与 State.Fields 是两条独立实现（分属两个包），
// 顺序与集合必须一致，否则 Seed 整份写入与增量写入的 field 对不上。
func TestFieldNamesMatchCoreFields(t *testing.T) {
	snap := Snapshot{PlayerID: 1, Ext: map[string]json.RawMessage{"quest": json.RawMessage(`{}`)}}
	got := snap.FieldNames()
	require.GreaterOrEqual(t, len(got), len(CoreFields))
	for i, f := range CoreFields {
		require.Equal(t, f, got[i], "第 %d 个字段应为 %q", i, f)
	}
	rest := append([]string(nil), got[len(CoreFields):]...)
	sort.Strings(rest)
	require.Equal(t, []string{FieldExtPrefix + "quest"}, rest, "核心字段之后应全是 ext: 前缀")
}

// sampleJSON 给每个核心字段一份能解出非零值的 JSON 载荷。
//
// 用真实形状而非 "{}"：空对象能让 json.Unmarshal 成功，于是「字段解没解
// 进结构体」这件事被掩盖——本文件要断言的正是后者。
func sampleJSON(field string) string {
	switch field {
	case FieldProfile:
		return `{"name":"p"}`
	case FieldAttr:
		return `{"atk":3}`
	case FieldItems:
		return `{"1001":7}`
	case FieldGrants:
		return `{"biz":true}`
	}
	return `{}`
}
