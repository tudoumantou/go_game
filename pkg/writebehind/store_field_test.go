// pkg/writebehind/store_field_test.go —— 分字段存储（R2.5-B）
package writebehind

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// stubPersister 让 Validate 认得测试用的模块名（它只校验「模块已注册」）。
type stubPersister struct{ name string }

func (p stubPersister) Name() string                                             { return p.name }
func (p stubPersister) Validate(json.RawMessage) error                           { return nil }
func (p stubPersister) PersistBatch(context.Context, *sql.Tx, []ModuleRow) error { return nil }
func (p stubPersister) Load(context.Context, *sql.Tx, int64) (json.RawMessage, error) {
	return nil, nil
}

// ensureStubPersisters 按需注册测试模块名。
//
// 其它测试（registry_test/flat_test）会 resetPersisters 清空全局表，
// 所以不能放在 init 里——每次构造 Store 前补一次。
func ensureStubPersisters() {
	for _, name := range []string{"signin", "quest"} {
		if _, ok := LookupPersister(name); ok {
			continue
		}
		RegisterPersister(stubPersister{name: name})
	}
}

func newFieldStore(t *testing.T) (*Store, *miniredis.Miniredis) {
	t.Helper()
	ensureStubPersisters()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return NewStore(rdb), mr
}

// TestCommitWritesOnlyChangedFields 只提交变化字段时，其余字段原样保留。
func TestCommitWritesOnlyChangedFields(t *testing.T) {
	ctx := context.Background()
	store, _ := newFieldStore(t)

	full := Snapshot{
		PlayerID: 7,
		Profile:  Profile{Name: "hero", Level: 10, Coin: 500},
		Attr:     Attr{Hp: 1000, Atk: 50},
		Items:    map[int32]int64{1001: 3},
		Ext:      map[string]json.RawMessage{"signin": json.RawMessage(`{"day":1}`)},
	}
	token, err := store.AcquireLease(ctx, 7)
	require.NoError(t, err)
	_, err = store.Commit(ctx, full.PlayerID, full, full.FieldNames(), nil, token)
	require.NoError(t, err)

	// 只改签到的 Ext，其余字段不该被重写（内容不变即可，用值判断）。
	full.Ext["signin"] = json.RawMessage(`{"day":2}`)
	_, err = store.Commit(ctx, full.PlayerID, full, []string{"ext:signin"}, nil, token)
	require.NoError(t, err)

	got, err := store.Load(ctx, 7)
	require.NoError(t, err)
	require.Equal(t, "hero", got.State.Profile.Name, "未提交的字段应保留")
	require.EqualValues(t, 3, got.State.Items[1001])
	require.Equal(t, `{"day":2}`, string(got.State.Ext["signin"]), "变化字段应更新")
	require.False(t, got.Legacy, "新格式不应标记 Legacy")
}

// TestLoadLegacyDataField 升级前的整份 data 仍能读，且标记 Legacy。
func TestLoadLegacyDataField(t *testing.T) {
	ctx := context.Background()
	store, mr := newFieldStore(t)

	old := Snapshot{
		PlayerID: 9,
		Profile:  Profile{Name: "old", Level: 5},
		Items:    map[int32]int64{2001: 8},
		Ext:      map[string]json.RawMessage{"quest": json.RawMessage(`{"p":1}`)},
	}
	raw, err := json.Marshal(old)
	require.NoError(t, err)
	mr.HSet(store.stateKey(9), "version", "3")
	mr.HSet(store.stateKey(9), "data", string(raw))

	got, err := store.Load(ctx, 9)
	require.NoError(t, err)
	require.True(t, got.Legacy, "整份 data 格式应标记 Legacy")
	require.Equal(t, "old", got.State.Profile.Name)
	require.EqualValues(t, 8, got.State.Items[2001])
}

// TestLegacyMigrationWritesFullSnapshot 旧格式经一次全量提交后迁成分字段。
func TestLegacyMigrationWritesFullSnapshot(t *testing.T) {
	ctx := context.Background()
	store, mr := newFieldStore(t)

	old := Snapshot{
		PlayerID: 9,
		Profile:  Profile{Name: "old", Level: 5},
		Items:    map[int32]int64{2001: 8},
	}
	raw, err := json.Marshal(old)
	require.NoError(t, err)
	mr.HSet(store.stateKey(9), "version", "3")
	mr.HSet(store.stateKey(9), "data", string(raw))

	token, err := store.AcquireLease(ctx, 9)
	require.NoError(t, err)
	// 调用方看到 Legacy 后必须传全量字段。
	_, err = store.Commit(ctx, old.PlayerID, old, old.FieldNames(), nil, token)
	require.NoError(t, err)

	fields, err := mr.HKeys(store.stateKey(9))
	require.NoError(t, err)
	require.NotContains(t, fields, "data", "迁移后应清掉旧格式字段")
	require.Contains(t, fields, "profile")
	require.Contains(t, fields, "items")

	got, err := store.Load(ctx, 9)
	require.NoError(t, err)
	require.False(t, got.Legacy)
	require.Equal(t, "old", got.State.Profile.Name, "迁移不能丢数据")
	require.EqualValues(t, 8, got.State.Items[2001])
}

// TestCommitRemovesDeletedExtExt 模块状态被 Reset 后，Redise 里的 ext field 要删掉，
// 否则下次 Load 会把已重置的模块状态读回来。
func TestCommitRemovesDeletedExt(t *testing.T) {
	ctx := context.Background()
	store, mr := newFieldStore(t)

	snap := Snapshot{
		PlayerID: 11,
		Profile:  Profile{Name: "x"},
		Items:    map[int32]int64{},
		Ext:      map[string]json.RawMessage{"signin": json.RawMessage(`{"d":1}`)},
	}
	token, err := store.AcquireLease(ctx, 11)
	require.NoError(t, err)
	_, err = store.Commit(ctx, snap.PlayerID, snap, snap.FieldNames(), nil, token)
	require.NoError(t, err)
	keys, err := mr.HKeys(store.stateKey(11))
	require.NoError(t, err)
	require.Contains(t, keys, "ext:signin")

	// 模块 Reset：Ext 里不再有 signin，调用方把 "ext:signin" 放进 removed。
	snap.Ext = map[string]json.RawMessage{}
	_, err = store.Commit(ctx, snap.PlayerID, snap, []string{"profile"}, []string{"ext:signin"}, token)
	require.NoError(t, err)

	keys, err = mr.HKeys(store.stateKey(11))
	require.NoError(t, err)
	require.NotContains(t, keys, "ext:signin", "删除的模块状态必须 HDEL")
}

// TestMarshalFieldSkipsEmptyExt 模块没有状态时不写该字段（避免写入 "null"）。
func TestMarshalFieldSkipsEmptyExt(t *testing.T) {
	snap := Snapshot{PlayerID: 1, Ext: map[string]json.RawMessage{}}
	raw, err := marshalField(snap, "ext:missing")
	require.NoError(t, err)
	require.Nil(t, raw, "空 Ext 应返回 nil 由调用方跳过")
}

// benchSnapshot 100 道具 + 5 个模块 Ext 的快照（与 player 包的基准同规模）。
func benchSnapshot() Snapshot {
	snap := Snapshot{
		PlayerID: 42,
		Profile:  Profile{Name: "bench_player", Level: 60, Exp: 12345, Coin: 987654, SceneID: 1001},
		Attr:     Attr{Career: 1, Hp: 8000, Mp: 3000, Atk: 1200, Def: 800, Spd: 150},
		Items:    make(map[int32]int64, 100),
		Ext:      make(map[string]json.RawMessage, 5),
	}
	for i := int32(0); i < 100; i++ {
		snap.Items[10000+i] = int64(i*13 + 1)
	}
	snap.Ext["signin"] = json.RawMessage(`{"days":[1,2,3,4,5],"rewards":{"1001":700}}`)
	snap.Ext["quest"] = json.RawMessage(`{"records":{"1":{"progress":3,"claimed":false}}}`)
	snap.Ext["equip"] = json.RawMessage(`{"equips":[{"uid":1,"equip_id":1001,"enhance":3}]}`)
	snap.Ext["activity"] = json.RawMessage(`{"records":{"101":{"progress":5}}}`)
	snap.Ext["loginreward"] = json.RawMessage(`{"day":3,"streak":7}`)
	return snap
}

// BenchmarkMarshalFull 改造前：每次提交序列化整份快照。
func BenchmarkMarshalFull(b *testing.B) {
	snap := benchSnapshot()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := json.Marshal(snap); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkMarshalSingleField 改造后：只序列化变化的那一个字段。
func BenchmarkMarshalSingleField(b *testing.B) {
	snap := benchSnapshot()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := marshalField(snap, "ext:signin"); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkMarshalItemsOnly 只改背包（另一个常见场景）。
func BenchmarkMarshalItemsOnly(b *testing.B) {
	snap := benchSnapshot()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := marshalField(snap, "items"); err != nil {
			b.Fatal(err)
		}
	}
}
