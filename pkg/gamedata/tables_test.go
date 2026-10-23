package gamedata

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// resetSnapshot 每个用例开始时回到编译期内嵌快照，避免用例之间互相污染。
func resetSnapshot(t *testing.T) {
	t.Helper()
	current.Store(mustEmbedded())
	previous.Store(nil)
}

func itemPatch(t *testing.T, body string) map[string][]byte {
	t.Helper()
	return map[string][]byte{"item": []byte(body)}
}

func TestEmbeddedSnapshot(t *testing.T) {
	s := MustEmbedded()
	require.NotEqualValues(t, "", s.Version, "embedded version must not be empty")
	// 不写死张数：加表是常态，写死只会让每个新增配置都来改测试。
	require.False(t, len(tableNames) < 34, fmt.Sprintf("table count = %d, want >= 34", len(tableNames)))
	require.EqualValues(t, len(tableNames), len(LoadedTables()))
	// validate 会逐表检查非空与主键唯一，等于把「生成物自洽」变成启动/测试期断言。
	err := s.validate()
	require.NoError(t, err, fmt.Sprintf("embedded snapshot invalid: %v", err))
	v, ok := GetItemCfg(1001)
	require.True(t, ok, fmt.Sprintf("item 1001 missing in embedded snapshot: %+v", v))
	require.NotEqualValues(t, "", v.Col1)
}

func TestReplaceAppliesAtomically(t *testing.T) {
	resetSnapshot(t)
	before := Cur()
	questLen := before.Quest.Len()
	oldVersion := before.Version

	ver, err := Replace(itemPatch(t, `[
		{"id":1001,"col1":"改名金币","col2":1,"col3":1},
		{"id":1002,"col1":"经验药水","col2":99,"col3":10}
	]`))
	require.NoError(t, err, "replace")
	require.NotEqualValues(t, oldVersion, ver, "version must change after replace")
	got := Version()
	require.EqualValues(t, ver, got, fmt.Sprintf("Version() = %s, want %s", got, ver))
	v, _ := GetItemCfg(1001)
	require.EqualValues(t, "改名金币", v.Col1, fmt.Sprintf("item not updated: %+v", v))
	// 未出现在 patch 里的表沿用旧快照，不能因为替换一张表就丢别的表。
	require.EqualValues(t, questLen, Cur().Quest.Len())
	// 旧快照本身不受影响（读取方手里已拿到的指针语义仍是旧的一整套）。
	v, _ = before.Item.Get(1001)
	require.NotEqualValues(t, "改名金币", v.Col1, "old snapshot must stay immutable")
}

func TestReplaceFailureKeepsOldSnapshot(t *testing.T) {
	resetSnapshot(t)
	oldVersion := Version()

	cases := []struct {
		name  string
		patch map[string][]byte
		want  string
	}{
		{"bad json", itemPatch(t, `{not json`), "decode json"},
		{"duplicate pk", itemPatch(t, `[{"id":1,"col1":"a"},{"id":1,"col1":"b"}]`), "duplicate primary key"},
		{"empty table", itemPatch(t, `[]`), "empty table"},
		{"unknown table", map[string][]byte{"nope": []byte(`[]`)}, "unknown table"},
		{"empty patch", map[string][]byte{}, "empty patch"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := Replace(c.patch)
			require.Error(t, err, fmt.Sprintf("err = %v, want contains %q", err, c.want))
			require.Contains(t, err.Error(), c.want)
			require.EqualValues(t, oldVersion, Version())
			v, _ := GetItemCfg(1001)
			require.NotEqualValues(t, "", v.Col1, "snapshot content changed after failed replace")
		})
	}
}

func TestRollback(t *testing.T) {
	resetSnapshot(t)
	oldVersion := Version()
	oldName := func() string { v, _ := GetItemCfg(1001); return v.Col1 }()

	_, err := Replace(itemPatch(t, `[{"id":1001,"col1":"临时配置","col2":1,"col3":1}]`))
	require.NoError(t, err, fmt.Sprintf("replace: %v", err))
	v, _ := GetItemCfg(1001)
	require.EqualValues(t, "临时配置", v.Col1, fmt.Sprintf("replace not applied: %+v", v))

	ver, err := Rollback()
	require.NoError(t, err, "rollback")
	require.EqualValues(t, oldVersion, ver)
	v, _ = GetItemCfg(1001)
	require.EqualValues(t, oldName, v.Col1, fmt.Sprintf("rollback content = %q, want %q", v.Col1, oldName))
	// 再回滚一次回到被替换的那版（previous 与 current 互换）。
	_, err = Rollback()
	require.NoError(t, err, fmt.Sprintf("second rollback: %v", err))
	v, _ = GetItemCfg(1001)
	require.EqualValues(t, "临时配置", v.Col1, fmt.Sprintf("second rollback content = %q", v.Col1))
}

func TestLoadDir(t *testing.T) {
	resetSnapshot(t)
	dir := t.TempDir()

	err := os.WriteFile(filepath.Join(dir, "item.json"), []byte(`[{"id":1001,"col1":"外部输入","col2":1,"col3":1}]`), 0o644)
	require.NoError(t, err)
	ver, err := LoadDir(dir)
	require.NoError(t, err, "load dir")
	require.NotEqualValues(t, "", ver, "version must not be empty")
	v, _ := GetItemCfg(1001)
	require.EqualValues(t, "外部输入", v.Col1, fmt.Sprintf("external config not applied: %+v", v))

	// 目录里没有 json 视为错误输入，不能静默当成「没变化」。
	_, err = LoadDir(t.TempDir())
	require.Error(t, err, "empty dir must fail")
	// 未知表名（文件名拼错）必须报错，否则线上会以为更新成功。
	bad := t.TempDir()
	err = os.WriteFile(filepath.Join(bad, "itme.json"), []byte(`[]`), 0o644)
	require.NoError(t, err)
	_, err = LoadDir(bad)
	require.Error(t, err, fmt.Sprintf("misspelled table name must fail, got %v", err))
	require.Contains(t, err.Error(), "unknown table")
}

// TestConcurrentReadDuringReplace 读取方无锁，替换是原子换指针：
// 任意时刻读到的都必须是一套完整快照（旧值或新值），不能读到半套。
func TestConcurrentReadDuringReplace(t *testing.T) {
	resetSnapshot(t)

	oldName := func() string { v, _ := GetItemCfg(1001); return v.Col1 }()
	const newName = "并发替换"

	var wg sync.WaitGroup
	stop := make(chan struct{})

	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				v, ok := GetItemCfg(1001)
				if !ok {
					t.Error("item disappeared during replace")
					return
				}
				if v.Col1 != oldName && v.Col1 != newName {
					t.Errorf("read inconsistent row: %q", v.Col1)
					return
				}
				// 同时读另一张表，验证跨表一致性（任一套快照内两张表都存在）。
				if Cur().Quest.Len() == 0 {
					t.Error("quest table empty during replace")
					return
				}
			}
		}()
	}

	patch := itemPatch(t, `[{"id":1001,"col1":"`+newName+`","col2":1,"col3":1}]`)
	for i := 0; i < 50; i++ {
		_, err := Replace(patch)
		require.NoError(t, err, fmt.Sprintf("replace %d: %v", i, err))
	}
	close(stop)
	wg.Wait()
}
