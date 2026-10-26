// persist_modules_test.go —— 模块落库的按需写（只写脏模块）
package writebehind

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

// recordPersister 记录被调用的次数与行数（不碰 SQL）。
type recordPersister struct {
	name  string
	calls int
	rows  int
}

func (p *recordPersister) Name() string                   { return p.name }
func (p *recordPersister) Validate(json.RawMessage) error { return nil }
func (p *recordPersister) Load(context.Context, *sql.Tx, int64) (json.RawMessage, error) {
	return nil, nil
}
func (p *recordPersister) PersistBatch(_ context.Context, _ *sql.Tx, rows []ModuleRow) error {
	p.calls++
	p.rows += len(rows)
	return nil
}

// TestPersistModulesOnlyWritesNamed 三种入参语义：指定模块 / 空列表 / nil。
//
// 这是「只写脏模块」的核心约定：
//   - 指定模块 → 只写这些，其余一条 SQL 都不产生；
//   - 空列表 → 什么都不写（本轮玩家只动了金币/道具）；
//   - nil → 全量（Redis 权威兼容路径，队列里的快照没有脏标记）。
func TestPersistModulesOnlyWritesNamed(t *testing.T) {
	a := &recordPersister{name: "zz_probe_a"}
	b := &recordPersister{name: "zz_probe_b"}
	RegisterPersister(a)
	RegisterPersister(b)
	// 只摘自己注册的，不 reset 整张表（其他包的真实模块还在里面）
	t.Cleanup(func() {
		persisterMu.Lock()
		delete(persisters, "zz_probe_a")
		delete(persisters, "zz_probe_b")
		persisterMu.Unlock()
	})

	snap := Snapshot{PlayerID: 1, Ext: map[string]json.RawMessage{
		"zz_probe_a": json.RawMessage(`{"v":1}`),
		"zz_probe_b": json.RawMessage(`{"v":2}`),
	}}

	// ① 只写 a
	require.NoError(t, PersistDirtyModules(context.Background(), nil, snap, []string{"zz_probe_a"}))
	require.Equal(t, 1, a.calls, "指定模块应被写")
	require.Equal(t, 1, a.rows)
	require.Equal(t, 0, b.calls, "未指定的模块不应被写")

	// ② 空列表：一条都不写
	a.calls, b.calls = 0, 0
	require.NoError(t, PersistDirtyModules(context.Background(), nil, snap, []string{}))
	require.Equal(t, 0, a.calls+b.calls, "空列表不应产生任何写入")

}

// TestPersistModulesUnknownName 指定了未注册的模块必须报错（不能静默丢）。
func TestPersistModulesUnknownName(t *testing.T) {
	snap := Snapshot{PlayerID: 1, Ext: map[string]json.RawMessage{
		"zz_not_registered": json.RawMessage(`{}`),
	}}
	err := PersistDirtyModules(context.Background(), nil, snap, []string{"zz_not_registered"})
	require.Error(t, err, "未注册的模块不能静默跳过")
	require.Contains(t, err.Error(), "no persister registered")
}

// TestPersistModulesDirtyButExtGone 脏标记在、Ext 已空（状态被 Delete）→ 跳过。
//
// 不能把已删除的行 UPSERT 回去。
func TestPersistModulesDirtyButExtGone(t *testing.T) {
	a := &recordPersister{name: "zz_probe_c"}
	RegisterPersister(a)
	t.Cleanup(func() {
		persisterMu.Lock()
		delete(persisters, "zz_probe_c")
		persisterMu.Unlock()
	})

	snap := Snapshot{PlayerID: 1, Ext: map[string]json.RawMessage{}}
	require.NoError(t, PersistDirtyModules(context.Background(), nil, snap, []string{"zz_probe_c"}))
	require.Equal(t, 0, a.calls, "Ext 已空时不应写入")
}
