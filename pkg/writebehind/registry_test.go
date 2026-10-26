package writebehind

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// demoPersister 是测试用模块，用来证明「注册即接入」不需要改框架代码。
type demoPersister struct {
	name     string
	validate func(json.RawMessage) error
	persist  func(context.Context, *sql.Tx, []ModuleRow) error
	load     func(context.Context, *sql.Tx, int64) (json.RawMessage, error)
}

func (d demoPersister) Name() string { return d.name }
func (d demoPersister) Validate(raw json.RawMessage) error {
	if d.validate != nil {
		return d.validate(raw)
	}
	return nil
}
func (d demoPersister) PersistBatch(ctx context.Context, tx *sql.Tx, rows []ModuleRow) error {
	if d.persist == nil {
		return nil
	}
	return d.persist(ctx, tx, rows)
}
func (d demoPersister) Load(ctx context.Context, tx *sql.Tx, playerID int64) (json.RawMessage, error) {
	if d.load == nil {
		return nil, nil
	}
	return d.load(ctx, tx, playerID)
}

// 注册一个模块后，框架自动把它的 Ext 交给它落库 —— 不需要改框架代码。
func TestRegisteredPersisterReceivesItsRows(t *testing.T) {
	resetPersisters()
	t.Cleanup(resetPersisters)

	var got []ModuleRow
	RegisterPersister(demoPersister{
		name: "demo",
		persist: func(_ context.Context, _ *sql.Tx, rows []ModuleRow) error {
			got = append(got, rows...)
			return nil
		},
	})

	snap := Snapshot{
		PlayerID: 7,
		Ext:      map[string]json.RawMessage{"demo": json.RawMessage(`{"counter":1}`)},
	}
	require.NoError(t, PersistDirtyModules(context.Background(), nil, snap, []string{"demo"}))

	require.Len(t, got, 1, fmt.Sprintf("persister rows = %+v", got))
	require.EqualValues(t, 7, got[0].PlayerID, "行必须带上玩家 id")
	require.EqualValues(t, `{"counter":1}`, string(got[0].Data), "行必须带上模块快照")
}

// 未注册的模块状态必须拒绝写入：宁可失败，也不能把数据写到没人认领的地方。
func TestUnregisteredModuleStateRejected(t *testing.T) {
	resetPersisters()
	t.Cleanup(resetPersisters)

	snap := Snapshot{PlayerID: 7, Ext: map[string]json.RawMessage{"ghost": json.RawMessage(`{}`)}}

	// ① 入口校验（Seed 前的公共闸门）
	err := snap.Validate()
	require.Error(t, err, fmt.Sprintf("unregistered module accepted: %v", err))
	require.Contains(t, err.Error(), "ghost")

	// ② 落库侧同样拒绝，且不能静默跳过
	err = PersistDirtyModules(context.Background(), nil, snap, []string{"ghost"})
	require.Error(t, err, "unregistered module persisted")
	require.Contains(t, err.Error(), "no persister registered")
}
