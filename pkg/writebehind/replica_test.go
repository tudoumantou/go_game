// pkg/writebehind/replica_test.go —— 异步回写通道的行为契约
//
// 这个通道承担「别人什么时候能看到我的修改」，它的 bug 都是数据可见性
// 级别的（读到旧值/旧会话覆盖新会话），所以每条不变量都要有测试兜着。
package writebehind

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"game_study/pkg/log"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// newReplicaForTest 建一个不启动后台循环的 Replica（测试自己驱动 Flush）。
func newReplicaForTest(t *testing.T, cfg ReplicaConfig) (*Replica, *Store, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	store := NewStore(rdb)
	return NewReplica(store, cfg, log.New("replica-test", log.WithLevel("error"))), store, mr
}

// profileJSON 一份合法的 profile 字段值（Load 会反序列化并校验）。
const profileJSON = `{"Name":"a","Level":1,"Exp":0,"Coin":100,"SceneID":0}`

// TestReplicaFlushMergesLatestWins 同一玩家一轮内多次改动只落最后一次（后写覆盖）。
func TestReplicaFlushMergesLatestWins(t *testing.T) {
	ctx := context.Background()
	rep, store, _ := newReplicaForTest(t, ReplicaConfig{})

	snap := Snapshot{PlayerID: 11, Items: map[int32]int64{}}
	_, err := store.Seed(ctx, snap, 0)
	require.NoError(t, err)
	token, err := store.AcquireLease(ctx, 11)
	require.NoError(t, err)

	first := `{"Name":"a","Level":1,"Exp":0,"Coin":100,"SceneID":0}`
	second := `{"Name":"a","Level":1,"Exp":0,"Coin":250,"SceneID":0}`
	require.NoError(t, rep.Enqueue(ReplicaRecord{PlayerID: 11, Token: token, Args: []string{FieldProfile, first}}))
	require.NoError(t, rep.Enqueue(ReplicaRecord{PlayerID: 11, Token: token, Args: []string{FieldProfile, second}}))
	require.Equal(t, 1, rep.Pending(), "同一玩家必须合并成一条")

	require.NoError(t, rep.Flush(ctx))
	require.Equal(t, 0, rep.Pending())

	rec, err := store.Load(ctx, 11)
	require.NoError(t, err)
	require.EqualValues(t, 250, rec.State.Profile.Coin, "应落最后一次的值")
	require.EqualValues(t, 1, rec.Version, "合并后只推一次版本")
}

// TestReplicaFlushKeepsOtherFields 合并不同字段，不能只留最后一个字段。
func TestReplicaFlushKeepsOtherFields(t *testing.T) {
	ctx := context.Background()
	rep, store, _ := newReplicaForTest(t, ReplicaConfig{})

	snap := Snapshot{PlayerID: 12, Items: map[int32]int64{}}
	_, err := store.Seed(ctx, snap, 0)
	require.NoError(t, err)
	token, err := store.AcquireLease(ctx, 12)
	require.NoError(t, err)

	attrJSON := `{"Career":1,"Hp":10,"Mp":10,"Atk":10,"Def":10,"Spd":10}`
	require.NoError(t, rep.Enqueue(ReplicaRecord{PlayerID: 12, Token: token, Args: []string{FieldProfile, profileJSON}}))
	require.NoError(t, rep.Enqueue(ReplicaRecord{PlayerID: 12, Token: token, Args: []string{FieldAttr, attrJSON}}))
	require.NoError(t, rep.Flush(ctx))

	rec, err := store.Load(ctx, 12)
	require.NoError(t, err)
	require.EqualValues(t, 100, rec.State.Profile.Coin)
	require.EqualValues(t, 10, rec.State.Attr.Atk)
}

// TestReplicaDropsSupersededLease 被顶号的旧会话记录必须整条丢弃，
// 否则旧值会盖掉新会话已经写入的数据。
func TestReplicaDropsSupersededLease(t *testing.T) {
	ctx := context.Background()
	rep, store, _ := newReplicaForTest(t, ReplicaConfig{})

	snap := Snapshot{PlayerID: 13, Items: map[int32]int64{}, Profile: Profile{Name: "a", Level: 1, Coin: 500}}
	_, err := store.Seed(ctx, snap, 0)
	require.NoError(t, err)

	oldToken, err := store.AcquireLease(ctx, 13)
	require.NoError(t, err)
	// 旧会话入队一条「金币 100」，随后被新会话接管（token 前进）
	require.NoError(t, rep.Enqueue(ReplicaRecord{
		PlayerID: 13, Token: oldToken,
		Args: []string{FieldProfile, `{"Name":"a","Level":1,"Exp":0,"Coin":100,"SceneID":0}`},
	}))
	_, err = store.AcquireLease(ctx, 13)
	require.NoError(t, err)

	require.NoError(t, rep.Flush(ctx))
	require.Equal(t, 0, rep.Pending(), "冲突记录应被丢弃而不是留在队列里")

	rec, err := store.Load(ctx, 13)
	require.NoError(t, err)
	require.EqualValues(t, 500, rec.State.Profile.Coin, "旧会话的值不能覆盖副本")
	require.EqualValues(t, 0, rec.Version, "被拒的写入不推版本")
}

// TestReplicaFlushPlayerDrainsOnlyThatPlayer 定向刷写只动目标玩家。
func TestReplicaFlushPlayerDrainsOnlyThatPlayer(t *testing.T) {
	ctx := context.Background()
	rep, store, _ := newReplicaForTest(t, ReplicaConfig{})

	for _, id := range []int64{21, 22} {
		_, err := store.Seed(ctx, Snapshot{PlayerID: id, Items: map[int32]int64{}}, 0)
		require.NoError(t, err)
		token, err := store.AcquireLease(ctx, id)
		require.NoError(t, err)
		require.NoError(t, rep.Enqueue(ReplicaRecord{
			PlayerID: id, Token: token, Args: []string{FieldProfile, profileJSON},
		}))
	}
	require.Equal(t, 2, rep.Pending())

	require.NoError(t, rep.FlushPlayer(ctx, 21))
	require.Equal(t, 1, rep.Pending(), "只应刷掉一个玩家")
	_, err := store.Load(ctx, 21)
	require.NoError(t, err, "目标玩家应已写入")
}

// TestReplicaQueueCapDropsOldest 队列超限丢最旧的（保内存不保完备）。
func TestReplicaQueueCapDropsOldest(t *testing.T) {
	ctx := context.Background()
	rep, store, _ := newReplicaForTest(t, ReplicaConfig{MaxPending: 2})

	for _, id := range []int64{31, 32, 33} {
		snap := Snapshot{PlayerID: id, Items: map[int32]int64{}}
		_, err := store.Seed(ctx, snap, 0)
		require.NoError(t, err)
		token, err := store.AcquireLease(ctx, id)
		require.NoError(t, err)
		require.NoError(t, rep.Enqueue(ReplicaRecord{
			PlayerID: id, Token: token, Args: []string{FieldProfile, profileJSON},
		}))
	}
	require.Equal(t, 2, rep.Pending(), "队列上限生效")
	require.NoError(t, rep.Flush(ctx))

	// 31 被丢：副本仍是 Seed 的空金币
	rec, err := store.Load(ctx, 31)
	require.NoError(t, err)
	require.EqualValues(t, 0, rec.State.Profile.Coin, "最旧的记录被丢弃")
	rec, err = store.Load(ctx, 33)
	require.NoError(t, err)
	require.EqualValues(t, 100, rec.State.Profile.Coin, "最新的记录必须保留")
}

// TestReplicaStartFlushesPeriodically 后台循环按 interval 自动刷写。
func TestReplicaStartFlushesPeriodically(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rep, store, _ := newReplicaForTest(t, ReplicaConfig{Interval: 20 * time.Millisecond})
	rep.Start(ctx)
	defer rep.Stop()

	_, err := store.Seed(ctx, Snapshot{PlayerID: 41, Items: map[int32]int64{}}, 0)
	require.NoError(t, err)
	token, err := store.AcquireLease(ctx, 41)
	require.NoError(t, err)
	require.NoError(t, rep.Enqueue(ReplicaRecord{
		PlayerID: 41, Token: token, Args: []string{FieldProfile, profileJSON},
	}))

	require.Eventually(t, func() bool {
		rec, err := store.Load(context.Background(), 41)
		return err == nil && rec.State.Profile.Coin == 100
	}, 2*time.Second, 10*time.Millisecond, "后台循环应在 interval 内把记录刷进副本")
}

// TestEncodeFieldsRejectsUnknownModule Ext 字段必须已注册（否则写进副本的是垃圾）。
func TestEncodeFieldsRejectsUnknownModule(t *testing.T) {
	snap := Snapshot{
		PlayerID: 51,
		Items:    map[int32]int64{},
		Ext:      map[string]json.RawMessage{"no_such_module": json.RawMessage(`{}`)},
	}
	_, err := EncodeFields(snap, []string{FieldExtPrefix + "no_such_module"}, nil)
	require.Error(t, err, "未注册模块的 Ext 字段必须被拒绝")
}

// TestEncodeFieldsEncodesRemoved 删除字段编码成 "-name" 前缀（syncScript 的约定）。
func TestEncodeFieldsEncodesRemoved(t *testing.T) {
	snap := Snapshot{PlayerID: 52, Items: map[int32]int64{}}
	args, err := EncodeFields(snap, nil, []string{FieldExtPrefix + "gone"})
	require.NoError(t, err)
	require.Equal(t, []string{"-" + FieldExtPrefix + "gone", ""}, args)
}
