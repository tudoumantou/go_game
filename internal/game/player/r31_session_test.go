// r31_session_test.go —— R3.1 会话身份：旧连接的延迟 UNBIND 不得解绑新会话
package player

import (
	"context"
	"fmt"
	"testing"
	"time"

	"game_study/internal/game/module"
	"game_study/internal/game/module/bag"
	"game_study/pkg/actor"
	"game_study/pkg/db"
	"game_study/pkg/log"
	"game_study/pkg/writebehind"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// TestStaleUnbindDoesNotDetachNewSession 玩家在设备 B 顶号后，设备 A 迟到的
// UNBIND 不能把 B 的会话解绑（当前实现会无条件置 online=false）。
func TestStaleUnbindDoesNotDetachNewSession(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: -1})
	defer rdb.Close()
	store := writebehind.NewStore(rdb)
	_, err := store.Seed(ctx, writebehind.Snapshot{PlayerID: 7, Items: map[int32]int64{}}, 0)
	require.NoError(t, err)
	database, _, err := sqlmock.New()
	require.NoError(t, err)
	defer database.Close()
	repo := NewRepository(&db.DB{DB: database})
	repo.SetCache(store)
	logger := log.New("r31-test", log.WithLevel("error"))
	registry := module.NewRegistry()
	registry.Register(bag.New(logger))

	a := NewActor(7, registry, logger, repo, nil, func(uint64, uint32, []byte) error { return nil })
	state, version, err := repo.LoadCached(ctx, 7)
	require.NoError(t, err)
	a.setState(state)
	a.cacheVersion = version
	a.loaded = true
	// 新会话 2 已绑定（顶号完成后的状态）
	a.sessionID = 2
	a.online = true

	sys := actor.NewSystem(actor.Config{ShardCount: 1, MailboxSize: 8})
	sys.Start(ctx)
	defer sys.Stop()
	pid, err := sys.Spawn("r31", a)
	require.NoError(t, err)

	// 旧会话 1 断开：必须被忽略
	err = sys.Send(pid, &UnbindMsg{SessionID: 1})
	require.NoError(t, err)
	waitIdle(t, sys)
	require.False(t, !a.online || a.sessionID != 2, fmt.Sprintf("stale unbind detached the new session: online=%v session=%d", a.online, a.sessionID))

	// 当前会话 2 断开：正常解绑
	err = sys.Send(pid, &UnbindMsg{SessionID: 2})
	require.NoError(t, err)
	waitIdle(t, sys)
	require.False(t, a.online || a.sessionID != 0, fmt.Sprintf("current session unbind ignored: online=%v session=%d", a.online, a.sessionID))
}

// waitIdle 等 Actor 把已投递的消息处理完，避免用固定 sleep 造成偶发失败。
func waitIdle(t *testing.T, sys *actor.System) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := sys.Drain(ctx)
	require.NoError(t, err, fmt.Sprintf("drain: %v", err))
}
