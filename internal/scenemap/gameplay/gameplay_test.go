package gameplay

import (
	"errors"
	"testing"

	"game_study/internal/scenemap"
	"game_study/internal/scenemap/entity"
	"game_study/internal/scenemap/grid"
	"game_study/internal/scenemap/mgr"
	"github.com/stretchr/testify/require"
)

// newTestState 构造一个可直接给玩法用的 State（不启动 MGR）。
func newTestState(size grid.Size) *mgr.State {
	return &mgr.State{
		Key:         scenemap.MapKey{MapID: 1},
		MapID:       1,
		Users:       make(map[scenemap.UUID]*mgr.UserState),
		NPES:        make(map[scenemap.UUID]*mgr.NpeAIInfo),
		Pets:        make(map[scenemap.UUID]scenemap.UUID),
		Bullets:     make(map[scenemap.UUID]*mgr.Bullet),
		Firewells:   make(map[scenemap.UUID]*mgr.Firewell),
		Ext:         make(map[string]any),
		RefreshData: mgr.NewRefreshData(),
		Index:       mgr.NewIndex(size),
	}
}

// addUser 模拟"图里有一个人"（只影响 len(s.Users)，玩法不读别的字段）。
func addUser(s *mgr.State, id scenemap.UUID) {
	s.Users[id] = &mgr.UserState{UUID: id, Kind: scenemap.EntityKindPlayer}
}

// killAll 把图里属于该配置的怪全部杀死并移出（模拟 MGR 的死亡链）。
func killAll(t *testing.T, s *mgr.State, d *Dungeon) {
	t.Helper()
	for _, id := range s.NpeUUIDs() {
		info := s.NPES[id]
		if info == nil || !info.Alive() {
			continue
		}
		npcID := info.TemplateID
		x, y := info.GridPos.X, info.GridPos.Y
		if !entity.Damage(s, id, 1<<30, 0) {
			t.Fatalf("伤害未致死: %s", id)
		}
		if _, ok := entity.Despawn(s, id); !ok {
			t.Fatalf("Despawn 失败: %s", id)
		}
		if err := d.HandleNpcDead(npcID, id, x, y, s); err != nil {
			t.Fatalf("HandleNpcDead 失败: %v", err)
		}
	}
}

var testPositions = []scenemap.GridPos{{X: 3, Y: 3}, {X: 4, Y: 3}, {X: 5, Y: 3}, {X: 3, Y: 4}}

// ============================================================================
// 注册表
// ============================================================================

func TestRegistryRegisterAndLookup(t *testing.T) {
	r := NewRegistry()
	city := NewCity(1)
	if err := r.Register(1, city); err != nil {
		t.Fatalf("首次注册失败: %v", err)
	}
	if err := r.Register(1, city); !errors.Is(err, scenemap.ErrAlreadyExists) {
		t.Fatalf("重复注册应返回 ErrAlreadyExists, got %v", err)
	}
	if err := r.Register(2, nil); !errors.Is(err, scenemap.ErrInvalidArgument) {
		t.Fatalf("nil 玩法应返回 ErrInvalidArgument, got %v", err)
	}

	got, err := r.Lookup(1)
	if err != nil || got != city {
		t.Fatalf("Lookup(1) = %v, %v", got, err)
	}
	// 有意偏离原实现：未配置必须显式报错，不能默认按主城处理。
	_, err = r.Lookup(99)
	if !errors.Is(err, scenemap.ErrMapTypeMissing) {
		t.Fatalf("未配置的类型应返回 ErrMapTypeMissing, got %v", err)
	}
	require.EqualValues(t, 1, r.Len())
}

// ============================================================================
// 主城
// ============================================================================

func TestCityNeverClosesAndRejectsSkill(t *testing.T) {
	s := newTestState(grid.Size{Width: 8, Height: 8})
	c := NewCity(1002)

	if err := c.MgrInit(s); err != nil {
		t.Fatalf("MgrInit: %v", err)
	}
	// 主城即使一个人都没有也不允许关图（否则主城会"自己消失"）。
	if c.CanCheckStopMap(s) {
		t.Fatal("主城应恒返回 false")
	}
	if c.AllowSkill(1002) {
		t.Fatal("主城不应结算技能")
	}

	addUser(s, 1)
	_ = c.PlayerEnter(1, s)
	require.EqualValues(t, 1, c.Players())
	delete(s.Users, 1)
	_ = c.PlayerLeave(1, scenemap.LeaveReasonQuit, s)
	require.EqualValues(t, 0, c.Players())
	// 多余的离图不能把人数减成负数。
	_ = c.PlayerLeave(1, scenemap.LeaveReasonQuit, s)
	require.EqualValues(t, 0, c.Players())
	if err := c.Loop(s); err != nil {
		t.Fatalf("Loop: %v", err)
	}
}

// ============================================================================
// 副本
// ============================================================================

func TestDungeonSpawnsFirstWaveOnInit(t *testing.T) {
	s := newTestState(grid.Size{Width: 20, Height: 20})
	d := NewDungeon(2, 2, 9001, testPositions)

	if err := d.MgrInit(s); err != nil {
		t.Fatalf("MgrInit 失败: %v", err)
	}
	require.EqualValues(t, 1, d.CurrentWave)
	if n := len(s.NPES); n != 2 {
		t.Fatalf("第一波应刷 2 只, got %d", n)
	}
	// 框架不解释 Data，玩法自己把实例存进去。
	require.EqualValues(t, d, s.Data, "Dungeon 未写进 State.Data")
}

func TestDungeonAdvancesAndClearsExactlyOnce(t *testing.T) {
	s := newTestState(grid.Size{Width: 20, Height: 20})
	d := NewDungeon(2, 2, 9001, testPositions)
	clearCount := 0
	d.OnClear = func(*mgr.State) { clearCount++ }

	if err := d.MgrInit(s); err != nil {
		t.Fatalf("MgrInit 失败: %v", err)
	}

	// 杀完第一波：HandleNpcDead 发现本波全灭，推进到第二波。
	killAll(t, s, d)
	require.EqualValues(t, 2, d.CurrentWave)
	if n := len(s.NPES); n != 2 {
		t.Fatalf("第二波应刷 2 只, got %d", n)
	}
	if d.Cleared || clearCount != 0 {
		t.Fatalf("还没打完就通关了: cleared=%v count=%d", d.Cleared, clearCount)
	}

	// 杀完第二波：通关，回调只发一次。
	killAll(t, s, d)
	if !d.Cleared {
		t.Fatal("应已通关")
	}
	require.EqualValues(t, 1, clearCount)

	// 幂等：再推 Loop/死亡回调都不会重复结算。
	for i := 0; i < 3; i++ {
		if err := d.Loop(s); err != nil {
			t.Fatalf("Loop 失败: %v", err)
		}
	}
	_ = d.HandleNpcDead(9001, 123, 0, 0, s)
	require.EqualValues(t, 1, clearCount)
	require.EqualValues(t, 4, d.Killed)
}

func TestDungeonLoopAdvancesWhenMonstersVanishWithoutDeath(t *testing.T) {
	s := newTestState(grid.Size{Width: 20, Height: 20})
	d := NewDungeon(3, 1, 9001, testPositions)
	if err := d.MgrInit(s); err != nil {
		t.Fatalf("MgrInit 失败: %v", err)
	}
	if d.CurrentWave != 1 || len(s.NPES) != 1 {
		t.Fatalf("初始状态不对: wave=%d npes=%d", d.CurrentWave, len(s.NPES))
	}

	// 非死亡路径消失（GM 清怪/召唤到期）：没有 HandleNpcDead，只能靠 Loop 兜底。
	for _, id := range s.NpeUUIDs() {
		if _, ok := entity.Despawn(s, id); !ok {
			t.Fatal("Despawn 失败")
		}
	}
	if err := d.Loop(s); err != nil {
		t.Fatalf("Loop 失败: %v", err)
	}
	require.EqualValues(t, 2, d.CurrentWave)
	require.Len(t, s.NPES, 1)
}

func TestDungeonCannotCloseWhilePlayersInside(t *testing.T) {
	s := newTestState(grid.Size{Width: 8, Height: 8})
	d := NewDungeon(2, 1, 9001, testPositions)
	_ = d.MgrInit(s)

	// 图里没人：允许关（避免"开错难度的空房"一直占着池子）。
	if !d.CanCheckStopMap(s) {
		t.Fatal("空副本应允许关图")
	}
	addUser(s, 1)
	if d.CanCheckStopMap(s) {
		t.Fatal("进行中且有玩家时不允许关图")
	}
	// 通关后即使有人也允许关（结算已经完成）。
	d.Cleared = true
	if !d.CanCheckStopMap(s) {
		t.Fatal("通关后应允许关图")
	}
}

func TestDungeonOnTerminateCallsOnEnd(t *testing.T) {
	s := newTestState(grid.Size{Width: 8, Height: 8})
	d := NewDungeon(1, 1, 9001, testPositions)
	ended := 0
	d.OnEnd = func(*mgr.State) { ended++ }
	if err := d.MgrInit(s); err != nil {
		t.Fatalf("MgrInit 失败: %v", err)
	}
	if err := d.OnTerminate(s); err != nil {
		t.Fatalf("OnTerminate 失败: %v", err)
	}
	require.EqualValues(t, 1, ended)
}
