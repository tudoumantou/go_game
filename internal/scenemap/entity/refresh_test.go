package entity

import (
	"fmt"
	"testing"

	"game_study/internal/scenemap"
	"game_study/internal/scenemap/grid"
	"github.com/stretchr/testify/require"
)

func TestRefreshNUMSpawnsOnce(t *testing.T) {
	s := newTestState(grid.Size{Width: 12, Height: 12})
	r := &Refresher{
		ID:        1,
		Kind:      RefreshNUM,
		CfgID:     501,
		Count:     3,
		NextAtMS:  0, // 已到期
		Positions: []scenemap.GridPos{{X: 2, Y: 2}, {X: 3, Y: 2}, {X: 4, Y: 2}},
		Template:  Spec{HP: 50, Attack: 3},
	}
	err := AddRefresher(s, r)
	require.NoError(t, err, fmt.Sprintf("AddRefresher 失败: %v", err))

	got := TickRefresh(s, 0)
	require.Len(t, got, 3)
	require.Len(t, s.NPES, 3)
	// 刷出来的怪必须带刷怪器 ID（存活统计按它精确归属）。
	for _, id := range got {
		info := s.NPES[id]
		require.EqualValues(t, 501, info.TemplateID)
		rt := runtimeOf(s, id)
		require.NotNil(t, rt, fmt.Sprintf("运行时里的 RefreshID 不对: %+v", rt))
		require.EqualValues(t, r.ID, rt.spec.RefreshID)
	}

	// 一次性：再 tick 也不会补。
	again := TickRefresh(s, 10000)
	require.Empty(t, again, fmt.Sprintf("NUM 型不应重复刷, got %d", len(again)))
	// 账本（RefreshData）同步。
	sp := s.RefreshData.Spawners[r.ID]
	require.NotNil(t, sp, fmt.Sprintf("RefreshData 账本不对: %+v", sp))
	require.EqualValues(t, 3, sp.Alive)
}

func TestRefreshTIMERefillsAndReschedules(t *testing.T) {
	s := newTestState(grid.Size{Width: 12, Height: 12})
	positions := []scenemap.GridPos{{X: 2, Y: 2}, {X: 2, Y: 3}, {X: 2, Y: 4}}
	r := &Refresher{
		ID:        9,
		Kind:      RefreshTIME,
		CfgID:     600,
		Count:     2,
		CycleMS:   1000,
		NextAtMS:  0,
		Positions: positions,
		Template:  Spec{HP: 10},
	}
	err := AddRefresher(s, r)
	require.NoError(t, err, fmt.Sprintf("AddRefresher 失败: %v", err))
	got := TickRefresh(s, 0)
	require.Len(t, got, 2, fmt.Sprintf("首次应刷 2 只, got %d", len(got)))
	require.EqualValues(t, 1000, r.NextAtMS)

	// 未到期：不刷。
	got = TickRefresh(s, 500)
	require.Empty(t, got, fmt.Sprintf("未到期不应刷, got %d", len(got)))

	// 打死一只（Despawn），到期后只补 1 只。
	var victim scenemap.UUID
	for id := range s.NPES {
		victim = id
		break
	}
	_, ok := Despawn(s, victim)
	require.True(t, ok, "Despawn 失败")
	got = TickRefresh(s, 1000)
	require.Len(t, got, 1)
	require.EqualValues(t, 2000, r.NextAtMS)
	n := len(s.NPES)
	require.EqualValues(t, 2, n, fmt.Sprintf("存活数应回到 2, got %d", n))
}

func TestRefreshRespectsMaxAlive(t *testing.T) {
	s := newTestState(grid.Size{Width: 12, Height: 12})
	r := &Refresher{
		ID:        3,
		Kind:      RefreshTIME,
		CfgID:     700,
		Count:     5,
		MaxAlive:  2,
		CycleMS:   1000,
		NextAtMS:  0,
		Positions: []scenemap.GridPos{{X: 1, Y: 1}, {X: 2, Y: 1}, {X: 3, Y: 1}, {X: 4, Y: 1}, {X: 5, Y: 1}},
	}
	err := AddRefresher(s, r)
	require.NoError(t, err, fmt.Sprintf("AddRefresher 失败: %v", err))
	got := TickRefresh(s, 0)
	require.Len(t, got, 2, fmt.Sprintf("MaxAlive=2 时最多刷 2 只, got %d", len(got)))
	got = TickRefresh(s, 1000)
	require.Empty(t, got, fmt.Sprintf("已达上限不应再刷, got %d", len(got)))
}

func TestAddRefresherRejectsBadInput(t *testing.T) {
	s := newTestState(grid.Size{Width: 8, Height: 8})
	cases := []*Refresher{
		nil,
		{ID: 1, Kind: 99, Count: 1, Positions: []scenemap.GridPos{{X: 1, Y: 1}}},
		{ID: 2, Kind: RefreshNUM, Count: 0, Positions: []scenemap.GridPos{{X: 1, Y: 1}}},
		{ID: 3, Kind: RefreshNUM, Count: 1},
	}
	for i, r := range cases {
		err := AddRefresher(s, r)
		require.Error(t, err, fmt.Sprintf("第 %d 个非法刷怪器应被拒绝", i))
	}
	ok := &Refresher{ID: 4, Kind: RefreshNUM, Count: 1, Positions: []scenemap.GridPos{{X: 1, Y: 1}}}
	err := AddRefresher(s, ok)
	require.NoError(t, err, fmt.Sprintf("合法刷怪器应被接受: %v", err))
	err = AddRefresher(s, ok)
	require.Error(t, err, "重复 ID 应返回 ErrAlreadyExists")
	RemoveRefresher(s, ok.ID)
	require.Nil(t, RefresherOf(s, ok.ID), "RemoveRefresher 后仍能查到")
}
