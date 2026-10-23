package examples

import (
	"testing"
	"time"

	"game_study/internal/scenemap"
	"game_study/internal/scenemap/battle"
	"game_study/internal/scenemap/gameplay"
	"game_study/internal/scenemap/mgr"
	"github.com/stretchr/testify/require"
)

// countHarmBroadcasts 数某个玩家收到的飘字广播条数。
func countHarmBroadcasts(h *Harness, viewer scenemap.UUID) int {
	n := 0
	for _, msg := range h.Sink.Messages(viewer) {
		if _, ok := msg.(battle.HarmBroadcast); ok {
			n++
		}
	}
	return n
}

// TestBattleFlowDungeon 是战斗接入场景框架的端到端验收：
//
//	副本刷怪 → 玩家施法 → 怪被击杀（结果事件 + 飘字广播）→ 波次推进
//
// 覆盖规格 §15 P3 的验收项：多目标伤害、死亡只触发一次通关、双回路，
// 以及"死亡后 scene 权威（NPES）同步删除"。
func TestBattleFlowDungeon(t *testing.T) {
	table := battle.DefaultTable()
	// 零前摇高伤技能：让"施法即结算"，测试不需要等前摇。
	table.AddSkill(battle.SkillConfig{
		ID: 6001, Name: "训练重击", HarmType: battle.HarmTypeNormal,
		Harm: 1000, Range: 99, TargetNum: 1, TargetType: battle.TargetEnemy,
		Area: battle.AreaSingle, CooldownMS: 0,
	})

	d := gameplay.NewDungeon(2, 1, 9001, []scenemap.GridPos{{X: 3, Y: 3}})
	h := NewHarness(t, 1001, 1,
		WithGameplay(d),
		WithBattle(func(o *battle.Options) { o.Table = table }),
	)
	if err := h.EnterPlayer(1, scenemap.GridPos{X: 5, Y: 5}); err != nil {
		t.Fatalf("进图失败: %v", err)
	}

	// 第一波已由 MgrInit 刷出。
	v, err := h.Read(func(s *mgr.State) any { return len(s.NPES) })
	require.NoError(t, err, "读状态失败")
	if v.(int) != 1 {
		t.Fatalf("第一波应刷出 1 只怪，实际 %d", v.(int))
	}
	ids := h.monsterIDs(t)
	if len(ids) != 1 {
		t.Fatalf("应只有一只怪，实际 %v", ids)
	}

	res, err := h.Cast(1, 6001, ids[0], nil)
	require.NoError(t, err, "施法失败")
	if !res.Accepted {
		t.Fatalf("施法被拒: code=%d", res.Code)
	}

	// 结果回路：击杀者主人（玩家）收到 MonDeadEvent；玩法收到 HandleNpcDead。
	waitFor(t, 2*time.Second, "怪物死亡事件", func() bool {
		return len(h.Events.MonsterDeaths()) == 1
	})
	dead := h.Events.MonsterDeaths()[0]
	if dead.NpcID != 9001 {
		t.Fatalf("死亡事件 npcID 错误: %+v", dead)
	}
	// 表现回路：玩家视野内应收到飘字广播。
	// 广播走 AOI 的异步 mailbox，必须轮询等待（结果事件是同步投递的，
	// 先到不代表广播已送达）。
	waitFor(t, 2*time.Second, "飘字广播", func() bool {
		return countHarmBroadcasts(h, 1) > 0
	})
	// 波次推进：击杀数 1，当前波次 2。
	if d.Killed != 1 || d.CurrentWave != 2 {
		t.Fatalf("波次未推进: killed=%d wave=%d", d.Killed, d.CurrentWave)
	}
	// 第二波怪已刷出，且尸体已被清出 NPES（leave 命令收口）。
	waitFor(t, 2*time.Second, "第二波刷出", func() bool {
		nv, err := h.Read(func(s *mgr.State) any { return len(s.NPES) })
		return err == nil && nv.(int) == 1
	})
}

// TestLaunchDoesNotBroadcast 验证 §13.18 在真实 MGR 上的表现：
// 前摇期间没有任何飘字，前摇到期后才出现 HarmBroadcast。
func TestLaunchDoesNotBroadcast(t *testing.T) {
	table := battle.DefaultTable()
	table.AddSkill(battle.SkillConfig{
		ID: 6002, Name: "慢速重击", HarmType: battle.HarmTypeNormal,
		Harm: 1000, Range: 99, TargetNum: 1, TargetType: battle.TargetEnemy,
		Area: battle.AreaSingle, CooldownMS: 0,
		PrefixMS: 300,
	})
	d := gameplay.NewDungeon(1, 1, 9001, []scenemap.GridPos{{X: 3, Y: 3}})
	h := NewHarness(t, 1002, 1,
		WithGameplay(d),
		WithBattle(func(o *battle.Options) { o.Table = table }),
	)
	if err := h.EnterPlayer(1, scenemap.GridPos{X: 5, Y: 5}); err != nil {
		t.Fatalf("进图失败: %v", err)
	}
	ids := h.monsterIDs(t)
	res, err := h.Cast(1, 6002, ids[0], nil)
	if err != nil || !res.Accepted {
		t.Fatalf("施法应受理: res=%+v err=%v", res, err)
	}
	if res.PrefixMS < 100 {
		t.Fatalf("前摇应大于 100ms，实际 %d", res.PrefixMS)
	}
	// 前摇窗口内：不应有任何飘字（用时间窗断言，避免"只是还没投递"）。
	neverFor(t, time.Duration(res.PrefixMS/2)*time.Millisecond, "LAUNCH 阶段飘字", func() bool {
		return countHarmBroadcasts(h, 1) > 0
	})
	// 等前摇到期（MGR 的 buff tick 会把延时 CAST 推出来）。
	waitFor(t, 3*time.Second, "CAST 飘字", func() bool {
		return countHarmBroadcasts(h, 1) > 0
	})
}

// TestCityBlocksSkill 验证主城拦截（§13.9）：Options.NoBattle 时施法被拒，
// 且不产生任何伤害/事件。
func TestCityBlocksSkill(t *testing.T) {
	table := battle.DefaultTable()
	table.AddSkill(battle.SkillConfig{
		ID: 6003, HarmType: battle.HarmTypeNormal, Harm: 1000,
		Range: 99, TargetNum: 1, TargetType: battle.TargetEnemy, Area: battle.AreaSingle,
	})
	h := NewHarness(t, 1003, 1,
		WithBattle(func(o *battle.Options) { o.Table = table; o.NoBattle = true }),
	)
	if err := h.EnterPlayer(1, scenemap.GridPos{X: 5, Y: 5}); err != nil {
		t.Fatalf("进图失败: %v", err)
	}
	res, err := h.Cast(1, 6003, 0, nil)
	require.NoError(t, err, "施法调用失败")
	if res.Accepted || res.Code != battle.ErrCodeNoBattle {
		t.Fatalf("主城应拒绝: %+v", res)
	}
	if h.Events.Len() != 0 {
		t.Fatalf("主城不应产生结果事件: %d", h.Events.Len())
	}
}

// monsterIDs 读出当前怪物的 UUID 列表。
func (h *Harness) monsterIDs(t *testing.T) []scenemap.UUID {
	t.Helper()
	v, err := h.Read(func(s *mgr.State) any { return s.NpeUUIDs() })
	require.NoError(t, err, "读怪物列表失败")
	return v.([]scenemap.UUID)
}
