package worldmap

import (
	"context"
	"testing"
	"time"

	"game_study/internal/worldmap/application"
	"game_study/internal/worldmap/domain"
	"game_study/internal/worldmap/runtime"
	"game_study/pkg/config"
	"github.com/stretchr/testify/require"
)

// testConfig 返回与 configs/map/map-dev.yaml 等价的小世界配置。
// 20×20 格、2×2 场景（每场景 10×10 格 = 10000×10000 units），
// 速度 2000 units/s、负载 100、采集 2.5 个/秒。
func testConfig() config.MapConfig {
	cfg := config.MapConfig{
		Enabled:                  true,
		WorldIDs:                 []uint64{1},
		WorldWidthCells:          20,
		WorldHeightCells:         20,
		SceneCols:                2,
		SceneRows:                2,
		PositionUnitsPerCell:     1000,
		AOIBlockCols:             2,
		AOIBlockRows:             2,
		AOICellSize:              5,
		RichBlockSize:            10,
		TickInterval:             "50ms",
		SceneMailboxSize:         512,
		SceneControlCapacity:     16,
		MaxCommandsPerTurn:       32,
		MaxDueEventsPerTurn:      32,
		TxWorkers:                2,
		TxQueueCapacity:          32,
		TxTimeout:                "500ms",
		CommandDeadline:          "2s",
		LeaseTTL:                 "15s",
		LeaseRenewInterval:       "3s",
		MaxViewWidth:             20,
		MaxViewHeight:            20,
		ViewPendingBytes:         1 << 20,
		AOIPayloadLimitBytes:     1 << 16,
		MaxBattleQueuePerTarget:  10,
		BattleTimeout:            "5s",
		RefreshInterval:          "1m",
		RefreshBatchSize:         5,
		ResourcePositionAttempts: 20,
		DeliveryRetryMin:         "1s",
		DeliveryRetryMax:         "10s",
		MaxPlayerReceipts:        100,
		MarchSpeedUnitsPerSec:    2000,
		CapacityUnits:            100,
		CollectRateMilli:         2500,
	}
	cfg.ApplyDefaults()
	return cfg
}

const (
	testWorld  = domain.WorldID(1)
	testScene  = domain.SceneID(1)
	testPlayer = int64(1001)
	testCityID = domain.ObjectID(9001)
	testResID  = domain.ObjectID(9002)
)

// 主堡 (5,5) 格、资源点 (5,8) 格：同属场景 1（每场景 10×10 格）。
// 用 var 而不是 const：Pos 是结构体，Go 的常量只能是基本类型。
var (
	testHomePos = domain.Pos{X: 5000, Y: 5000}
	testResPos  = domain.Pos{X: 5000, Y: 8000}
	testCityKey = domain.ObjectKey{WorldID: testWorld, ID: testCityID}
	testResKey  = domain.ObjectKey{WorldID: testWorld, ID: testResID}
)

// seedObject 构造主堡与资源点的注入快照。
func seedObject() []domain.ObjectSnapshot {
	city := &domain.City{
		Header: domain.ObjectHeader{
			WorldID: testWorld, ID: testCityID, Kind: domain.ObjectKindCity,
			OwnerScene: testScene, Version: 1, SchemaVersion: 1,
		},
		PlayerID: testPlayer,
		Position: testHomePos,
	}
	res := &domain.ResourceNode{
		Header: domain.ObjectHeader{
			WorldID: testWorld, ID: testResID, Kind: domain.ObjectKindResource,
			OwnerScene: testScene, Version: 1, SchemaVersion: 1,
		},
		Position:      testResPos,
		ResourceType:  1,
		ItemID:        1001,
		Total:         1000,
		Remaining:     1000,
		RichBlockID:   0,
		ConfigVersion: "m1",
	}
	return []domain.ObjectSnapshot{
		{Key: testCityKey, Kind: domain.ObjectKindCity, OwnerScene: testScene, Version: 1, PlayerID: testPlayer, City: city},
		{Key: testResKey, Kind: domain.ObjectKindResource, OwnerScene: testScene, Version: 1, Resource: res},
	}
}

// waitFor 轮询等待条件。场景循环是异步的，断言必须等状态稳定；
// 用真实时间轮询而不是 sleep 固定时长：固定 sleep 在慢机器上会假失败，
// 在快机器上会白等。
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("等待超时: %s", what)
}

// TestServerMarchCollectRecallLoop 是 M1 的端到端验收：
// 出征 → 到达 → 采集 → 提前召回（部分结算）→ 回城交接。
//
// 覆盖的关键不变量：
//   - 命令经邮箱 → 规划 → 事务池 → 控制通道 → 应用，revision 单调推进；
//   - 到达事件由到期堆驱动，位置按 MoveSegment 推导；
//   - 采集结算在召回时按 floor 计算（10s × 2.5/s = 25）；
//   - 回城后进入 SETTLING 并生成交接单，Cargo 不被提前清空。
func TestServerMarchCollectRecallLoop(t *testing.T) {
	clock := runtime.NewFakeClock(time.Unix(1_700_000_000, 0))
	srv, err := New(testConfig(), runtime.NopLogger{}, WithClock(clock))
	require.NoError(t, err, "构造服务失败")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	world, err := srv.Manager().LoadWorld(ctx, testWorld)
	require.NoError(t, err, "加载世界失败")
	defer func() { _ = srv.Shutdown(context.Background()) }()

	scene, ok := world.Scene(testScene)
	if !ok {
		t.Fatalf("场景 %d 不存在", testScene)
	}
	if err := scene.Seed(ctx, seedObject(), nil, 1); err != nil {
		t.Fatalf("注入种子失败: %v", err)
	}

	// ---- 1. 出征 ----
	marchEnv := domain.CommandEnvelope{
		WorldID: testWorld, PlayerID: testPlayer, RequestID: "req-march-1",
		PayloadHash: domain.PayloadHashOf([]byte("march-1")),
	}
	marchCmd := application.Command{
		Kind:        application.CmdMarchStart,
		TargetScene: testScene,
		March: application.MarchParams{
			SlotID:     1,
			HomeCityID: testCityID,
			Target:     domain.TargetRef{TargetID: testResID, Intent: domain.MarchIntentCollect},
		},
	}
	if err := world.SubmitSync(ctx, runtime.RoutedCommand{SceneID: testScene, Env: marchEnv, Cmd: marchCmd}); err != nil {
		t.Fatalf("出征失败: %v", err)
	}
	troops := scene.QueryTroops(testPlayer)
	if len(troops) != 1 {
		t.Fatalf("出征后应有 1 支部队，实际 %d", len(troops))
	}
	troop := troops[0]
	if troop.Phase != domain.TroopPhaseMarching {
		t.Fatalf("出征后阶段应为 MARCHING，实际 %d", int32(troop.Phase))
	}
	if troop.Move == nil {
		t.Fatal("出征后应存在 MoveSegment")
	}
	// 距离 3000 units / 2000 units/s = 1.5s，按整秒向上取整为 2000ms
	// （文档 9.1：end_at_ms = start + ceil(len/speed)*1000，不足一秒按一秒计）。
	if got := troop.Move.EndAtMS - troop.Move.StartAtMS; got != 2000 {
		t.Fatalf("到达时长应为 2000ms（整秒向上取整），实际 %d", got)
	}

	// ---- 2. 到达 → 建立采集 ----
	clock.Advance(2100 * time.Millisecond)
	waitFor(t, "部队进入采集", func() bool {
		ts := scene.QueryTroops(testPlayer)
		return len(ts) == 1 && ts[0].Phase == domain.TroopPhaseCollecting
	})

	res, ok := scene.Snapshot().Resource(testResID)
	if !ok {
		t.Fatal("资源点应存在")
	}
	if res.ActiveCollectID == "" {
		t.Fatal("采集开始后资源点应有 active_collect_id")
	}

	// ---- 3. 采集 10 秒后召回：floor(10 × 2.5) = 25 ----
	clock.Advance(10 * time.Second)
	recallEnv := domain.CommandEnvelope{
		WorldID: testWorld, PlayerID: testPlayer, RequestID: "req-recall-1",
		PayloadHash: domain.PayloadHashOf([]byte("recall-1")),
	}
	recallCmd := application.Command{
		Kind:        application.CmdRecall,
		TargetScene: testScene,
		Recall:      application.RecallParams{TroopID: troop.Header.ID},
	}
	if err := world.SubmitSync(ctx, runtime.RoutedCommand{SceneID: testScene, Env: recallEnv, Cmd: recallCmd}); err != nil {
		t.Fatalf("召回失败: %v", err)
	}
	troops = scene.QueryTroops(testPlayer)
	if len(troops) != 1 {
		t.Fatalf("召回后应有 1 支部队，实际 %d", len(troops))
	}
	troop = troops[0]
	if troop.Phase != domain.TroopPhaseReturning {
		t.Fatalf("召回后阶段应为 RETURNING，实际 %d", int32(troop.Phase))
	}
	if got := troop.Cargo[1001]; got != 25 {
		t.Fatalf("10 秒采集应得 25 个（floor），实际 %d", got)
	}

	// 资源余量应同步减少（INV-04：同事务扣减）。
	res, _ = scene.Snapshot().Resource(testResID)
	if res.Remaining != 975 {
		t.Fatalf("资源余量应为 975，实际 %d", res.Remaining)
	}

	// ---- 4. 回城：进入 SETTLING 并生成交接单 ----
	clock.Advance(2100 * time.Millisecond)
	waitFor(t, "部队进入交接", func() bool {
		ts := scene.QueryTroops(testPlayer)
		return len(ts) == 1 && ts[0].Phase == domain.TroopPhaseSettling
	})
	troop = scene.QueryTroops(testPlayer)[0]
	if troop.SettlementID == "" {
		t.Fatal("SETTLING 阶段应有 settlement_id（交接单先落再清货）")
	}
	if troop.Cargo[1001] != 25 {
		t.Fatalf("交接前 Cargo 不应被清空，实际 %d", troop.Cargo[1001])
	}

	// ---- 5. 事务确实落到了存储层 ----
	if _, ok := srv.Store().Object(testResKey); !ok {
		t.Fatal("资源点应已提交到存储")
	}
	if srv.Store().Commits() == 0 {
		t.Fatal("应至少有一次成功提交")
	}
	if rev := srv.Store().Revision(testScene); rev == 0 {
		t.Fatal("场景 revision 应已推进")
	}
}

// TestServerRejectsInvalidConfig 验证配置校验在构造期失败，
// 而不是等到运行中才炸。
func TestServerRejectsInvalidConfig(t *testing.T) {
	cases := []struct {
		name string
		mod  func(*config.MapConfig)
	}{
		{"世界尺寸不整除", func(c *config.MapConfig) { c.WorldWidthCells = 21 }},
		{"场景网格为负", func(c *config.MapConfig) { c.SceneCols = -1 }},
		{"AOI 块不整除", func(c *config.MapConfig) { c.AOIBlockCols = 3 }},
		{"事务超时不小于命令超时", func(c *config.MapConfig) {
			c.TxTimeout = "5s"
			c.CommandDeadline = "3s"
		}},
		{"采集速率为负", func(c *config.MapConfig) { c.CollectRateMilli = -1 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testConfig()
			tc.mod(&cfg)
			if _, err := New(cfg, runtime.NopLogger{}); err == nil {
				t.Fatal("非法配置应在构造期被拒绝")
			}
		})
	}
}

// TestServerLifecycle 验证 Run/Shutdown 的幂等与 ctx 取消语义。
func TestServerLifecycle(t *testing.T) {
	srv, err := New(testConfig(), runtime.NopLogger{})
	require.NoError(t, err, "构造失败")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()

	// 等世界加载完成（Run 内部 LoadWorld 是同步的）。
	waitFor(t, "世界加载", func() bool {
		_, ok := srv.Manager().World(testWorld)
		return ok
	})

	cancel()
	select {
	case err := <-done:
		require.NoError(t, err, "正常取消不应返回错误")
	case <-time.After(5 * time.Second):
		t.Fatal("Run 未在取消后退出")
	}

	// Shutdown 幂等。
	shutdownCtx, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("首次关闭失败: %v", err)
	}
	if err := srv.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("重复关闭应幂等，实际报错: %v", err)
	}
}
