package rpc

// rpc_test.go —— 直接调用 handler（不监听真实端口）。
//
// 为什么不启一个 bufconn/真实端口再走客户端：
//   - 本文件要验证的是 handler 的**决策**（鉴权顺序、错误分层、业务码
//     映射、输出确定性），这些都在 handler 内部完成；起连接只是多一层
//     gRPC 序列化，既慢又难在失败时定位是编码问题还是业务问题。
//   - metadata 可以用 metadata.NewIncomingContext 直接注入，等价于
//     真实链路上服务端看到的 incoming metadata。
//   - StreamView 是唯一无法“直接调用”的接口（它需要 ServerStream），
//     用一个只实现 Context/Send 的假流即可，不需要网络。
//
// 世界组装：Server 依赖真实 *runtime.WorldManager（WorldRouter 的
// 生产实现），测试里用 MemoryCommitter + application.NewPlanner +
// FakeClock 拼一个小世界（20×20 格、2×2 场景、每场景 10×10 格）。
// 为什么不用 worldmap 包里的 testConfig：
//
//	worldmap 包会 import rpc（装配 Register），rpc 的测试若 import
//	worldmap 就形成循环依赖。所以这里自己构造 Topology/ManagerConfig/
//	Deps——顺带让测试不依赖整份 MapConfig 的几十个字段。

import (
	"context"
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"game_study/internal/worldmap/application"
	"game_study/internal/worldmap/domain"
	"game_study/internal/worldmap/runtime"
	"game_study/pkg/pb/internalv2"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	testWorld  = domain.WorldID(1)
	testScene  = domain.SceneID(1)
	testPlayer = int64(1001)
	testToken  = "dev-internal-token"

	testCityID  = domain.ObjectID(9001)
	testResID   = domain.ObjectID(9002)
	testOtherID = domain.ObjectID(9100) // 另一个玩家的部队（验证货物排序）
	testOther   = int64(1002)
)

// fixedVerifier 把任意 token 绑定到固定玩家，用于验证防伪造分支。
type fixedVerifier struct{ playerID int64 }

func (v fixedVerifier) Verify(context.Context, string) (int64, error) {
	return v.playerID, nil
}

// nilRouter 模拟“世界还没加载”的部署状态。
type nilRouter struct{}

func (nilRouter) World(domain.WorldID) (*runtime.WorldInstance, bool) { return nil, false }

// testTopology 与 worldmap/server_test.go 的小世界一致：
// 20×20 格、2×2 场景，每场景 10×10 格 = 10000×10000 units。
func testTopology() domain.Topology {
	return domain.Topology{
		WorldID:           testWorld,
		WidthCells:        20,
		HeightCells:       20,
		SceneCols:         2,
		SceneRows:         2,
		PositionUnitsCell: 1000,
	}
}

// testRules 是 M1 的玩法参数：速度 2000 units/s、负载 100、采集 2.5 个/秒。
func testRules() application.Rules {
	return application.Rules{
		MarchSpeedUnitsPerSec:    2000,
		CapacityUnits:            100,
		CollectRateMilliPerSec:   2500,
		MaxBattleQueuePerTarget:  10,
		BattleTimeoutMS:          5000,
		RefreshBatchSize:         5,
		ResourcePositionAttempts: 20,
		RichBlock: application.RichBlockRule{
			MaxCount: 5,
			Tiers: []application.RichTier{
				{ResourceType: 1, ItemID: 1001, Weight: 5, Minimum: 2, TotalPerNode: 1000},
			},
		},
	}
}

// seedObjects 注入主堡、资源点，以及一支“带多物品货物”的他人的部队。
//
// 为什么要预置第三支队伍：Query 的货物排序必须用“多物品”才能验证，
// 而正常出征流程要到采集结算后才有货物，且第一版只有一种物品
// （Cargo 永远只有一个 key，排序无从观察）。直接 Seed 一个三物品的
// 部队，是对“输出确定性”最直接的断言。
func seedObjects() []domain.ObjectSnapshot {
	city := &domain.City{
		Header: domain.ObjectHeader{
			WorldID: testWorld, ID: testCityID, Kind: domain.ObjectKindCity,
			OwnerScene: testScene, Version: 1, SchemaVersion: 1,
		},
		PlayerID: testPlayer,
		Position: domain.Pos{X: 5000, Y: 5000}, // (5,5) 格
	}
	resource := &domain.ResourceNode{
		Header: domain.ObjectHeader{
			WorldID: testWorld, ID: testResID, Kind: domain.ObjectKindResource,
			OwnerScene: testScene, Version: 1, SchemaVersion: 1,
		},
		Position:      domain.Pos{X: 5000, Y: 8000}, // (5,8) 格
		ResourceType:  1,
		ItemID:        1001,
		Total:         1000,
		Remaining:     1000,
		ConfigVersion: "m1",
	}
	otherTroop := &domain.Troop{
		Header: domain.ObjectHeader{
			WorldID: testWorld, ID: testOtherID, Kind: domain.ObjectKindTroop,
			OwnerScene: testScene, Version: 3, SchemaVersion: 1,
		},
		PlayerID:      testOther,
		SlotID:        2,
		Phase:         domain.TroopPhaseCollecting,
		Cargo:         map[int32]int64{1003: 5, 1001: 7, 1002: 3}, // 故意乱序插入
		CapacityUnits: 100,
	}
	return []domain.ObjectSnapshot{
		{Key: city.Header.Key(), Kind: domain.ObjectKindCity, OwnerScene: testScene, Version: 1, PlayerID: testPlayer, City: city},
		{Key: resource.Header.Key(), Kind: domain.ObjectKindResource, OwnerScene: testScene, Version: 1, Resource: resource},
		{Key: otherTroop.Header.Key(), Kind: domain.ObjectKindTroop, OwnerScene: testScene, Version: 3, PlayerID: testOther, Troop: otherTroop},
	}
}

// fixture 是一个已装配好的“微型地图进程”。
type fixture struct {
	srv    *Server
	world  *runtime.WorldInstance
	scene  *runtime.SceneLoop
	clock  *runtime.FakeClock
	ctx    context.Context
	cancel context.CancelFunc
}

// newFixture 组装并启动一个小世界。
//
// 依赖方向（与生产一致）：rpc → runtime/application/domain。
// 这里不 import worldmap 包（server.go），否则测试包会与 rpc 的
// 上层装配形成 import 循环。
func newFixture(t *testing.T, verifier TokenVerifier) *fixture {
	t.Helper()

	clock := runtime.NewFakeClock(time.Unix(1_700_000_000, 0))
	topo := testTopology()

	// ID 生成器：命令 ID 只需进程内唯一；对象 ID 用原子自增模拟 SQL 主键。
	// 起点 100，避开 Seed 里固定的 9001/9002/9100。
	var seq atomic.Int64
	idGen := func() string { return fmt.Sprintf("cmd-%d", seq.Add(1)) }
	objectIDGen := func() domain.ObjectID { return domain.ObjectID(100 + seq.Add(1)) }
	seedGen := func(block, generation, slot int64) int64 {
		return block*31 ^ generation*7 ^ slot
	}

	planner, err := application.NewPlanner(topo, testRules(), idGen, objectIDGen, seedGen)
	require.NoError(t, err, "构造规划器失败")
	manager, err := runtime.NewWorldManager(runtime.ManagerConfig{
		Topo: topo,
		Scene: runtime.SceneConfig{
			MailboxSize:         128,
			ControlCapacity:     16,
			MaxCommandsPerTurn:  16,
			MaxDueEventsPerTurn: 16,
			CommandDeadlineMS:   2000,
			TickInterval:        10 * time.Millisecond,
		},
		MaxWorlds: 2,
	}, runtime.Deps{
		Planner:         planner,
		Committer:       runtime.NewMemoryCommitter(),
		Sink:            runtime.NoopSink{},
		Clock:           clock,
		Logger:          runtime.NopLogger{},
		TxWorkers:       2,
		TxQueueCapacity: 32,
	})
	require.NoError(t, err, "构造世界管理器失败")

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)

	world, err := manager.LoadWorld(ctx, testWorld)
	require.NoError(t, err, "加载世界失败")
	scene, ok := world.Scene(testScene)
	if !ok {
		t.Fatalf("场景 %d 不存在", testScene)
	}
	if err := scene.Seed(ctx, seedObjects(), nil, 1); err != nil {
		t.Fatalf("注入种子失败: %v", err)
	}

	// Shutdown 必须在测试结束前完成：否则场景 goroutine 会泄漏到
	// 下一个用例，-race 下表现为难以复现的竞态。
	t.Cleanup(func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()
		if err := manager.Shutdown(shutdownCtx); err != nil {
			t.Errorf("关闭世界失败: %v", err)
		}
	})

	return &fixture{
		srv:    NewServer(manager, nil, runtime.NopLogger{}, verifier),
		world:  world,
		scene:  scene,
		clock:  clock,
		ctx:    ctx,
		cancel: cancel,
	}
}

// inboundContext 注入 incoming metadata，模拟“经过 gRPC 传输层”的请求。
//
// token 为空时不注入 metadata，用于验证“完全没带凭证”的分支。
func inboundContext(t *testing.T, token string) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	if token == "" {
		return ctx
	}
	return metadata.NewIncomingContext(ctx, metadata.Pairs(tokenMetadataKey, token))
}

// marchRequest 构造一条出征请求（采集 9002 号资源点）。
func marchRequest(requestID string, slotID int32) *internalv2.CommandRequest {
	return &internalv2.CommandRequest{
		WorldId:   uint64(testWorld),
		PlayerId:  testPlayer,
		RequestId: requestID,
		SceneId:   int32(testScene),
		Body: &internalv2.CommandRequest_March{
			March: &internalv2.MarchRequest{
				SlotId:     slotID,
				HomeCityId: uint64(testCityID),
				TargetId:   uint64(testResID),
				Intent:     internalv2.MarchIntent_MARCH_INTENT_COLLECT,
			},
		},
	}
}

// troopsQuery 构造“查某玩家全部部队”的请求。
func troopsQuery(playerID int64) *internalv2.MapQueryRequest {
	return &internalv2.MapQueryRequest{
		WorldId:  uint64(testWorld),
		PlayerId: playerID,
		SceneId:  int32(testScene),
		Body: &internalv2.MapQueryRequest_QueryTroops{
			QueryTroops: &internalv2.QueryTroops{SlotId: 0},
		},
	}
}

// resourcesQuery 构造矩形资源查询。
func resourcesQuery(minX, minY, maxX, maxY int64) *internalv2.MapQueryRequest {
	return &internalv2.MapQueryRequest{
		WorldId:  uint64(testWorld),
		PlayerId: testPlayer,
		SceneId:  int32(testScene),
		Body: &internalv2.MapQueryRequest_QueryResources{
			QueryResources: &internalv2.QueryResources{
				MinX: minX, MinY: minY, MaxX: maxX, MaxY: maxY,
			},
		},
	}
}

// ---- 1. 鉴权：未带 token ----

func TestSubmitUnauthenticatedWithoutToken(t *testing.T) {
	// manager 传 nil 也安全：鉴权在路由之前，永远不会走到 manager。
	srv := NewServer(nil, nil, runtime.NopLogger{}, NopVerifier{})

	_, err := srv.Submit(context.Background(), marchRequest("req-no-token", 1))
	if got := status.Code(err); got != codes.Unauthenticated {
		t.Fatalf("未带 token 应返回 Unauthenticated，实际 %v (%v)", got, err)
	}
}

// ---- 2. 防伪造：token 绑定的 player 与请求不一致 ----

func TestSubmitRejectsPlayerMismatch(t *testing.T) {
	srv := NewServer(nil, nil, runtime.NopLogger{}, fixedVerifier{playerID: testPlayer + 1})

	_, err := srv.Submit(inboundContext(t, testToken), marchRequest("req-forge", 1))
	if got := status.Code(err); got != codes.PermissionDenied {
		t.Fatalf("player_id 与 token 不一致应返回 PermissionDenied，实际 %v (%v)", got, err)
	}
}

// ---- 3. 正常出征 → 业务码 0，部队出现在查询结果 ----

func TestSubmitMarchThenQueryTroops(t *testing.T) {
	f := newFixture(t, NopVerifier{})
	ctx := inboundContext(t, testToken)

	reply, err := f.srv.Submit(ctx, marchRequest("req-march-1", 1))
	require.NoError(t, err, "出征应被接受，实际 gRPC 错误")
	if reply.GetCode() != domain.CodeOK {
		t.Fatalf("出征业务码应为 0，实际 %d（%s）", reply.GetCode(), reply.GetMessage())
	}
	if reply.GetReplayed() {
		t.Fatal("首次提交不应标记为重放")
	}
	if reply.GetCommandId() == "" {
		t.Fatal("成功回执应带 command_id")
	}
	if len(reply.GetResultJson()) == 0 {
		t.Fatal("成功回执应带 result_json（首次结果）")
	}

	qr, err := f.srv.Query(ctx, troopsQuery(testPlayer))
	require.NoError(t, err, "查询部队失败")
	if qr.GetRevision() == 0 {
		t.Fatal("查询结果应带场景 revision")
	}
	troops := qr.GetTroops()
	if len(troops) != 1 {
		t.Fatalf("出征后应有 1 支部队，实际 %d", len(troops))
	}
	troop := troops[0]
	if troop.GetPlayerId() != testPlayer {
		t.Fatalf("部队归属玩家应为 %d，实际 %d", testPlayer, troop.GetPlayerId())
	}
	if troop.GetPhase() != int32(domain.TroopPhaseMarching) {
		t.Fatalf("出征后阶段应为 MARCHING，实际 %d", troop.GetPhase())
	}
	// 坐标是 MoveSegment 按快照 nowMS 推导的：起点即主堡位置。
	if troop.GetX() != 5000 || troop.GetY() != 5000 {
		t.Fatalf("出征瞬间坐标应为 (5000,5000)，实际 (%d,%d)", troop.GetX(), troop.GetY())
	}
	if troop.GetArriveAtMs() == 0 {
		t.Fatal("行军中的部队应有 arrive_at_ms")
	}
}

// ---- 4. 幂等重放：同 request_id 第二次返回首次结果 ----

func TestSubmitReplayIsIdempotent(t *testing.T) {
	f := newFixture(t, NopVerifier{})
	ctx := inboundContext(t, testToken)

	first, err := f.srv.Submit(ctx, marchRequest("req-replay", 1))
	require.NoError(t, err, "首次出征失败")
	second, err := f.srv.Submit(ctx, marchRequest("req-replay", 1))
	require.NoError(t, err, "重放应成功返回首次结果，实际错误")
	if !second.GetReplayed() {
		t.Fatal("第二次同 request_id 提交应标记 Replayed=true")
	}
	if second.GetCode() != domain.CodeOK {
		t.Fatalf("重放业务码应为 0，实际 %d", second.GetCode())
	}
	if second.GetCommandId() != first.GetCommandId() {
		t.Fatalf("重放应复用首次 command_id：first=%q second=%q",
			first.GetCommandId(), second.GetCommandId())
	}

	// 重放不得产生第二支部队（否则幂等只是“看起来成功”）。
	qr, err := f.srv.Query(ctx, troopsQuery(testPlayer))
	require.NoError(t, err, "查询失败")
	if len(qr.GetTroops()) != 1 {
		t.Fatalf("重放后仍应只有 1 支部队，实际 %d", len(qr.GetTroops()))
	}
}

// ---- 5. 槽位占用：业务码 44005（不是 gRPC error） ----

func TestSubmitSlotBusyReturnsBusinessCode(t *testing.T) {
	f := newFixture(t, NopVerifier{})
	ctx := inboundContext(t, testToken)

	if _, err := f.srv.Submit(ctx, marchRequest("req-slot-1", 1)); err != nil {
		t.Fatalf("首次出征失败: %v", err)
	}
	reply, err := f.srv.Submit(ctx, marchRequest("req-slot-2", 1))
	if err != nil {
		// 业务拒绝必须是“正常回复”，不能变成 gRPC error，
		// 否则客户端分不清“没到”与“到了被拒绝”。
		t.Fatalf("槽位占用应通过业务码返回，实际 gRPC 错误: %v", err)
	}
	if reply.GetCode() != domain.CodeSlotBusy {
		t.Fatalf("业务码应为 %d(SlotBusy)，实际 %d（%s）",
			domain.CodeSlotBusy, reply.GetCode(), reply.GetMessage())
	}
	if reply.GetReplayed() {
		t.Fatal("不同 request_id 不应被标记为重放")
	}
}

// ---- 6. 同 request_id 换参数：业务码 44009（不是 Internal） ----

// 这条路径很容易被忽略：规划器直接返回 IdempotencyConflict，而
// rejectionPlan 想写的回执与既有回执同主键、不同 hash，事务层会拒绝
// 写入。于是“错误稳定但内存里没有本次回执”，如果不特判就会被错误地
// 降级成 gRPC Internal。
func TestSubmitIdempotencyConflictReturnsBusinessCode(t *testing.T) {
	f := newFixture(t, NopVerifier{})
	ctx := inboundContext(t, testToken)

	first := marchRequest("req-conflict", 1)
	// 两个 32 字节摘要：只差内容，长度合法。
	first.PayloadHash = []byte{
		1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
		1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	}
	if _, err := f.srv.Submit(ctx, first); err != nil {
		t.Fatalf("首次出征失败: %v", err)
	}

	second := marchRequest("req-conflict", 2)
	second.PayloadHash = []byte{
		2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
		2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
	}
	reply, err := f.srv.Submit(ctx, second)
	require.NoError(t, err, "同键不同参应通过业务码返回，实际 gRPC 错误")
	if reply.GetCode() != domain.CodeIdempotencyConflict {
		t.Fatalf("业务码应为 %d(IdempotencyConflict)，实际 %d（%s）",
			domain.CodeIdempotencyConflict, reply.GetCode(), reply.GetMessage())
	}
}

// ---- 7. 未知 intent：InvalidArgument（协议错误，不进业务层） ----

func TestSubmitUnknownIntentIsInvalidArgument(t *testing.T) {
	f := newFixture(t, NopVerifier{})
	ctx := inboundContext(t, testToken)

	req := marchRequest("req-bad-intent", 1)
	// 直接构造一个枚举表之外的值：模拟上游版本不一致/序列化错误。
	req.Body = &internalv2.CommandRequest_March{
		March: &internalv2.MarchRequest{
			SlotId:     1,
			HomeCityId: uint64(testCityID),
			TargetId:   uint64(testResID),
			Intent:     internalv2.MarchIntent(99),
		},
	}
	_, err := f.srv.Submit(ctx, req)
	if got := status.Code(err); got != codes.InvalidArgument {
		t.Fatalf("未知 intent 应返回 InvalidArgument，实际 %v (%v)", got, err)
	}
}

// ---- 8. 查询输出确定性：多物品 cargo 每次顺序一致 ----

func TestQueryCargoOrderIsStable(t *testing.T) {
	f := newFixture(t, NopVerifier{})
	ctx := inboundContext(t, testToken)

	idsOf := func(reply *internalv2.MapQueryReply) []int32 {
		t.Helper()
		if len(reply.GetTroops()) != 1 {
			t.Fatalf("玩家 %d 应有 1 支部队，实际 %d", testOther, len(reply.GetTroops()))
		}
		entries := reply.GetTroops()[0].GetCargo()
		ids := make([]int32, 0, len(entries))
		for _, e := range entries {
			ids = append(ids, e.GetItemId())
		}
		return ids
	}

	first, err := f.srv.Query(ctx, troopsQuery(testOther))
	require.NoError(t, err, "首次查询失败")
	want := []int32{1001, 1002, 1003} // item_id 升序
	if got := idsOf(first); !reflect.DeepEqual(got, want) {
		t.Fatalf("cargo 应按 item_id 升序，实际 %v", got)
	}

	// 连续查询多次：Go map 的 range 顺序每轮随机，未排序时这里会抖动。
	for i := 0; i < 5; i++ {
		next, err := f.srv.Query(ctx, troopsQuery(testOther))
		if err != nil {
			t.Fatalf("第 %d 次查询失败: %v", i+1, err)
		}
		if got := idsOf(next); !reflect.DeepEqual(got, want) {
			t.Fatalf("第 %d 次查询顺序漂移: %v", i+1, got)
		}
	}
}

// ---- 9. 资源查询：格坐标矩形左闭右开过滤 ----

func TestQueryResourcesFiltersByCellRect(t *testing.T) {
	f := newFixture(t, NopVerifier{})
	ctx := inboundContext(t, testToken)

	// 资源点在 (5,8) 格。
	inside, err := f.srv.Query(ctx, resourcesQuery(0, 0, 10, 10))
	require.NoError(t, err, "查询资源失败")
	if len(inside.GetResources()) != 1 {
		t.Fatalf("矩形内应有 1 个资源点，实际 %d", len(inside.GetResources()))
	}
	res := inside.GetResources()[0]
	if res.GetResourceId() != uint64(testResID) {
		t.Fatalf("资源点 ID 应为 %d，实际 %d", testResID, res.GetResourceId())
	}
	if res.GetX() != 5000 || res.GetY() != 8000 {
		t.Fatalf("资源点坐标应为 (5000,8000)，实际 (%d,%d)", res.GetX(), res.GetY())
	}
	if res.GetTotal() != 1000 || res.GetRemaining() != 1000 {
		t.Fatalf("资源点总量/余量不符: total=%d remaining=%d", res.GetTotal(), res.GetRemaining())
	}

	// 相邻矩形（左闭右开）不包含 (5,8)。
	outside, err := f.srv.Query(ctx, resourcesQuery(10, 10, 20, 20))
	require.NoError(t, err, "查询资源失败")
	if len(outside.GetResources()) != 0 {
		t.Fatalf("矩形外不应有资源点，实际 %d", len(outside.GetResources()))
	}
}

// ---- 10. 世界未加载：NotFound（基础设施错误，不是业务码） ----

func TestSubmitWorldNotLoaded(t *testing.T) {
	srv := NewServer(nilRouter{}, nil, runtime.NopLogger{}, NopVerifier{})

	_, err := srv.Submit(inboundContext(t, testToken), marchRequest("req-no-world", 1))
	if got := status.Code(err); got != codes.NotFound {
		t.Fatalf("世界未加载应返回 NotFound，实际 %v (%v)", got, err)
	}
}

// ---- 11. 没装配 coordinator：StreamView → Unimplemented ----

// fakeServerStream 只实现 StreamView 用到的两个方法。
//
// Go 知识点：生成代码里的 ServerStreamingServer[Res] 是一个
// 「ServerStream + Send(*Res)」的接口。嵌入 nil 的 grpc.ServerStream
// 可以提供其余方法（SetHeader/SendHeader/SetTrailer/RecvMsg…）的
// 签名；本用例在返回前不会调用它们，所以不会触发 nil panic。
type fakeServerStream struct {
	grpc.ServerStream
	ctx  context.Context
	sent []*internalv2.ViewEvent
}

func (f *fakeServerStream) Context() context.Context { return f.ctx }

func (f *fakeServerStream) Send(ev *internalv2.ViewEvent) error {
	f.sent = append(f.sent, ev)
	return nil
}

func TestStreamViewUnimplementedWithoutCoordinator(t *testing.T) {
	srv := NewServer(nil, nil, runtime.NopLogger{}, NopVerifier{})

	stream := &fakeServerStream{ctx: inboundContext(t, testToken)}
	err := srv.StreamView(&internalv2.StreamViewRequest{
		WorldId:  uint64(testWorld),
		PlayerId: testPlayer,
		SceneId:  int32(testScene),
		MaxX:     10,
		MaxY:     10,
	}, stream)
	if got := status.Code(err); got != codes.Unimplemented {
		t.Fatalf("coordinator 为 nil 时应返回 Unimplemented，实际 %v (%v)", got, err)
	}
	if len(stream.sent) != 0 {
		t.Fatalf("Unimplemented 时不应发送任何事件，实际 %d", len(stream.sent))
	}
}

// ---- 12. MarchIntent 双向映射对称（防止正向/反向漂移） ----

func TestMarchIntentRoundTrip(t *testing.T) {
	cases := []internalv2.MarchIntent{
		internalv2.MarchIntent_MARCH_INTENT_COLLECT,
		internalv2.MarchIntent_MARCH_INTENT_ATTACK,
	}
	for _, protoIntent := range cases {
		domainIntent, err := toDomainIntent(protoIntent)
		if err != nil {
			t.Fatalf("正向映射 %v 失败: %v", protoIntent, err)
		}
		back, err := toProtoIntent(domainIntent)
		if err != nil {
			t.Fatalf("反向映射 %v 失败: %v", domainIntent, err)
		}
		if back != protoIntent {
			t.Fatalf("映射不对称: %v → %v → %v", protoIntent, domainIntent, back)
		}
	}

	// 未知值必须报错，不能被默认成 COLLECT。
	if _, err := toDomainIntent(internalv2.MarchIntent(42)); err == nil {
		t.Fatal("未知 proto intent 应返回错误")
	}
	if _, err := toProtoIntent(domain.MarchIntent(42)); err == nil {
		t.Fatal("未知 domain intent 应返回错误")
	}
}
