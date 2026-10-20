package store

// store_test.go 用 sqlmock 覆盖持久化层的 SQL 序列与错误分类。
//
// 测试策略（重要）：
//   - 期望 SQL 用「关键片段」而不是完整文本。sqlmock 的 QueryMatcherRegexp
//     对去掉多余空白后的 SQL 做子串正则匹配，因此写
//     `SELECT owner_id, epoch, lease_until_ms FROM map_world` 就够了。
//     过度绑定整条 SQL 的后果是：将来给 SELECT 加一列（哪怕与逻辑无关），
//     几十个用例一起红，真正的回归信号被淹没。
//   - 顺序仍然严格：sqlmock 按注册顺序消费期望，这正好用来验证
//     文档 7.2 的加锁顺序（世界行 → 场景行 → 对象行 …）。
//   - 不断言参数细节（除少数关键参数）同样是为了抗演化；参数正确性由
//     真实 MySQL 集成测试兜底（文档 19.3）。

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"game_study/internal/worldmap/domain"

	"github.com/DATA-DOG/go-sqlmock"
	mysqlDriver "github.com/go-sql-driver/mysql"
)

// 测试固定时钟：所有租约/回执时间都基于它，断言才可复现。
const testNowMS int64 = 1_700_000_000_000

func newTestRepo(t *testing.T) (*Repository, sqlmock.Sqlmock) {
	t.Helper()
	sqlDB, mock, err := sqlmock.New(
		// 显式声明匹配器：默认就是 RegExp，但写出来让读者知道
		// 「期望串是正则、是子串匹配」，避免误以为要写完整 SQL。
		sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp),
	)
	if err != nil {
		t.Fatalf("创建 sqlmock: %v", err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	r := NewRepository(sqlDB, nil, Options{
		OwnerID:    "node-a",
		LeaseTTL:   30 * time.Second,
		TxTimeout:  2 * time.Second,
		MaxRetries: 3,
	})
	r.nowMS = func() int64 { return testNowMS }
	return r, mock
}

// expectFence 注册「世界行 + 一个场景行」的期望，返回加锁后的 revision 行。
func expectFence(mock sqlmock.Sqlmock, owner string, epoch uint64, leaseUntil int64, sceneID int32, revision uint64) {
	mock.ExpectQuery(`SELECT owner_id, epoch, lease_until_ms FROM map_world`).
		WithArgs(int64(1)).
		WillReturnRows(sqlmock.NewRows([]string{"owner_id", "epoch", "lease_until_ms"}).
			AddRow(owner, epoch, leaseUntil))
	mock.ExpectQuery(`SELECT revision FROM map_scene`).
		WithArgs(int64(1), sceneID).
		WillReturnRows(sqlmock.NewRows([]string{"revision"}).AddRow(revision))
}

// expectNoReceipt 模拟「幂等键不存在」。
func expectNoReceipt(mock sqlmock.Sqlmock) {
	mock.ExpectQuery(`SELECT command_id, payload_hash, result_json FROM map_command`).
		WillReturnRows(sqlmock.NewRows([]string{"command_id", "payload_hash", "result_json"}))
}

func testPayloadHash(s string) [32]byte { return domain.PayloadHashOf([]byte(s)) }

// hashBytes 返回可切片形式。Go 里 [32]byte 的切片只能对**可寻址**的
// 数组取，函数返回值不可寻址（`f()[:]` 编译不过），必须先落变量。
func hashBytes(s string) []byte {
	h := testPayloadHash(s)
	return h[:]
}

// testTroopSnapshot 构造一个兵力快照（值类型，不需要深拷贝断言）。
func testTroopSnapshot(id domain.ObjectID, scene domain.SceneID, version uint64) domain.ObjectSnapshot {
	header := domain.ObjectHeader{
		WorldID: 1, ID: id, Kind: domain.ObjectKindTroop, OwnerScene: scene,
		Version: version, SchemaVersion: 1,
	}
	return domain.ObjectSnapshot{
		Key:           domain.ObjectKey{WorldID: 1, ID: id},
		Kind:          domain.ObjectKindTroop,
		OwnerScene:    scene,
		Version:       version,
		SchemaVersion: 1,
		PlayerID:      7,
		Troop: &domain.Troop{
			Header:   header,
			PlayerID: 7,
			SlotID:   2,
			Phase:    domain.TroopPhaseMarching,
			Target:   domain.TargetRef{TargetID: 500, Intent: domain.MarchIntentCollect},
			Move: &domain.MoveSegment{
				MoveID: "mv-1", From: domain.Pos{X: 1000, Y: 2000}, To: domain.Pos{X: 3000, Y: 4000},
				StartAtMS: 100, EndAtMS: 200, SpeedUnitsPerSec: 10, SegmentIndex: 0, PathVersion: 1,
				Path: []domain.Pos{{X: 2000, Y: 3000}},
			},
			Cargo:         map[int32]int64{1001: 5},
			CapacityUnits: 100,
			Stats:         domain.CombatSnapshot{Attack: 100, Defense: 100, HP: 1000, RuleVersion: "m1"},
			DispatchID:    "dp-1",
		},
	}
}

// fullPlan 构造一份「每种写入都有一点」的计划，用于验证完整 SQL 序列。
func fullPlan() *domain.MutationPlan {
	h := testPayloadHash("canonical-params")
	return &domain.MutationPlan{
		CommandID:              "cmd-1",
		Key:                    domain.CommandKey{WorldID: 1, PlayerID: 7, RequestID: "req-1"},
		PayloadHash:            h,
		Epoch:                  3,
		AffectedSceneRevisions: []domain.SceneRevision{{SceneID: 1, Revision: 5}},
		ExpectedObjectVersions: []domain.ObjectVersion{{Key: domain.ObjectKey{WorldID: 1, ID: 101}, Version: 2}},
		ObjectsUpsert:          []domain.ObjectSnapshot{testTroopSnapshot(101, 1, 3)},
		OccupancyClaims:        []domain.OccupancyClaim{{CellX: 10, CellY: 20, Layer: 0, ObjectKey: domain.ObjectKey{WorldID: 1, ID: 101}}},
		CollectStart: []domain.CollectSession{{
			ID: "sess-1", TroopID: 101, ResourceID: 500, StartAtMS: 1000, EndAtMS: 2000,
			RateMilliItemsPerSec: 1000, PlannedQty: 5, RuleVersion: "m1", Status: domain.CollectStatusActive,
		}},
		CollectSettle: []domain.CollectSettleChange{{SessionID: "sess-1", SettledQty: 5, SettleAtMS: 2000}},
		BattleUpsert: []domain.BattleRecord{{
			BattleID: "battle-1", WorldID: 1, TargetID: 500, AttackerID: 101, DefenderID: 500,
			InputHash:     testPayloadHash("battle-input"),
			AttackerStats: domain.CombatSnapshot{Attack: 100, HP: 1000, RuleVersion: "m1"},
			DefenderStats: domain.CombatSnapshot{Attack: 80, HP: 1200, RuleVersion: "m1"},
			RuleVersion:   "m1", RandomSeed: 42, Status: domain.BattleStatusPending, DeadlineMS: 9000,
		}},
		QueueDequeue: []domain.QueueEntry{{TargetID: 500, TroopID: 99}},
		QueueEnqueue: []domain.QueueEntry{{TargetID: 500, TroopID: 101, Seq: 1001}},
		DeliveryCreate: []domain.Delivery{{
			DeliveryID: "dl-1", WorldID: 1, TroopID: 101, PlayerID: 7,
			Cargo: map[int32]int64{1001: 5}, PayloadHash: testPayloadHash("cargo"),
			CreatedAtMS: 5000, Status: domain.DeliveryStatusPending, SettlementNo: 1,
		}},
		CommandReceipt: &domain.CommandReceipt{
			CommandID: "cmd-1", Key: domain.CommandKey{WorldID: 1, PlayerID: 7, RequestID: "req-1"},
			PayloadHash: h, ResultCode: 0, ResultJSON: []byte(`{"ok":true}`), CommittedAtMS: 6000,
		},
		OutboxEvents: []domain.OutboxEvent{{
			EventID: "ev-1", WorldID: 1, SceneID: 1, Revision: 6, Kind: "troop_update",
			Payload: []byte(`{"object_id":101}`), Status: 0,
		}},
	}
}

// ---------- Commit：正常路径 ----------

func TestCommitNormal(t *testing.T) {
	r, mock := newTestRepo(t)
	plan := fullPlan()

	mock.ExpectBegin()
	expectFence(mock, "node-a", 3, testNowMS+30000, 1, 5)
	expectNoReceipt(mock)
	mock.ExpectQuery(`SELECT version FROM map_object`).
		WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow(2))
	mock.ExpectExec(`INSERT INTO map_object`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`INSERT INTO map_occupancy`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`INSERT INTO map_collect`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(`SELECT resource_id, status FROM map_collect`).
		WillReturnRows(sqlmock.NewRows([]string{"resource_id", "status"}).AddRow(500, 1))
	mock.ExpectExec(`UPDATE map_collect SET status = 2`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`INSERT INTO map_resource_ledger`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`INSERT INTO map_battle`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`DELETE FROM map_battle_queue`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`INSERT INTO map_battle_queue`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`INSERT INTO map_delivery`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`INSERT INTO map_command`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`INSERT INTO map_outbox`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`UPDATE map_scene SET revision`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	res := r.Commit(context.Background(), plan)
	if !res.Committed || res.Unknown || res.Err != nil {
		t.Fatalf("应提交成功, got %+v", res)
	}
	if len(res.SceneRevisions) != 1 || res.SceneRevisions[0].Revision != 6 {
		t.Fatalf("场景 revision 应为 5+1=6, got %+v", res.SceneRevisions)
	}
	if len(res.ObjectVersions) != 1 || res.ObjectVersions[0].Version != 3 {
		t.Fatalf("对象版本应回传计划版本, got %+v", res.ObjectVersions)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// ---------- Commit：幂等 ----------

func TestCommitIdempotentHit(t *testing.T) {
	r, mock := newTestRepo(t)
	plan := fullPlan()

	mock.ExpectBegin()
	// 场景 revision 已经是 6：首次提交已经推进过。
	expectFence(mock, "node-a", 3, testNowMS+30000, 1, 6)
	mock.ExpectQuery(`SELECT command_id, payload_hash, result_json FROM map_command`).
		WillReturnRows(sqlmock.NewRows([]string{"command_id", "payload_hash", "result_json"}).
			AddRow("cmd-1", plan.PayloadHash[:], []byte(`{"ok":true}`)))
	// 幂等命中不写任何行，只回滚释放锁。
	mock.ExpectRollback()

	res := r.Commit(context.Background(), plan)
	if !res.Committed || res.Unknown || res.Err != nil {
		t.Fatalf("幂等命中应返回已提交, got %+v", res)
	}
	// 关键：返回的是库里当前 revision，而不是「期望+1」。
	// 若这里返回 6（5+1），上层会把内存 revision 停在 6，
	// 但库其实已经 6，下一次提交仍会以 6 为期望——这里必须精确。
	if len(res.SceneRevisions) != 1 || res.SceneRevisions[0].Revision != 6 {
		t.Fatalf("幂等命中应回传当前 revision=6, got %+v", res.SceneRevisions)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestCommitIdempotencyConflict(t *testing.T) {
	r, mock := newTestRepo(t)
	plan := fullPlan()

	mock.ExpectBegin()
	expectFence(mock, "node-a", 3, testNowMS+30000, 1, 5)
	mock.ExpectQuery(`SELECT command_id, payload_hash, result_json FROM map_command`).
		WillReturnRows(sqlmock.NewRows([]string{"command_id", "payload_hash", "result_json"}).
			// 同一个 request_id，但参数摘要不同。
			AddRow("cmd-old", hashBytes("other-params"), []byte(`{}`)))
	mock.ExpectRollback()

	res := r.Commit(context.Background(), plan)
	if res.Committed || res.Unknown {
		t.Fatalf("同键不同参不应提交, got %+v", res)
	}
	if !errors.Is(res.Err, domain.ErrIdempotencyConflict) {
		t.Fatalf("应返回 ErrIdempotencyConflict, got %v", res.Err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// ---------- Commit：栅栏与版本校验 ----------

func TestCommitStaleEpoch(t *testing.T) {
	r, mock := newTestRepo(t)
	plan := fullPlan()

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT owner_id, epoch, lease_until_ms FROM map_world`).
		WithArgs(int64(1)).
		WillReturnRows(sqlmock.NewRows([]string{"owner_id", "epoch", "lease_until_ms"}).
			AddRow("node-a", 4, testNowMS+30000)) // 库里已经是第 4 代
	mock.ExpectRollback()

	res := r.Commit(context.Background(), plan)
	if !errors.Is(res.Err, ErrStaleEpoch) {
		t.Fatalf("应返回 ErrStaleEpoch, got %v", res.Err)
	}
	if res.Committed || res.Unknown {
		t.Fatalf("旧 epoch 的计划不能提交, got %+v", res)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestCommitLeaseLostOnOwnerMismatch(t *testing.T) {
	r, mock := newTestRepo(t)
	plan := fullPlan()

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT owner_id, epoch, lease_until_ms FROM map_world`).
		WillReturnRows(sqlmock.NewRows([]string{"owner_id", "epoch", "lease_until_ms"}).
			AddRow("node-b", 3, testNowMS+30000))
	mock.ExpectRollback()

	res := r.Commit(context.Background(), plan)
	if !errors.Is(res.Err, ErrLeaseLost) {
		t.Fatalf("owner 不匹配应返回 ErrLeaseLost, got %v", res.Err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestCommitStaleRevision(t *testing.T) {
	r, mock := newTestRepo(t)
	plan := fullPlan()

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT owner_id, epoch, lease_until_ms FROM map_world`).
		WillReturnRows(sqlmock.NewRows([]string{"owner_id", "epoch", "lease_until_ms"}).
			AddRow("node-a", 3, testNowMS+30000))
	mock.ExpectQuery(`SELECT revision FROM map_scene`).
		WithArgs(int64(1), int32(1)).
		WillReturnRows(sqlmock.NewRows([]string{"revision"}).AddRow(9)) // 期望 5，实际 9
	// revision 比对在幂等查询之后（否则「已提交但回包丢失」的重试
	// 永远只能拿到 StaleVersion），所以这里仍要先查回执。
	expectNoReceipt(mock)
	mock.ExpectRollback()

	res := r.Commit(context.Background(), plan)
	if !errors.Is(res.Err, domain.ErrStaleVersion) {
		t.Fatalf("应返回 ErrStaleVersion, got %v", res.Err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestCommitObjectVersionConflict(t *testing.T) {
	r, mock := newTestRepo(t)
	plan := fullPlan()

	mock.ExpectBegin()
	expectFence(mock, "node-a", 3, testNowMS+30000, 1, 5)
	expectNoReceipt(mock)
	mock.ExpectQuery(`SELECT version FROM map_object`).
		WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow(7)) // 期望 2
	mock.ExpectRollback()

	res := r.Commit(context.Background(), plan)
	if !errors.Is(res.Err, domain.ErrStaleVersion) {
		t.Fatalf("对象版本不符应返回 ErrStaleVersion, got %v", res.Err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// ---------- Commit：唯一键冲突 ----------

func TestCommitPositionOccupied(t *testing.T) {
	r, mock := newTestRepo(t)
	plan := fullPlan()
	plan.ObjectsUpsert = nil
	plan.ExpectedObjectVersions = nil

	mock.ExpectBegin()
	expectFence(mock, "node-a", 3, testNowMS+30000, 1, 5)
	expectNoReceipt(mock)
	mock.ExpectExec(`INSERT INTO map_occupancy`).
		WillReturnError(&mysqlDriver.MySQLError{Number: 1062, Message: "Duplicate entry"})
	mock.ExpectRollback()

	res := r.Commit(context.Background(), plan)
	if !errors.Is(res.Err, domain.ErrPositionOccupied) {
		t.Fatalf("1062 应映射为 ErrPositionOccupied, got %v", res.Err)
	}
	if res.Committed || res.Unknown {
		t.Fatalf("占位冲突必须整笔回滚, got %+v", res)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// ---------- Commit：结果未知 ----------

func TestCommitUnknownOnCommitError(t *testing.T) {
	r, mock := newTestRepo(t)
	plan := fullPlan()

	mock.ExpectBegin()
	expectFence(mock, "node-a", 3, testNowMS+30000, 1, 5)
	expectNoReceipt(mock)
	mock.ExpectQuery(`SELECT version FROM map_object`).
		WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow(2))
	mock.ExpectExec(`INSERT INTO map_object`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`INSERT INTO map_occupancy`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`INSERT INTO map_collect`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(`SELECT resource_id, status FROM map_collect`).
		WillReturnRows(sqlmock.NewRows([]string{"resource_id", "status"}).AddRow(500, 1))
	mock.ExpectExec(`UPDATE map_collect SET status = 2`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`INSERT INTO map_resource_ledger`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`INSERT INTO map_battle`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`DELETE FROM map_battle_queue`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`INSERT INTO map_battle_queue`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`INSERT INTO map_delivery`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`INSERT INTO map_command`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`INSERT INTO map_outbox`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`UPDATE map_scene SET revision`).WillReturnResult(sqlmock.NewResult(0, 1))
	// COMMIT 报文丢失：服务端可能已经提交，绝不能按失败处理。
	mock.ExpectCommit().WillReturnError(errors.New("driver: bad connection"))

	res := r.Commit(context.Background(), plan)
	if !res.Unknown {
		t.Fatalf("COMMIT 出错必须返回 Unknown, got %+v", res)
	}
	if res.Committed {
		t.Fatal("Unknown 不能同时标记 Committed")
	}
	if res.Err == nil {
		t.Fatal("Unknown 应携带底层错误供日志/解析使用")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// ---------- Commit：死锁重试 ----------

func TestCommitRetryOnDeadlock(t *testing.T) {
	r, mock := newTestRepo(t)
	plan := fullPlan()

	// 第 1 次尝试：在锁对象行时死锁，InnoDB 已回滚本事务。
	mock.ExpectBegin()
	expectFence(mock, "node-a", 3, testNowMS+30000, 1, 5)
	expectNoReceipt(mock)
	mock.ExpectQuery(`SELECT version FROM map_object`).
		WillReturnError(&mysqlDriver.MySQLError{Number: 1213, Message: "Deadlock found"})
	mock.ExpectRollback()

	// 第 2 次尝试：重新开事务，object_id 不变（沿用计划里的 101/...
	// 这里体现为 SELECT version 的期望仍然命中同一个对象）。
	mock.ExpectBegin()
	expectFence(mock, "node-a", 3, testNowMS+30000, 1, 5)
	expectNoReceipt(mock)
	mock.ExpectQuery(`SELECT version FROM map_object`).
		WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow(2))
	mock.ExpectExec(`INSERT INTO map_object`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`INSERT INTO map_occupancy`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`INSERT INTO map_collect`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(`SELECT resource_id, status FROM map_collect`).
		WillReturnRows(sqlmock.NewRows([]string{"resource_id", "status"}).AddRow(500, 1))
	mock.ExpectExec(`UPDATE map_collect SET status = 2`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`INSERT INTO map_resource_ledger`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`INSERT INTO map_battle`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`DELETE FROM map_battle_queue`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`INSERT INTO map_battle_queue`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`INSERT INTO map_delivery`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`INSERT INTO map_command`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`INSERT INTO map_outbox`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`UPDATE map_scene SET revision`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	res := r.Commit(context.Background(), plan)
	if !res.Committed || res.Unknown || res.Err != nil {
		t.Fatalf("死锁后重试应成功, got %+v", res)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestCommitLockWaitTimeoutExhaustsRetries(t *testing.T) {
	r, mock := newTestRepo(t)
	r.opts.MaxRetries = 1 // 只重试一次
	plan := fullPlan()

	for i := 0; i < 2; i++ {
		mock.ExpectBegin()
		mock.ExpectQuery(`SELECT owner_id, epoch, lease_until_ms FROM map_world`).
			WillReturnError(&mysqlDriver.MySQLError{Number: 1205, Message: "Lock wait timeout exceeded"})
		mock.ExpectRollback()
	}

	res := r.Commit(context.Background(), plan)
	if res.Committed || res.Unknown {
		t.Fatalf("锁等待超时最终应确定失败, got %+v", res)
	}
	if !isMySQLCode(res.Err, mysqlErrLockWaitTimeout) {
		t.Fatalf("应保留原始 1205 错误, got %v", res.Err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestCommitSkipsRetryWhenContextCanceled(t *testing.T) {
	r, mock := newTestRepo(t)
	plan := fullPlan()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// ctx 已取消：连 BEGIN 都不会发起。
	res := r.Commit(ctx, plan)
	if res.Committed || res.Unknown {
		t.Fatalf("取消的上下文不能提交, got %+v", res)
	}
	if !errors.Is(res.Err, context.Canceled) {
		t.Fatalf("应返回 context.Canceled, got %v", res.Err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// ---------- payload JSON 往返 ----------

func TestPayloadJSONRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		snap domain.ObjectSnapshot
	}{
		{
			name: "city",
			snap: domain.ObjectSnapshot{
				Key: domain.ObjectKey{WorldID: 1, ID: 11}, Kind: domain.ObjectKindCity,
				OwnerScene: 2, Version: 7, SchemaVersion: 1, PlayerID: 9,
				City: &domain.City{
					Header:   domain.ObjectHeader{WorldID: 1, ID: 11, Kind: domain.ObjectKindCity, OwnerScene: 2, Version: 7, SchemaVersion: 1},
					PlayerID: 9, GuildID: 3, Position: domain.Pos{X: 1234, Y: 5678}, ConfigVersion: "c1",
				},
			},
		},
		{
			name: "troop",
			snap: testTroopSnapshot(101, 1, 3),
		},
		{
			name: "resource",
			snap: domain.ObjectSnapshot{
				Key: domain.ObjectKey{WorldID: 1, ID: 500}, Kind: domain.ObjectKindResource,
				OwnerScene: 1, Version: 2, SchemaVersion: 1,
				Resource: &domain.ResourceNode{
					Header:      domain.ObjectHeader{WorldID: 1, ID: 500, Kind: domain.ObjectKindResource, OwnerScene: 1, Version: 2, SchemaVersion: 1},
					Position:    domain.Pos{X: 100, Y: 200},
					RichBlockID: 8, ResourceType: 1, ItemID: 1001, Total: 1000, Remaining: 400,
					ActiveCollectID: "sess-1", RefreshGeneration: 2, ConfigVersion: "r1",
				},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := encodePayload(&tc.snap)
			if err != nil {
				t.Fatalf("编码: %v", err)
			}
			// snake_case 契约：数据库里绝不能出现 Go 字段名。
			for _, want := range []string{`"kind"`, `"header"`, `"object_id"`, `"schema_version"`} {
				if !strings.Contains(string(raw), want) {
					t.Fatalf("payload 缺少键 %s: %s", want, raw)
				}
			}
			if strings.Contains(string(raw), `"Header"`) || strings.Contains(string(raw), `"PlayerID"`) {
				t.Fatalf("payload 泄漏了 Go 字段名: %s", raw)
			}

			row := objectRow{
				worldID: uint64(tc.snap.Key.WorldID), objectID: uint64(tc.snap.Key.ID),
				kind: int32(tc.snap.Kind), ownerScene: int32(tc.snap.OwnerScene),
				version: tc.snap.Version, schemaVersion: tc.snap.SchemaVersion,
				playerID: tc.snap.PlayerID, payload: raw,
			}
			got, err := row.toSnapshot()
			if err != nil {
				t.Fatalf("解码: %v", err)
			}
			if !reflect.DeepEqual(got, tc.snap) {
				t.Fatalf("JSON 往返不相等:\n got=%+v\nwant=%+v", got, tc.snap)
			}
		})
	}
}

func TestDecodePayloadRejectsBrokenJSON(t *testing.T) {
	_, _, _, err := decodePayload(1, 101, domain.ObjectKindTroop, []byte(`{"kind":2,"troop":{"cargo":{"abc":1}}}`))
	if err == nil {
		t.Fatal("非法 cargo 键应报错")
	}
}

func TestLoadScene(t *testing.T) {
	r, mock := newTestRepo(t)

	troop := testTroopSnapshot(101, 1, 4)
	payload, err := encodePayload(&troop)
	if err != nil {
		t.Fatal(err)
	}
	rows := sqlmock.NewRows([]string{"object_id", "kind", "owner_scene", "version", "schema_version", "player_id", "payload", "deleted"}).
		AddRow(uint64(101), int32(2), int32(1), uint64(4), uint32(1), int64(7), payload, int32(0))
	mock.ExpectQuery(`SELECT object_id, kind, owner_scene, version`).
		WithArgs(int64(1), int32(1)).
		WillReturnRows(rows)

	objects, maxVersion, err := r.LoadScene(context.Background(), 1, 1)
	if err != nil {
		t.Fatalf("LoadScene: %v", err)
	}
	if len(objects) != 1 || maxVersion != 4 {
		t.Fatalf("应读到 1 个对象且 maxVersion=4, got %d/%d", len(objects), maxVersion)
	}
	if objects[0].Troop == nil || objects[0].Troop.Header.ID != 101 {
		t.Fatalf("对象未正确解码: %+v", objects[0].Troop)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestAllocObjectID(t *testing.T) {
	r, mock := newTestRepo(t)
	mock.ExpectExec(`INSERT INTO map_id_allocator`).WillReturnResult(sqlmock.NewResult(4321, 1))

	id, err := r.AllocObjectID(context.Background(), r.db)
	if err != nil {
		t.Fatalf("AllocObjectID: %v", err)
	}
	if id != 4321 {
		t.Fatalf("应返回自增 ID 4321, got %d", id)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// ---------- 恢复 ----------

func TestRecoverWorldOrder(t *testing.T) {
	r, mock := newTestRepo(t)

	troop := testTroopSnapshot(101, 2, 4)
	payload, err := encodePayload(&troop)
	if err != nil {
		t.Fatal(err)
	}

	mock.ExpectBegin()
	// 1) 世界行（epoch）
	mock.ExpectQuery(`SELECT epoch FROM map_world`).
		WithArgs(int64(1)).
		WillReturnRows(sqlmock.NewRows([]string{"epoch"}).AddRow(9))
	// 2) 场景行，必须按 scene_id 升序返回
	mock.ExpectQuery(`SELECT scene_id, revision FROM map_scene`).
		WithArgs(int64(1)).
		WillReturnRows(sqlmock.NewRows([]string{"scene_id", "revision"}).
			AddRow(1, 11).AddRow(2, 12))
	// 3) 对象
	mock.ExpectQuery(`FROM map_object`).
		WithArgs(int64(1)).
		WillReturnRows(sqlmock.NewRows([]string{"object_id", "kind", "owner_scene", "version", "schema_version", "player_id", "payload", "deleted"}).
			AddRow(uint64(101), int32(2), int32(2), uint64(4), uint32(1), int64(7), payload, int32(0)))
	// 4) 活动会话
	mock.ExpectQuery(`FROM map_collect`).
		WithArgs(int64(1), int32(1)).
		WillReturnRows(sqlmock.NewRows([]string{"collect_id", "troop_id", "resource_id", "start_at_ms", "end_at_ms", "planned_qty", "rate_milli", "rule_version", "status", "settled_qty"}).
			AddRow("sess-1", uint64(101), uint64(500), int64(1000), int64(2000), int64(5), int64(1000), "m1", int32(1), int64(0)))
	// 5) 待裁决战斗
	mock.ExpectQuery(`FROM map_battle`).
		WithArgs(int64(1), int32(1)).
		WillReturnRows(sqlmock.NewRows([]string{"battle_id", "target_id", "attacker_id", "defender_id", "input_hash", "attacker_stats", "defender_stats", "rule_version", "random_seed", "status", "deadline_ms", "result", "applied_operation_id"}).
			AddRow("battle-1", uint64(500), uint64(101), uint64(500), hashBytes("b"), []byte(`{"attack":100}`), []byte(`{"attack":80}`), "m1", int64(42), int32(1), int64(9000), nil, ""))
	// 6) 战斗队列
	mock.ExpectQuery(`FROM map_battle_queue`).
		WithArgs(int64(1)).
		WillReturnRows(sqlmock.NewRows([]string{"target_id", "troop_id"}).
			AddRow(uint64(500), uint64(101)).AddRow(uint64(500), uint64(102)))
	// 7) 交接单
	mock.ExpectQuery(`FROM map_delivery`).
		WithArgs(int64(1), int32(1)).
		WillReturnRows(sqlmock.NewRows([]string{"delivery_id", "world_id", "troop_id", "player_id", "payload_hash", "cargo", "status", "attempts", "next_retry_ms", "settlement_no", "applied_player_version", "created_at_ms"}).
			AddRow("dl-1", int64(1), uint64(101), int64(7), hashBytes("c"), []byte(`{"1001":5}`), int32(1), int32(0), int64(0), int64(1), int64(0), int64(5000)))
	mock.ExpectCommit()

	got, err := r.RecoverWorld(context.Background(), 1)
	if err != nil {
		t.Fatalf("RecoverWorld: %v", err)
	}
	if got.Epoch != 9 {
		t.Fatalf("epoch 应为 9, got %d", got.Epoch)
	}
	if len(got.Scenes) != 2 || got.Scenes[2].Revision != 12 {
		t.Fatalf("场景状态不符: %+v", got.Scenes)
	}
	if len(got.Objects) != 1 || got.Objects[0].Troop == nil {
		t.Fatalf("对象恢复失败: %+v", got.Objects)
	}
	if len(got.Sessions) != 1 || got.Sessions[0].ID != "sess-1" {
		t.Fatalf("会话恢复失败: %+v", got.Sessions)
	}
	if len(got.Battles) != 1 || got.Battles[0].Status != domain.BattleStatusPending {
		t.Fatalf("战斗恢复失败: %+v", got.Battles)
	}
	// FIFO：enqueue_seq 升序被保留。
	if q := got.Queues[500]; len(q) != 2 || q[0] != 101 || q[1] != 102 {
		t.Fatalf("队列顺序错误: %+v", got.Queues)
	}
	if len(got.Deliveries) != 1 || got.Deliveries[0].Cargo[1001] != 5 {
		t.Fatalf("交接单恢复失败: %+v", got.Deliveries)
	}
	// ExpectationsWereMet 同时证明了查询顺序（sqlmock 按注册顺序匹配）。
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// ---------- 租约 ----------

func TestAcquireLease(t *testing.T) {
	t.Run("首次创建", func(t *testing.T) {
		r, mock := newTestRepo(t)
		mock.ExpectBegin()
		mock.ExpectQuery(`SELECT owner_id, epoch, lease_until_ms FROM map_world`).
			WithArgs(int64(1)).
			WillReturnRows(sqlmock.NewRows([]string{"owner_id", "epoch", "lease_until_ms"}))
		mock.ExpectExec(`INSERT INTO map_world`).WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectCommit()

		epoch, err := r.Acquire(context.Background(), 1, "node-a", 30*time.Second)
		if err != nil {
			t.Fatalf("Acquire: %v", err)
		}
		if epoch != 1 {
			t.Fatalf("首次创建 epoch 应为 1, got %d", epoch)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("租约被他人持有", func(t *testing.T) {
		r, mock := newTestRepo(t)
		mock.ExpectBegin()
		mock.ExpectQuery(`SELECT owner_id, epoch, lease_until_ms FROM map_world`).
			WillReturnRows(sqlmock.NewRows([]string{"owner_id", "epoch", "lease_until_ms"}).
				AddRow("node-b", 5, testNowMS+10000))
		mock.ExpectRollback()

		_, err := r.Acquire(context.Background(), 1, "node-a", 30*time.Second)
		if !errors.Is(err, ErrLeaseHeld) {
			t.Fatalf("应返回 ErrLeaseHeld, got %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("旧租约过期后接管", func(t *testing.T) {
		r, mock := newTestRepo(t)
		mock.ExpectBegin()
		mock.ExpectQuery(`SELECT owner_id, epoch, lease_until_ms FROM map_world`).
			WillReturnRows(sqlmock.NewRows([]string{"owner_id", "epoch", "lease_until_ms"}).
				AddRow("node-b", 5, testNowMS-1)) // 已过期
		mock.ExpectExec(`UPDATE map_world SET owner_id`).WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectCommit()

		epoch, err := r.Acquire(context.Background(), 1, "node-a", 30*time.Second)
		if err != nil {
			t.Fatalf("Acquire: %v", err)
		}
		if epoch != 6 {
			t.Fatalf("接管后 epoch 应为 6, got %d", epoch)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestRenewAndReleaseLease(t *testing.T) {
	r, mock := newTestRepo(t)

	mock.ExpectExec(`UPDATE map_world SET lease_until_ms`).WillReturnResult(sqlmock.NewResult(0, 1))
	if err := r.Renew(context.Background(), 1, "node-a", 6, 30*time.Second); err != nil {
		t.Fatalf("Renew: %v", err)
	}

	// epoch 不符：说明已被接管，续租必须失败。
	mock.ExpectExec(`UPDATE map_world SET lease_until_ms`).WillReturnResult(sqlmock.NewResult(0, 0))
	if err := r.Renew(context.Background(), 1, "node-a", 5, 30*time.Second); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("过期 epoch 续租应返回 ErrLeaseLost, got %v", err)
	}

	mock.ExpectExec(`UPDATE map_world SET owner_id = ''`).WillReturnResult(sqlmock.NewResult(0, 1))
	if err := r.Release(context.Background(), 1, "node-a", 6); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// ---------- 幂等回执与 actor_key ----------

func TestFindReceipt(t *testing.T) {
	r, mock := newTestRepo(t)
	hash := testPayloadHash("canonical-params")

	mock.ExpectQuery(`SELECT command_id, payload_hash, result_code, result_json, committed_at_ms FROM map_command`).
		WithArgs(int64(1), "p:7", "req-1").
		WillReturnRows(sqlmock.NewRows([]string{"command_id", "payload_hash", "result_code", "result_json", "committed_at_ms"}).
			AddRow("cmd-1", hash[:], int32(0), []byte(`{"ok":true}`), int64(6000)))

	rc, found, err := r.FindReceiptByKey(context.Background(), domain.CommandKey{WorldID: 1, PlayerID: 7, RequestID: "req-1"})
	if err != nil || !found {
		t.Fatalf("应找到回执 found=%v err=%v", found, err)
	}
	if rc.CommandID != "cmd-1" || rc.PayloadHash != hash || rc.Key.PlayerID != 7 {
		t.Fatalf("回执内容不符: %+v", rc)
	}

	// 未命中：返回 (nil,false,nil) —— 与查询失败区分开。
	mock.ExpectQuery(`SELECT command_id, payload_hash, result_code, result_json, committed_at_ms FROM map_command`).
		WillReturnRows(sqlmock.NewRows([]string{"command_id", "payload_hash", "result_code", "result_json", "committed_at_ms"}))
	rc, found, err = r.FindReceipt(context.Background(), 1, "p:8", "req-x")
	if err != nil || found || rc != nil {
		t.Fatalf("未命中应返回 (nil,false,nil), got %+v/%v/%v", rc, found, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestActorKey(t *testing.T) {
	if got := ActorKey(7); got != "p:7" {
		t.Fatalf("ActorKey(7)=%q", got)
	}
	if got := SystemActorKey("refresh"); got != "sys:refresh" {
		t.Fatalf("SystemActorKey=%q", got)
	}
	// 解析回填只在 p: 前缀时发生。
	if id, ok := parseActorKey("p:123"); !ok || id != 123 {
		t.Fatalf("parseActorKey(p:123)=%d,%v", id, ok)
	}
	if _, ok := parseActorKey("sys:refresh"); ok {
		t.Fatal("sys: 前缀不应解析出 player_id")
	}
	if _, ok := parseActorKey("p:12x"); ok {
		t.Fatal("非数字后缀不应被当作合法 player_id")
	}
}

// ---------- 交接单 ----------

func TestDeliveryLifecycle(t *testing.T) {
	r, mock := newTestRepo(t)
	hash := testPayloadHash("cargo")

	mock.ExpectQuery(`FROM map_delivery WHERE status = . AND next_retry_ms <= .`).
		WillReturnRows(sqlmock.NewRows([]string{"delivery_id", "world_id", "troop_id", "player_id", "payload_hash", "cargo", "status", "attempts", "next_retry_ms", "settlement_no", "applied_player_version", "created_at_ms"}).
			AddRow("dl-1", int64(1), uint64(101), int64(7), hash[:], []byte(`{"1001":5}`), int32(1), int32(0), int64(0), int64(1), int64(0), int64(5000)))
	list, err := r.ListDeliveryPending(context.Background(), 10)
	if err != nil {
		t.Fatalf("ListDeliveryPending: %v", err)
	}
	if len(list) != 1 || list[0].DeliveryID != "dl-1" || list[0].Cargo[1001] != 5 {
		t.Fatalf("待投递单不符: %+v", list)
	}

	// APPLIED 是 CAS：0 行时再查一次以区分「不存在」与「已不是 PENDING」。
	mock.ExpectExec(`UPDATE map_delivery SET status = .`).WillReturnResult(sqlmock.NewResult(0, 1))
	if err := r.MarkDeliveryApplied(context.Background(), "dl-1", 88); err != nil {
		t.Fatalf("MarkDeliveryApplied: %v", err)
	}

	mock.ExpectExec(`UPDATE map_delivery SET attempts = .`).WillReturnResult(sqlmock.NewResult(0, 1))
	if err := r.MarkDeliveryRetry(context.Background(), "dl-1", 2, testNowMS+2000); err != nil {
		t.Fatalf("MarkDeliveryRetry: %v", err)
	}

	mock.ExpectExec(`UPDATE map_delivery SET status = .* WHERE delivery_id`).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(`SELECT delivery_id, world_id, troop_id`).
		WillReturnRows(sqlmock.NewRows([]string{"delivery_id", "world_id", "troop_id", "player_id", "payload_hash", "cargo", "status", "attempts", "next_retry_ms", "settlement_no", "applied_player_version", "created_at_ms"}).
			AddRow("dl-1", int64(1), uint64(101), int64(7), hash[:], []byte(`{"1001":5}`), int32(2), int32(0), int64(0), int64(1), int64(88), int64(5000)))
	if err := r.MarkDeliveryApplied(context.Background(), "dl-1", 99); !errors.Is(err, ErrDeliveryNotPending) {
		t.Fatalf("重复确认应返回 ErrDeliveryNotPending, got %v", err)
	}

	mock.ExpectExec(`UPDATE map_delivery SET status = .* WHERE delivery_id`).
		WillReturnResult(sqlmock.NewResult(0, 1))
	if err := r.QuarantineDelivery(context.Background(), "dl-1", "payload hash mismatch"); err != nil {
		t.Fatalf("QuarantineDelivery: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// ---------- 刷新进度 ----------

func TestRefreshProgress(t *testing.T) {
	r, mock := newTestRepo(t)

	mock.ExpectExec(`INSERT INTO map_refresh`).WillReturnResult(sqlmock.NewResult(0, 1))
	if err := r.UpsertRefreshProgress(context.Background(), 1, 8, 2, 120, false); err != nil {
		t.Fatalf("UpsertRefreshProgress: %v", err)
	}

	mock.ExpectQuery(`SELECT world_id, rich_block_id, generation`).
		WillReturnRows(sqlmock.NewRows([]string{"world_id", "rich_block_id", "generation", "config_version", "seed", "cursor", "finished", "version"}).
			AddRow(int64(1), int64(8), int64(2), "r1", int64(99), int32(120), int32(0), uint64(3)))
	got, found, err := r.LoadRefreshProgress(context.Background(), 1, 8, 2)
	if err != nil || !found {
		t.Fatalf("LoadRefreshProgress found=%v err=%v", found, err)
	}
	if got.Cursor != 120 || got.Seed != 99 || got.Finished {
		t.Fatalf("刷新进度不符: %+v", got)
	}

	// 没有进度不是错误：刷新任务第一次跑就处于这个状态。
	mock.ExpectQuery(`SELECT world_id, rich_block_id, generation`).
		WillReturnRows(sqlmock.NewRows([]string{"world_id", "rich_block_id", "generation", "config_version", "seed", "cursor", "finished", "version"}))
	if _, found, err := r.LoadRefreshProgress(context.Background(), 1, 9, 2); err != nil || found {
		t.Fatalf("无进度应返回 (nil,false,nil), got found=%v err=%v", found, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// ---------- 辅助函数单测 ----------

func TestCommitRejectsEmptyPlan(t *testing.T) {
	r, _ := newTestRepo(t)
	if res := r.Commit(context.Background(), nil); res.Err == nil {
		t.Fatal("nil 计划应报错")
	}
	if res := r.Commit(context.Background(), &domain.MutationPlan{}); res.Err == nil {
		t.Fatal("缺少 command_id/world_id 的计划应报错")
	}
	// 确认返回值不是 *sql.Tx 相关的零值哨兵（避免调用方误判为已提交）。
	var zero domain.CommitResult
	if reflect.DeepEqual(r.Commit(context.Background(), &domain.MutationPlan{}), zero) {
		t.Fatal("失败结果不应是全零值")
	}
}

func TestSortedHelpersDoNotMutateInput(t *testing.T) {
	in := []domain.SceneRevision{{SceneID: 3, Revision: 1}, {SceneID: 1, Revision: 2}, {SceneID: 1, Revision: 2}}
	out := sortedSceneRevisions(in)
	if in[0].SceneID != 3 {
		t.Fatal("排序不应修改调用方切片")
	}
	if len(out) != 2 || out[0].SceneID != 1 || out[1].SceneID != 3 {
		t.Fatalf("排序/去重结果错误: %+v", out)
	}

	snaps := []domain.ObjectSnapshot{
		{Key: domain.ObjectKey{ID: 20}}, {Key: domain.ObjectKey{ID: 10}},
	}
	sorted := sortedSnapshots(snaps)
	if snaps[0].Key.ID != 20 {
		t.Fatal("对象排序不应修改调用方切片")
	}
	if sorted[0].Key.ID != 10 {
		t.Fatalf("对象排序结果错误: %+v", sorted)
	}
}

func TestRetryBackoff(t *testing.T) {
	if retryBackoff(0) != 5*time.Millisecond || retryBackoff(1) != 10*time.Millisecond || retryBackoff(2) != 20*time.Millisecond {
		t.Fatalf("退避序列应为 5/10/20ms, got %v/%v/%v", retryBackoff(0), retryBackoff(1), retryBackoff(2))
	}
	// 上界钳制：不能出现 0（0 等待等于忙等）或溢出成负数。
	for i := 0; i < 20; i++ {
		if d := retryBackoff(i); d <= 0 {
			t.Fatalf("退避必须为正, attempt=%d got=%v", i, d)
		}
	}
}
