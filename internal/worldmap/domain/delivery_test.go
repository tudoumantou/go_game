package domain

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

// TestDeliveryKey 验证业务去重键格式（文档 13.2）。
func TestDeliveryKey(t *testing.T) {
	tests := []struct {
		deliveryID string
		want       string
	}{
		{"d-0001", "slg:delivery:d-0001"},
		{"550e8400-e29b-41d4-a716-446655440000", "slg:delivery:550e8400-e29b-41d4-a716-446655440000"},
		{"", DeliveryKeyPrefix},
	}
	for _, tc := range tests {
		if got := DeliveryKey(tc.deliveryID); got != tc.want {
			t.Errorf("DeliveryKey(%q) = %q, want %q", tc.deliveryID, got, tc.want)
		}
	}
}

// TestRetryBackoff 验证指数退避序列：1s、2s、4s……60s 封顶
// （文档 17.1 delivery_retry_min/max）。
func TestRetryBackoff(t *testing.T) {
	tests := []struct {
		attempt int32
		want    time.Duration
	}{
		{-1, time.Second},       // 负 attempt 按首次重试，绝不 0 等待
		{0, time.Second},        // 同上
		{1, time.Second},        // 1s
		{2, 2 * time.Second},    // 2s
		{3, 4 * time.Second},    // 4s
		{4, 8 * time.Second},    // 8s
		{5, 16 * time.Second},   // 16s
		{6, 32 * time.Second},   // 32s
		{7, 60 * time.Second},   // 64s 被夹紧到 60s
		{8, 60 * time.Second},   // 封顶
		{100, 60 * time.Second}, // 大 attempt 不溢出成 0
		{1 << 30, 60 * time.Second},
	}
	for _, tc := range tests {
		if got := RetryBackoff(tc.attempt); got != tc.want {
			t.Errorf("RetryBackoff(%d) = %v, want %v", tc.attempt, got, tc.want)
		}
	}
	// 序列在封顶前必须严格递增（指数退避的定义），
	// 封顶后恒定。
	prev := time.Duration(0)
	for attempt := int32(1); attempt <= 10; attempt++ {
		got := RetryBackoff(attempt)
		if got < prev {
			t.Fatalf("attempt=%d 退避 %v 小于前值 %v，不是递增序列", attempt, got, prev)
		}
		if got > time.Duration(DeliveryRetryMaxMS)*time.Millisecond {
			t.Fatalf("attempt=%d 退避 %v 超过上限", attempt, got)
		}
		prev = got
	}
}

// TestNextRetryAtMS 验证下次重试时刻计算。
func TestNextRetryAtMS(t *testing.T) {
	tests := []struct {
		nowMS   int64
		attempt int32
		want    int64
	}{
		{1_700_000_000_000, 1, 1_700_000_001_000},
		{1_700_000_000_000, 2, 1_700_000_002_000},
		{1_700_000_000_000, 3, 1_700_000_004_000},
		{1_700_000_000_000, 7, 1_700_000_060_000}, // 封顶 60s
	}
	for _, tc := range tests {
		if got := NextRetryAtMS(tc.nowMS, tc.attempt); got != tc.want {
			t.Errorf("NextRetryAtMS(%d, %d) = %d, want %d", tc.nowMS, tc.attempt, got, tc.want)
		}
	}
}

// TestDeliveryAckMatches 验证 Ack 与交接单的一致性比较
// （文档 13.5：同 id 不同 payload 必须隔离）。
func TestDeliveryAckMatches(t *testing.T) {
	d := &Delivery{
		DeliveryID:  "d-1",
		WorldID:     1,
		TroopID:     4001,
		PlayerID:    100,
		Cargo:       map[int32]int64{9001: 5},
		PayloadHash: PayloadHashOf([]byte("d-1|4001|100|9001:5")),
	}
	same := DeliveryAck{DeliveryID: d.DeliveryID, PayloadHash: d.PayloadHash, PlayerVersion: 42}
	diffHash := DeliveryAck{DeliveryID: d.DeliveryID, PayloadHash: PayloadHashOf([]byte("other")), PlayerVersion: 42}
	diffID := DeliveryAck{DeliveryID: "d-2", PayloadHash: d.PayloadHash, PlayerVersion: 42}

	if !same.Matches(d) {
		t.Error("同 id 同 hash 的 Ack 应匹配")
	}
	if diffHash.Matches(d) {
		t.Error("同 id 不同 hash 的 Ack 不应匹配（payload 冲突，隔离）")
	}
	if diffID.Matches(d) {
		t.Error("不同 id 的 Ack 不应匹配")
	}
	if same.Matches(nil) {
		t.Error("nil 交接单不应匹配")
	}
}

// TestDeliveryCursorAdvance 验证 keyset 游标推进。
func TestDeliveryCursorAdvance(t *testing.T) {
	c := DeliveryCursor{WorldID: 1, Limit: 100}
	last := &Delivery{DeliveryID: "d-7", CreatedAtMS: 1_700_000_005_000}
	c.Advance(last)
	if c.AfterCreatedAtMS != last.CreatedAtMS || c.AfterDeliveryID != last.DeliveryID {
		t.Fatalf("游标推进错误: %+v", c)
	}
	// nil 不前移（空批次不应打乱扫描位置）。
	before := c
	c.Advance(nil)
	require.EqualValues(t, before, c)
}

// fakeDeliveryApplier 是 DeliveryApplier 的内存替身，
// 模拟玩家侧“回执 map + 可选阻塞”。
type fakeDeliveryApplier struct {
	receipts map[string]DeliveryAck
	blocked  string
}

func newFakeDeliveryApplier() *fakeDeliveryApplier {
	return &fakeDeliveryApplier{receipts: map[string]DeliveryAck{}}
}

// 编译期断言：替身必须满足 18.2 的接口形状。
var _ DeliveryApplier = (*fakeDeliveryApplier)(nil)

func (f *fakeDeliveryApplier) Apply(_ context.Context, d Delivery) (DeliveryApplyResult, error) {
	if f.blocked != "" {
		return NewRetryableBlocked(f.blocked), nil
	}
	if a, ok := f.receipts[d.DeliveryID]; ok {
		if a.PayloadHash != d.PayloadHash {
			return NewQuarantined("payload 冲突"), nil
		}
		return DeliveryApplyResult{
			Status:        DeliveryApplyAlreadyApplied,
			PlayerVersion: a.PlayerVersion,
			ReceiptHash:   a.PayloadHash,
		}, nil
	}
	// 首次应用：写回执（模拟玩家 CAS 提交后的事实）。
	f.receipts[d.DeliveryID] = DeliveryAck{
		DeliveryID:    d.DeliveryID,
		PayloadHash:   d.PayloadHash,
		PlayerVersion: 42,
	}
	return DeliveryApplyResult{
		Status:        DeliveryApplyApplied,
		PlayerVersion: 42,
		ReceiptHash:   d.PayloadHash,
	}, nil
}

func (f *fakeDeliveryApplier) Query(_ context.Context, q DeliveryReceiptQuery) (DeliveryApplyResult, error) {
	if a, ok := f.receipts[q.DeliveryID]; ok {
		if a.PayloadHash != q.PayloadHash {
			return NewQuarantined("回执 hash 与单据不符"), nil
		}
		return DeliveryApplyResult{
			Status:        DeliveryApplyAlreadyApplied,
			PlayerVersion: a.PlayerVersion,
			ReceiptHash:   a.PayloadHash,
		}, nil
	}
	// 无回执：Map 应重投而不是再次加奖（13.2 步骤 5）。
	return NewRetryableBlocked("玩家无回执，重新投递"), nil
}

// testDelivery 构造一份回城交接单。
func testDelivery() Delivery {
	return Delivery{
		DeliveryID:  "d-1",
		WorldID:     1,
		TroopID:     4001,
		PlayerID:    100,
		Cargo:       map[int32]int64{9001: 5},
		PayloadHash: PayloadHashOf([]byte("d-1|4001|100|9001:5")),
		CreatedAtMS: 1_700_000_000_000,
		Status:      DeliveryStatusPending,
	}
}

// TestDeliveryApplierFlow 覆盖交接应用的状态机：
// Applied → AlreadyApplied（INV-07 最多加一次）、
// RetryableBlocked（背包满）、Quarantined（payload 冲突）、
// Query 的无回执与已回执路径。
func TestDeliveryApplierFlow(t *testing.T) {
	ctx := context.Background()

	t.Run("首次 Applied 再次 AlreadyApplied", func(t *testing.T) {
		app := newFakeDeliveryApplier()
		d := testDelivery()
		first, err := app.Apply(ctx, d)
		require.NoError(t, err, "Apply 失败")
		require.EqualValues(t, DeliveryApplyApplied, first.Status)
		second, err := app.Apply(ctx, d)
		require.NoError(t, err, "重复 Apply 失败")
		require.EqualValues(t, DeliveryApplyAlreadyApplied, second.Status)
		// 回执只应存在一条：玩家侧最多加一次奖。
		require.Len(t, app.receipts, 1)
		require.EqualValues(t, 42, second.PlayerVersion)
		// Query 命中的是同一份回执。
		q, err := app.Query(ctx, DeliveryReceiptQuery{WorldID: 1, DeliveryID: d.DeliveryID, PayloadHash: d.PayloadHash})
		require.NoError(t, err, "Query 失败")
		require.EqualValues(t, DeliveryApplyAlreadyApplied, q.Status)
	})

	t.Run("背包满返回可重试阻塞", func(t *testing.T) {
		app := newFakeDeliveryApplier()
		app.blocked = "背包已满"
		got, err := app.Apply(ctx, testDelivery())
		require.NoError(t, err, "Apply 失败")
		require.EqualValues(t, DeliveryApplyRetryableBlocked, got.Status)
		if got.Reason == "" {
			t.Fatal("阻塞必须带原因")
		}
		require.Len(t, app.receipts, 0, "阻塞时不应写回执")
	})

	t.Run("payload 冲突隔离", func(t *testing.T) {
		app := newFakeDeliveryApplier()
		d := testDelivery()
		if _, err := app.Apply(ctx, d); err != nil {
			t.Fatalf("Apply 失败: %v", err)
		}
		tampered := d
		tampered.Cargo = map[int32]int64{9001: 999}
		tampered.PayloadHash = PayloadHashOf([]byte("d-1|4001|100|9001:999"))
		got, err := app.Apply(ctx, tampered)
		require.NoError(t, err, "Apply 失败")
		require.EqualValues(t, DeliveryApplyQuarantined, got.Status)
	})

	t.Run("Query 无回执返回可重试", func(t *testing.T) {
		app := newFakeDeliveryApplier()
		d := testDelivery()
		got, err := app.Query(ctx, DeliveryReceiptQuery{WorldID: 1, DeliveryID: d.DeliveryID, PayloadHash: d.PayloadHash})
		require.NoError(t, err, "Query 失败")
		require.EqualValues(t, DeliveryApplyRetryableBlocked, got.Status)
	})
}

// TestDeliveryApplyStatusValid 验证状态枚举合法性。
func TestDeliveryApplyStatusValid(t *testing.T) {
	if !DeliveryApplyApplied.Valid() || !DeliveryApplyAlreadyApplied.Valid() ||
		!DeliveryApplyRetryableBlocked.Valid() || !DeliveryApplyQuarantined.Valid() {
		t.Fatal("已知状态必须合法")
	}
	if DeliveryApplyUnspecified.Valid() || DeliveryApplyStatus(99).Valid() {
		t.Fatal("未知状态必须非法")
	}
}
