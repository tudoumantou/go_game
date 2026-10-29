// pkg/flush/coalesce_test.go —— 键级合并落库的语义
//
// 三件事必须钉死，它们是「合并」这件事的全部风险：
//   - 同一个键一个窗口内只写一次，写的是最后那个值；
//   - 写失败的键留在集合里等下一轮，不能悄悄丢；
//   - 重试时不覆盖「本轮写库期间新来的变更」——覆盖就是新状态被旧状态顶掉。
package flush

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"game_study/pkg/log"
	"github.com/stretchr/testify/require"
)

// recordingSink 记录每次落库收到的键值。
type recordingSink struct {
	mu    sync.Mutex
	calls map[int64]int
	fail  bool
}

func newRecordingSink() *recordingSink { return &recordingSink{calls: map[int64]int{}} }

func (s *recordingSink) fn(_ context.Context, key int64, val int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail {
		return errors.New("db down")
	}
	s.calls[key] = val
	return nil
}

func (s *recordingSink) snapshot() map[int64]int {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[int64]int, len(s.calls))
	for k, v := range s.calls {
		out[k] = v
	}
	return out
}

func newTestCoalescer(sink *recordingSink) *Coalescer[int64, int] {
	return NewCoalescer(CoalesceConfig{Interval: time.Hour, Timeout: time.Second},
		sink.fn, log.New("coalesce-test", log.WithLevel("error")))
}

// TestCoalescerMergesSameKey 同一个键多次变更只落最后一次。
func TestCoalescerMergesSameKey(t *testing.T) {
	sink := newRecordingSink()
	c := newTestCoalescer(sink)

	for i := 1; i <= 5; i++ {
		c.Mark(7, i*10)
	}
	c.Mark(8, 99)
	require.Equal(t, 2, c.Len(), "两个不同的键")

	require.NoError(t, c.Flush(context.Background()))
	require.Equal(t, map[int64]int{7: 50, 8: 99}, sink.snapshot(), "键 7 只写最后值")
	require.Zero(t, c.Len(), "写完清空")
}

// TestCoalescerRetriesFailedKeys 写失败的键保留，下一轮重试成功才清空。
func TestCoalescerRetriesFailedKeys(t *testing.T) {
	sink := newRecordingSink()
	sink.fail = true
	c := newTestCoalescer(sink)

	c.Mark(1, 10)
	require.Error(t, c.Flush(context.Background()))
	require.Equal(t, 1, c.Len(), "失败的键必须留着")

	sink.fail = false
	require.NoError(t, c.Flush(context.Background()))
	require.Equal(t, map[int64]int{1: 10}, sink.snapshot())
	require.Zero(t, c.Len())
}

// TestCoalescerRetryKeepsNewerValue 重试放回时不覆盖本轮新来的变更。
//
// 场景：Flush 换出 batch 后、写库返回失败之前，业务又 Mark 了同一个键。
// 此时集合里已经是新值，把旧值放回去就是「新状态被旧状态顶掉」。
func TestCoalescerRetryKeepsNewerValue(t *testing.T) {
	sink := newRecordingSink()
	var c *Coalescer[int64, int]

	// 第一次写失败，且在失败回调里立刻 Mark 一个新值（模拟并发更新）。
	attempt := 0
	c = NewCoalescer(CoalesceConfig{Interval: time.Hour, Timeout: time.Second},
		func(ctx context.Context, key int64, val int) error {
			attempt++
			if attempt == 1 {
				c.Mark(key, val+1) // 更新的意图
				return errors.New("db down")
			}
			return sink.fn(ctx, key, val)
		}, log.New("coalesce-test", log.WithLevel("error")))

	c.Mark(1, 10)
	require.Error(t, c.Flush(context.Background()))
	require.Equal(t, 1, c.Len())

	require.NoError(t, c.Flush(context.Background()))
	require.Equal(t, 11, sink.snapshot()[1], "落库的应是新值，不是被放回的旧值")
}

// TestCoalescerEmptyFlushNoop 空集合不调用落库函数。
func TestCoalescerEmptyFlushNoop(t *testing.T) {
	called := 0
	c := NewCoalescer(CoalesceConfig{Interval: time.Hour},
		func(context.Context, int64, int) error { called++; return nil },
		log.New("coalesce-test", log.WithLevel("error")))

	require.NoError(t, c.Flush(context.Background()))
	require.Zero(t, called)
}

// TestCoalescerStopDrains Stop 排空待写键（关服路径不能留数据在内存）。
func TestCoalescerStopDrains(t *testing.T) {
	sink := newRecordingSink()
	c := NewCoalescer(CoalesceConfig{Interval: time.Hour, Timeout: time.Second},
		sink.fn, log.New("coalesce-test", log.WithLevel("error")))
	c.Start()

	c.Mark(3, 30)
	require.NoError(t, c.Stop())
	require.Equal(t, map[int64]int{3: 30}, sink.snapshot())
	require.Zero(t, c.Len())
}

// TestCoalescerStartIdempotent 重复 Start 不会起第二个 worker。
//
// 两个 worker 同时 Flush 会让同一个键被写两次（虽然值相同，但落库语义
// 不再是「一个窗口一次」）。用「Stop 能返回」间接验证只有一个 done 通道
// 被关闭——多起一个就会有两个 goroutine 抢同一个 ticker。
func TestCoalescerStartIdempotent(t *testing.T) {
	sink := newRecordingSink()
	c := NewCoalescer(CoalesceConfig{Interval: 10 * time.Millisecond, Timeout: time.Second},
		sink.fn, log.New("coalesce-test", log.WithLevel("error")))
	c.Start()
	c.Start()
	c.Start()

	c.Mark(1, 1)
	require.Eventually(t, func() bool { return len(sink.snapshot()) == 1 }, time.Second, 5*time.Millisecond)
	require.NoError(t, c.Stop())
}

// TestCoalescerNilLogger 不传 logger 时自建一个。
//
// 降级路径本来就在告警，日志器为 nil 会让「告警」这件事本身再炸一次
// （nil pointer dereference 发生在错误处理里，最难查）。
func TestCoalescerNilLogger(t *testing.T) {
	c := NewCoalescer[int64, struct{}](CoalesceConfig{},
		func(context.Context, int64, struct{}) error { return nil }, nil)
	require.NotNil(t, c.logger)
	c.Mark(1, struct{}{})
	require.Equal(t, 1, c.Len())
}
