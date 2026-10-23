// pkg/mq/kafka_test.go —— 消费提交规则：死信没写成功就不能推进 offset
package mq

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
)

// fakeReader 按顺序吐出预置消息，记录每次 commit。
type fakeReader struct {
	msgs      []kafka.Message
	idx       int
	committed []kafka.Message
	commitErr error
}

func (r *fakeReader) FetchMessage(context.Context) (kafka.Message, error) {
	if r.idx >= len(r.msgs) {
		return kafka.Message{}, context.Canceled
	}
	msg := r.msgs[r.idx]
	r.idx++
	return msg, nil
}

func (r *fakeReader) CommitMessages(_ context.Context, msgs ...kafka.Message) error {
	if r.commitErr != nil {
		return r.commitErr
	}
	r.committed = append(r.committed, msgs...)
	return nil
}

func (r *fakeReader) Stats() kafka.ReaderStats { return kafka.ReaderStats{} }
func (r *fakeReader) Close() error             { return nil }

// fakeDLQ 记录写入的死信，可注入失败。
type fakeDLQ struct {
	got    []*Envelope
	causes []error
	err    error
}

func (d *fakeDLQ) DeadLetter(_ context.Context, env *Envelope, cause error) error {
	if d.err != nil {
		return d.err
	}
	d.got = append(d.got, env)
	d.causes = append(d.causes, cause)
	return nil
}

// validMessage 构造一条可被 Unmarshal 解析的合法消息（信封 + 指定 offset），
// 让提交规则测试聚焦在 commit/DLQ 行为，而不是消息构造细节。
func validMessage(t *testing.T, offset int64) kafka.Message {
	t.Helper()
	env, err := NewEnvelope("biz-1", "gs.test", "k1", map[string]string{"v": "1"})
	require.NoError(t, err)
	raw, err := env.Marshal()
	require.NoError(t, err)
	return kafka.Message{Topic: "gs.test", Partition: 0, Offset: offset, Key: []byte("k1"), Value: raw}
}

// newTestConsumer 用注入的 reader/deadLetterWriter 组装 Consumer，
// 完全不碰真实 Kafka；retries 显式指定，避开 NewConsumer 的默认值干扰断言。
func newTestConsumer(r messageReader, d deadLetterWriter, retries int) *Consumer {
	return &Consumer{reader: r, producer: d, maxRetry: retries, topic: "gs.test"}
}

// TestRunCommitsOnSuccess 业务成功即提交，一条消息一次 commit。
func TestRunCommitsOnSuccess(t *testing.T) {
	reader := &fakeReader{msgs: []kafka.Message{validMessage(t, 1)}}
	c := newTestConsumer(reader, &fakeDLQ{}, 0)
	if err := c.Run(context.Background(), func(context.Context, *Envelope) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if len(reader.committed) != 1 || reader.committed[0].Offset != 1 {
		t.Fatalf("committed: %+v", reader.committed)
	}
}

// TestRunRetriesThenDeadLettersAndCommits 处理失败重试耗尽后写死信，死信成功才提交。
func TestRunRetriesThenDeadLettersAndCommits(t *testing.T) {
	reader := &fakeReader{msgs: []kafka.Message{validMessage(t, 2)}}
	dlq := &fakeDLQ{}
	c := newTestConsumer(reader, dlq, 1)
	attempts := 0
	err := c.Run(context.Background(), func(context.Context, *Envelope) error {
		attempts++
		return errors.New("boom")
	})
	require.NoError(t, err)
	if attempts != 2 {
		t.Fatalf("retry attempts: got %d want 2", attempts)
	}
	if len(dlq.got) != 1 || len(reader.committed) != 1 {
		t.Fatalf("dead=%d committed=%d", len(dlq.got), len(reader.committed))
	}
}

// TestRunDeadLetterFailureKeepsOffset 死信写失败必须不提交并返回错误，
// 否则消息既没处理也没留痕，offset 却前进了。
func TestRunDeadLetterFailureKeepsOffset(t *testing.T) {
	reader := &fakeReader{msgs: []kafka.Message{validMessage(t, 3)}}
	dlq := &fakeDLQ{err: errors.New("dlq unavailable")}
	c := newTestConsumer(reader, dlq, 0)
	err := c.Run(context.Background(), func(context.Context, *Envelope) error {
		return errors.New("boom")
	})
	if err == nil {
		t.Fatal("dead letter failure must surface as an error")
	}
	if !strings.Contains(err.Error(), "dead letter") {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(reader.committed) != 0 {
		t.Fatalf("offset must not advance: %+v", reader.committed)
	}
}

// TestRunMalformedGoesToDeadLetter 解析失败的消息进死信（带原始字节），成功后才提交。
func TestRunMalformedGoesToDeadLetter(t *testing.T) {
	reader := &fakeReader{msgs: []kafka.Message{{
		Topic: "gs.test", Partition: 0, Offset: 4, Key: []byte("k2"), Value: []byte("not-json"),
	}}}
	dlq := &fakeDLQ{}
	c := newTestConsumer(reader, dlq, 0)
	handled := 0
	if err := c.Run(context.Background(), func(context.Context, *Envelope) error { handled++; return nil }); err != nil {
		t.Fatal(err)
	}
	if handled != 0 {
		t.Fatal("malformed message must not reach the handler")
	}
	if len(dlq.got) != 1 {
		t.Fatalf("dead letters: %d", len(dlq.got))
	}
	if raw := dlq.got[0].Headers["dlq_raw"]; raw != "bm90LWpzb24=" { // base64("not-json")
		t.Fatalf("raw payload not preserved: %q", raw)
	}
	if len(reader.committed) != 1 {
		t.Fatalf("committed: %+v", reader.committed)
	}
}

// TestRunWithoutDeadLetterWriterKeepsOffset 没有死信通道时不能假装处理完。
func TestRunWithoutDeadLetterWriterKeepsOffset(t *testing.T) {
	reader := &fakeReader{msgs: []kafka.Message{validMessage(t, 5)}}
	c := newTestConsumer(reader, nil, 0)
	err := c.Run(context.Background(), func(context.Context, *Envelope) error {
		return errors.New("boom")
	})
	if err == nil || !strings.Contains(err.Error(), "no dead letter writer") {
		t.Fatalf("expected dead letter writer error, got %v", err)
	}
	if len(reader.committed) != 0 {
		t.Fatal("offset advanced without a dead letter channel")
	}
}

// TestRunCommitFailureSurfaces commit 失败要上报，让上层重启后重放。
func TestRunCommitFailureSurfaces(t *testing.T) {
	reader := &fakeReader{msgs: []kafka.Message{validMessage(t, 6)}, commitErr: errors.New("coordinator down")}
	c := newTestConsumer(reader, &fakeDLQ{}, 0)
	err := c.Run(context.Background(), func(context.Context, *Envelope) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "commit") {
		t.Fatalf("expected commit error, got %v", err)
	}
}

// TestRunCancelDoesNotCommit 关服中断时不提交，重启后从该 offset 重放。
func TestRunCancelDoesNotCommit(t *testing.T) {
	reader := &fakeReader{msgs: []kafka.Message{validMessage(t, 7)}}
	c := newTestConsumer(reader, &fakeDLQ{}, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	err := c.Run(ctx, func(ctx context.Context, _ *Envelope) error {
		<-ctx.Done()
		return ctx.Err()
	})
	require.NoError(t, err, "cancel should exit cleanly")
	if len(reader.committed) != 0 {
		t.Fatal("offset committed while shutting down")
	}
}
