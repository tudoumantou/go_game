// pkg/mq/mq_test.go —— 消息信封/幂等键/topic 单测（不依赖真实 Kafka）
package mq

import (
	"context"
	"testing"
	"time"
)

func TestEnvelopeRoundTrip(t *testing.T) {
	msg := RechargeOrderMsg{OrderID: "O20260101", PlayerID: 1001, ProductID: 3, AmountFen: 600, Channel: 1}
	env, err := NewEnvelope("O20260101", TopicRechargeOrder, "O20260101", msg)
	if err != nil {
		t.Fatalf("new envelope: %v", err)
	}
	raw, err := env.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	got, err := Unmarshal(raw)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.BizID != "O20260101" || got.Topic != TopicRechargeOrder || got.Key != "O20260101" {
		t.Fatalf("envelope mismatch: %+v", got)
	}

	var decoded RechargeOrderMsg
	if err := got.DecodePayload(&decoded); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if decoded.PlayerID != 1001 || decoded.AmountFen != 600 {
		t.Fatalf("payload mismatch: %+v", decoded)
	}
}

func TestEnvelopeRequiresBizID(t *testing.T) {
	env, err := NewEnvelope("", TopicMail, "1", MailMsg{MailID: 1})
	if err != nil {
		t.Fatalf("new envelope: %v", err)
	}
	if _, err := env.Marshal(); err == nil {
		t.Fatalf("empty biz_id should fail")
	}
	if _, err := Unmarshal([]byte(`{"topic":"x"}`)); err == nil {
		t.Fatalf("unmarshal without biz_id should fail")
	}
}

func TestDecodePayloadEmpty(t *testing.T) {
	env := &Envelope{BizID: "x"}
	var v MailMsg
	if err := env.DecodePayload(&v); err == nil {
		t.Fatalf("empty payload should fail")
	}
}

func TestIdemKey(t *testing.T) {
	got := IdemKey(TopicRechargeOrder, "O123")
	want := "idem:gs.recharge.order:O123"
	if got != want {
		t.Fatalf("idem key = %q, want %q", got, want)
	}
}

func TestNoopDeduperAlwaysFirst(t *testing.T) {
	var d Deduper = NoopDeduper{}
	for i := 0; i < 3; i++ {
		ok, err := d.FirstTime(context.Background(), TopicMail, "same", time.Minute)
		if err != nil || !ok {
			t.Fatalf("noop deduper should always return true")
		}
	}
}

func TestAllTopicsUnique(t *testing.T) {
	topics := AllTopics()
	// 断言唯一性与最小规模，不写死条数：新增 topic 不需要改测试。
	if len(topics) < 9 {
		t.Fatalf("expect >= 9 topics, got %d", len(topics))
	}
	seen := make(map[string]bool, len(topics))
	for _, tp := range topics {
		if tp == "" {
			t.Fatalf("empty topic")
		}
		if seen[tp] {
			t.Fatalf("duplicate topic %s", tp)
		}
		seen[tp] = true
	}
}
