// pkg/mq/mq_test.go —— 消息信封/幂等键/topic 单测（不依赖真实 Kafka）
package mq

import (
	"context"
	"testing"
	"time"
)

// TestEnvelopeRoundTrip 覆盖「构造→Marshal→Unmarshal→DecodePayload」全链路：
// 信封三要素（biz_id/topic/key）与业务字段必须在往返后逐一不变，
// 这是所有 topic 共用的基础契约，坏了会影响全部业务消息。
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

// TestEnvelopeRequiresBizID biz_id 是强制项：NewEnvelope 允许构造空值
// （字段级不设防），但 Marshal 与 Unmarshal 两道关卡都必须拒绝，
// 防止没有幂等键的消息流入消费端造成重复应用。
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

// TestDecodePayloadEmpty payload 为空时 DecodePayload 必须报错：
// 避免业务方把「没有载荷」误当成零值结构继续执行业务逻辑。
func TestDecodePayloadEmpty(t *testing.T) {
	env := &Envelope{BizID: "x"}
	var v MailMsg
	if err := env.DecodePayload(&v); err == nil {
		t.Fatalf("empty payload should fail")
	}
}

// TestIdemKey 锁定幂等键格式 idem:{topic}:{biz_id}：格式变化意味着
// 存量 Redis 标记全部失效（线上表现为重复放行），故用精确断言而非包含匹配。
func TestIdemKey(t *testing.T) {
	got := IdemKey(TopicRechargeOrder, "O123")
	want := "idem:gs.recharge.order:O123"
	if got != want {
		t.Fatalf("idem key = %q, want %q", got, want)
	}
}

// TestNoopDeduperAlwaysFirst NoopDeduper 必须恒真：本地/单测场景不去重，
// 若误返回 false 会静默丢掉本该执行的消息，且没有任何报错线索。
func TestNoopDeduperAlwaysFirst(t *testing.T) {
	var d Deduper = NoopDeduper{}
	for i := 0; i < 3; i++ {
		ok, err := d.FirstTime(context.Background(), TopicMail, "same", time.Minute)
		if err != nil || !ok {
			t.Fatalf("noop deduper should always return true")
		}
	}
}

// TestAllTopicsUnique AllTopics 供本地一键建 topic：出现空串或重复会让
// 建表脚本半途失败，这里只锁下界与唯一性，条数不写死。
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
