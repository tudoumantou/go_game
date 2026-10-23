// pkg/mq/mq_test.go —— 消息信封/幂等键/topic 单测（不依赖真实 Kafka）
package mq

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

// TestEnvelopeRoundTrip 覆盖「构造→Marshal→Unmarshal→DecodePayload」全链路：
// 信封三要素（biz_id/topic/key）与业务字段必须在往返后逐一不变，
// 这是所有 topic 共用的基础契约，坏了会影响全部业务消息。
func TestEnvelopeRoundTrip(t *testing.T) {
	msg := RechargeOrderMsg{OrderID: "O20260101", PlayerID: 1001, ProductID: 3, AmountFen: 600, Channel: 1}
	env, err := NewEnvelope("O20260101", TopicRechargeOrder, "O20260101", msg)
	require.NoError(t, err, "new envelope")
	raw, err := env.Marshal()
	require.NoError(t, err, "marshal")

	got, err := Unmarshal(raw)
	require.NoError(t, err, "unmarshal")
	require.EqualValues(t, "O20260101", got.BizID, fmt.Sprintf("envelope mismatch: %+v", got))
	require.EqualValues(t, TopicRechargeOrder, got.Topic)
	require.EqualValues(t, "O20260101", got.Key)

	var decoded RechargeOrderMsg
	err = got.DecodePayload(&decoded)
	require.NoError(t, err, fmt.Sprintf("decode payload: %v", err))
	require.EqualValues(t, 1001, decoded.PlayerID, fmt.Sprintf("payload mismatch: %+v", decoded))
	require.EqualValues(t, 600, decoded.AmountFen)
}

// TestEnvelopeRequiresBizID biz_id 是强制项：NewEnvelope 允许构造空值
// （字段级不设防），但 Marshal 与 Unmarshal 两道关卡都必须拒绝，
// 防止没有幂等键的消息流入消费端造成重复应用。
func TestEnvelopeRequiresBizID(t *testing.T) {
	env, err := NewEnvelope("", TopicMail, "1", MailMsg{MailID: 1})
	require.NoError(t, err, "new envelope")
	_, err = env.Marshal()
	require.Error(t, err, "empty biz_id should fail")
	_, err = Unmarshal([]byte(`{"topic":"x"}`))
	require.Error(t, err, "unmarshal without biz_id should fail")
}

// TestDecodePayloadEmpty payload 为空时 DecodePayload 必须报错：
// 避免业务方把「没有载荷」误当成零值结构继续执行业务逻辑。
func TestDecodePayloadEmpty(t *testing.T) {
	env := &Envelope{BizID: "x"}
	var v MailMsg
	err := env.DecodePayload(&v)
	require.Error(t, err, "empty payload should fail")
}

// TestIdemKey 锁定幂等键格式 idem:{topic}:{biz_id}：格式变化意味着
// 存量 Redis 标记全部失效（线上表现为重复放行），故用精确断言而非包含匹配。
func TestIdemKey(t *testing.T) {
	got := IdemKey(TopicRechargeOrder, "O123")
	want := "idem:gs.recharge.order:O123"
	require.EqualValues(t, want, got)
}

// TestNoopDeduperAlwaysFirst NoopDeduper 必须恒真：本地/单测场景不去重，
// 若误返回 false 会静默丢掉本该执行的消息，且没有任何报错线索。
func TestNoopDeduperAlwaysFirst(t *testing.T) {
	var d Deduper = NoopDeduper{}
	for i := 0; i < 3; i++ {
		ok, err := d.FirstTime(context.Background(), TopicMail, "same", time.Minute)
		require.NoError(t, err, "noop deduper should always return true")
		require.True(t, ok)
	}
}

// TestAllTopicsUnique AllTopics 供本地一键建 topic：出现空串或重复会让
// 建表脚本半途失败，这里只锁下界与唯一性，条数不写死。
func TestAllTopicsUnique(t *testing.T) {
	topics := AllTopics()
	// 断言唯一性与最小规模，不写死条数：新增 topic 不需要改测试。
	require.False(t, len(topics) < 9, fmt.Sprintf("expect >= 9 topics, got %d", len(topics)))
	seen := make(map[string]bool, len(topics))
	for _, tp := range topics {
		require.NotEqualValues(t, "", tp, "empty topic")
		require.False(t, seen[tp], fmt.Sprintf("duplicate topic %s", tp))
		seen[tp] = true
	}
}
