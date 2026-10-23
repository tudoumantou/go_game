// pay_channel_test.go —— R3.3 回调验签与重放保护
package service

import (
	"errors"
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

const testSecret = "unit-test-secret"

func signedPayload(orderID string, amount int32) CallbackPayload {
	p := CallbackPayload{
		OrderID:   orderID,
		AmountFen: amount,
		Timestamp: time.Now().Unix(),
		Nonce:     "nonce-1",
	}
	p.Sign = SignSandbox(testSecret, p)
	return p
}

// TestSandboxChannelAcceptsValidSignature 合法签名通过。
func TestSandboxChannelAcceptsValidSignature(t *testing.T) {
	c, err := NewSandboxChannel(testSecret, time.Minute)
	require.NoError(t, err)
	err = c.Verify(signedPayload("O1", 600))
	require.NoError(t, err, fmt.Sprintf("valid signature rejected: %v", err))
}

// TestSandboxChannelRejectsTamperedAmount 改金额后签名失效——这是防「改价」的关键。
func TestSandboxChannelRejectsTamperedAmount(t *testing.T) {
	c, _ := NewSandboxChannel(testSecret, time.Minute)
	p := signedPayload("O1", 600)
	p.AmountFen = 1
	err := c.Verify(p)
	require.Error(t, err, "tampered amount must be rejected")
}

// TestSandboxChannelRejectsTamperedOrder 改订单号后签名失效。
func TestSandboxChannelRejectsTamperedOrder(t *testing.T) {
	c, _ := NewSandboxChannel(testSecret, time.Minute)
	p := signedPayload("O1", 600)
	p.OrderID = "O2"
	err := c.Verify(p)
	require.Error(t, err, "tampered order id must be rejected")
}

// TestSandboxChannelRejectsExpiredTimestamp 过期回调（重放）拒绝。
func TestSandboxChannelRejectsExpiredTimestamp(t *testing.T) {
	c, _ := NewSandboxChannel(testSecret, time.Minute)
	p := CallbackPayload{OrderID: "O1", AmountFen: 600, Timestamp: time.Now().Add(-10 * time.Minute).Unix(), Nonce: "n"}
	p.Sign = SignSandbox(testSecret, p)
	err := c.Verify(p)
	require.Error(t, err, "expired timestamp must be rejected")
}

// TestSandboxChannelRejectsFutureTimestamp 时钟超前同样拒绝，避免绕过时间窗。
func TestSandboxChannelRejectsFutureTimestamp(t *testing.T) {
	c, _ := NewSandboxChannel(testSecret, time.Minute)
	p := CallbackPayload{OrderID: "O1", AmountFen: 600, Timestamp: time.Now().Add(10 * time.Minute).Unix(), Nonce: "n"}
	p.Sign = SignSandbox(testSecret, p)
	err := c.Verify(p)
	require.Error(t, err, "future timestamp must be rejected")
}

// TestSandboxChannelRejectsMissingSign 缺签名字段。
func TestSandboxChannelRejectsMissingSign(t *testing.T) {
	c, _ := NewSandboxChannel(testSecret, time.Minute)
	err := c.Verify(CallbackPayload{OrderID: "O1", AmountFen: 600, Timestamp: time.Now().Unix()})
	require.Error(t, err, "missing sign must be rejected")
}

// TestSandboxChannelRequiresSecret 空密钥不允许构造。
func TestSandboxChannelRequiresSecret(t *testing.T) {
	_, err := NewSandboxChannel("", time.Minute)
	require.Error(t, err, "empty secret must be rejected")
}

// TestNewPayChannelDisabledWithoutSecret 未配置密钥时降级为「拒绝一切回调」，
// 服务仍可启动，但绝不会放行伪造回调。
func TestNewPayChannelDisabledWithoutSecret(t *testing.T) {
	c, err := NewPayChannel("sandbox", "", time.Minute)
	require.NoError(t, err, "disabled channel should not fail startup")
	require.EqualValues(t, "disabled", c.Name())
	err = c.Verify(signedPayload("O1", 600))
	require.Error(t, err, "disabled channel must reject everything")
}

// TestNewPayChannelRejectsUnknownName 未登记的渠道名直接报错，避免配置写错静默放行。
func TestNewPayChannelRejectsUnknownName(t *testing.T) {
	_, err := NewPayChannel("alipay", "x", time.Minute)
	require.Error(t, err, "unknown channel must be rejected")
}

// TestPayChannelErrorIsTyped 验签失败返回可识别的类型，便于 handler 映射错误码。
func TestPayChannelErrorIsTyped(t *testing.T) {
	c, _ := NewSandboxChannel(testSecret, time.Minute)
	err := c.Verify(CallbackPayload{OrderID: "O1", AmountFen: 600, Timestamp: time.Now().Unix(), Sign: "deadbeef"})
	var pe *PayChannelError
	require.False(t, !errors.As(err, &pe), fmt.Sprintf("expected PayChannelError, got %T", err))
	require.Contains(t, pe.Error(), "signature", fmt.Sprintf("unexpected reason: %v", pe))
}
