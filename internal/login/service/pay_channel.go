// internal/login/service/pay_channel.go —— 支付渠道适配器（R3.3）
//
// 真实渠道未定，这里只提供沙箱渠道：用 HMAC-SHA256 模拟「渠道签名 + 时间窗」，
// 把验签、金额匹配、重放保护的代码路径先跑通。接入真实渠道时新增一个适配器实现
// PayChannel，并在 NewPayChannel 里登记名称；未登记的名称启动即报错，避免
// 「配置写了个不存在的渠道，回调直接放行」。
package service

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// CallbackPayload 渠道回调的原始字段。
type CallbackPayload struct {
	OrderID   string
	AmountFen int32
	Timestamp int64  // unix 秒
	Nonce     string // 渠道侧唯一串，用于日志与人工排查
	Sign      string
}

// PayChannel 支付渠道适配器。
type PayChannel interface {
	Name() string
	// Verify 校验签名与时间窗；失败返回可直接判断的错误。
	Verify(p CallbackPayload) error
}

// PayChannelError 验签类错误（回调方应拒绝）。
type PayChannelError struct{ Reason string }

func (e *PayChannelError) Error() string { return "pay channel: " + e.Reason }

// SandboxChannel HMAC-SHA256 模拟渠道。
//
// 签名串：order_id|amount_fen|timestamp|nonce
// 时间窗防止重放；同一订单重复回调由订单状态机幂等吸收。
type SandboxChannel struct {
	secret []byte
	window time.Duration
}

// NewSandboxChannel 创建沙箱渠道；secret 为空直接报错，不允许「无密钥验签」。
func NewSandboxChannel(secret string, window time.Duration) (*SandboxChannel, error) {
	if secret == "" {
		return nil, fmt.Errorf("sandbox pay channel requires non-empty secret")
	}
	if window <= 0 {
		window = 5 * time.Minute
	}
	return &SandboxChannel{secret: []byte(secret), window: window}, nil
}

// Name 实现 PayChannel。
func (c *SandboxChannel) Name() string { return "sandbox" }

// Verify 实现 PayChannel。
func (c *SandboxChannel) Verify(p CallbackPayload) error {
	if p.OrderID == "" {
		return &PayChannelError{Reason: "missing order_id"}
	}
	if p.AmountFen <= 0 {
		return &PayChannelError{Reason: "invalid amount_fen"}
	}
	if p.Sign == "" {
		return &PayChannelError{Reason: "missing sign"}
	}
	now := time.Now()
	at := time.Unix(p.Timestamp, 0)
	if p.Timestamp <= 0 || now.Sub(at) > c.window || at.Sub(now) > c.window {
		return &PayChannelError{Reason: fmt.Sprintf("timestamp out of window: %d", p.Timestamp)}
	}
	want := signPayload(c.secret, p)
	if !hmac.Equal([]byte(want), []byte(strings.ToLower(p.Sign))) {
		return &PayChannelError{Reason: "signature mismatch"}
	}
	return nil
}

// signPayload 计算签名（导出给测试与联调工具复用）。
func signPayload(secret []byte, p CallbackPayload) string {
	raw := strings.Join([]string{
		p.OrderID,
		strconv.FormatInt(int64(p.AmountFen), 10),
		strconv.FormatInt(p.Timestamp, 10),
		p.Nonce,
	}, "|")
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(raw))
	return hex.EncodeToString(mac.Sum(nil))
}

// SignSandbox 供联调/测试生成签名。
func SignSandbox(secret string, p CallbackPayload) string {
	return signPayload([]byte(secret), p)
}

// disabledChannel 未配置渠道密钥时的占位实现：服务能正常启动，但所有回调验签失败。
//
// 这比「启动直接失败」更适合开发环境（登录、游戏链路不依赖支付），
// 同时保证「无密钥 = 无回调放行」这条安全底线。
type disabledChannel struct{}

func (disabledChannel) Name() string { return "disabled" }

func (disabledChannel) Verify(CallbackPayload) error {
	return &PayChannelError{Reason: "pay channel not configured"}
}

// NewPayChannel 按配置构造渠道。
func NewPayChannel(name, secret string, window time.Duration) (PayChannel, error) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "", "sandbox":
		if secret == "" {
			return disabledChannel{}, nil
		}
		return NewSandboxChannel(secret, window)
	default:
		return nil, fmt.Errorf("unsupported pay channel %q", name)
	}
}
