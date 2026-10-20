// internal/login/service/pay_channel.go —— 支付渠道适配器（R3.3）
//
// 架构职责：支付回调的「信任边界」——校验渠道签名、时间窗，回答
// 「这条已支付通知是不是渠道真发来的」。验签通过的 PayCallbackReq
// 才允许进入订单状态机（handler.PayCallback 的顺序保证）。
// 协作关系：handler.RechargeHandler 持有 PayChannel 接口并在
// PayCallback 第一步调用 Verify；server.go 的 NewPayChannel 按配置
// 选择实现（sandbox/disabled），未登记渠道名启动即报错。
// 设计取舍：① 接口抽象渠道——真实渠道（微信/支付宝/App Store）
// 接入时各写一个 Verify 实现，handler 与状态机零改动；② 时间窗
// 防重放：签名里含 timestamp，窗外一律拒，堵住「截获旧回调
// 反复重放」；同订单的重复回调由订单状态机幂等吸收（职责分离：
// 渠道管「这条消息真不真」，状态机管「这个事实处理没处理过」）；
// ③ disabledChannel 兜底——未配密钥时服务能起（开发/联调环境），
// 但所有回调验签失败，守住「无密钥 = 无放行」的安全底线。
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
// 与渠道报文一一对应：验签要覆盖全部字段，字段集即签名集——
// 加字段必须同步加进签名串，否则新字段可被篡改（经典漏洞）。
type CallbackPayload struct {
	OrderID   string
	AmountFen int32
	Timestamp int64  // unix 秒
	Nonce     string // 渠道侧唯一串，用于日志与人工排查
	Sign      string
}

// PayChannel 支付渠道适配器。
// 接口即边界：handler 只依赖 Name/Verify 两个方法，新增渠道不改
// 调用方；单测可注入总是成功/失败的 fake 渠道驱动状态机各分支。
type PayChannel interface {
	Name() string
	// Verify 校验签名与时间窗；失败返回可直接判断的错误。
	// 契约：返回 nil 表示「这条回调可信」，调用方（handler）随即
	// 推进订单状态——Verify 是放行前的最后一道关。
	Verify(p CallbackPayload) error
}

// PayChannelError 验签类错误（回调方应拒绝）。
// 知识点：自定义错误类型 + Reason 字段——调用方可用 errors.As
// 提取结构化原因（如按 Reason 分渠道错误码），比字符串匹配稳健。
type PayChannelError struct{ Reason string }

func (e *PayChannelError) Error() string { return "pay channel: " + e.Reason }

// SandboxChannel HMAC-SHA256 模拟渠道。
//
// 签名串：order_id|amount_fen|timestamp|nonce
// 时间窗防止重放；同一订单重复回调由订单状态机幂等吸收。
// 用途：真实渠道接入前把「验签 → 金额匹配 → 状态推进 → 发货」全
// 链路跑通；联调时用 SignSandbox 生成签名，与生产同构。
type SandboxChannel struct {
	secret []byte
	window time.Duration
}

// NewSandboxChannel 创建沙箱渠道；secret 为空直接报错，不允许「无密钥验签」。
// window<=0 回落 5min：未配置时间窗时给安全默认值，而不是
// 「不过期」——配置缺失向安全侧兜底。
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
// 检查顺序即信任顺序：字段完整 → 时间窗 → 签名。
// 时间窗前后都查（now-at 与 at-now）：既挡旧重放，也挡「来自
// 未来」的畸形时间戳（时钟未同步的渠道/攻击载荷）。
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
	// 知识点：hmac.Equal 做常量时间比较——普通 == 短路比较会因
	// 首个不同字节提前返回，时序差异可被用来逐字节爆破签名；
	// 安全场景（签名/token/密码）比较必须用常量时间函数。
	// ToLower 是宽容渠道大小写习惯（hex 签名大小写不敏感）。
	want := signPayload(c.secret, p)
	if !hmac.Equal([]byte(want), []byte(strings.ToLower(p.Sign))) {
		return &PayChannelError{Reason: "signature mismatch"}
	}
	return nil
}

// signPayload 计算签名（导出给测试与联调工具复用）。
// 拼接顺序固定为 order_id|amount|timestamp|nonce：渠道与沙箱必须
// 用同一顺序——改顺序等于换签名算法，历史回调全部验不过。
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
// 与 Verify 共用 signPayload：保证「生成的签名一定能验过」——
// 联调工具与服务端不会因两套实现漂移而互相扯皮。
func SignSandbox(secret string, p CallbackPayload) string {
	return signPayload([]byte(secret), p)
}

// disabledChannel 未配置渠道密钥时的占位实现：服务能正常启动，但所有回调验签失败。
//
// 这比「启动直接失败」更适合开发环境（登录、游戏链路不依赖支付），
// 同时保证「无密钥 = 无回调放行」这条安全底线。
// 知识点：空结构体值即单例——disabledChannel{} 无字段，零内存，
// 可直接值传（值方法集满足接口）；比指针单例少一次分配与判空。
type disabledChannel struct{}

func (disabledChannel) Name() string { return "disabled" }

func (disabledChannel) Verify(CallbackPayload) error {
	return &PayChannelError{Reason: "pay channel not configured"}
}

// NewPayChannel 按配置构造渠道。
// 白名单登记制：default 分支对未知渠道名直接报错——把「配置写错
// 渠道名」从运行期静默失败（回调全拒但没人知道为什么）提前到
// 启动期硬失败；空名按 sandbox 处理（本地默认）。
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
