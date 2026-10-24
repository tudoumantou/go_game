// internal/login/handler/limit_test.go —— 登录频控
//
// 频控放在 handler 而不是 service：爆破发生在拿到 token 之前，
// 而 service 只负责「校验 + 签发」，不该关心来源 IP。
package handler

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"game_study/pkg/ratelimit"

	"github.com/stretchr/testify/require"
)

// clock 可推进的测试时钟。
type clock struct{ t time.Time }

func (c *clock) Now() time.Time          { return c.t }
func (c *clock) Advance(d time.Duration) { c.t = c.t.Add(d) }

// TestGuestLoginRateLimited 额度耗尽后回 429 + 1004，且带 Retry-After。
//
// 断言在 auth 之前返回：h.auth 为 nil，若真走到业务层会 panic——
// 这本身就是「频控必须前置」的证明（也省掉造 AuthService 的桩）。
func TestGuestLoginRateLimited(t *testing.T) {
	h := NewAuthHandler(nil) // auth 为 nil：任何走到业务层的路径都会 panic

	l := ratelimit.NewKeyed(1, 1, time.Minute)
	now := &clock{t: time.Unix(1700000000, 0)}
	l.SetClock(now.Now)
	h.SetLoginLimiter(l)

	// 先把该 IP 的唯一令牌用掉
	require.True(t, l.Allow("1.2.3.4"))

	req := httptest.NewRequest(http.MethodPost, "/v1/auth/guest", strings.NewReader(`{"device_id":"d1"}`))
	req.RemoteAddr = "1.2.3.4:5555"
	w := httptest.NewRecorder()

	require.NotPanics(t, func() { h.GuestLogin(w, req) })
	require.Equal(t, http.StatusTooManyRequests, w.Code)
	require.Equal(t, "60", w.Header().Get("Retry-After"))
	require.Contains(t, w.Body.String(), "1004")
}

// TestClientIP 从 RemoteAddr 取 IP；解析不了就原样返回（不 panic）。
//
// 不限流时这个函数也会被调用，必须对任何输入都有确定行为。
func TestClientIP(t *testing.T) {
	cases := []struct{ remote, want string }{
		{"1.2.3.4:5678", "1.2.3.4"},
		{"[::1]:5678", "::1"},
		{"no-port", "no-port"},
	}
	for _, c := range cases {
		r := httptest.NewRequest(http.MethodPost, "/", nil)
		r.RemoteAddr = c.remote
		require.Equal(t, c.want, clientIP(r), "remote=%s", c.remote)
	}
}

// TestGuestLoginRateLimitDisabled limiter 为 nil 时不拦截（配置关闭路径）。
//
// 这里只验证「不会因为 nil 而 panic」：真正的业务路径需要 AuthService，
// 由集成测试覆盖。
func TestGuestLoginRateLimitDisabled(t *testing.T) {
	h := NewAuthHandler(nil)
	h.SetLoginLimiter(nil)

	req := httptest.NewRequest(http.MethodPost, "/v1/auth/guest", strings.NewReader(`{`))
	req.RemoteAddr = "1.2.3.4:5555"
	w := httptest.NewRecorder()

	// 非法 JSON 会先返回 400——说明确实跳过了频控、走到了解析
	require.NotPanics(t, func() { h.GuestLogin(w, req) })
	require.Equal(t, http.StatusBadRequest, w.Code)
}
