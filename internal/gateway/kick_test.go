// internal/gateway/kick_test.go —— GM 踢人链路（网关侧）
//
// 这条链路的价值在于「封禁能真的把人踢下线」，而不是等他自己掉线。
// 三层要分别验证：帧内容正确（协议层）、鉴权生效（安全）、
// 无后端流时明确报错（不静默成功）。
package gateway

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"game_study/internal/gateway/backend"
	"game_study/pkg/config"
	"game_study/pkg/log"
	internalv2 "game_study/pkg/pb/internalv2"
	"github.com/stretchr/testify/require"
)

func newTestServer(adminToken string) *Server {
	return &Server{
		cfg:    &config.Config{Admin: config.AdminConfig{Token: adminToken}},
		logger: log.New("gw-test", log.WithLevel("error")),
	}
}

// TestKickFrameCarriesPlayerID 帧内容：类型必须是 KICK、角色号不能丢。
//
// 类型写错（比如退化成默认值 UNBIND）时游戏服会把它当成解绑处理——
// 玩家掉线但收不到 KickNotify，客户端表现为「莫名断线」；
// player_id 漏填则游戏服不知道踢谁（它不认会话号）。
func TestKickFrameCarriesPlayerID(t *testing.T) {
	frame := kickFrame(42, "banned")

	ctrl := frame.GetCtrl()
	require.NotNil(t, ctrl)
	require.Equal(t, internalv2.CtrlMsg_KICK, ctrl.GetType(), "必须是 KICK，不能是 UNBIND")
	require.EqualValues(t, 42, ctrl.GetPlayerId(), "游戏服按角色号定位会话")
	require.EqualValues(t, 0, ctrl.GetSessionId(), "这条指令不针对某个会话，会话号必须留空")
	require.Equal(t, "banned", string(ctrl.GetPayload()), "原因要传给客户端显示")
}

// TestKickPlayerWithoutBackend 无后端连接池时报错，不能假装成功。
func TestKickPlayerWithoutBackend(t *testing.T) {
	s := newTestServer("")
	err := s.KickPlayer(42, "test")
	require.Error(t, err, "没有游戏服流时必须报错，否则运维以为踢成功了")
}

// TestAdminKickRequiresLoopbackWhenNoToken 未配 token 时只允许本机来源。
func TestAdminKickRequiresLoopbackWhenNoToken(t *testing.T) {
	s := newTestServer("")

	cases := []struct {
		remote string
		allow  bool
	}{
		{"127.0.0.1:12345", true},
		{"[::1]:12345", true},
		{"192.0.2.5:12345", false},
		{"203.0.113.9:443", false},
	}
	for _, c := range cases {
		r := httptest.NewRequest(http.MethodPost, "/admin/kick", strings.NewReader(`{"session_id":1}`))
		r.RemoteAddr = c.remote
		require.Equal(t, c.allow, s.adminAllowed(r), "remote=%s", c.remote)
	}
}

// TestAdminKickToken 配了 token 后必须带 Bearer；本机也不例外。
func TestAdminKickToken(t *testing.T) {
	s := newTestServer("s3cret")

	cases := []struct {
		auth  string
		allow bool
	}{
		{"Bearer s3cret", true},
		{"Bearer wrong", false},
		{"", false},
		{"s3cret", false}, // 少了 Bearer 前缀
	}
	for _, c := range cases {
		r := httptest.NewRequest(http.MethodPost, "/admin/kick", strings.NewReader(`{}`))
		r.RemoteAddr = "127.0.0.1:1"
		if c.auth != "" {
			r.Header.Set("Authorization", c.auth)
		}
		require.Equal(t, c.allow, s.adminAllowed(r), "auth=%q", c.auth)
	}
}

// TestAdminKickHTTP 端到端校验：方法、鉴权、参数、后端不可用四种结果。
func TestAdminKickHTTP(t *testing.T) {
	s := newTestServer("")
	h := s.httpHandler()

	// GET 不允许
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/admin/kick", nil)
	req.RemoteAddr = "127.0.0.1:1"
	h.ServeHTTP(w, req)
	require.Equal(t, http.StatusMethodNotAllowed, w.Code)

	// 非本机来源被拒
	w = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/admin/kick", strings.NewReader(`{"session_id":1}`))
	req.RemoteAddr = "192.0.2.9:1"
	h.ServeHTTP(w, req)
	require.Equal(t, http.StatusForbidden, w.Code)

	// 缺 player_id
	w = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/admin/kick", strings.NewReader(`{}`))
	req.RemoteAddr = "127.0.0.1:1"
	h.ServeHTTP(w, req)
	require.Equal(t, http.StatusBadRequest, w.Code)

	// 本机 + 合法参数：无后端流 → 502（区别「请求不合法」与「发不出去」）
	w = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/admin/kick", strings.NewReader(`{"player_id":7,"reason":"gm"}`))
	req.RemoteAddr = "127.0.0.1:1"
	h.ServeHTTP(w, req)
	require.Equal(t, http.StatusBadGateway, w.Code)
}

// TestKickPlayerWithoutRegistry 有连接池但拿不到节点列表时报错。
//
// 静默成功是最坏的结果：运营以为封禁生效了，玩家其实还在线上。
func TestKickPlayerWithoutRegistry(t *testing.T) {
	s := newTestServer("")
	s.backend = backend.NewPool(nil, s.logger, nil)

	err := s.KickPlayer(42, "banned")
	require.Error(t, err)
	require.Contains(t, err.Error(), "registry")
}
