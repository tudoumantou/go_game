// internal/login/handler/auth.go —— 认证相关 HTTP handler
package handler

import (
	"context"
	"encoding/json"
	"net"
	"net/http"

	"game_study/internal/login/service"
)

// LoginEvent 一次登录尝试（流水事实，R3.2）。
type LoginEvent struct {
	AccountID int64
	PlayerID  int64 // 未创角为 0
	IP        string
	Device    string
	Result    int32 // 0 成功 1 失败
	Reason    string
}

// LoginLogger 登录流水记录器；未注入时不记录。
type LoginLogger interface {
	Log(ctx context.Context, ev LoginEvent)
}

// AuthHandler 认证 handler。
type AuthHandler struct {
	auth  *service.AuthService
	audit LoginLogger
}

// NewAuthHandler 创建 AuthHandler（audit 可选）。
func NewAuthHandler(auth *service.AuthService, audit ...LoginLogger) *AuthHandler {
	h := &AuthHandler{auth: auth}
	if len(audit) > 0 {
		h.audit = audit[0]
	}
	return h
}

// GuestLoginReq 游客登录请求。
type GuestLoginReq struct {
	DeviceID string `json:"device_id"`
}

// GuestLoginResp 游客登录响应。
// account_id 为账号 ID（一个账号可有多角色）；player_id 保留兼容旧客户端（同值）。
type GuestLoginResp struct {
	Token     string `json:"token"`
	AccountID int64  `json:"account_id"`
	PlayerID  int64  `json:"player_id"`
}

// GuestLogin POST /v1/auth/guest
func (h *AuthHandler) GuestLogin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req GuestLoginReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if req.DeviceID == "" {
		req.DeviceID = "anonymous"
	}
	token, accountID, err := h.auth.GuestLogin(r.Context(), req.DeviceID)
	if err != nil {
		h.record(r, LoginEvent{AccountID: accountID, Device: req.DeviceID, Result: 1, Reason: err.Error()})
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	h.record(r, LoginEvent{AccountID: accountID, PlayerID: accountID, Device: req.DeviceID, Result: 0})
	writeJSON(w, GuestLoginResp{Token: token, AccountID: accountID, PlayerID: accountID})
}

// record 记录登录流水；日志失败不能影响登录结果。
func (h *AuthHandler) record(r *http.Request, ev LoginEvent) {
	if h.audit == nil {
		return
	}
	ev.IP = clientIP(r)
	h.audit.Log(r.Context(), ev)
}

// clientIP 取直连地址；网关/反代场景可再按 X-Forwarded-For 扩展。
func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
