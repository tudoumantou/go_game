// internal/login/handler/auth.go —— 认证相关 HTTP handler
package handler

import (
	"encoding/json"
	"net/http"

	"game_study/internal/login/service"
)

// AuthHandler 认证 handler。
type AuthHandler struct {
	auth *service.AuthService
}

// NewAuthHandler 创建 AuthHandler。
func NewAuthHandler(auth *service.AuthService) *AuthHandler {
	return &AuthHandler{auth: auth}
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
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, GuestLoginResp{Token: token, AccountID: accountID, PlayerID: accountID})
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
