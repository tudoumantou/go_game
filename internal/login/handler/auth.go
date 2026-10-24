// internal/login/handler/auth.go —— 认证相关 HTTP handler
//
// 架构职责：登录服对外的 HTTP 门面（薄层）——解析请求、调用
// service.AuthService、把结果序列化为 JSON，并旁路记录登录流水。
// 协作关系：service.AuthService 提供 GuestLogin/ValidateToken；
// LoginLogger 接口（由 login 包的 loginAudit 实现）旁路审计；
// 客户端拿到 token 后去网关握手（网关再经 Redis 校验同一 token）。
// 设计取舍：① handler 只做协议转换，业务规则全在 service——单测可以
// 不起 HTTP 直接测 service；② 审计失败绝不影响登录结果（R3.2），
// 因此 record 无返回错误；③ DeviceID 缺省填 anonymous，让无设备号
// 的客户端也能走游客登录，而不是 400 拒绝。
package handler

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"

	"game_study/internal/login/service"
)

// LoginEvent 一次登录尝试（流水事实，R3.2）。
// 知识点：纯数据载体（DTO）不含行为，字段导出供 JSON/日志使用；
// Result 用 int32 而非 error——审计要记录的是「事实」而不是错误对象，
// 字符串化后的 Reason 更适合跨服务消费。
type LoginEvent struct {
	AccountID int64
	PlayerID  int64 // 未创角为 0
	IP        string
	Device    string
	Result    int32 // 0 成功 1 失败
	Reason    string
}

// LoginLogger 登录流水记录器；未注入时不记录。
// 接口（而非具体类型）是为了让 audit 实现可替换/可空——单测传 nil
// 即关闭审计，这是「可选依赖用接口 + nil 判空」的标准形态。
type LoginLogger interface {
	Log(ctx context.Context, ev LoginEvent)
}

// AuthHandler 认证 handler。
// 依赖：auth 是必需业务服务；audit 是可选旁路（NewAuthHandler 可变参数）。
type AuthHandler struct {
	auth  *service.AuthService
	audit LoginLogger
}

// NewAuthHandler 创建 AuthHandler（audit 可选）。
// 知识点：可变参数 audit ...LoginLogger 实现「可选注入」——不传即
// nil，传多个只取第一个；比「传 interface 再判 nil」调用处更干净。
func NewAuthHandler(auth *service.AuthService, audit ...LoginLogger) *AuthHandler {
	h := &AuthHandler{auth: auth}
	if len(audit) > 0 {
		h.audit = audit[0]
	}
	return h
}

// GuestLoginReq 游客登录请求。
// 知识点：json tag 显式声明 snake_case——Go 字段是 PascalCase，
// 不声明时 encoding/json 按字段名原样匹配（大小写不敏感），跨语言
// 客户端容易踩坑；tag 让协议契约与语言习惯解耦。
type GuestLoginReq struct {
	DeviceID string `json:"device_id"`
}

// GuestLoginResp 游客登录响应。
// account_id 为账号 ID（一个账号可有多角色）；player_id 保留兼容旧客户端（同值）。
// 知识点：AccountID/PlayerID 用 int64——游戏内 ID 普遍超 int32 上限，
// 早期用 int32 的项目都做过一次痛苦的数据迁移；这里一次到位。
type GuestLoginResp struct {
	Token     string `json:"token"`
	AccountID int64  `json:"account_id"`
	PlayerID  int64  `json:"player_id"`
}

// GuestLogin POST /v1/auth/guest
// 流程：方法校验 → 解 JSON → 设备号兜底 → 调 service → 旁路流水 → 回包。
// 失败时 accountID 可能非 0（service 找到了账号但后续失败），
// 一并写进流水便于定位失败发生在哪一步。
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
		// 账号不可用是「可预期的拒绝」，用 403 + 业务码返回：
		// 客户端据此弹「账号已被封禁」，而不是当成服务器故障去重试。
		switch {
		case errors.Is(err, service.ErrAccountFrozen):
			writeJSONStatus(w, http.StatusForbidden, map[string]any{
				"code": 2001, "msg": "账号已被封禁",
			})
			return
		case errors.Is(err, service.ErrAccountDeleted):
			writeJSONStatus(w, http.StatusForbidden, map[string]any{
				"code": 2001, "msg": "账号已注销",
			})
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	// 成功事件 PlayerID 同填 accountID：游客登录时尚无角色概念，
	// 旧客户端读 player_id 字段不报错（兼容层）
	h.record(r, LoginEvent{AccountID: accountID, PlayerID: accountID, Device: req.DeviceID, Result: 0})
	writeJSON(w, GuestLoginResp{Token: token, AccountID: accountID, PlayerID: accountID})
}

// record 记录登录流水；日志失败不能影响登录结果。
// IP 在这里补取（而非 service 层）：r.RemoteAddr 是 HTTP 层信息，
// service 不该依赖 *http.Request——分层依赖方向保持 handler→service。
func (h *AuthHandler) record(r *http.Request, ev LoginEvent) {
	if h.audit == nil {
		return
	}
	ev.IP = clientIP(r)
	h.audit.Log(r.Context(), ev)
}

// clientIP 取直连地址；网关/反代场景可再按 X-Forwarded-For 扩展。
// 知识点：net.SplitHostPort 处理 "ip:port" 形式，失败时回退整个
// RemoteAddr——防御式解析，宁可多带信息也不丢 IP。
func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// writeJSON 统一 JSON 响应出口。
// 先写 Content-Type 再 Encode：头必须在 body 之前落；
// Encode 错误被忽略——响应已经开始写入，无法再改状态码，
// 由 server 的 withMetrics 中间件记录 5xx。
func writeJSON(w http.ResponseWriter, v any) {
	writeJSONStatus(w, http.StatusOK, v)
}

// writeJSONStatus 带状态码的 JSON 响应（错误分支用）。
func writeJSONStatus(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
