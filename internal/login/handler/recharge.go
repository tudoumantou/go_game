// internal/login/handler/recharge.go —— 充值下单 / 支付回调 HTTP handler
package handler

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"game_study/internal/login/repo"
	"game_study/internal/login/service"
	"game_study/pkg/log"
)

// RechargeHandler 充值 handler。
type RechargeHandler struct {
	svc    *service.RechargeService
	auth   *service.AuthService
	roles  *repo.RoleRepo
	pay    service.PayChannel
	logger *log.Logger
}

// NewRechargeHandler 创建 handler。
func NewRechargeHandler(svc *service.RechargeService, auth *service.AuthService, roles *repo.RoleRepo, pay service.PayChannel, logger *log.Logger) *RechargeHandler {
	return &RechargeHandler{svc: svc, auth: auth, roles: roles, pay: pay, logger: logger}
}

// CreateOrderReq 下单请求：只有商品与渠道，金额由服务端定价。
type CreateOrderReq struct {
	ProductID int32 `json:"product_id"`
	Channel   int32 `json:"channel"`
}

// CreateOrderResp 下单响应。
type CreateOrderResp struct {
	Code      int32  `json:"code"`
	OrderID   string `json:"order_id"`
	AmountFen int32  `json:"amount_fen"`
}

// CreateOrder POST /v1/order/create
//
// 归属由登录态决定：Bearer token → account → 角色。客户端不能指定 player_id，
// 也不能指定金额（否则 98 元的档位可以报成 1 分）。
func (h *RechargeHandler) CreateOrder(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	token := bearerToken(r)
	if token == "" {
		http.Error(w, "missing bearer token", http.StatusUnauthorized)
		return
	}
	accountID, err := h.auth.ValidateToken(r.Context(), token)
	if err != nil {
		http.Error(w, "invalid token", http.StatusUnauthorized)
		return
	}
	playerID, err := h.roles.FindByAccount(r.Context(), accountID)
	if err != nil {
		if errors.Is(err, repo.ErrNoRole) {
			http.Error(w, "account has no role", http.StatusForbidden)
			return
		}
		h.logger.Error("resolve role failed", "account_id", accountID, "err", err)
		http.Error(w, "resolve role failed", http.StatusInternalServerError)
		return
	}

	var req CreateOrderReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	orderID, amountFen, err := h.svc.CreateOrder(r.Context(), playerID, req.ProductID, req.Channel)
	if err != nil {
		h.logger.Error("create order failed", "player_id", playerID, "product_id", req.ProductID, "err", err)
		http.Error(w, "create order failed", http.StatusBadRequest)
		return
	}
	writeJSON(w, CreateOrderResp{Code: 0, OrderID: orderID, AmountFen: amountFen})
}

// PayCallbackReq 渠道支付回调（字段由渠道适配器验签）。
type PayCallbackReq struct {
	OrderID   string `json:"order_id"`
	AmountFen int32  `json:"amount_fen"`
	Timestamp int64  `json:"timestamp"`
	Nonce     string `json:"nonce"`
	Sign      string `json:"sign"`
}

// PayCallbackResp 支付回调响应。
type PayCallbackResp struct {
	Code int32 `json:"code"`
}

// PayCallback POST /v1/pay/callback
//
// 顺序不能换：先验签（渠道事实可信）→ 再校验金额（防改价）→ 最后推进订单状态。
// 任何一步失败都返回非 0，渠道会重试；重复回调由订单状态机幂等吸收。
func (h *RechargeHandler) PayCallback(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req PayCallbackReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if req.OrderID == "" {
		http.Error(w, "order_id required", http.StatusBadRequest)
		return
	}

	if err := h.pay.Verify(service.CallbackPayload{
		OrderID:   req.OrderID,
		AmountFen: req.AmountFen,
		Timestamp: req.Timestamp,
		Nonce:     req.Nonce,
		Sign:      req.Sign,
	}); err != nil {
		h.logger.Warn("pay callback rejected", "order_id", req.OrderID, "err", err)
		writeJSON(w, PayCallbackResp{Code: 5005}) // 验签失败
		return
	}

	res, err := h.svc.PayCallback(r.Context(), req.OrderID, req.AmountFen)
	if err != nil {
		if service.IsNotFound(err) {
			writeJSON(w, PayCallbackResp{Code: 5004}) // 订单不存在
			return
		}
		h.logger.Error("pay callback failed", "order_id", req.OrderID, "err", err)
		writeJSON(w, PayCallbackResp{Code: 5006}) // 金额不匹配/状态非法
		return
	}
	writeJSON(w, PayCallbackResp{Code: res.Code})
}

// bearerToken 从 Authorization 头取 token。
func bearerToken(r *http.Request) string {
	h := r.Header.Get("Authorization")
	if h == "" {
		return ""
	}
	const prefix = "Bearer "
	if !strings.HasPrefix(h, prefix) {
		return ""
	}
	return strings.TrimSpace(h[len(prefix):])
}
