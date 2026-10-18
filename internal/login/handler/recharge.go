// internal/login/handler/recharge.go —— 充值下单 / 支付回调 HTTP handler
package handler

import (
	"encoding/json"
	"net/http"

	"game_study/internal/login/service"
	"game_study/pkg/log"
)

// RechargeHandler 充值 handler。
type RechargeHandler struct {
	svc    *service.RechargeService
	logger *log.Logger
}

// NewRechargeHandler 创建 handler。
func NewRechargeHandler(svc *service.RechargeService, logger *log.Logger) *RechargeHandler {
	return &RechargeHandler{svc: svc, logger: logger}
}

// CreateOrderReq 下单请求。
type CreateOrderReq struct {
	PlayerID  int64 `json:"player_id"`
	ProductID int32 `json:"product_id"`
	AmountFen int32 `json:"amount_fen"`
	Channel   int32 `json:"channel"`
}

// CreateOrderResp 下单响应。
type CreateOrderResp struct {
	Code    int32  `json:"code"`
	OrderID string `json:"order_id"`
}

// CreateOrder POST /v1/order/create
func (h *RechargeHandler) CreateOrder(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req CreateOrderReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if req.PlayerID <= 0 || req.AmountFen <= 0 {
		http.Error(w, "invalid player_id or amount_fen", http.StatusBadRequest)
		return
	}
	orderID, err := h.svc.CreateOrder(r.Context(), req.PlayerID, req.ProductID, req.AmountFen, req.Channel)
	if err != nil {
		h.logger.Error("create order failed", "err", err)
		http.Error(w, "create order failed", http.StatusInternalServerError)
		return
	}
	writeJSON(w, CreateOrderResp{Code: 0, OrderID: orderID})
}

// PayCallbackReq 支付回调请求（P5 简化：真实需渠道验签）。
type PayCallbackReq struct {
	OrderID string `json:"order_id"`
	Sign    string `json:"sign"`
}

// PayCallbackResp 支付回调响应。
type PayCallbackResp struct {
	Code int32 `json:"code"`
}

// PayCallback POST /v1/pay/callback
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

	// TODO(P6): 渠道验签（PayCallbackReq.sign）
	res, err := h.svc.PayCallback(r.Context(), req.OrderID)
	if err != nil {
		if service.IsNotFound(err) {
			writeJSON(w, PayCallbackResp{Code: 5004}) // 订单不存在
			return
		}
		h.logger.Error("pay callback failed", "order_id", req.OrderID, "err", err)
		http.Error(w, "pay callback failed", http.StatusInternalServerError)
		return
	}
	writeJSON(w, PayCallbackResp{Code: res.Code})
}
