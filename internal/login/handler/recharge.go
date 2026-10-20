// internal/login/handler/recharge.go —— 充值下单 / 支付回调 HTTP handler
//
// 架构职责：充值域的 HTTP 门面——两个端点：/v1/order/create（客户端
// 下单）与 /v1/pay/callback（渠道服务端回调）。handler 只做协议与
// 编排：鉴权、角色归属解析、参数校验、调用 service、错误码映射。
// 协作关系：AuthService.ValidateToken 解析登录态；RoleRepo.FindByAccount
// 把账号落到角色（下单鉴权 R3.3）；RechargeService 做订单状态机；
// PayChannel 适配器验签；订单消息由 service 内经 Kafka 发往游戏服发货。
// 设计取舍：① 归属与金额只信服务端——客户端只传商品与渠道，
// player_id 从 token 推导、金额从商品配置定价，杜绝改价下单；
// ② 回调先验签再动状态——渠道事实是外部输入，验签是信任边界；
// ③ 错误码分段（5004 订单不存在 / 5005 验签失败 / 5006 金额或状态
// 非法），渠道按码决定是否重试。
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
// 聚合四个协作者：svc 订单状态机、auth 登录态、roles 角色归属、
// pay 渠道验签；logger 用于错误现场留痕（对账排查靠它）。
type RechargeHandler struct {
	svc    *service.RechargeService
	auth   *service.AuthService
	roles  *repo.RoleRepo
	pay    service.PayChannel
	logger *log.Logger
}

// NewRechargeHandler 创建 handler。
// 全部依赖显式注入——无隐藏全局变量，单测可传入 fake/mem 实现。
func NewRechargeHandler(svc *service.RechargeService, auth *service.AuthService, roles *repo.RoleRepo, pay service.PayChannel, logger *log.Logger) *RechargeHandler {
	return &RechargeHandler{svc: svc, auth: auth, roles: roles, pay: pay, logger: logger}
}

// CreateOrderReq 下单请求：只有商品与渠道，金额由服务端定价。
// 知识点：请求体「最小信任」原则——客户端能指定的越少，攻击面越小；
// 金额/玩家 ID 一律不进请求体，全部由服务端从登录态与配置推导。
type CreateOrderReq struct {
	ProductID int32 `json:"product_id"`
	Channel   int32 `json:"channel"`
}

// CreateOrderResp 下单响应。
// amount_fen 回显服务端定价：客户端可展示「你将支付 X 元」，
// 同时与渠道回调金额形成对账锚点（回调金额必须等于它）。
type CreateOrderResp struct {
	Code      int32  `json:"code"`
	OrderID   string `json:"order_id"`
	AmountFen int32  `json:"amount_fen"`
}

// CreateOrder POST /v1/order/create
//
// 归属由登录态决定：Bearer token → account → 角色。客户端不能指定 player_id，
// 也不能指定金额（否则 98 元的档位可以报成 1 分）。
// 顺序：方法 → token → account → role → 参数 → service。
// token 最先校验：未登录请求不消耗后续任何资源（含 DB 查询）。
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
	// token → account → 该账号下最早创建的角色：充值必须落到角色上，
	// 而登录服只读 player 表判断归属，不写（写入只在游戏服）
	playerID, err := h.roles.FindByAccount(r.Context(), accountID)
	if err != nil {
		if errors.Is(err, repo.ErrNoRole) {
			// 403 而非 400：账号合法但未创角，属于「状态不满足」，
			// 客户端应引导玩家先进游戏创角
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
		// 商品不存在/价格非法都是客户端可修正的问题：400 而非 500
		h.logger.Error("create order failed", "player_id", playerID, "product_id", req.ProductID, "err", err)
		http.Error(w, "create order failed", http.StatusBadRequest)
		return
	}
	writeJSON(w, CreateOrderResp{Code: 0, OrderID: orderID, AmountFen: amountFen})
}

// PayCallbackReq 渠道支付回调（字段由渠道适配器验签）。
// 字段即渠道报文原文：验签要覆盖全字段，任何字段被改写都会导致
// sign 不匹配——这是防篡改的机制而非约定。
type PayCallbackReq struct {
	OrderID   string `json:"order_id"`
	AmountFen int32  `json:"amount_fen"`
	Timestamp int64  `json:"timestamp"`
	Nonce     string `json:"nonce"`
	Sign      string `json:"sign"`
}

// PayCallbackResp 支付回调响应。
// 非 0 code 表示拒绝，渠道按约定重试；返回 0 表示已受理（幂等）。
type PayCallbackResp struct {
	Code int32 `json:"code"`
}

// PayCallback POST /v1/pay/callback
//
// 顺序不能换：先验签（渠道事实可信）→ 再校验金额（防改价）→ 最后推进订单状态。
// 任何一步失败都返回非 0，渠道会重试；重复回调由订单状态机幂等吸收。
// 知识点：验签先于一切 DB 写——把「不可信输入」挡在状态机之外；
// 反序（先查单）会允许未验签请求探测订单是否存在（信息泄露）。
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

	// ① 验签 + 时间窗（防重放）：Sign 覆盖 order_id|amount|timestamp|nonce，
	// 时间窗外或签名不符一律拒——此时尚不触碰任何订单数据
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

	// ② 推进订单状态机：金额校验 + CAS 待支付→已支付 + 发 Kafka，
	// 全部在 service 内；handler 只把 service 错误翻译为渠道错误码
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
// 大小写敏感地匹配 "Bearer " 前缀：HTTP 认证方案名规范上大小写不敏感，
// 但渠道/客户端联调时统一大写，先严格后兼容（真出现小写再放宽）。
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
