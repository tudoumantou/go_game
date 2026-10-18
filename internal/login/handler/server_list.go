// internal/login/handler/server_list.go —— 服务器列表 / 分派 handler
package handler

import (
	"net/http"

	"game_study/internal/login/service"
)

// ServerListHandler 服务器列表 handler。
type ServerListHandler struct {
	dispatch *service.DispatchService
}

// NewServerListHandler 创建 ServerListHandler。
func NewServerListHandler(dispatch *service.DispatchService) *ServerListHandler {
	return &ServerListHandler{dispatch: dispatch}
}

// AssignResp /v1/server/assign 响应。
type AssignResp struct {
	Gateway *service.GatewayNode `json:"gateway"`
	Token   string               `json:"token,omitempty"`
}

// Assign GET /v1/server/assign
func (h *ServerListHandler) Assign(w http.ResponseWriter, r *http.Request) {
	gw := h.dispatch.AssignGateway()
	if gw == nil {
		http.Error(w, "no gateway available", http.StatusServiceUnavailable)
		return
	}
	writeJSON(w, AssignResp{Gateway: gw})
}

// GatewayList GET /v1/server/gateway/list
func (h *ServerListHandler) GatewayList(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, h.dispatch.ListGateways())
}
