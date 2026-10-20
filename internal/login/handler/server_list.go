// internal/login/handler/server_list.go —— 服务器列表 / 分派 handler
//
// 架构职责：登录服的「选服入口」——客户端进游戏前先问 login：有哪些
// 网关、该连哪个。本文件是薄门面，策略全在 DispatchService。
// 协作关系：DispatchService 当前从静态配置取网关列表（P1），
// P2 计划改 etcd 发现（与 gateway 的注册体系打通）。
// 设计取舍：assign 返回单个推荐网关（配合客户端重试即可），
// list 返回全量列表供展示/自选；两者都不做鉴权——进游戏前的
// 公开信息，敏感度低，且鉴权入口在 auth/guest 已发 token。
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
// 知识点：Token 字段带 omitempty——未签发 token 时（P1 分派不直接
// 发 token）字段整体省略，客户端靠「字段不存在」区分「无 token」
// 与「空 token」两种语义。
type AssignResp struct {
	Gateway *service.GatewayNode `json:"gateway"`
	Token   string               `json:"token,omitempty"`
}

// Assign GET /v1/server/assign
// 无可用网关返回 503（而非 200 + 空 body）：让客户端/LB 按状态码
// 走退避重试；200 空列表容易被误读成「成功但没服可选」。
func (h *ServerListHandler) Assign(w http.ResponseWriter, r *http.Request) {
	gw := h.dispatch.AssignGateway()
	if gw == nil {
		http.Error(w, "no gateway available", http.StatusServiceUnavailable)
		return
	}
	writeJSON(w, AssignResp{Gateway: gw})
}

// GatewayList GET /v1/server/gateway/list
// 全量列表：运营后台「服务器状态」页与客户端自选服都用它；
// 内部返回拷贝（见 DispatchService.ListGateways），调用方改动
// 不会污染服务内状态。
func (h *ServerListHandler) GatewayList(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, h.dispatch.ListGateways())
}
