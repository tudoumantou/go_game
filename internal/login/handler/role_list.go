// internal/login/handler/role_list.go —— 选角列表 handler
//
// 架构职责：把登录服的 user_role 索引表读成「这个账号在各个区服有哪些角色」，
// 供客户端选角界面展示。是 gs.role.created 这条同步链路的**出口**——
// 没有它，索引表再准也没有人读。
//
// 协作关系：auth.ValidateToken 解出 account_id（归属只信登录态，不信请求参数）；
// UserRoleRepo.ListByAccount 读分片表 + Redis cache-aside（内部已按
// account_id%15 路由，本 handler 不感知分片）。
//
// 设计取舍：
//   - 不接受 account_id 入参：能查任意账号的角色列表等于把「这个设备上有
//     哪些角色」变成公开信息。账号必须从 Bearer token 推导。
//   - 空列表返回 [] 而不是 null：客户端解析 List 时不用做空值分支，
//     而「没有角色」本身是合法状态（新账号），不是错误。
//   - 不做分页：一个账号的角色数上限由「区服数 × 每服一个」决定，
//     现实中是几十量级；为此加 offset/limit 只会让客户端多一次请求。
package handler

import (
	"net/http"

	"game_study/internal/login/repo"
	"game_study/internal/login/service"
	"game_study/pkg/log"
)

// RoleListHandler 角色列表 handler。
type RoleListHandler struct {
	auth   *service.AuthService
	roles  *repo.UserRoleRepo
	logger *log.Logger
}

// NewRoleListHandler 创建 handler。
func NewRoleListHandler(auth *service.AuthService, roles *repo.UserRoleRepo, logger *log.Logger) *RoleListHandler {
	return &RoleListHandler{auth: auth, roles: roles, logger: logger}
}

// RoleItem 单个角色（选角界面展示用）。
//
// 只暴露展示需要的字段：角色详情（等级/装备/背包）要等进入该区服后由
// 游戏服提供，登录服不持有也不应该持有——那是另一份会过期的副本。
type RoleItem struct {
	PlayerID  int64  `json:"player_id"`
	ServerID  int32  `json:"server_id"`
	Nickname  string `json:"nickname"`
	Level     int32  `json:"level"`
	CreatedAt int64  `json:"created_at"` // unix 毫秒
}

// RoleListResp /v1/role/list 响应。
type RoleListResp struct {
	Roles []RoleItem `json:"roles"`
}

// List GET /v1/role/list
//
// 顺序：方法 → token → account → 查索引。token 最先校验，
// 未登录请求不消耗任何 DB/Redis 资源。
func (h *RoleListHandler) List(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
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

	rows, err := h.roles.ListByAccount(r.Context(), accountID)
	if err != nil {
		h.logger.Error("list roles failed", "account_id", accountID, "err", err)
		http.Error(w, "list roles failed", http.StatusInternalServerError)
		return
	}

	// 预分配容量：len==0 时仍然返回非 nil 切片，
	// 让 JSON 编码成 [] 而不是 null（见文件头取舍第三条）。
	resp := RoleListResp{Roles: make([]RoleItem, 0, len(rows))}
	for _, m := range rows {
		resp.Roles = append(resp.Roles, RoleItem{
			PlayerID:  m.PlayerId,
			ServerID:  m.ServerId,
			Nickname:  m.Nickname,
			Level:     m.Level,
			CreatedAt: m.CreatedAt.UnixMilli(),
		})
	}
	writeJSON(w, resp)
}
