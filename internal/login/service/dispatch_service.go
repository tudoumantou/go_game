// internal/login/service/dispatch_service.go —— 网关分派服务
package service

import (
	"game_study/pkg/config"
)

// GatewayNode 网关节点。
type GatewayNode struct {
	ID      string `json:"id"`
	Addr    string `json:"addr"`
	Load    int    `json:"load"`
	Version string `json:"version"`
}

// DispatchService 网关分派。
type DispatchService struct {
	// P1：静态配置；P2：改为 etcd 发现
	gateways []GatewayNode
}

// NewDispatchService 创建 DispatchService。
func NewDispatchService(cfg *config.Config) *DispatchService {
	// 从配置读取固定网关列表（P1 简化）
	return &DispatchService{
		gateways: []GatewayNode{
			{ID: "gw-1", Addr: cfg.Server.TCPAddr, Load: 0, Version: "1.0.0"},
		},
	}
}

// AssignGateway 按负载排序返回推荐网关。
func (s *DispatchService) AssignGateway() *GatewayNode {
	if len(s.gateways) == 0 {
		return nil
	}
	// 简单策略：取第一个（P2 改成按 load 排序）
	return &s.gateways[0]
}

// ListGateways 返回网关列表。
func (s *DispatchService) ListGateways() []GatewayNode {
	out := make([]GatewayNode, len(s.gateways))
	copy(out, s.gateways)
	return out
}
