// internal/login/service/dispatch_service.go —— 网关分派服务
//
// 架构职责：回答「客户端应该连哪个网关」——登录服对客户端的选服
// 策略载体。当前为 P1 静态配置版：网关列表来自配置文件（单网关），
// P2 计划改 etcd 发现真实节点 + 按负载排序。
// 协作关系：handler.ServerListHandler.Assign/GatewayList 是唯一
// 调用方；与 gateway 的 etcd 注册体系（gameetcd）是天然配对——
// 网关注册、登录服发现，P2 打通后本服务的 gateways 即来自 etcd。
// 设计取舍：① 静态列表起步——P1 单网关部署不需要发现机制，
// 先把接口契约定下来，P2 换数据源不改 handler；② ListGateways
// 返回拷贝——[]GatewayNode 是引用类型，直接返回内部切片等于
// 把可变状态暴露给调用方（客户端 JSON 序列化虽只读，但未来若有
// 写路径就是数据竞争）；拷贝一次成本可忽略。
package service

import (
	"game_study/pkg/config"
)

// GatewayNode 网关节点。
// 知识点：JSON tag 显式 snake_case——本结构直接序列化为 /v1/server
// 响应体，字段名即对外协议；Load 当前恒 0（P1 无真实负载数据），
// 保留字段是为 P2 客户端可直接消费，无需改协议。
type GatewayNode struct {
	ID      string `json:"id"`
	Addr    string `json:"addr"`
	Load    int    `json:"load"`
	Version string `json:"version"`
}

// DispatchService 网关分派。
// 无锁设计：gateways 构造后只读（P1 静态），将来接 etcd 动态更新
// 时需引入 RWMutex 或 atomic.Pointer——现在提前加锁是过度设计。
type DispatchService struct {
	// P1：静态配置；P2：改为 etcd 发现
	gateways []GatewayNode
}

// NewDispatchService 创建 DispatchService。
// 从配置读取固定网关列表（P1 简化）
// Addr 直接取 cfg.Server.TCPAddr：单节点部署时登录服与网关同机
// 配置，客户端拿到的地址即可直连；多网关部署需改为各节点真实地址。
func NewDispatchService(cfg *config.Config) *DispatchService {
	return &DispatchService{
		gateways: []GatewayNode{
			{ID: "gw-1", Addr: cfg.Server.TCPAddr, Load: 0, Version: "1.0.0"},
		},
	}
}

// AssignGateway 按负载排序返回推荐网关。
// 简单策略：取第一个（P2 改成按 load 排序）
// 返回 &s.gateways[0]（切首元素地址）：内部切片地址不外泄的
// 前提是调用方只读——JSON 序列化即只读，安全；P2 改动态更新时
// 这里要连同返回值一起改为拷贝，防止调用方持有过期快照。
func (s *DispatchService) AssignGateway() *GatewayNode {
	if len(s.gateways) == 0 {
		return nil
	}
	return &s.gateways[0]
}

// ListGateways 返回网关列表。
// copy 到新切片：防御性拷贝——调用方拿到的副本与内部状态解耦，
// 未来内部改造成动态列表也不会让调用方观察到中间态。
func (s *DispatchService) ListGateways() []GatewayNode {
	out := make([]GatewayNode, len(s.gateways))
	copy(out, s.gateways)
	return out
}
