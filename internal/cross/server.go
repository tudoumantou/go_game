// Package cross 小组跨服节点（设计文档第 10 章）
package cross

import (
	"context"

	"game_study/pkg/config"
)

// Server 小组跨服节点（组装层入口，职责见设计文档 3.1 目录树）
type Server struct {
	cfg *config.Config
}

// New 组装服务器（3.3：配置 → 组件 → 服务 → 注册中心 → 优雅退出）
func New(cfg *config.Config) *Server { return &Server{cfg: cfg} }

// Start 启动服务（阻塞至 ctx 取消）。
// TODO(P13): CrossService.Channel（被游戏服连）+ 房间 Actor（不注册 etcd、不连网关）
func (s *Server) Start(ctx context.Context) error {
	<-ctx.Done()
	return nil
}

// Shutdown 优雅关闭（18.1：DRAIN→DRAINING→FLUSH→STOP，pkg/shutdown 编排）。
// TODO(P13): 注册关闭步骤（摘注册 → 停止新流量 → Flush 屏障 → 停 gRPC/Kafka）。
func (s *Server) Shutdown(ctx context.Context) error { return nil }
