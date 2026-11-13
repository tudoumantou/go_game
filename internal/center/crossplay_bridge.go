// internal/center/crossplay_bridge.go —— 跨服玩法在中心服的装配与协议转换
//
// 职责边界：
//   - 本文件只做「proto ↔ 业务参数」的转换和依赖注入（把 registry 包装成
//     crossplay.Dispatcher），业务规则全在 internal/center/crossplay；
//   - crossplay 包不 import registry：它按 server_id 下发，由本文件负责
//     反查 node_id 并找到发送通道。
//
// 为什么不放在 server.go：server.go 已经承载了装配顺序，跨服玩法的协议
// 转换单独成文件，日后加结算/归档时不用再挤进同一个文件。

package center

import (
	"context"
	"encoding/json"
	"fmt"

	"game_study/internal/center/crossplay"
	centerRPC "game_study/internal/center/rpc"
	internalv2 "game_study/pkg/pb/internalv2"
)

// infoJSON 玩家基本信息列表的落库形态，与设计文档一致：[{k,v}]。
type infoJSON struct {
	K string `json:"k"`
	V string `json:"v"`
}

// Enroll 实现 centerRPC.CrossHandler：报名 / 取消报名。
//
// 这里只做字段搬运，不做任何判定——报名期校验、幂等、参数校验都在
// crossplay.Service.Enroll，这样同一套规则在单测里可以不经过 gRPC 验证。
func (s *Server) Enroll(ctx context.Context, req *internalv2.CrossEnrollReq) *internalv2.CrossEnrollResp {
	if s.crossSvc == nil {
		return &internalv2.CrossEnrollResp{Code: 2, Msg: "跨服玩法未启用"}
	}

	if req.GetCancel() {
		code, msg := s.crossSvc.Cancel(ctx, req.GetPlayerId())
		return &internalv2.CrossEnrollResp{Code: code, Msg: msg}
	}

	m := crossplay.Member{
		PlayerID: req.GetPlayerId(),
		ServerID: req.GetServerId(),
		Power:    req.GetPower(),
	}
	info, err := marshalInfo(req.GetInfo())
	if err != nil {
		// 基本信息只是展示字段，序列化失败不该拦住报名：玩家报名成功
		// 比头像框显示不出来重要得多。
		s.logger.Warn("cross enroll info marshal failed, continue without info",
			"player", m.PlayerID, "err", err)
		info = nil
	}

	code, msg := s.crossSvc.Enroll(ctx, m, info)
	return &internalv2.CrossEnrollResp{Code: code, Msg: msg}
}

// OnScoreBatch 实现 centerRPC.CrossHandler：接收积分上报。
func (s *Server) OnScoreBatch(entries []*internalv2.CrossScoreEntry) {
	if s.crossSvc == nil {
		return
	}
	s.crossSvc.OnScoreBatch(entries)
}

// OnCrossDelta 实现 centerRPC.Handler：跨服玩法改分通知的中转（A 服 → 中心服 → B 服）。
//
// 为什么不许 A 服直接改 B 服玩家的分：分数归属玩家所在服，任何服只能写自己
// 玩家的行。A 服手上的 B 服分数是 60s 前的快照，直接覆盖会抹掉 B 服本地刚
// 产生的增量，所以只能传**差值**过去请 B 服自己改。
//
// 投递是尽力而为：目标节点不在线就丢弃并记 Warn，不进重试队列。丢一条 delta
// 只影响 1 分，而 B 服下一轮上报（60s）会把该玩家的当前值同步上去——为一条
// 会自然收敛的消息加重试队列，只会把「重试积压」变成新的故障面。
func (s *Server) OnCrossDelta(nodeID string, d *internalv2.CrossScoreDelta) {
	if d == nil || d.GetPlayerId() == 0 || d.GetToServer() == 0 {
		return
	}
	if d.GetDelta() == 0 {
		return // 零变化没有转发价值，还会让接收端白跑一次加锁
	}
	// 来源以会话为准，不信任报文里的 FromNode：调用方可以随便填，
	// 而接收端要按 (from_node, relay_id) 去重——伪造成本必须为零。
	// 与 chat 的 from_id 一律取会话是同一个理由。
	d.FromNode = nodeID

	// 游戏服只给 server_id（业务标识），节点 ID 是部署态的，只有这里有映射。
	toNode, ok := s.reg.NodeIDForServer(d.GetToServer())
	if !ok {
		s.logger.Warn("cross delta target server has no node",
			"from", nodeID, "server", d.GetToServer(),
			"player", d.GetPlayerId(), "relay_id", d.GetRelayId())
		return
	}
	sender, ok := s.reg.SenderFor(toNode)
	if !ok {
		s.logger.Warn("cross delta target not connected",
			"from", nodeID, "to", toNode, "server", d.GetToServer(),
			"player", d.GetPlayerId(), "relay_id", d.GetRelayId())
		return
	}
	cmd := &internalv2.CenterCommand{
		Body: &internalv2.CenterCommand_CrossScore{CrossScore: d},
	}
	if err := sender.SendCommand(cmd); err != nil {
		s.logger.Warn("cross delta send failed",
			"from", nodeID, "to", toNode, "server", d.GetToServer(),
			"player", d.GetPlayerId(), "relay_id", d.GetRelayId(), "err", err)
	}
}

// NodeIDForServer 实现 crossplay.Dispatcher：区服编号 → 节点 ID。
func (s *Server) NodeIDForServer(serverID int32) (string, bool) {
	return s.reg.NodeIDForServer(serverID)
}

// SendTo 实现 crossplay.Dispatcher：向节点发送一条下行指令。
func (s *Server) SendTo(nodeID string, cmd *internalv2.CenterCommand) error {
	sender, ok := s.reg.SenderFor(nodeID)
	if !ok {
		return fmt.Errorf("center: node %s not connected", nodeID)
	}
	if err := sender.SendCommand(cmd); err != nil {
		return fmt.Errorf("center: send to %s: %w", nodeID, err)
	}
	return nil
}

// marshalInfo 把 proto 的 KV 列表转成落库 JSON。
func marshalInfo(items []*internalv2.CrossInfoItem) ([]byte, error) {
	if len(items) == 0 {
		return nil, nil
	}
	out := make([]infoJSON, 0, len(items))
	for _, it := range items {
		if it == nil || it.GetK() == "" {
			continue // 空 key 没有意义，展示层也取不到
		}
		out = append(out, infoJSON{K: it.GetK(), V: it.GetV()})
	}
	if len(out) == 0 {
		return nil, nil
	}
	return json.Marshal(out)
}

// 编译期断言：Server 必须同时满足这三个接口，改签名时立刻报错而不是
// 等到装配那一行才发现。
var (
	_ crossplay.Dispatcher   = (*Server)(nil)
	_ centerRPC.CrossHandler = (*Server)(nil)
)
