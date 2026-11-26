// internal/game/crossplay_nats.go —— 跨服玩法在游戏服侧的 NATS 装配
//
// 职责：订阅本服的两条下行 subject——group（中心服下发的分组/积分）、
// delta（别服发来的改分）——回调到 crossplay.Module。上行（积分上报、改分
// 发出）在 centerAccessor 里，不在这里。
//
// 按 server_id 订阅而不是 node_id：subject 是业务标识，游戏服重启/扩缩容
// 换 node 也能收到发给本服的消息，不需要中心服维护节点映射。
package game

import (
	"game_study/pkg/natsx"
	internalv2 "game_study/pkg/pb/internalv2"
	"google.golang.org/protobuf/proto"
)

// startCrossplayNATS 订阅本服下行 subject。natsBus 为 nil 时不接。
//
// 必须在 crossPlayMod 创建之后调用。订阅回调在 nats.go 的读 goroutine 里
// 执行，Module 内部自带 Registry 锁，这里不再额外加锁。
func (s *Server) startCrossplayNATS(natsBus *natsx.Bus) {
	if s.crossPlayMod == nil || natsBus == nil {
		return
	}
	serverID := s.cfg.Server.ID

	// 分组快照 / 增量：center → 本服。
	if err := natsBus.Subscribe(natsBus.GroupSubject(serverID),
		func() proto.Message { return &internalv2.CrossGroupDeltaPush{} },
		func(msg proto.Message) error {
			push, ok := msg.(*internalv2.CrossGroupDeltaPush)
			if !ok {
				return nil
			}
			s.crossPlayMod.OnGroupDelta(push)
			return nil
		},
	); err != nil {
		s.logger.Warn("subscribe crossplay group failed", "server", serverID, "err", err)
	}

	// 跨服改分：别服 → 本服（直达，不经 center）。
	if err := natsBus.Subscribe(natsBus.DeltaSubject(serverID),
		func() proto.Message { return &internalv2.CrossScoreDelta{} },
		func(msg proto.Message) error {
			d, ok := msg.(*internalv2.CrossScoreDelta)
			if !ok {
				return nil
			}
			s.crossPlayMod.OnScoreDelta(d)
			return nil
		},
	); err != nil {
		s.logger.Warn("subscribe crossplay delta failed", "server", serverID, "err", err)
	}

	s.logger.Info("crossplay nats subscribed",
		"server", serverID,
		"group", natsBus.GroupSubject(serverID),
		"delta", natsBus.DeltaSubject(serverID))
}
