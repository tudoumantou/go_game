// internal/center/crossplay_nats.go —— 跨服玩法在中心服侧的 NATS 装配
//
// 职责边界：本文件只做「NATS ↔ crossplay.Service」的适配——订阅 score
// subject 转成 Service.OnScoreBatch、把下发回调接到 natsx.Bus.Publish。
// 业务规则仍在 internal/center/crossplay，路由规则仍在 natsx 的 subject 约定。
//
// 为什么上报用普通 Subscribe（广播）而不是 QueueSubscribe：中心服多实例
// 时，每个实例都要有一份完整的内存 Store——leader 负责下发，非 leader
// 不写权威状态，但 leader 切换后新 leader 的 Store 必须已经是最新的。
// QueueSubscribe 会把一条上报只投给一个实例，新 leader 的数据就是残缺的。
// NATS Core 广播的代价是每个实例多收一份（几十服 × 60s 一批，量很小）。
package center

import (
	"game_study/pkg/natsx"
	"game_study/pkg/pb/internalv2"
	"google.golang.org/protobuf/proto"
)

// startCrossplayNATS 把跨服玩法接到 NATS 总线上。
//
// 必须在 crossSvc 创建之后调用。返回的 Bus 由 Server 持有并在关服时 Close。
// natsBus 为 nil（未配 NATS）时不接——玩法退回 gRPC 上报/下发路径。
func (s *Server) startCrossplayNATS(natsBus *natsx.Bus) {
	if s.crossSvc == nil || natsBus == nil {
		return
	}

	// 上报：game → center。所有 center 实例都收（广播），保证各自 Store 完整。
	err := natsBus.Subscribe(natsx.ScoreWildcard,
		func() proto.Message { return &internalv2.CrossScoreBatch{} },
		func(msg proto.Message) error {
			batch, ok := msg.(*internalv2.CrossScoreBatch)
			if !ok {
				return nil
			}
			s.crossSvc.OnScoreBatch(batch.GetEntries())
			return nil
		},
	)
	if err != nil {
		s.logger.Warn("subscribe crossplay score failed", "err", err)
	}

	// 下发：center → game。注入到 Service，sendToServer 优先走它。
	s.crossSvc.SetSendGroup(func(serverID int32, push *internalv2.CrossGroupDeltaPush) error {
		return natsBus.Publish(natsx.GroupSubject(serverID), push)
	})

	s.logger.Info("crossplay nats attached", "score", natsx.ScoreWildcard, "group", natsx.GroupWildcard)
}
