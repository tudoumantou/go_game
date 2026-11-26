// internal/center/crossplay_bridge.go —— 跨服玩法在中心服的装配与协议转换
//
// 职责边界：
//   - 本文件只做「proto ↔ 业务参数」的转换和依赖注入，业务规则全在
//     internal/center/crossplay；
//   - 跨服玩法只在配了 NATS 时启用：积分上报订阅、分组下发发布都在
//     crossplay_nats.go 里装配，本文件只保留报名这一个 gRPC Unary 入口。
//
// 为什么不放在 server.go：server.go 已经承载了装配顺序，跨服玩法的协议
// 转换单独成文件，日后加结算/归档时不用再挤进同一个文件。

package center

import (
	"context"
	"encoding/json"

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

// 编译期断言：Server 必须满足报名接口，改签名时立刻报错而不是等到装配
// 那一行才发现。
var _ centerRPC.CrossHandler = (*Server)(nil)
