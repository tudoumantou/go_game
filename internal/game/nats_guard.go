// internal/game/nats_guard.go —— NATS 接收侧的发送者校验
//
// 三个订阅点（relay 点对点 / relay 广播 / crossplay 分组 / crossplay 改分）
// 都要做同一件事：把 subject 尾部的发送者与载荷里自称的 from_node 比一比，
// 不一致就丢弃。抽到这里是为了让「不做校验」这件事在代码里显眼——想加新的
// 订阅时，看到的样板就是带校验的那一份。
//
// 为什么要两处比对（而不是只信 subject）：subject 由 NATS 服务端的发布
// 权限保证，载荷字段由发送方自己填。只信载荷 → 任意节点可冒充；只信
// subject → 一旦某条 allow_publish 配漏（例如 relay.group.*.from.* 写成了
// 通配），冒充就无声无息地通过了。两份对上才认，配漏会立刻变成指标上涨。
package game

import (
	"game_study/pkg/metrics"
	"game_study/pkg/natsx"
)

// acceptSender 校验一条消息的发送者，返回是否应当继续处理。
//
// kind 是日志/指标里的事件名（如 "relay.node"），只用于区分是哪条订阅
// 出的问题；不要把它拼成高基数标签（见 metrics.registerNATS）。
func (s *Server) acceptSender(subject, claimed, kind string) bool {
	sender, ok := natsx.SenderFromSubject(subject)
	if !ok {
		// subject 不符合约定：只可能来自手写客户端，或运维改了 subject 拼法。
		metrics.NATSRejected("bad_subject")
		s.logger.Warn("nats message rejected: subject has no valid sender",
			"kind", kind, "subject", subject)
		return false
	}
	if claimed != sender {
		// 自称 A、实际由 B 发出：要么是伪造，要么是 B 的账号权限配得过宽。
		metrics.NATSRejected("sender_mismatch")
		s.logger.Warn("nats message rejected: sender mismatch",
			"kind", kind, "subject", subject, "subject_sender", sender, "claimed", claimed)
		return false
	}
	return true
}
