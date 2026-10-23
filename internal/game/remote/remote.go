// internal/game/remote/remote.go —— 跨节点 Actor 投递发送端（P0）
//
// 架构职责：实现 actor.RemoteSender，把本节点 Actor 发出的远程消息
// 经**中心服中转**投递给目标游戏节点。为什么走中心服而不是节点间
// 直连：
//   - 零新增基础设施：中心服的 RelayRequest/RelayPush 就是为节点间
//     消息中转设计的（9.3），游戏服的 center.Client 已经在跑；
//   - 不需要 etcd 地址解析：目标节点 ID 即路由键，直连方案要先查
//     etcd 拿 addr 再建 gRPC 连接，多一跳发现服务；
//   - 代价是中转多一跳延迟（内网 <1ms），且 relay 语义是尽力而为
//     （不持久化、不重投）——远程投递的下行推送恰好容得这个代价：
//     推送丢失由客户端重连拉全量权威状态兜底。
//
// 接收侧对称实现见 server.go 的 centerHandler.OnRelayPush。
package remote

import (
	"fmt"
	"sync/atomic"

	"game_study/pkg/actor"
	"game_study/pkg/log"
)

// CenterRelay 中心服中转能力（*gamecenter.Client 实现；测试可注入内存实现）。
//
// 只声明本包用到的方法：Sender 不依赖整个 center 客户端。
type CenterRelay interface {
	// SendRelay 投递中转消息；toNode 为空 + broadcast=true 表示组播。
	SendRelay(relayID, toNode string, broadcast bool, groupID string, payload []byte) error
}

// Sender 经中心服中转的跨节点投递器。
type Sender struct {
	relay  CenterRelay
	nodeID string
	logger *log.Logger
	// seq 中转 ID 序号（中心服按 relay_id 幂等/ACK，保证唯一即可）。
	seq atomic.Int64
}

// NewSender 创建发送器。relay 为 nil 时 Deliver 返回错误——
// 组装层应保证「配了多节点才注入 Remote」，nil 注入属于配置错误，
// 宁可每条远程消息显式失败，也不静默丢。
func NewSender(relay CenterRelay, nodeID string, logger *log.Logger) *Sender {
	return &Sender{relay: relay, nodeID: nodeID, logger: logger}
}

// Deliver 实现 actor.RemoteSender。
//
// 两步：消息 → 信封（actor.EncodeRemote）→ 中心服中转。
// 非 RemoteMessage 的消息显式拒绝：带 Done channel 的请求-响应消息
// （如 GrantItemsMsg）天然不可跨节点，让它在这里失败，调用方的
// 重试/对账路径接管，而不是序列化到一半才炸。
func (s *Sender) Deliver(to *actor.PID, msg any) error {
	rm, ok := msg.(actor.RemoteMessage)
	if !ok {
		return fmt.Errorf("remote: message %T is not remotable (no actor.RemoteMessage)", msg)
	}
	if s.relay == nil {
		return fmt.Errorf("remote: center relay is nil (node %s not wired for cross-node)", s.nodeID)
	}
	env, err := actor.EncodeRemote(to, rm)
	if err != nil {
		return err
	}
	relayID := fmt.Sprintf("%s-%d", s.nodeID, s.seq.Add(1))
	return s.relay.SendRelay(relayID, to.Node, false, "", env)
}
