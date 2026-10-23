// pkg/actor/remote.go —— 跨节点投递（P0：单进程 Actor → 多节点 Actor）
//
// 解决什么问题：System 原本是单进程的——PID 只有 {ID, Shard}，Send 永远
// 投递到本进程的分片。多游戏服部署时，玩家按 player_id % 节点数 分布在
// 不同节点，「给玩家 X 发消息」必须能投到**别的进程**。
//
// 设计（三块，框架不绑定任何传输）：
//   - PID.Node：空 = 本地；非空 = 远端节点 ID。本地 PID 一律 Node=""，
//     单机部署下本文件全部逻辑自然短路，零行为变化；
//   - RemoteSender：跨节点投递接口，由组装层实现（当前实现经中心服
//     RelayRequest/RelayPush 中转，见 internal/game/remote）；
//   - RemoteMessage + 编解码注册表：消息显式声明「我可跨节点」并自带
//     序列化。框架不猜消息格式——不可序列化的消息（如带 Done channel
//     的请求-响应消息）根本不该出现在远程路径上，让它显式失败。
//
// 信封用 JSON 而不是 gob：gob 需要提前 Register 所有具体类型（漏注册
// 运行时才炸），JSON + 显式 type_url 注册表在启动期就能发现漏注册；
// 代价是体量大一点，但远程消息是 KB 级全量快照/协议包，不在乎。
package actor

import (
	"encoding/json"
	"fmt"
	"sync"
)

// RemoteSender 跨节点投递能力（由组装层注入 System.Config.Remote）。
//
// 实现方负责：序列化传输、超时、失败上报。框架只保证「目标节点非本节点
// 且 Remote 非 nil」时调用本接口，其余一律走本地分片。
// 不带 context：Send 的调用链上没有 ctx（timerLoop/业务 goroutine 混用），
// 超时由实现方自管（内部 context.WithTimeout），避免为加一个参数改动
// 全部 Send 调用方。
type RemoteSender interface {
	// Deliver 把 msg 投递给 to.Node 上的 Actor（to.ID）。
	Deliver(to *PID, msg any) error
}

// RemoteMessage 可跨节点投递的消息。
//
// 两个方法都手写而不是用反射：反射版「什么都能发」会把带 channel 的
// 消息也送进远程路径，序列化到一半才失败；显式实现 = 编译期承诺
// 「这个消息是纯数据」。
type RemoteMessage interface {
	// RemoteTypeURL 全局唯一类型标识（编解码注册表的键）。
	RemoteTypeURL() string
	// MarshalRemote 序列化消息体（信封之外的 payload）。
	MarshalRemote() ([]byte, error)
}

// RemoteEnvelope 跨节点投递信封。
//
// 经中心服 RelayRequest.Payload（不透明 bytes）透传，所以ActorID 必须
// 进信封——中转层不知道也不该知道消息要投给哪个 Actor。
// Payload 在 JSON 里是 base64 字符串（[]byte 的默认 JSON 编码）。
type RemoteEnvelope struct {
	ActorID string `json:"actor_id"`
	TypeURL string `json:"type_url"`
	Payload []byte `json:"payload"`
}

// 编解码注册表：type_url → 反序列化函数。
//
// sync.Map：注册发生在进程启动期（各业务包 init），读取发生在每条远程
// 消息——读远多于写，sync.Map 正是为这个场景优化的。
var remoteCodecs sync.Map // string -> func([]byte) (any, error)

// RegisterRemote 注册可远程投递的消息类型（启动期调用，重复注册 panic）。
//
// 与 Thin.Handle 同思路：注册冲突是编码错误，启动期暴露比线上静默
// 顶掉好——两个类型共用一个 type_url 时，后注册的会把先注册的
// 反序列化逻辑替换掉，故障现象是「消息类型莫名错乱」。
func RegisterRemote(typeURL string, decode func([]byte) (any, error)) {
	if _, dup := remoteCodecs.LoadOrStore(typeURL, decode); dup {
		panic(fmt.Sprintf("actor: remote type %q registered twice", typeURL))
	}
}

// DecodeRemote 按注册表反序列化远程消息（未注册的 type_url 显式报错）。
func DecodeRemote(typeURL string, payload []byte) (any, error) {
	v, ok := remoteCodecs.Load(typeURL)
	if !ok {
		return nil, fmt.Errorf("actor: remote type %q not registered", typeURL)
	}
	decode, ok := v.(func([]byte) (any, error))
	if !ok {
		return nil, fmt.Errorf("actor: remote codec for %q has wrong type", typeURL)
	}
	return decode(payload)
}

// EncodeRemote 把远程消息打包成信封（to.ID 是目标 Actor，to.Node 由传输层使用）。
func EncodeRemote(to *PID, msg RemoteMessage) ([]byte, error) {
	if to == nil {
		return nil, fmt.Errorf("actor: nil target pid")
	}
	payload, err := msg.MarshalRemote()
	if err != nil {
		return nil, fmt.Errorf("actor: marshal remote %T: %w", msg, err)
	}
	return json.Marshal(&RemoteEnvelope{
		ActorID: to.ID,
		TypeURL: msg.RemoteTypeURL(),
		Payload: payload,
	})
}

// DecodeRemoteEnvelope 解信封并反序列化消息，返回目标 ActorID 与消息。
//
// 接收侧（组装层的中转入口）一次调用拿到投递所需的一切：
// actorID 用于本地路由，msg 用于 Receive。
func DecodeRemoteEnvelope(data []byte) (actorID string, msg any, err error) {
	var env RemoteEnvelope
	if err := json.Unmarshal(data, &env); err != nil {
		return "", nil, fmt.Errorf("actor: decode remote envelope: %w", err)
	}
	if env.ActorID == "" {
		return "", nil, fmt.Errorf("actor: remote envelope without actor_id")
	}
	m, err := DecodeRemote(env.TypeURL, env.Payload)
	if err != nil {
		return "", nil, err
	}
	return env.ActorID, m, nil
}
