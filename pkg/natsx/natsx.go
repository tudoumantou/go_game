package natsx

import (
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
)

// 默认连接参数。
const (
	DefaultReconnectWait = 2 * time.Second
	DefaultMaxReconnect  = 60
	DefaultTimeout       = 2 * time.Second
)

// Subject 构造。集中在一处，避免调用方手写字符串拼错导致「发出去没人收」。
func ScoreSubject(serverID int32) string { return fmt.Sprintf("crossplay.score.%d", serverID) }
func GroupSubject(serverID int32) string { return fmt.Sprintf("crossplay.group.%d", serverID) }
func DeltaSubject(serverID int32) string { return fmt.Sprintf("crossplay.delta.%d", serverID) }

// 通配订阅：中心服收所有服的上报；游戏服只订阅自己那一条。
const (
	ScoreWildcard = "crossplay.score.*"
	GroupWildcard = "crossplay.group.*"
	DeltaWildcard = "crossplay.delta.*"
)

// Config 连接配置。零值取 Default* 常量。
type Config struct {
	URL           string
	ReconnectWait time.Duration
	MaxReconnect  int
	Timeout       time.Duration
}

// Bus 对 nats.Conn 的封装。
//
// 只暴露 publish / 订阅回调 / Ready / Close 四个能力：业务包不需要也不应该
// 拿到裸 *nats.Conn（拿到就会有人直接用 subject 字符串绕过约定）。
type Bus struct {
	nc *nats.Conn

	mu   sync.RWMutex
	subs []*nats.Subscription
}

// Connect 建立连接。url 为空时返回 (nil, nil)——与 Kafka 一致，未配置即降级禁用。
//
// 重连参数显式设置：nats.go 默认 MaxReconnect=60、ReconnectWait=2s，这里
// 写一遍是为了让「断线多久算彻底失败」成为可配置项，而不是藏在库默认值里。
// 连接建立是同步的（Timeout 到期即失败），不做后台重试——重连交给 nats.go
// 在建连成功之后自己做，失败建连由上层在装配时报错，不静默降级。
func Connect(cfg Config) (*Bus, error) {
	if cfg.URL == "" {
		return nil, nil
	}
	if cfg.ReconnectWait <= 0 {
		cfg.ReconnectWait = DefaultReconnectWait
	}
	if cfg.MaxReconnect <= 0 {
		cfg.MaxReconnect = DefaultMaxReconnect
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = DefaultTimeout
	}
	nc, err := nats.Connect(cfg.URL,
		nats.Name("game_study"),
		nats.Timeout(cfg.Timeout),
		nats.ReconnectWait(cfg.ReconnectWait),
		nats.MaxReconnects(cfg.MaxReconnect),
	)
	if err != nil {
		return nil, fmt.Errorf("nats connect %s: %w", cfg.URL, err)
	}
	return &Bus{nc: nc}, nil
}

// NewFromConn 用已有连接构造（测试注入 nats-server 的连接；生产走 Connect）。
func NewFromConn(nc *nats.Conn) *Bus { return &Bus{nc: nc} }

// Ready 是否连接可用。nats.go 的 IsConnected 在重连窗口内为 false，
// 调用方（积分上报）据此决定「这一拍跳过，下一拍再发」。
func (b *Bus) Ready() bool {
	return b != nil && b.nc != nil && b.nc.IsConnected()
}

// Publish 序列化并发布一条 protobuf 消息。
func (b *Bus) Publish(subject string, msg proto.Message) error {
	if b == nil || b.nc == nil {
		return fmt.Errorf("nats bus not initialized")
	}
	data, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal %s: %w", subject, err)
	}
	return b.nc.Publish(subject, data)
}

// Handler 订阅回调。返回 error 只用于记日志，NATS Core 不据此重投。
type Handler func(msg proto.Message) error

// Subscribe 订阅一个 subject，按工厂构造消息类型后反序列化再回调。
//
// 不直接传 *nats.Msg 给业务层：业务层不该依赖传输层的字节细节。factory
// 每次返回新的空消息（proto.Unmarshal 会写入它），调用方传 &pb.Xxx{} 的工厂。
func (b *Bus) Subscribe(subject string, factory func() proto.Message, h Handler) error {
	if b == nil || b.nc == nil {
		return fmt.Errorf("nats bus not initialized")
	}
	sub, err := b.nc.Subscribe(subject, func(m *nats.Msg) {
		pb := factory()
		if err := proto.Unmarshal(m.Data, pb); err != nil {
			// 解码失败的消息直接丢弃：Core 无重投，重试也不会变对。
			return
		}
		_ = h(pb)
	})
	if err != nil {
		return fmt.Errorf("subscribe %s: %w", subject, err)
	}
	b.mu.Lock()
	b.subs = append(b.subs, sub)
	b.mu.Unlock()
	return nil
}

// Close 关闭连接（同时也退订所有订阅）。
func (b *Bus) Close() {
	if b == nil || b.nc == nil {
		return
	}
	b.nc.Close()
}
