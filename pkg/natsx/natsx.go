package natsx

import (
	"fmt"
	"strings"
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

// Config 连接配置。零值取 Default* 常量。
type Config struct {
	URL string
	// Prefix subject 命名空间前缀（取部署的环境名，如 dev/test/prod）。
	//
	// 多个环境共用一套 NATS 集群时必须隔离：subject 是全局扁平的，
	// 两个环境的 game-01 会订到同一条 relay.node.game-01 —— 消息直接串台，
	// 而且这种事故在客户端上表现为"玩家收到别人的消息"，极难定位。
	//
	// 非空时所有 subject 变成 "{prefix}.<subject>"。空则不加前缀（与旧
	// 部署兼容），生产环境应显式配置。
	Prefix        string
	ReconnectWait time.Duration
	MaxReconnect  int
	Timeout       time.Duration
}

// Bus 对 nats.Conn 的封装。
//
// 只暴露 publish / 订阅回调 / Ready / Close / subject 构造五个能力：业务包
// 不需要也不应该拿到裸 *nats.Conn（拿到就会有人直接用 subject 字符串绕过
// 约定，前缀也就漏了）。
//
// subject 构造做成方法而不是包级函数，正是为了让「带前缀」成为唯一可能：
// 调用方拿不到不带前缀的 subject，也就不可能发出跨环境串台的消息。
type Bus struct {
	nc *nats.Conn
	// prefix 已归一化为「不带首尾点、带末尾点」的形式，如 "prod."；空表示不加前缀。
	prefix string

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
	return &Bus{nc: nc, prefix: normalizePrefix(cfg.Prefix)}, nil
}

// normalizePrefix 把各种写法归一成 "xxx."：容忍 "prod" / "prod." / " .prod. "。
// 空串或只有点号时返回空（不加前缀）。
func normalizePrefix(p string) string {
	p = strings.Trim(strings.TrimSpace(p), ".")
	if p == "" {
		return ""
	}
	return p + "."
}

// Ready 是否连接可用。nats.go 的 IsConnected 在重连窗口内为 false，
// 调用方（积分上报）据此决定「这一拍跳过，下一拍再发」。
func (b *Bus) Ready() bool {
	return b != nil && b.nc != nil && b.nc.IsConnected()
}

// subj 拼上命名空间前缀。b 为 nil 时原样返回——subject 构造不该 panic，
// 真正的错误由 Publish/Subscribe 的 nil 检查报出。
func (b *Bus) subj(name string) string {
	if b == nil {
		return name
	}
	return b.prefix + name
}

// ScoreSubject 积分上报：game → center（所有实例广播收）。
func (b *Bus) ScoreSubject(serverID int32) string {
	return b.subj(fmt.Sprintf("crossplay.score.%d", serverID))
}

// GroupSubject 分组快照/增量：center(leader) → game。
func (b *Bus) GroupSubject(serverID int32) string {
	return b.subj(fmt.Sprintf("crossplay.group.%d", serverID))
}

// DeltaSubject 跨服改分：game → game 直达（不经 center）。
func (b *Bus) DeltaSubject(serverID int32) string {
	return b.subj(fmt.Sprintf("crossplay.delta.%d", serverID))
}

// RelayNodeSubject 通用 relay 点对点：发给一个具体的游戏节点。
//
// 键用 node_id（部署态标识，如 game-01）而非 server_id：跨节点 Actor 投递
// 的目标 PID.Node 就是 node_id，且 relay 的语义是「投到某台机器的 Actor
// System」，与跨服玩法按区服路由是两码事。两个 namespace 天然隔离——
// 业务中心只认 crossplay.*，relay.* 全程不经它。
func (b *Bus) RelayNodeSubject(nodeID string) string {
	return b.subj("relay.node." + nodeID)
}

// RelayGroupSubject 通用 relay 广播：发给一个节点分组内的所有成员。
//
// 每个节点订阅自己的 RelayNodeSubject 之外，再订阅所属组的这一条；
// 广播方只需 publish 一次，NATS 按订阅扇出，中心服不再承担组内遍历下发。
func (b *Bus) RelayGroupSubject(groupID string) string {
	return b.subj("relay.group." + groupID)
}

// 通配订阅：中心服收所有服的上报；游戏服只订阅自己那一条。
func (b *Bus) ScoreWildcard() string { return b.subj("crossplay.score.*") }
func (b *Bus) GroupWildcard() string { return b.subj("crossplay.group.*") }
func (b *Bus) DeltaWildcard() string { return b.subj("crossplay.delta.*") }

// RelayWildcard 供调试/审计订阅所有点对点 relay，生产节点不会订阅它。
func (b *Bus) RelayWildcard() string { return b.subj("relay.node.*") }

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

// Flush 确认已发出的订阅/发布都被服务端处理。
//
// 测试里订阅后调它，确保订阅在服务端生效再发布，消除竞态；
// 生产路径通常不需要（订阅建立是异步的，首条消息丢了由业务兜底）。
func (b *Bus) Flush() error {
	if b == nil || b.nc == nil {
		return fmt.Errorf("nats bus not initialized")
	}
	return b.nc.Flush()
}

// Close 关闭连接（同时也退订所有订阅）。
func (b *Bus) Close() {
	if b == nil || b.nc == nil {
		return
	}
	b.nc.Close()
}
