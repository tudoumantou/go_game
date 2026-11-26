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
	Prefix string

	// 认证：三选一，优先级 CredsFile > Token > User/Password。
	//
	// 为什么必须配：NATS 无认证时任何能连上 4222 的进程都能发布任意 subject，
	// 也就是能冒充任意游戏服发 relay、冒充中心服下发分组。subject 里的
	// .from.{sender} 只在**服务端有发布权限约束**时才有意义（见 doc.go）。
	//
	// 凭据也可以直接写在 URL 里（nats://user:pass@host:4222），但那样密码
	// 会进日志和进程参数；独立字段配合 ${ENV} 注入更安全。
	User      string
	Password  string
	Token     string
	CredsFile string

	// ErrorHandler 异步错误回调（权限拒绝、慢消费者、服务端 -ERR 等）。
	//
	// 这些错误不会从 Publish 返回——NATS 是异步的，服务端拒绝一条发布
	// 时消息已经进了本地缓冲。不接这个回调，越权发布就是"静默失败"：
	// 发送方以为发出去了，接收方永远收不到，只有权限规则和实际行为对不上
	// 的时候才会发现。生产装配把它接到日志上（Warn 级别）。
	ErrorHandler func(error)

	ReconnectWait time.Duration
	MaxReconnect  int
	Timeout       time.Duration
}

// HasAuth 是否配置了任一认证方式。
//
// 装配层用它决定是否打「无认证」告警：这里不做硬校验，因为本地开发与
// 内网部署确实可能跑无认证的 NATS；但生产环境启动日志里必须有这条警告，
// 否则一个「谁都能发消息」的总线看起来和正常的一模一样。
func (c Config) HasAuth() bool {
	return c.CredsFile != "" || c.Token != "" || c.User != ""
}

// Bus 对 nats.Conn 的封装。
//
// 只暴露 publish / 订阅回调 / Ready / Close / subject 构造五个能力：业务包
// 不需要也不应该拿到裸 *nats.Conn（拿到就会有人直接用 subject 字符串绕过
// 约定，前缀也就漏了）。
//
// subject 构造做成方法而不是包级函数，正是为了让「带前缀」「带发送者」成为
// 唯一可能：调用方拿不到裸 subject，也就不可能发出跨环境串台、或署别人名字
// 的消息。
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
	opts := []nats.Option{
		nats.Name("game_study"),
		nats.Timeout(cfg.Timeout),
		nats.ReconnectWait(cfg.ReconnectWait),
		nats.MaxReconnects(cfg.MaxReconnect),
	}
	switch {
	case cfg.CredsFile != "":
		// creds 文件（NKey/JWT）：读失败在建连时才会暴露，这里原样返回错误。
		opts = append(opts, nats.UserCredentials(cfg.CredsFile))
	case cfg.Token != "":
		opts = append(opts, nats.Token(cfg.Token))
	case cfg.User != "":
		opts = append(opts, nats.UserInfo(cfg.User, cfg.Password))
	}
	if cfg.ErrorHandler != nil {
		h := cfg.ErrorHandler
		opts = append(opts, nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			h(err)
		}))
	}
	nc, err := nats.Connect(cfg.URL, opts...)
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

// ============================ 发送者进 subject ============================
//
// 所有业务 subject 都形如 "{目标}.from.{发送者}"，两个作用：
//
//  1. 让 NATS 服务端的发布权限能表达「每个节点只能以自己的名义发」。
//     nats.conf 里给每个节点配 allow_publish: "*.from.{自己}"，冒充别人
//     在服务端就被拒绝——这是唯一无法在客户端绕过的一层。
//  2. 让接收端有第二个发送者来源可以交叉校验：subject 里的（服务端强制）
//     与载荷字段里的（发送方自填）一致，才认这条消息。
//
// 若 subject 不带发送者，A 服给 B 服发消息用的 subject 与 C 服给 B 服发的
// 完全一样，服务端无法区分，载荷里的 from_node 也就只能是"自称"。

// SenderFromSubject 从 subject 尾部的 ".from.{sender}" 解出发送者。
//
// 返回 false 表示这条 subject 不符合约定（缺后缀、发送者为空、含通配符）：
// 调用方应当丢弃该消息——不合约定的 subject 只可能来自手写的客户端或
// 权限配置漏掉的账号。
func SenderFromSubject(subject string) (string, bool) {
	const marker = ".from."
	i := strings.LastIndex(subject, marker)
	if i < 0 {
		return "", false
	}
	sender := subject[i+len(marker):]
	if !ValidToken(sender) {
		return "", false
	}
	return sender, true
}

// ValidToken 校验一个标识能安全地拼进 subject。
//
// NATS subject 用 "." 分段、"*"/">" 做通配，所以节点名/分组名里出现这些
// 字符就不是"名字带点"这么简单，而是**改变路由语义**：名叫 "game.01" 的
// 节点会把消息发到 relay.node.game.01.from.x，订阅 relay.node.game-*.from.*
// 的调试卷也会收到它。装配层用它在启动时直接拒绝，而不是运行期拼出畸形 subject。
func ValidToken(s string) bool {
	if s == "" || len(s) > 64 {
		return false
	}
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
		default:
			return false
		}
	}
	return true
}

// ValidateToken 带字段名的校验封装，装配层直接把它当启动错误返回。
func ValidateToken(field, value string) error {
	if !ValidToken(value) {
		return fmt.Errorf("%s %q: 只能是字母/数字/'-'/'_'（长度 1-64），不能含 '.'、'*'、'>' 或空白", field, value)
	}
	return nil
}

// ScoreSubject 积分上报：game → center（所有实例广播收）。
func (b *Bus) ScoreSubject(serverID int32, from string) string {
	return b.subj(fmt.Sprintf("crossplay.score.%d.from.%s", serverID, from))
}

// GroupSubject 分组快照/增量：center(leader) → game。
func (b *Bus) GroupSubject(serverID int32, from string) string {
	return b.subj(fmt.Sprintf("crossplay.group.%d.from.%s", serverID, from))
}

// DeltaSubject 跨服改分：game → game 直达（不经 center）。
func (b *Bus) DeltaSubject(serverID int32, from string) string {
	return b.subj(fmt.Sprintf("crossplay.delta.%d.from.%s", serverID, from))
}

// RelayNodeSubject 通用 relay 点对点：发给一个具体的游戏节点。
//
// 键用 node_id（部署态标识，如 game-01）而非 server_id：跨节点 Actor 投递
// 的目标 PID.Node 就是 node_id，且 relay 的语义是「投到某台机器的 Actor
// System」，与跨服玩法按区服路由是两码事。两个 namespace 天然隔离——
// 业务中心只认 crossplay.*，relay.* 全程不经它。
func (b *Bus) RelayNodeSubject(to, from string) string {
	return b.subj("relay.node." + to + ".from." + from)
}

// RelayGroupSubject 通用 relay 广播：发给一个节点分组内的所有成员。
//
// 每个节点订阅自己的 RelayNodeSubSubject 之外，再订阅所属组的这一条；
// 广播方只需 publish 一次，NATS 按订阅扇出，中心服不再承担组内遍历下发。
func (b *Bus) RelayGroupSubject(groupID, from string) string {
	return b.subj("relay.group." + groupID + ".from." + from)
}

// ============================ 订阅 subject ============================
//
// 订阅侧用通配发送者：自己对谁发的都收，但每条都记着"是谁发的"。
// 订阅不需要服务端授权（读权限泄漏无害，写权限才致命），所以这里用 *。

// ScoreWildcard 中心服收所有服的上报。
func (b *Bus) ScoreWildcard() string { return b.subj("crossplay.score.*.from.*") }

// GroupWildcard 中心服侧审计/排查用；游戏服订阅自己那一条即可。
func (b *Bus) GroupWildcard() string { return b.subj("crossplay.group.*.from.*") }

// DeltaWildcard 跨服改分的审计订阅；生产节点订阅 DeltaSubSubject 更精确。
func (b *Bus) DeltaWildcard() string { return b.subj("crossplay.delta.*.from.*") }

// GroupSubSubject 游戏服订阅本服的分组下发：{env}.crossplay.group.{self}.from.*
func (b *Bus) GroupSubSubject(serverID int32) string {
	return b.subj(fmt.Sprintf("crossplay.group.%d.from.*", serverID))
}

// DeltaSubSubject 游戏服订阅发给本服的改分：{env}.crossplay.delta.{self}.from.*
func (b *Bus) DeltaSubSubject(serverID int32) string {
	return b.subj(fmt.Sprintf("crossplay.delta.%d.from.*", serverID))
}

// RelayNodeSubSubject 本节点收点对点 relay：{env}.relay.node.{self}.from.*
func (b *Bus) RelayNodeSubSubject(self string) string {
	return b.subj("relay.node." + self + ".from.*")
}

// RelayGroupSubSubject 本节点收所属分组的广播：{env}.relay.group.{group}.from.*
func (b *Bus) RelayGroupSubSubject(groupID string) string {
	return b.subj("relay.group." + groupID + ".from.*")
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

// HandlerWithSubject 同上，额外把命中的 subject 交给回调。
//
// 接收端要靠它取 subject 里的发送者（SenderFromSubject）与载荷字段比对；
// 订阅带通配符时，只有真实的 subject 才知道这条是谁发的。
type HandlerWithSubject func(subject string, msg proto.Message) error

// Subscribe 订阅一个 subject，按工厂构造消息类型后反序列化再回调。
//
// 不直接传 *nats.Msg 给业务层：业务层不该依赖传输层的字节细节。factory
// 每次返回新的空消息（proto.Unmarshal 会写入它），调用方传 &pb.Xxx{} 的工厂。
func (b *Bus) Subscribe(subject string, factory func() proto.Message, h Handler) error {
	return b.SubscribeWithSubject(subject, factory, func(_ string, msg proto.Message) error {
		return h(msg)
	})
}

// SubscribeWithSubject 与 Subscribe 相同，但把命中的 subject 一并传给回调。
//
// 需要校验发送者的订阅（relay / crossplay 全部）都用这个入口。
func (b *Bus) SubscribeWithSubject(subject string, factory func() proto.Message, h HandlerWithSubject) error {
	if b == nil || b.nc == nil {
		return fmt.Errorf("nats bus not initialized")
	}
	sub, err := b.nc.Subscribe(subject, func(m *nats.Msg) {
		pb := factory()
		if err := proto.Unmarshal(m.Data, pb); err != nil {
			// 解码失败的消息直接丢弃：Core 无重投，重试也不会变对。
			return
		}
		_ = h(m.Subject, pb)
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
