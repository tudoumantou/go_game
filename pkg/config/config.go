// Package config 服务器配置加载（设计文档 19.1）。
//
// 约定：
//   - YAML 按「服务 × 环境」拆分：configs/<service>/<service>-<env>.yaml；
//   - 支持 ${VAR} 环境变量注入（敏感信息如 MySQL DSN 不入库，见 3.2 目录铁律）；
//   - 时间类字段统一用字符串（"45s"），由 ParseDuration 解析，避免 yaml 库
//     将 Duration 当纳秒整数解析的坑。
//
// 架构位置：五个进程（gateway/game/center/cross/login）的启动入口都先
// Load 本包——它是全项目唯一读 YAML 的地方，其余组件只吃 *Config。
// 协作关系：Config 各段被对应包校验/消费（actor 段→pkg/actor、redis
// 段→pkg/cache、crypto 段→pkg/crypto……）；本包不反向依赖任何业务包，
// 保持「叶子层」。
// 关键取舍：Load = 读文件 → ExpandEnv → Unmarshal → normalize →
// Validate 五步，前四步容错（缺省/空值有默认或降级），Validate 一步
// 硬失败——启动期把配置错误全部暴露，不留到运行期。
package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// PayConfig 支付渠道配置（R3.3）。
//
// channel 目前只支持 "sandbox"（HMAC 模拟渠道，供联调与测试）；
// 真实渠道接入时新增适配器并在此登记名称，未登记的名称启动直接报错。
type PayConfig struct {
	Channel       string `yaml:"channel"`        // sandbox
	SandboxSecret string `yaml:"sandbox_secret"` // HMAC 密钥；空则拒绝启用沙箱渠道
	SignWindow    string `yaml:"sign_window"`    // 回调时间窗，默认 5m
}

// Config 全服务配置（各服务只使用自己需要的段，未用段留空）
//
// 分段按「公共 → 登录服 → 游戏服 → 中心服 → 跨服」组织，与五个进程
// 的职责一一对应；一个进程只填自己那几段，其余留零值。
// Go 知识点：各段是嵌套**值**字段（非指针）——YAML 缺段时自动为零值，
// 业务侧读出来就是「未配置」，不需要判指针 nil。
type Config struct {
	// 公共段：五个进程都可能用到
	Server   ServerConfig   `yaml:"server"`
	Admin    AdminConfig    `yaml:"admin"`
	Crypto   CryptoConfig   `yaml:"crypto"`
	RPC      RPCConfig      `yaml:"rpc"`
	Redis    RedisConfig    `yaml:"redis"`
	Kafka    KafkaConfig    `yaml:"kafka"`
	Etcd     EtcdConfig     `yaml:"etcd"`
	MySQL    MySQLConfig    `yaml:"mysql"`
	Log      LogConfig      `yaml:"log"`
	Limit    LimitConfig    `yaml:"limit"`
	Metrics  MetricsConfig  `yaml:"metrics"`
	Shutdown ShutdownConfig `yaml:"shutdown"`

	// 登录服
	Token TokenConfig `yaml:"token"`
	Pay   PayConfig   `yaml:"pay"`
	// 游戏服
	Actor   ActorConfig   `yaml:"actor"`
	Cross   CrossConfig   `yaml:"cross"`
	Flush   FlushConfig   `yaml:"flush"`
	Replica ReplicaConfig `yaml:"replica"`
	IDTab   IDTabConfig   `yaml:"idtab"`
	Center  CenterConfig  `yaml:"center"`
	// 中心服
	Report     ReportConfig     `yaml:"report"`
	Rank       RankConfig       `yaml:"rank"`
	Stats      StatsConfig      `yaml:"stats"`
	RelayKafka RelayKafkaConfig `yaml:"relay_kafka"`
	// 跨服节点
	Battle   BattleConfig   `yaml:"battle"`
	XService XServiceConfig `yaml:"xservice"`
	// 大地图（SLG，世界地图子系统，设计文档 17.1）
	//
	// 用值字段而不是指针：与其它段保持一致的「缺段=零值」语义。
	// 旧配置文件（gateway/game/login 等）不带 map: 段时，
	// Enabled=false，Validate 跳过地图校验，现有进程完全无感。
	// 只有 map.enabled=true 的进程（cmd/map）才会执行地图段校验。
	Map MapConfig `yaml:"map"`
}

type ServerConfig struct {
	Name             string `yaml:"name"`                  // 服务名，日志/指标标签
	Env              string `yaml:"env"`                   // dev/staging/prod
	TCPAddr          string `yaml:"tcp_addr"`              // 网关：客户端长连接
	HTTPAddr         string `yaml:"http_addr"`             // 登录服：HTTPS API
	GRPCAddr         string `yaml:"grpc_addr"`             // 游戏/中心/跨服：gRPC
	MaxConns         int    `yaml:"max_conns"`             // 单网关最大连接
	MaxPacketSize    int    `yaml:"max_packet_size"`       // 含加密开销（NonceSize+Overhead）的上限
	ReadTimeout      string `yaml:"read_timeout"`          // 空闲读超时（心跳保活依据）
	WriteQueueSize   int    `yaml:"write_queue_size"`      // 每连接写队列；满则背压/断连
	SessionReconnTTL string `yaml:"session_reconnect_ttl"` // 断线重连会话保留窗
}

// LimitConfig 限流阈值。
//
// 语义：0 = 用内置默认（默认是**开启**的，漏配不能变成不限流）；
// 负数 = 关闭该类限流（排查问题时的临时开关）。
type LimitConfig struct {
	// PacketRate/PacketBurst 每连接每秒包数与突发容量。
	// 默认 20/40：够一个正常客户端在登录后连发十几个请求，
	// 又能压住「一个连接把 Actor 信箱打满」的刷包。
	PacketRate  float64 `yaml:"packet_rate"`
	PacketBurst float64 `yaml:"packet_burst"`
	// ChatIntervalMS 聊天最小间隔（毫秒），默认 1000。
	// 比包级限流更严：聊天会写库 + 推对手 + 进 Kafka，成本高得多。
	ChatIntervalMS int `yaml:"chat_interval_ms"`
	// LoginPerMin 每 IP 每分钟登录次数，默认 20。
	// 登录要查库、签 token，是爆破的首选目标。
	LoginPerMin float64 `yaml:"login_per_min"`
}

// 内置默认值。
const (
	DefaultPacketRate     = 20
	DefaultPacketBurst    = 40
	DefaultChatIntervalMS = 1000
	DefaultLoginPerMinute = 20
)

// PacketLimit 返回连接级限流参数；rate<=0 表示关闭。
func (c LimitConfig) PacketLimit() (rate, burst float64) {
	if c.PacketRate < 0 {
		return 0, 0
	}
	rate, burst = c.PacketRate, c.PacketBurst
	if rate == 0 {
		rate = DefaultPacketRate
	}
	if burst == 0 {
		burst = DefaultPacketBurst
	}
	return rate, burst
}

// ChatInterval 返回聊天最小间隔；<=0 表示关闭。
func (c LimitConfig) ChatInterval() time.Duration {
	if c.ChatIntervalMS < 0 {
		return 0
	}
	ms := c.ChatIntervalMS
	if ms == 0 {
		ms = DefaultChatIntervalMS
	}
	return time.Duration(ms) * time.Millisecond
}

// LoginLimit 返回每 IP 每分钟登录次数；<=0 表示关闭。
func (c LimitConfig) LoginLimit() float64 {
	if c.LoginPerMin < 0 {
		return 0
	}
	if c.LoginPerMin == 0 {
		return DefaultLoginPerMinute
	}
	return c.LoginPerMin
}

// AdminConfig 管理接口（GM 操作）鉴权。
//
// Token 为空时管理接口只接受本机来源（127.0.0.1/::1）：默认部署不必
// 额外配置也能给运维用 curl；一旦暴露到内网/公网就必须配 Token。
type AdminConfig struct {
	Token string `yaml:"token"`
}

type CryptoConfig struct {
	Enabled          bool   `yaml:"enabled"`             // 关闭后走明文（仅内网联调）
	RSAPrivateKey    string `yaml:"rsa_private_key"`     // PEM 路径；私钥只进网关进程
	RSAKeyID         string `yaml:"rsa_key_id"`          // 密钥标识，日志/对账用
	PrevRSAPublicKey string `yaml:"prev_rsa_public_key"` // 轮换期上一把公钥（客户端过渡期兼容）
}

type RPCConfig struct {
	EtcdEndpoints     []string `yaml:"etcd_endpoints"`     // 服务发现地址
	ServicePrefix     string   `yaml:"service_prefix"`     // etcd key 前缀
	ChannelPerTarget  int      `yaml:"channel_per_target"` // 每目标几条 gRPC 流（并发上限）
	MaxStreamsPerChan int      `yaml:"max_streams_per_channel"`
	DialTimeout       string   `yaml:"dial_timeout"` // 默认 5s（GetDialTimeout）
	Keepalive         string   `yaml:"keepalive"`    // 连接保活参数
}

type RedisConfig struct {
	Addrs       []string `yaml:"addrs"`          // 多地址 = 集群模式
	Password    string   `yaml:"password"`       // 经 ${VAR} 注入，不入库
	DB          int      `yaml:"db"`             // 仅单点模式有效
	PoolSize    int      `yaml:"pool_size"`      // 连接池上限
	MinIdleConn int      `yaml:"min_idle_conns"` // 必须 ≤ PoolSize（19.1）

	// CircuitBreaker 熔断（docs/缓存架构设计.md §9）：Redis 故障时快速失败，
	// 不让每条命令都等满超时再把降级路径拖死。
	CircuitBreaker CircuitBreakerConfig `yaml:"circuit_breaker"`
}

// CircuitBreakerConfig 熔断参数。
//
// 三种配置形态（与 limit 段同一惯例）：
//   - 不写或 0 值 = 默认开启（阈值 5、冷却 5s、探测 1）；
//   - threshold 负数 = 关闭熔断（每条命令照旧等超时）；
//   - 显式给值 = 按给定值。
type CircuitBreakerConfig struct {
	// Threshold 连续失败多少次打开熔断。
	Threshold int `yaml:"threshold"`
	// Cooldown 打开后多久进入半开（放行探测）。
	Cooldown string `yaml:"cooldown"`
	// Probes 半开状态放行的探测请求数。
	Probes int `yaml:"probes"`
}

// CooldownDuration 冷却时长；空值返回 0（由 cache 侧补默认 5s）。
func (c CircuitBreakerConfig) CooldownDuration() time.Duration {
	d, _ := ParseDuration(c.Cooldown)
	return d
}

// Validate 校验熔断段（配错要在启动第一秒炸，而不是等故障发生时才发现）。
func (c CircuitBreakerConfig) Validate() error {
	if c.Threshold < -1 {
		return fmt.Errorf("redis.circuit_breaker.threshold: 只接受 0（默认）/正数/ -1（关闭），得到 %d", c.Threshold)
	}
	if c.Cooldown != "" {
		d, err := ParseDuration(c.Cooldown)
		if err != nil {
			return fmt.Errorf("redis.circuit_breaker.cooldown: %w", err)
		}
		if d <= 0 {
			return fmt.Errorf("redis.circuit_breaker.cooldown: 必须为正，得到 %q", c.Cooldown)
		}
	}
	if c.Probes < 0 {
		return fmt.Errorf("redis.circuit_breaker.probes: 不能为负，得到 %d", c.Probes)
	}
	return nil
}

type KafkaConfig struct {
	Brokers              []string `yaml:"brokers"`                // 空 = 自动禁用 Kafka
	ProducerRequiredAcks string   `yaml:"producer_required_acks"` // all/1/0
	ConsumerGroup        string   `yaml:"consumer_group"`
}

type EtcdConfig struct {
	Endpoints         []string `yaml:"endpoints"`          // 注册/选主/配置中心
	LeaseTTL          int64    `yaml:"lease_ttl"`          // 秒
	KeepaliveInterval int64    `yaml:"keepalive_interval"` // 秒
	LeaderKey         string   `yaml:"leader_key"`         // 中心服选主
}

type MySQLConfig struct {
	DSN          string `yaml:"dsn"`            // 经 ${VAR} 注入
	MaxOpenConns int    `yaml:"max_open_conns"` // 业务池（第 14 章容量推导）
	MaxIdleConns int    `yaml:"max_idle_conns"`
}

type LogConfig struct {
	Level       string `yaml:"level"`        // debug/info/warn/error
	Format      string `yaml:"format"`       // json / text
	SampleFirst int    `yaml:"sample_first"` // 启动期前 N 条全量，之后采样
}

type MetricsConfig struct {
	Enable bool   `yaml:"enable"`
	Addr   string `yaml:"addr"` // /metrics 暴露地址
}

type ShutdownConfig struct {
	TotalTimeout string `yaml:"total_timeout"` // 关闭总预算，超时强退
	DrainTimeout string `yaml:"drain_timeout"` // 等在途消息（actor.Drain）
	FlushTimeout string `yaml:"flush_timeout"` // 等 Redis 脏数据落库
	// RegisterDrain 摘注册后等待网关 resolver 感知的时间（默认 3s）。
	// 立即停 RPC 会让这段时间的请求打到已关闭实例——滚动发布实测约 0.6% 失败。
	RegisterDrain string `yaml:"register_drain"`
}

type TokenConfig struct {
	TTL           string `yaml:"ttl"`             // Token 有效期
	RSAPrivateKey string `yaml:"rsa_private_key"` // 签发 Token 用，与网关握手密钥分离
}

type ActorConfig struct {
	ShardCount  int `yaml:"shard_count"`  // 2 的幂（位掩码寻址，Validate 强校验）
	MailboxSize int `yaml:"mailbox_size"` // 每 Actor 有界信箱容量
}

type FlushConfig struct {
	ShardCount int    `yaml:"shard_count"` // 10 分片分时落库（worker 共享连接池）
	Interval   string `yaml:"interval"`
	Stagger    string `yaml:"stagger"`    // 相邻 worker 唤醒间隔
	BatchRows  int    `yaml:"batch_rows"` // 单批写回行数
	DirtyTTL   string `yaml:"dirty_ttl"`  // 脏 key 存活上限（异常兜底）
}

// ReplicaConfig Redis 副本的异步回写（R14）。
//
// 消费方：internal/game/server_start.go 的 initRedis → writebehind.NewReplica。
// 零值全部走组件默认（200ms / 256 / 100000 / 300），未配 replica 段的老配置
// 无需改动即可获得异步回写。
type ReplicaConfig struct {
	// Interval 批量刷写周期。200ms 是「跨节点读的新鲜度」与「Redis 写压力」
	// 的折中；调到秒级以上会让别的玩家看到明显过期的数据（排行榜/阵容）。
	Interval string `yaml:"interval"`
	// MaxBatch 单轮最多刷多少个玩家（一轮 = 一次 pipeline RTT）。
	MaxBatch int `yaml:"max_batch"`
	// MaxPending 待刷玩家上限；超限丢最旧的（Redis 长时间不可用时保内存）。
	MaxPending int `yaml:"max_pending"`
	// MaxAttempts 单条记录最大重试次数；超过即丢弃并告警。
	MaxAttempts int `yaml:"max_attempts"`
}

type IDTabConfig struct { // BuildPageSize 构建 ID 表/归属索引时的分页行数（默认 5000）。
	// 消费方：internal/game/server_start.go 装配时调 IDTable.SetPageSize
	// 与 OwnerIndex.SetPageSize。配 0 或负数走组件默认值。
	BuildPageSize int `yaml:"build_page_size"`

	// ReconcileInterval 数量核对周期（默认 10m）。
	// 消费方：internal/game/server.go 的 maintainIDTables / maintainOwnerIndexes。
	// 解析失败或 <=0 时回落默认值——核对停摆不会报错，只会让索引悄悄漂移。
	ReconcileInterval string `yaml:"reconcile_interval"`

	// 关于 build_on_start（v3.21 删除）：它从来没有被读过，而且语义是伪需求
	// ——构建本来就是 `go buildIDTables(...)` 异步进行的，不阻塞启动；
	// 把它关掉只会让 ID 表永不 ready，所有存在性判断回落 DB、性能掉一半，
	// 日志一个字都没有。留着它等于给「性能掉了但没人知道」留一个开关。
	//
	// 旧配置里的 build_on_start 会被 yaml 忽略（未开 KnownFields），
	// 不报错也不生效——与它此前的行为一致。

	// Mappings 一对多索引的声明（见 docs/缓存架构设计.md）。
	//
	// 只有「一个归属者对应多行」的表需要配（伙伴/装备/邮件/好友）；
	// 一对一的表（签到/属性/任务进度）配了是纯浪费——按 player_id 一次
	// 查询就够，白多一份索引要维护。
	Mappings []CacheMapping `yaml:"mappings"`
}

// CacheMapping 声明一张表的归属关系，启动时据此在 Redis 建
// by_owner:{table} 这个 ZSET（score = 归属者，member = 实体 id）。
type CacheMapping struct {
	// Table 表名。
	Table string `yaml:"table"`
	// By 归属者列，成为 ZSET 的 score（如 role_id / player_id / guild_id）。
	//
	// preload_all 时可留空：整表预热不需要归属列。
	By string `yaml:"by"`
	// Key 实体 id 列，成为 ZSET 的 member（通常是主键）。
	//
	// 必填：索引构建与实体预热都以它为 keyset 分页游标。
	Key string `yaml:"key"`
	// PreloadAll 启动时连整行数据一起搬进 Redis（公会、排行榜这类
	// 「上线后几乎必然被全量访问」的表）。
	//
	// 历史注记：曾有 LoadOnLogin（登录时按角色展开）与它互斥。登录展开
	// 入口在 v3.9 删除后该字段就成了死配置（写了不生效），v3.11 一并删除。
	PreloadAll bool `yaml:"preload_all"`
}

// Validate 校验声明是否自洽。
func (m CacheMapping) Validate() error {
	if m.Table == "" {
		return fmt.Errorf("idtab.mappings: table 不能为空")
	}
	if m.Key == "" {
		return fmt.Errorf("idtab.mappings[%s]: key（实体 id 列）必填——构建与预热都以它为分页游标", m.Table)
	}
	if m.PreloadAll {
		// by 可空：整表预热只搬数据，不建归属索引
		return nil
	}
	if m.By == "" {
		return fmt.Errorf("idtab.mappings[%s]: 非 preload_all 必须声明 by（归属者列）", m.Table)
	}
	if m.By == m.Key {
		return fmt.Errorf("idtab.mappings[%s]: by 与 key 不能是同一列（%s）", m.Table, m.Key)
	}
	return nil
}

type CenterConfig struct {
	Addr             string `yaml:"addr"` // 多地址逗号分隔，故障自动切换
	ReconnectMaxWait string `yaml:"reconnect_max_wait"`
	ReportBuffer     int    `yaml:"report_buffer"` // 上报队列深度，满则丢弃/背压
}

type ReportConfig struct {
	HeartbeatInterval string `yaml:"heartbeat_interval"` // 心跳上报周期
	HeartbeatTimeout  string `yaml:"heartbeat_timeout"`  // 超时判定掉线
	RelayRetry        int    `yaml:"relay_retry"`        // 转发重试次数
}

type RankConfig struct {
	SnapshotInterval string   `yaml:"snapshot_interval"`
	ArchiveCron      string   `yaml:"archive_cron"`  // 保留：调度已改为「赛季到期」判定，见 archive.Service
	SeasonDays       int      `yaml:"season_days"`   // 赛季长度（天），默认 30
	TopN             int      `yaml:"archive_top_n"` // 归档前 N 名，默认 1000
	RankTypes        []string `yaml:"rank_types"`    // 需要归档的榜单
	RewardTop        int      `yaml:"reward_top"`    // 前 N 名发赛季奖励，0 = 不发
	RewardItemID     int32    `yaml:"reward_item_id"`
	RewardCount      int64    `yaml:"reward_count"`
}

type StatsConfig struct {
	AggregateWindows []string `yaml:"aggregate_windows"` // 聚合窗口（如 1m/5m/1h）
	SnapshotInterval string   `yaml:"snapshot_interval"` // 快照落库周期
	MySQLShard       int      `yaml:"mysql_shard"`       // 统计库分片序号
}

type RelayKafkaConfig struct {
	Brokers []string `yaml:"brokers"`
	Topic   string   `yaml:"topic"` // center.relay
}

type BattleConfig struct {
	RoomMaxIdle string `yaml:"room_max_idle"` // 房间空闲回收时限
	ResultRetry int    `yaml:"result_retry"`  // 战绩回写重试次数
}

// CrossConfig 小组跨服节点地址（10.2：生产由中心服 GroupUpdate 下发，此处为
// 静态配置的降级路径 —— P8 中心服上线前，游戏服直接从配置读跨服地址）。
type CrossConfig struct {
	Nodes         []string `yaml:"nodes"`          // 跨服节点 gRPC 地址
	BattleTimeout string   `yaml:"battle_timeout"` // 单场战斗时限（超时平局兜底）
	ReconnectMax  string   `yaml:"reconnect_max"`  // 断流重连最大退避
}

type XServiceConfig struct {
	MaxStreamsPerGame int `yaml:"max_streams_per_game"` // 每个游戏服维持一条 CrossService 流
}

// Load 读取配置文件：先展开 ${VAR} 环境变量，再 YAML 解析
//
// 五步流水线（任一步失败即返回 error，进程应直接退出——配置错不是
// 能「带病运行」的问题）：
//  1. ReadFile：文件不存在/权限错在此暴露；
//  2. ExpandEnv：${VAR} 替换为环境变量值，**未设置的变量展开为空串**
//     （不是报错）——这正是 normalize 要过滤空值的原因；
//  3. yaml.Unmarshal：按 struct tag 映射，类型不符在此暴露；
//  4. normalize：清理空值，让「未配置」归一成空列表；
//  5. Validate：跨字段约束（如 min_idle ≤ pool_size）。
//
// Go 知识点：错误链全程 %w 包装，运维看到的是一条完整路径
// （read config x: ... / validate config x: server.name 不能为空），
// 而不是孤立的底层错误。
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %s: %w", path, err)
	}
	expanded := os.ExpandEnv(string(raw))

	var cfg Config
	if err := yaml.Unmarshal([]byte(expanded), &cfg); err != nil {
		return nil, fmt.Errorf("parse config %s: %w", path, err)
	}
	cfg.normalize()
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validate config %s: %w", path, err)
	}
	return &cfg, nil
}

// normalize 清理空值：
//   - 环境变量未设置时 ${KAFKA_BROKERS} 会展开为空串，过滤后 broker 列表为空 → Kafka 自动降级禁用
//   - 其余列表字段同理
//
// 为什么不在解析前就报错：本地开发/单功能部署经常不配 Kafka/Etcd，
// 「空列表 = 功能关闭」让同一份配置骨架适配多种部署形态。
// Go 知识点：filterNonEmpty 返回空 slice（非 nil）——len()==0 的判断
// 对两者等价，但空 slice 更友好（序列化成 [] 而非 null，日志与调试
// 输出更干净）。
func (c *Config) normalize() {
	c.Kafka.Brokers = filterNonEmpty(c.Kafka.Brokers)
	c.Redis.Addrs = filterNonEmpty(c.Redis.Addrs)
	c.Etcd.Endpoints = filterNonEmpty(c.Etcd.Endpoints)
	c.RPC.EtcdEndpoints = filterNonEmpty(c.RPC.EtcdEndpoints)
}

// filterNonEmpty 移除空串元素，保持原顺序。
//
// 预分配 cap=len(in)：最坏情况（无空串）一次分配到位，不触发 append
// 扩容——slice 扩容是翻倍式增长，会白白多分配一倍内存。
func filterNonEmpty(in []string) []string {
	out := make([]string, 0, len(in))
	for _, s := range in {
		if s != "" {
			out = append(out, s)
		}
	}
	return out
}

// Validate 必填字段校验（与 19.1 端口规划/字段对齐）
//
// 分两类：存在性（name/env/监听地址必须有）与跨字段约束（min_idle ≤
// pool_size、shard_count 为 2 的幂）。
// Go 知识点：`x&(x-1) != 0` 是判 2 的幂的经典位运算——2 的幂二进制
// 只有一位 1，减 1 后低位全变 1，相与为 0；外层 `> 0` 必须保留：
// 0 的 0&-1 也是 0，会被误判为 2 的幂。
func (c *Config) Validate() error {
	if c.Server.Name == "" {
		return fmt.Errorf("server.name 不能为空")
	}
	if c.Server.Env == "" {
		return fmt.Errorf("server.env 不能为空")
	}
	// 各服务至少声明一个监听地址
	if c.Server.TCPAddr == "" && c.Server.HTTPAddr == "" && c.Server.GRPCAddr == "" {
		return fmt.Errorf("server 至少需要一个监听地址（tcp_addr/http_addr/grpc_addr）")
	}
	if len(c.Redis.Addrs) > 0 && c.Redis.MinIdleConn > c.Redis.PoolSize {
		return fmt.Errorf("redis.min_idle_conns(%d) 必须 ≤ pool_size(%d)", c.Redis.MinIdleConn, c.Redis.PoolSize)
	}
	if c.Actor.ShardCount > 0 && c.Actor.ShardCount&(c.Actor.ShardCount-1) != 0 {
		return fmt.Errorf("actor.shard_count(%d) 必须是 2 的幂", c.Actor.ShardCount)
	}
	// flush 分片用 crc32(key) % shard_count 取模寻址（12.4），不要求 2 的幂；
	// actor 分片用 hash(actorID) & shardMask 位掩码寻址（第 7 章），必须 2 的幂。
	if c.Flush.ShardCount < 0 {
		return fmt.Errorf("flush.shard_count(%d) 不能为负", c.Flush.ShardCount)
	}
	// 地图段只在启用时校验：未启用地图的进程（gateway/game/login…）
	// 不该因为配置里没有 map 段而启动失败。
	if c.Map.Enabled {
		if err := c.Map.Validate(); err != nil {
			return fmt.Errorf("map: %w", err)
		}
	}
	if err := c.Redis.CircuitBreaker.Validate(); err != nil {
		return err
	}
	// 一对多索引声明：配错会表现为「登录后功能列表为空」且不报错，
	// 所以放在启动校验里，让配置错误第一秒就炸。
	seen := make(map[string]struct{}, len(c.IDTab.Mappings))
	for _, m := range c.IDTab.Mappings {
		if err := m.Validate(); err != nil {
			return err
		}
		if _, dup := seen[m.Table]; dup {
			return fmt.Errorf("idtab.mappings: 表 %s 重复声明", m.Table)
		}
		seen[m.Table] = struct{}{}
	}
	return nil
}

// MapConfig 是 SLG 大地图子系统的配置（设计文档 17.1）。
//
// 字段分四类：
//   - 拓扑类（尺寸/网格）：只能在停世界时迁移，不能热更新；
//   - 调度类（tick/邮箱/预算）：影响吞吐与延迟，改配置需重启场景；
//   - 事务类（workers/queue/timeout）：I/O 参数；
//   - 玩法类（采集速率/刷新/战斗队列）：可以按世界覆盖（M2 起）。
//
// 时间字段用字符串 + ParseDuration，理由与全项目其它段一致：
// yaml.v3 对 time.Duration 只认纳秒整数。
type MapConfig struct {
	Enabled bool `yaml:"enabled"`
	// WorldIDs 本进程加载的世界编号。空 = 不加载任何世界（只提供管理面）。
	WorldIDs []uint64 `yaml:"world_ids"`

	// WorldWidthCells/WorldHeightCells 世界尺寸（格）。
	WorldWidthCells  int64 `yaml:"world_width_cells"`
	WorldHeightCells int64 `yaml:"world_height_cells"`
	// SceneCols/SceneRows 逻辑场景网格。世界尺寸必须能被它整除。
	SceneCols int32 `yaml:"scene_cols"`
	SceneRows int32 `yaml:"scene_rows"`
	// PositionUnitsPerCell 1 格对应的定点单位数（默认 1000）。
	PositionUnitsPerCell int64 `yaml:"position_units_per_cell"`

	// AOIBlockCols/AOIBlockRows 广播块网格；AOICellSize 块内 cell 边长（格）。
	AOIBlockCols int32 `yaml:"aoi_block_cols"`
	AOIBlockRows int32 `yaml:"aoi_block_rows"`
	AOICellSize  int64 `yaml:"aoi_cell_size"`
	// RichBlockSize 富饶块边长（格），自身网格编号，不复用场景编号。
	RichBlockSize int64 `yaml:"rich_block_size"`

	// TickInterval 场景定时唤醒间隔（"1s"）。
	TickInterval string `yaml:"tick_interval"`
	// SceneMailboxSize 业务命令邮箱容量，满即拒收。
	SceneMailboxSize int `yaml:"scene_mailbox_size"`
	// SceneControlCapacity 控制通道容量，必须大于在途事务数。
	SceneControlCapacity int `yaml:"scene_control_capacity"`
	// MaxCommandsPerTurn/MaxDueEventsPerTurn 每轮预算，防热点饿死定时器。
	MaxCommandsPerTurn  int `yaml:"max_commands_per_turn"`
	MaxDueEventsPerTurn int `yaml:"max_due_events_per_turn"`

	// TxWorkers 事务工作池大小；TxQueueCapacity 在途事务上限。
	TxWorkers       int `yaml:"tx_workers"`
	TxQueueCapacity int `yaml:"tx_queue_capacity"`
	// TxTimeout 单次提交超时；CommandDeadline 命令端到端超时。
	// 约束：TxTimeout 必须小于 CommandDeadline（否则命令先超时，
	// 事务还在跑，会产生"客户端认为失败但实际成功"）。
	TxTimeout       string `yaml:"tx_timeout"`
	CommandDeadline string `yaml:"command_deadline"`

	// LeaseTTL/LeaseRenewInterval 世界租约（M2 启用；M1 只做参数校验）。
	LeaseTTL           string `yaml:"lease_ttl"`
	LeaseRenewInterval string `yaml:"lease_renew_interval"`

	// MaxViewWidth/MaxViewHeight 单次视野查询上限（格）。
	MaxViewWidth  int64 `yaml:"max_view_width"`
	MaxViewHeight int64 `yaml:"max_view_height"`
	// ViewPendingBytes 单连接投影积压上限（字节）。
	ViewPendingBytes int64 `yaml:"view_pending_bytes"`
	// AOIPayloadLimitBytes 单次投影消息上限。
	AOIPayloadLimitBytes int `yaml:"aoi_payload_limit_bytes"`

	// MaxBattleQueuePerTarget 单目标战斗队列上限。
	MaxBattleQueuePerTarget int `yaml:"max_battle_queue_per_target"`
	// BattleTimeout 战斗裁决超时。
	BattleTimeout string `yaml:"battle_timeout"`
	// RefreshInterval 富饶块刷新周期。
	RefreshInterval string `yaml:"refresh_interval"`
	// RefreshBatchSize 每批刷新对象数；ResourcePositionAttempts 单点选址尝试次数。
	RefreshBatchSize         int `yaml:"refresh_batch_size"`
	ResourcePositionAttempts int `yaml:"resource_position_attempts"`
	// DeliveryRetryMin/Max 交接单重试退避区间。
	DeliveryRetryMin string `yaml:"delivery_retry_min"`
	DeliveryRetryMax string `yaml:"delivery_retry_max"`
	// MaxPlayerReceipts 单玩家保留的命令回执条数上限（防止幂等表无限增长）。
	MaxPlayerReceipts int64 `yaml:"max_player_receipts"`

	// 玩法规则（M1 内置默认；M2 起由 gamedata 注入）
	MarchSpeedUnitsPerSec int64 `yaml:"march_speed_units_per_sec"`
	CapacityUnits         int64 `yaml:"capacity_units"`
	CollectRateMilli      int64 `yaml:"collect_rate_milli_per_sec"`
}

// MapDefaults 返回地图段的默认值（用于 cmd/map 在配置缺省时补齐）。
func MapDefaults() MapConfig {
	return MapConfig{
		WorldWidthCells:          5000,
		WorldHeightCells:         5000,
		SceneCols:                5,
		SceneRows:                5,
		PositionUnitsPerCell:     1000,
		AOIBlockCols:             4,
		AOIBlockRows:             4,
		AOICellSize:              25,
		RichBlockSize:            100,
		TickInterval:             "1s",
		SceneMailboxSize:         4096,
		SceneControlCapacity:     64,
		MaxCommandsPerTurn:       64,
		MaxDueEventsPerTurn:      64,
		TxWorkers:                4,
		TxQueueCapacity:          256,
		TxTimeout:                "2s",
		CommandDeadline:          "3s",
		LeaseTTL:                 "15s",
		LeaseRenewInterval:       "3s",
		MaxViewWidth:             2560,
		MaxViewHeight:            2560,
		ViewPendingBytes:         1048576,
		AOIPayloadLimitBytes:     65536,
		MaxBattleQueuePerTarget:  100,
		BattleTimeout:            "30s",
		RefreshInterval:          "30m",
		RefreshBatchSize:         20,
		ResourcePositionAttempts: 100,
		DeliveryRetryMin:         "1s",
		DeliveryRetryMax:         "60s",
		MaxPlayerReceipts:        1000,
		MarchSpeedUnitsPerSec:    100000,
		CapacityUnits:            100000,
		CollectRateMilli:         2500,
	}
}

// ApplyDefaults 用默认值补齐所有零值字段。
//
// 只补零值，不覆盖显式配置：这样"配了 0 想要 0"与"没配"无法区分，
// 但地图参数里没有合法取 0 的语义（尺寸/速率/容量为 0 都是错误配置），
// 因此补零是安全的。任何非法值仍会在 Validate 中报错。
func (m *MapConfig) ApplyDefaults() {
	d := MapDefaults()
	if m.WorldWidthCells == 0 {
		m.WorldWidthCells = d.WorldWidthCells
	}
	if m.WorldHeightCells == 0 {
		m.WorldHeightCells = d.WorldHeightCells
	}
	if m.SceneCols == 0 {
		m.SceneCols = d.SceneCols
	}
	if m.SceneRows == 0 {
		m.SceneRows = d.SceneRows
	}
	if m.PositionUnitsPerCell == 0 {
		m.PositionUnitsPerCell = d.PositionUnitsPerCell
	}
	if m.AOIBlockCols == 0 {
		m.AOIBlockCols = d.AOIBlockCols
	}
	if m.AOIBlockRows == 0 {
		m.AOIBlockRows = d.AOIBlockRows
	}
	if m.AOICellSize == 0 {
		m.AOICellSize = d.AOICellSize
	}
	if m.RichBlockSize == 0 {
		m.RichBlockSize = d.RichBlockSize
	}
	if m.TickInterval == "" {
		m.TickInterval = d.TickInterval
	}
	if m.SceneMailboxSize == 0 {
		m.SceneMailboxSize = d.SceneMailboxSize
	}
	if m.SceneControlCapacity == 0 {
		m.SceneControlCapacity = d.SceneControlCapacity
	}
	if m.MaxCommandsPerTurn == 0 {
		m.MaxCommandsPerTurn = d.MaxCommandsPerTurn
	}
	if m.MaxDueEventsPerTurn == 0 {
		m.MaxDueEventsPerTurn = d.MaxDueEventsPerTurn
	}
	if m.TxWorkers == 0 {
		m.TxWorkers = d.TxWorkers
	}
	if m.TxQueueCapacity == 0 {
		m.TxQueueCapacity = d.TxQueueCapacity
	}
	if m.TxTimeout == "" {
		m.TxTimeout = d.TxTimeout
	}
	if m.CommandDeadline == "" {
		m.CommandDeadline = d.CommandDeadline
	}
	if m.LeaseTTL == "" {
		m.LeaseTTL = d.LeaseTTL
	}
	if m.LeaseRenewInterval == "" {
		m.LeaseRenewInterval = d.LeaseRenewInterval
	}
	if m.MaxViewWidth == 0 {
		m.MaxViewWidth = d.MaxViewWidth
	}
	if m.MaxViewHeight == 0 {
		m.MaxViewHeight = d.MaxViewHeight
	}
	if m.ViewPendingBytes == 0 {
		m.ViewPendingBytes = d.ViewPendingBytes
	}
	if m.AOIPayloadLimitBytes == 0 {
		m.AOIPayloadLimitBytes = d.AOIPayloadLimitBytes
	}
	if m.MaxBattleQueuePerTarget == 0 {
		m.MaxBattleQueuePerTarget = d.MaxBattleQueuePerTarget
	}
	if m.BattleTimeout == "" {
		m.BattleTimeout = d.BattleTimeout
	}
	if m.RefreshInterval == "" {
		m.RefreshInterval = d.RefreshInterval
	}
	if m.RefreshBatchSize == 0 {
		m.RefreshBatchSize = d.RefreshBatchSize
	}
	if m.ResourcePositionAttempts == 0 {
		m.ResourcePositionAttempts = d.ResourcePositionAttempts
	}
	if m.DeliveryRetryMin == "" {
		m.DeliveryRetryMin = d.DeliveryRetryMin
	}
	if m.DeliveryRetryMax == "" {
		m.DeliveryRetryMax = d.DeliveryRetryMax
	}
	if m.MaxPlayerReceipts == 0 {
		m.MaxPlayerReceipts = d.MaxPlayerReceipts
	}
	if m.MarchSpeedUnitsPerSec == 0 {
		m.MarchSpeedUnitsPerSec = d.MarchSpeedUnitsPerSec
	}
	if m.CapacityUnits == 0 {
		m.CapacityUnits = d.CapacityUnits
	}
	if m.CollectRateMilli == 0 {
		m.CollectRateMilli = d.CollectRateMilli
	}
}

// Validate 校验地图配置（设计文档 17.1 的启动校验清单）。
//
// 校验失败必须拒绝启动：地图拓扑错误（尺寸不整除、区块对不齐）
// 会在运行期表现为"某个边缘格子永远查不到场景"，
// 这类问题一旦带病上线极难定位。
func (m MapConfig) Validate() error {
	if m.WorldWidthCells <= 0 || m.WorldHeightCells <= 0 {
		return fmt.Errorf("world 尺寸必须为正, got %dx%d", m.WorldWidthCells, m.WorldHeightCells)
	}
	if m.SceneCols <= 0 || m.SceneRows <= 0 {
		return fmt.Errorf("scene 网格必须为正, got %dx%d", m.SceneCols, m.SceneRows)
	}
	if m.WorldWidthCells%int64(m.SceneCols) != 0 || m.WorldHeightCells%int64(m.SceneRows) != 0 {
		return fmt.Errorf("世界 %dx%d 无法被场景网格 %dx%d 整除",
			m.WorldWidthCells, m.WorldHeightCells, m.SceneCols, m.SceneRows)
	}
	if m.PositionUnitsPerCell <= 0 {
		return fmt.Errorf("position_units_per_cell 必须为正, got %d", m.PositionUnitsPerCell)
	}
	if m.WorldWidthCells > (1<<62)/m.PositionUnitsPerCell ||
		m.WorldHeightCells > (1<<62)/m.PositionUnitsPerCell {
		return fmt.Errorf("世界尺寸与单位换算乘积溢出")
	}
	if m.AOIBlockCols <= 0 || m.AOIBlockRows <= 0 {
		return fmt.Errorf("aoi 块网格必须为正, got %dx%d", m.AOIBlockCols, m.AOIBlockRows)
	}
	if m.WorldWidthCells%int64(m.AOIBlockCols) != 0 || m.WorldHeightCells%int64(m.AOIBlockRows) != 0 {
		return fmt.Errorf("世界 %dx%d 无法被 AOI 块 %dx%d 整除",
			m.WorldWidthCells, m.WorldHeightCells, m.AOIBlockCols, m.AOIBlockRows)
	}
	if m.AOICellSize <= 0 {
		return fmt.Errorf("aoi_cell_size 必须为正, got %d", m.AOICellSize)
	}
	if blockW := m.WorldWidthCells / int64(m.AOIBlockCols); blockW%m.AOICellSize != 0 {
		return fmt.Errorf("AOI 块宽 %d 无法被 cell 尺寸 %d 整除", blockW, m.AOICellSize)
	}
	if m.RichBlockSize <= 0 {
		return fmt.Errorf("rich_block_size 必须为正, got %d", m.RichBlockSize)
	}
	if m.WorldWidthCells%m.RichBlockSize != 0 || m.WorldHeightCells%m.RichBlockSize != 0 {
		return fmt.Errorf("世界 %dx%d 无法被富饶块 %d 整除",
			m.WorldWidthCells, m.WorldHeightCells, m.RichBlockSize)
	}
	if m.SceneMailboxSize <= 0 || m.SceneControlCapacity < 2 {
		return fmt.Errorf("scene 邮箱容量非法: mailbox=%d control=%d", m.SceneMailboxSize, m.SceneControlCapacity)
	}
	if m.MaxCommandsPerTurn <= 0 || m.MaxDueEventsPerTurn <= 0 {
		return fmt.Errorf("每轮预算必须为正: commands=%d due=%d", m.MaxCommandsPerTurn, m.MaxDueEventsPerTurn)
	}
	if m.TxWorkers <= 0 || m.TxQueueCapacity < m.TxWorkers {
		return fmt.Errorf("事务池参数非法: workers=%d queue=%d", m.TxWorkers, m.TxQueueCapacity)
	}
	txTimeout, err := ParseDuration(m.TxTimeout)
	if err != nil {
		return fmt.Errorf("tx_timeout 解析失败: %w", err)
	}
	cmdDeadline, err := ParseDuration(m.CommandDeadline)
	if err != nil {
		return fmt.Errorf("command_deadline 解析失败: %w", err)
	}
	if txTimeout <= 0 || cmdDeadline <= 0 {
		return fmt.Errorf("tx_timeout(%s)/command_deadline(%s) 必须为正", m.TxTimeout, m.CommandDeadline)
	}
	if txTimeout >= cmdDeadline {
		return fmt.Errorf("tx_timeout(%s) 必须小于 command_deadline(%s)", m.TxTimeout, m.CommandDeadline)
	}
	leaseTTL, err := ParseDuration(m.LeaseTTL)
	if err != nil {
		return fmt.Errorf("lease_ttl 解析失败: %w", err)
	}
	renew, err := ParseDuration(m.LeaseRenewInterval)
	if err != nil {
		return fmt.Errorf("lease_renew_interval 解析失败: %w", err)
	}
	if renew*3 > leaseTTL {
		return fmt.Errorf("lease_renew_interval(%s) 必须明显小于 lease_ttl(%s)", m.LeaseRenewInterval, m.LeaseTTL)
	}
	if m.MaxViewWidth <= 0 || m.MaxViewHeight <= 0 {
		return fmt.Errorf("视野上限必须为正, got %dx%d", m.MaxViewWidth, m.MaxViewHeight)
	}
	if m.MaxViewWidth > m.WorldWidthCells || m.MaxViewHeight > m.WorldHeightCells {
		return fmt.Errorf("视野上限 %dx%d 超过世界尺寸", m.MaxViewWidth, m.MaxViewHeight)
	}
	if m.ViewPendingBytes <= 0 || m.AOIPayloadLimitBytes <= 0 {
		return fmt.Errorf("投影缓冲参数必须为正")
	}
	if m.MaxBattleQueuePerTarget <= 0 {
		return fmt.Errorf("战斗队列上限必须为正, got %d", m.MaxBattleQueuePerTarget)
	}
	if _, err := ParseDuration(m.BattleTimeout); err != nil {
		return fmt.Errorf("battle_timeout 解析失败: %w", err)
	}
	if _, err := ParseDuration(m.RefreshInterval); err != nil {
		return fmt.Errorf("refresh_interval 解析失败: %w", err)
	}
	if m.RefreshBatchSize <= 0 || m.ResourcePositionAttempts <= 0 {
		return fmt.Errorf("刷新批次与选址尝试必须为正: batch=%d attempts=%d",
			m.RefreshBatchSize, m.ResourcePositionAttempts)
	}
	if m.MaxPlayerReceipts <= 0 {
		return fmt.Errorf("max_player_receipts 必须为正, got %d", m.MaxPlayerReceipts)
	}
	if m.MarchSpeedUnitsPerSec <= 0 || m.CapacityUnits <= 0 || m.CollectRateMilli <= 0 {
		return fmt.Errorf("玩法规则必须为正: speed=%d capacity=%d rate=%d",
			m.MarchSpeedUnitsPerSec, m.CapacityUnits, m.CollectRateMilli)
	}
	return nil
}

// ParseDuration 解析 "45s" / "10m" / "24h" 等时长字段
//
// 空串返回 (0, nil)：约定「未配置」= 零时长，调用方用 GetXxx 系列
// 方法再套默认值——解析层不塞默认，默认集中在取值处，避免两套默认
// 值各自漂移。
// 为什么 Duration 字段在 YAML 里是字符串：yaml.v3 对 time.Duration
// 只认 int64（纳秒），写 "45s" 会直接类型报错；用字符串 + 显式
// ParseDuration 把格式控制权拿回来。
func ParseDuration(s string) (time.Duration, error) {
	if s == "" {
		return 0, nil
	}
	return time.ParseDuration(s)
}

// GetDialTimeout 返回 RPC 拨号超时，未配置默认 5s。
//
// err != nil || d == 0 双条件兜底：解析失败（配了非法值如 "abc"）与
// 未配置（空串 → 0）都回落默认——拨号超时是安全参数，配错时用默认
// 值比启动失败更合理（其余关键字段仍由 Validate 拦）。
func (c *Config) GetDialTimeout() time.Duration {
	d, err := ParseDuration(c.RPC.DialTimeout)
	if err != nil || d == 0 {
		return 5 * time.Second
	}
	return d
}
