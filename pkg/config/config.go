// Package config 服务器配置加载（设计文档 19.1）。
//
// 约定：
//   - YAML 按「服务 × 环境」拆分：configs/<service>/<service>-<env>.yaml；
//   - 支持 ${VAR} 环境变量注入（敏感信息如 MySQL DSN 不入库，见 3.2 目录铁律）；
//   - 时间类字段统一用字符串（"45s"），由 ParseDuration 解析，避免 yaml 库
//     将 Duration 当纳秒整数解析的坑。
package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config 全服务配置（各服务只使用自己需要的段，未用段留空）
// PayConfig 支付渠道配置（R3.3）。
//
// channel 目前只支持 "sandbox"（HMAC 模拟渠道，供联调与测试）；
// 真实渠道接入时新增适配器并在此登记名称，未登记的名称启动直接报错。
type PayConfig struct {
	Channel       string `yaml:"channel"`        // sandbox
	SandboxSecret string `yaml:"sandbox_secret"` // HMAC 密钥；空则拒绝启用沙箱渠道
	SignWindow    string `yaml:"sign_window"`    // 回调时间窗，默认 5m
}

type Config struct {
	Server   ServerConfig   `yaml:"server"`
	Crypto   CryptoConfig   `yaml:"crypto"`
	RPC      RPCConfig      `yaml:"rpc"`
	Redis    RedisConfig    `yaml:"redis"`
	Kafka    KafkaConfig    `yaml:"kafka"`
	Etcd     EtcdConfig     `yaml:"etcd"`
	MySQL    MySQLConfig    `yaml:"mysql"`
	Log      LogConfig      `yaml:"log"`
	Metrics  MetricsConfig  `yaml:"metrics"`
	Shutdown ShutdownConfig `yaml:"shutdown"`

	// 登录服
	Token TokenConfig `yaml:"token"`
	Pay   PayConfig   `yaml:"pay"`
	// 游戏服
	Actor  ActorConfig  `yaml:"actor"`
	Cross  CrossConfig  `yaml:"cross"`
	Flush  FlushConfig  `yaml:"flush"`
	IDTab  IDTabConfig  `yaml:"idtab"`
	Center CenterConfig `yaml:"center"`
	// 中心服
	Report     ReportConfig     `yaml:"report"`
	Rank       RankConfig       `yaml:"rank"`
	Stats      StatsConfig      `yaml:"stats"`
	RelayKafka RelayKafkaConfig `yaml:"relay_kafka"`
	// 跨服节点
	Battle   BattleConfig   `yaml:"battle"`
	XService XServiceConfig `yaml:"xservice"`
}

type ServerConfig struct {
	Name             string `yaml:"name"`
	Env              string `yaml:"env"`
	TCPAddr          string `yaml:"tcp_addr"`  // 网关：客户端长连接
	HTTPAddr         string `yaml:"http_addr"` // 登录服：HTTPS API
	GRPCAddr         string `yaml:"grpc_addr"` // 游戏/中心/跨服：gRPC
	MaxConns         int    `yaml:"max_conns"` // 单网关最大连接
	MaxPacketSize    int    `yaml:"max_packet_size"`
	ReadTimeout      string `yaml:"read_timeout"`
	WriteQueueSize   int    `yaml:"write_queue_size"`
	SessionReconnTTL string `yaml:"session_reconnect_ttl"`
}

type CryptoConfig struct {
	Enabled          bool   `yaml:"enabled"`
	RSAPrivateKey    string `yaml:"rsa_private_key"`
	RSAKeyID         string `yaml:"rsa_key_id"`
	PrevRSAPublicKey string `yaml:"prev_rsa_public_key"` // 轮换期上一把公钥
}

type RPCConfig struct {
	EtcdEndpoints     []string `yaml:"etcd_endpoints"`
	ServicePrefix     string   `yaml:"service_prefix"`
	ChannelPerTarget  int      `yaml:"channel_per_target"`
	MaxStreamsPerChan int      `yaml:"max_streams_per_channel"`
	DialTimeout       string   `yaml:"dial_timeout"`
	Keepalive         string   `yaml:"keepalive"`
}

type RedisConfig struct {
	Addrs       []string `yaml:"addrs"`
	Password    string   `yaml:"password"`
	DB          int      `yaml:"db"`
	PoolSize    int      `yaml:"pool_size"`
	MinIdleConn int      `yaml:"min_idle_conns"` // 必须 ≤ PoolSize（19.1）
}

type KafkaConfig struct {
	Brokers              []string `yaml:"brokers"`
	ProducerRequiredAcks string   `yaml:"producer_required_acks"`
	ConsumerGroup        string   `yaml:"consumer_group"`
}

type EtcdConfig struct {
	Endpoints         []string `yaml:"endpoints"`
	LeaseTTL          int64    `yaml:"lease_ttl"`          // 秒
	KeepaliveInterval int64    `yaml:"keepalive_interval"` // 秒
	LeaderKey         string   `yaml:"leader_key"`         // 中心服选主
}

type MySQLConfig struct {
	DSN          string `yaml:"dsn"`
	MaxOpenConns int    `yaml:"max_open_conns"` // 业务池（第 14 章容量推导）
	MaxIdleConns int    `yaml:"max_idle_conns"`
}

type LogConfig struct {
	Level       string `yaml:"level"`
	Format      string `yaml:"format"` // json / text
	SampleFirst int    `yaml:"sample_first"`
}

type MetricsConfig struct {
	Enable bool   `yaml:"enable"`
	Addr   string `yaml:"addr"`
}

type ShutdownConfig struct {
	TotalTimeout string `yaml:"total_timeout"`
	DrainTimeout string `yaml:"drain_timeout"`
	FlushTimeout string `yaml:"flush_timeout"`
}

type TokenConfig struct {
	TTL           string `yaml:"ttl"`
	RSAPrivateKey string `yaml:"rsa_private_key"` // 签发 Token 用，与网关握手密钥分离
}

type ActorConfig struct {
	ShardCount  int `yaml:"shard_count"` // 2 的幂
	MailboxSize int `yaml:"mailbox_size"`
}

type FlushConfig struct {
	ShardCount int    `yaml:"shard_count"` // 10 分片分时落库（worker 共享连接池）
	Interval   string `yaml:"interval"`
	Stagger    string `yaml:"stagger"` // 相邻 worker 唤醒间隔
	BatchRows  int    `yaml:"batch_rows"`
	DirtyTTL   string `yaml:"dirty_ttl"`
	// DisableInlineWritebehind 关闭游戏服进程内的写穿 worker。
	//
	// Redis 权威状态下，脏队列的唯一消费者是 writebehind worker。默认开启进程内
	// worker，保证「只启动游戏服」也能落库；已部署独立 cmd/flusher 时置 true 省资源。
	// 两者同时运行安全：SQL 版本栅栏与带版本校验的 Ack 保证不会重复生效。
	DisableInlineWritebehind bool `yaml:"disable_inline_writebehind"`
}

type IDTabConfig struct {
	BuildOnStart      bool   `yaml:"build_on_start"`
	BuildPageSize     int    `yaml:"build_page_size"`
	ReconcileInterval string `yaml:"reconcile_interval"`
}

type CenterConfig struct {
	Addr             string `yaml:"addr"` // 多地址逗号分隔，故障自动切换
	ReconnectMaxWait string `yaml:"reconnect_max_wait"`
	ReportBuffer     int    `yaml:"report_buffer"`
}

type ReportConfig struct {
	HeartbeatInterval string `yaml:"heartbeat_interval"`
	HeartbeatTimeout  string `yaml:"heartbeat_timeout"`
	RelayRetry        int    `yaml:"relay_retry"`
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
	AggregateWindows []string `yaml:"aggregate_windows"`
	SnapshotInterval string   `yaml:"snapshot_interval"`
	MySQLShard       int      `yaml:"mysql_shard"`
}

type RelayKafkaConfig struct {
	Brokers []string `yaml:"brokers"`
	Topic   string   `yaml:"topic"` // center.relay
}

type BattleConfig struct {
	RoomMaxIdle string `yaml:"room_max_idle"`
	ResultRetry int    `yaml:"result_retry"`
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
func (c *Config) normalize() {
	c.Kafka.Brokers = filterNonEmpty(c.Kafka.Brokers)
	c.Redis.Addrs = filterNonEmpty(c.Redis.Addrs)
	c.Etcd.Endpoints = filterNonEmpty(c.Etcd.Endpoints)
	c.RPC.EtcdEndpoints = filterNonEmpty(c.RPC.EtcdEndpoints)
}

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
	return nil
}

// ParseDuration 解析 "45s" / "10m" / "24h" 等时长字段
func ParseDuration(s string) (time.Duration, error) {
	if s == "" {
		return 0, nil
	}
	return time.ParseDuration(s)
}

// GetDialTimeout 返回 RPC 拨号超时，未配置默认 5s。
func (c *Config) GetDialTimeout() time.Duration {
	d, err := ParseDuration(c.RPC.DialTimeout)
	if err != nil || d == 0 {
		return 5 * time.Second
	}
	return d
}
