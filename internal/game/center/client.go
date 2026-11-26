// internal/game/center/client.go —— 游戏服 → 中心服的 Report 流（设计文档 9.2）
//
// 一条 Report 流既是上报通道（注册/心跳/匹配/中转/统计），也是指令通道
// （MatchResult / RelayPush / GroupUpdate / KickNode 下发）。
//
// 断流后指数退避重连（1s→2s→4s…→30s 封顶），重连后补发 Register + 全量状态（9.2）。
//
// 架构职责与协作：battle 模块经 centerMatchAdapter/rankReporter/rankQuerier
// 三个适配器使用本客户端（server.go 接线）；Handler 接口由 server.go 的
// centerHandler 实现，把下行指令转成模块调用。
//
// 设计取舍：
//   - 为什么心跳数据用 atomic 而不是锁：SetPlayerNum/SetQPS 由业务侧高频
//     调用，读侧只有心跳 goroutine——atomic.Int32/Int64 让两侧都无锁；
//   - 为什么 ready 是 chan struct{} 而不是 bool：多个 goroutine 可能同时
//     等流就绪，channel 的关闭广播语义天然支持「一次关闭、全员唤醒」，
//     bool + 轮询要么忙等要么漏唤醒；sync.Once 保证关闭只发生一次
//     （重复 close channel 会 panic）；
//   - QueryRank 不走 Report 流而复用 conn 起独立 RPC：查询是请求-响应
//     语义，塞进单向流需要自己维护 reqID ↔ 响应 的配对，反而更复杂。
package center

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"game_study/pkg/backoff"
	"game_study/pkg/breaker"
	"game_study/pkg/log"
	"game_study/pkg/metrics"
	"game_study/pkg/periodic"

	internalv2 "game_study/pkg/pb/internalv2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// 重连退避参数（9.2）。
const (
	minBackoff = 1 * time.Second
	maxBackoff = 30 * time.Second
)

// Handler 中心服下行指令处理器。
type Handler interface {
	// OnMatchResult 匹配成功（分配了跨服房间）。
	OnMatchResult(res *internalv2.MatchResult)
	// OnGroupUpdate 分组变更（组内游戏服/跨服地址变化）。
	OnGroupUpdate(g *internalv2.GroupUpdate)
	// OnKick 中心服要求本节点重连（滚动重启）。
	OnKick(k *internalv2.KickNode)
	// OnConfigPush 配置热更新。
	OnConfigPush(c *internalv2.ConfigPush)
}

// Client 中心服客户端。
type Client struct {
	selfID string
	// serverID 本区区服编号（业务标识）。
	//
	// 与 selfID 的分工：selfID 是部署态标识（game-01，扩缩容会变），
	// serverID 是玩家数据里记的、跨服表里存的业务标识。跨服玩法按
	// server_id 路由 NATS subject（score/group/delta），所以两者必须在
	// Register 里显式声明映射——靠「node_id 一定是 game-{server_id}」
	// 这种字符串约定，部署一改就失效。
	serverID int32
	nodeType string
	addr     string
	groupID  string
	h        Handler
	logger   *log.Logger

	// mu 保护 stream/conn 的替换：重连 goroutine 写、Send/QueryRank 读，
	// 不用 atomic 是因为要同时维护两个字段的一致性
	mu     sync.RWMutex
	stream internalv2.CenterService_ReportClient
	conn   *grpc.ClientConn

	ready     chan struct{}
	readyOnce sync.Once
	stopCh    chan struct{}
	stopOnce  sync.Once

	// cb 出向 Unary RPC 的熔断器。挂在 Client 而非 ClientConn 上：
	// 断线重连会重建 conn，但中心服的故障状态必须跨重连保留，否则每次
	// 重连都给对端一轮全量探测。哨兵名 "center" 用于指标/日志定位。
	cb *breaker.Breaker

	// timeouts 出向 Unary RPC 的方法级超时。装配期由 SetTimeouts 注入；
	// 与 cb 一样挂在 Client 上、跨重连保留（超时预算不该因换连接而漂移）。
	timeouts RPCTimeoutConfig

	// 心跳数据（由业务侧更新）
	//
	// atomic 而非锁：写侧是业务 goroutine（SetPlayerNum），读侧只有
	// heartbeatLoop 一个 goroutine，单写单读用原子操作最便宜
	playerNum atomic.Int32
	qps       atomic.Int64
	version   string
}

// NewClient 创建客户端。
func NewClient(selfID, addr string, h Handler, logger *log.Logger) *Client {
	c := &Client{
		selfID:   selfID,
		nodeType: "game",
		addr:     addr,
		h:        h,
		logger:   logger,
		ready:    make(chan struct{}),
		stopCh:   make(chan struct{}),
		version:  "1.0.0",
	}
	c.initBreaker(breaker.Options{Name: "center"})
	return c
}

// initBreaker 建熔断器并接上日志/指标。
func (c *Client) initBreaker(opts breaker.Options) {
	opts.Name = "center"
	opts.OnChange = func(s breaker.State) {
		if metrics.Enabled() {
			metrics.RPCBreakerState(opts.Name, int32(s))
			if s == breaker.Open {
				metrics.RPCBreakerOpened(opts.Name)
			}
		}
		if c.logger == nil {
			return
		}
		if s == breaker.Open {
			c.logger.Warn("center RPC breaker opened; fail fast until cooldown", "center", c.addr)
		} else {
			c.logger.Info("center RPC breaker closed; traffic resumed", "center", c.addr)
		}
	}
	c.cb = breaker.New(opts)
}

// SetBreaker 覆盖熔断参数（装配期调用；Start 之前）。负数 Threshold = 关闭。
func (c *Client) SetBreaker(opts breaker.Options) { c.initBreaker(opts) }

// BreakerState 当前熔断状态（GM / 健康检查用）。
func (c *Client) BreakerState() breaker.State { return c.cb.State() }

// DefaultRPCTimeout 出向 Unary RPC 的默认超时（方法未单独配置时）。
//
// 与 module.QueryTimeout（管 MySQL）是不同层的预算：这里专管跨服 gRPC。
// 默认 3s 与改造前的硬编码一致——改造只让它「可配」，不改变默认行为。
const DefaultRPCTimeout = 3 * time.Second

// RPCTimeoutConfig 方法级 RPC 超时。零值/负数由 SetTimeouts 归一化成默认值。
type RPCTimeoutConfig struct {
	CrossEnroll time.Duration
	QueryRank   time.Duration
}

// SetTimeouts 覆盖方法级超时（装配期调用；Start 之前）。
func (c *Client) SetTimeouts(t RPCTimeoutConfig) {
	if t.CrossEnroll <= 0 {
		t.CrossEnroll = DefaultRPCTimeout
	}
	if t.QueryRank <= 0 {
		t.QueryRank = DefaultRPCTimeout
	}
	c.timeouts = t
}

// rpcCtx 生成一次 Unary RPC 的 ctx，做两件事：
//
//  1. deadline 取 min(上游, 本地配置)：上游若带更早的 deadline（GM 工具、
//     压测、未来的非 Actor 调用方），尊重它——下游不该等得比上游还久。
//     Actor 模型下消息入口没有可取消 ctx（见 module.QueryCtx 注释），
//     这类调用方传的是 Background，本地配置就是唯一预算。
//  2. 取消信号不继承：Actor 处理完回包后请求 ctx 即取消，若原样透传会掐断
//     中心服正在进行的写（报名可能已落库，反而留下不一致）。WithoutCancel
//     只保留 Value（trace span / metadata），丢掉 Done。这也是把 actor.packet
//     与 center.rpc 串成父子 span 的关键——此前 handler 传 Background，
//     中心服侧 span 成了新的 root，链路在这里断开。
func (c *Client) rpcCtx(parent context.Context, local time.Duration) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	if local <= 0 {
		local = DefaultRPCTimeout // 未调 SetTimeouts 的单测/零装配回落默认
	}
	if dl, ok := parent.Deadline(); ok {
		if remain := time.Until(dl); remain > 0 && remain < local {
			local = remain
		}
	}
	return context.WithTimeout(context.WithoutCancel(parent), local)
}

// SetNodeType 设置节点类型（game / cross）。
func (c *Client) SetNodeType(t string) { c.nodeType = t }

// SetGroup 设置分组（空表示由中心服分配）。
func (c *Client) SetGroup(g string) { c.groupID = g }

// SetServerID 设置本区区服编号（装配期调用，Register 首帧带上去）。
//
// 不设也能跑：中心服拿不到映射时按服下发的路径全部失效（跨服玩法分组与
// 改分都发不出去），日志里是「target server has no node」。所以装配层
// 一定要设，0 表示真的没配置。
func (c *Client) SetServerID(id int32) { c.serverID = id }

// SetPlayerNum 更新在线玩家数（心跳上报）。
func (c *Client) SetPlayerNum(n int) { c.playerNum.Store(int32(n)) }

// SetQPS 更新消息 QPS。
func (c *Client) SetQPS(q int64) { c.qps.Store(q) }

// Start 建流并常驻重连（阻塞至 ctx 取消或 Stop）。
//
// 退避只在「真的断过」时翻倍：runOnce 正常返回（ctx 取消）时 err 为 nil，
// 不会打 Warn 也不会继续退避——循环直接退出。
//
// 节奏交给 pkg/backoff：本包与 cross 客户端、中心服选主三处曾各写一遍
// 同一个循环，其中 leader 那份的封顶判断还写错了（从 20s 翻到 40s）。
func (c *Client) Start(ctx context.Context) {
	b := backoff.New(minBackoff, maxBackoff)
	_ = backoff.Loop(ctx, c.stopCh, b,
		func(ctx context.Context) error { return c.runOnce(ctx) },
		func(err error, wait time.Duration) {
			// ctx 已取消时 runOnce 返回错误是预期内的：记了只会误导排查
			// （把正常退出当成故障）。
			if ctx.Err() != nil {
				return
			}
			c.logger.Warn("center report stream broken",
				"center", c.addr, "backoff", wait.String(), "err", err)
		},
	)
}

// runOnce 建一次流并跑到断开。
//
// 每次重连都新建 ClientConn（不复用旧 conn）：旧 conn 可能处于半开状态，
// 复用会把上一次的故障带进来；defer Close 保证 attempts 不泄漏连接。
func (c *Client) runOnce(ctx context.Context) error {
	conn, err := grpc.NewClient(c.addr,
		grpc.WithTransportCredentials(insecureCreds()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(4<<20)),
		// 只拦 Unary：Report 流有自己的退避重连，套熔断会把单条上报
		// 失败误算成整个中心服故障。外层记 RED 指标，内层做熔断闸。
		grpc.WithChainUnaryInterceptor(c.metricsInterceptor,
			breaker.UnaryInterceptor(c.cb, breaker.InterceptorOptions{
				OnReject: func(string) {
					if metrics.Enabled() {
						metrics.RPCBreakerRejected("center")
					}
				},
			})),
	)
	if err != nil {
		return fmt.Errorf("dial center %s: %w", c.addr, err)
	}
	defer func() { _ = conn.Close() }()

	client := internalv2.NewCenterServiceClient(conn)
	stream, err := client.Report(ctx)
	if err != nil {
		return fmt.Errorf("open report stream: %w", err)
	}

	// 首帧必须 Register（9.2）
	if err := stream.Send(&internalv2.NodeReport{
		Body: &internalv2.NodeReport_Register{Register: &internalv2.NodeRegister{
			NodeId:   c.selfID,
			NodeType: c.nodeType,
			Addr:     c.addr,
			GroupId:  c.groupID,
			StartAt:  time.Now().Unix(),
			ServerId: c.serverID,
		}},
	}); err != nil {
		return fmt.Errorf("send register: %w", err)
	}

	c.mu.Lock()
	c.stream = stream
	c.conn = conn
	c.mu.Unlock()

	// sync.Once 保证 ready 只 close 一次：重连后 Ready() 已为 true，
	// 无需（也不能）再次关闭 channel
	c.readyOnce.Do(func() { close(c.ready) })
	c.logger.Info("center report stream established", "center", c.addr, "node", c.selfID)

	// 心跳 + 下行接收
	hbDone := make(chan struct{})
	go func() { c.heartbeatLoop(ctx, stream); close(hbDone) }()

	for {
		cmd, err := stream.Recv()
		if err != nil {
			// 只清「自己还是当前流」的标记：期间若已重连过，
			// c.stream 指向新流，不能把新流误置 nil
			c.mu.Lock()
			if c.stream == stream {
				c.stream = nil
			}
			c.mu.Unlock()
			return err
		}
		c.dispatch(cmd)
	}
}

// heartbeatLoop 周期心跳（间隔取服务端期望值的一半，容忍一次丢失）。
//
// goroutine 生命周期：随本次 runOnce 的 stream 生死——stream 断开后
// Send 必失败，StopOnError 让循环直接退出；ctx/stopCh 取消时经 ctx 与
// Done 两个通道退出，不会泄漏。
//
// 这是唯一一处用 Done 而不是 ctx 的任务：循环的寿命等于一条连接，
// 连接断了只该停心跳，不该 cancel 整个客户端。
func (c *Client) heartbeatLoop(ctx context.Context, stream internalv2.CenterService_ReportClient) {
	periodic.Run(ctx, periodic.Options{
		Name:        "heartbeat",
		Interval:    5 * time.Second,
		Done:        c.stopCh,
		StopOnError: true,
		Fn: func(context.Context) error {
			rep := &internalv2.NodeReport{
				Body: &internalv2.NodeReport_Heartbeat{Heartbeat: &internalv2.NodeHeartbeat{
					NodeId:      c.selfID,
					PlayerCount: c.playerNum.Load(),
					MsgQps:      c.qps.Load(),
					Version:     c.version,
				}},
			}
			return stream.Send(rep)
		},
	})
}

// dispatch 分发下行指令。
//
// 只在本 goroutine（runOnce 的 Recv 循环）内调用，Handler 实现不需要
// 考虑并发；每个分支判 nil 是因为 oneof 的 payload 可能显式为 nil。
func (c *Client) dispatch(cmd *internalv2.CenterCommand) {
	if cmd == nil || c.h == nil {
		return
	}
	switch body := cmd.Body.(type) {
	case *internalv2.CenterCommand_MatchResult:
		if body.MatchResult != nil {
			c.h.OnMatchResult(body.MatchResult)
		}
	case *internalv2.CenterCommand_Group:
		if body.Group != nil {
			c.h.OnGroupUpdate(body.Group)
		}
	case *internalv2.CenterCommand_Kick:
		if body.Kick != nil {
			c.h.OnKick(body.Kick)
		}
	case *internalv2.CenterCommand_Config:
		if body.Config != nil {
			c.h.OnConfigPush(body.Config)
		}
	}
}

// Send 上报一条消息（流未就绪返回错误，调用方决定是否降级）。
//
// RLock 只保护「取 stream」这一下：stream.Send 在锁外执行，否则慢对端
// 会把所有上报方堵死在锁上。
func (c *Client) Send(rep *internalv2.NodeReport) error {
	c.mu.RLock()
	stream := c.stream
	c.mu.RUnlock()
	if stream == nil {
		return fmt.Errorf("center stream not ready")
	}
	return stream.Send(rep)
}

// SendMatch 上报匹配请求。
func (c *Client) SendMatch(playerID int64, mode string, elo int32, snapshot []byte) error {
	return c.Send(&internalv2.NodeReport{
		Body: &internalv2.NodeReport_Match{Match: &internalv2.MatchRequest{
			NodeId:   c.selfID,
			PlayerId: playerID,
			Mode:     mode,
			Snapshot: snapshot,
			Elo:      elo,
		}},
	})
}

// SendRank 上报排行分数；bizID 非空时中心服按它去重（同一次结算重投不重复加分）。
func (c *Client) SendRank(rankType string, playerID, score int64, season, bizID string) error {
	return c.Send(&internalv2.NodeReport{
		Body: &internalv2.NodeReport_Rank{Rank: &internalv2.RankReport{
			NodeId:   c.selfID,
			RankType: rankType,
			PlayerId: playerID,
			Score:    score,
			Season:   season,
			BizId:    bizID,
		}},
	})
}

// QueryRank 查询榜单（Top-N + 我的名次 + 周边）。
//
// 未与中心服建连时返回错误，调用方回「榜单稍后重试」，而不是空榜
// （空榜会让玩家误以为自己没有成绩）。
func (c *Client) QueryRank(ctx context.Context, rankType string, playerID int64, topN int32) (*internalv2.QueryRankResp, error) {
	c.mu.RLock()
	conn := c.conn
	c.mu.RUnlock()
	if conn == nil {
		return nil, fmt.Errorf("center not connected")
	}
	ctx, cancel := c.rpcCtx(ctx, c.timeouts.QueryRank)
	defer cancel()
	return internalv2.NewCenterServiceClient(conn).QueryRank(ctx, &internalv2.QueryRankReq{
		RankType:       rankType,
		TopN:           topN,
		AroundPlayerId: playerID,
	})
}

// SendFeature 上报功能统计。
func (c *Client) SendFeature(groupID, feature string, metrics map[string]int64) error {
	return c.Send(&internalv2.NodeReport{
		Body: &internalv2.NodeReport_Feature{Feature: &internalv2.FeatureReport{
			NodeId:  c.selfID,
			GroupId: groupID,
			Feature: feature,
			Metrics: metrics,
			Ts:      time.Now().Unix(),
		}},
	})
}

// CrossEnroll 报名 / 取消报名（Unary，同步等中心服裁决）。
//
// 为什么这一条不用流：报名是玩家点出来的，必须当场告诉玩家成功还是失败
// （「非报名期」这种结果要立刻回包）。用流发出去就变成了单向通知，
// 客户端只能靠轮询查状态。频率也撑得住——报名集中在一个 24h 窗口里，
// 单服峰值是每秒几十次，与 RPC 的启动成本完全不冲突。
func (c *Client) CrossEnroll(ctx context.Context, req *internalv2.CrossEnrollReq) (*internalv2.CrossEnrollResp, error) {
	c.mu.RLock()
	conn := c.conn
	c.mu.RUnlock()
	if conn == nil {
		return nil, fmt.Errorf("center not connected")
	}
	ctx, cancel := c.rpcCtx(ctx, c.timeouts.CrossEnroll)
	defer cancel()
	return internalv2.NewCenterServiceClient(conn).CrossEnroll(ctx, req)
}

// Ready 流是否已建立过。
//
// 语义是「曾经就绪」而非「此刻就绪」：重连窗口内 Ready() 仍为 true，
// 调用方（battle 的 Available）应容忍短时不可用，由 Send 的错误兜底。
func (c *Client) Ready() bool {
	select {
	case <-c.ready:
		return true
	default:
		return false
	}
}

// Stop 停止重连并关闭流。
func (c *Client) Stop() {
	c.stopOnce.Do(func() { close(c.stopCh) })
	c.mu.Lock()
	conn := c.conn
	c.stream = nil
	c.mu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}
}

// insecureCreds 内网明文（同机房；生产可换 mTLS）。
func insecureCreds() credentials.TransportCredentials {
	return insecure.NewCredentials()
}
