// internal/gateway/backend/pool.go —— 网关到游戏服的 gRPC Channel 池
//
// 架构职责：网关进程与 game 进程之间唯一的 RPC 通道。按设计约束（4.5），
// 每个客户端会话对应一条 GameService.Channel 双向 stream，响应沿同一条流
// 回写，天然保证单会话内消息有序，网关无需在业务层做重排。
// 协作关系：server.go 在 bindToGame 鉴权成功后调 GetOrConnect 建流
// （首帧即 BIND），forwardPacket 把客户端上行帧交给 Send；deliverToClient
// 以 onFrame 回调身份被 recvLoop 触发，把游戏服回包加密写回客户端。
// 设计取舍：池按 session_id 索引而非按 game 节点复用——会话级流让游戏服
// 凭 session_id 即可找回 Actor，路由极简；代价是 gRPC 流数量与在线玩家
// 数成正比（O(N)），用连接资源换有序与简单。
// 生命周期不变量：流只应由 recvLoop 出错或 Close/CloseSession 显式移除，
// 绝不留「socket 已断但 map 仍持有」的半死流——否则 Send 持续报错、
// recvLoop goroutine 永不退出。
package backend

import (
	"context"
	"fmt"
	"sync"

	"game_study/pkg/log"
	"game_study/pkg/metrics"
	rpcpkg "game_study/pkg/rpc"

	internalv2 "game_study/pkg/pb/internalv2"
)

// FrameHandler 下行帧回调（游戏服 → 网关 → 客户端）。
//
// 契约：由 recvLoop goroutine 串行触发，实现方不应做阻塞操作，否则拖慢
// 同一条流上的后续收帧；入参 sessionID 与建流时传入的 numID 一致，
// msgID/body 即游戏服下发的原始业务包（网关不解析业务语义）。
// 注意：回调执行在 Pool 的 recvLoop goroutine 内，若实现里再调用 Pool
// 的写方法（remove/Close）会与 p.mu 形成重入，需自行避让。
type FrameHandler func(sessionID uint64, msgID uint32, body []byte)

// Pool 管理到 game server 的会话级 stream。
//
// 线程安全：mu（RWMutex）保护 streams。读路径（GetOrConnect 命中、Send）
// 持读锁，允许多个上行 goroutine 并发；建流/移除持写锁。选 RWMutex 而非
// Mutex 的原因是在线稳态下绝大多数 GetOrConnect 都命中缓存，是纯读路径。
// 不变量：streams 的 key 与 server.go 的 sessGame、session.Manager 的
// byNum 三方一致，会话消亡时必须同时清理两侧，否则出现路由黑洞。
type Pool struct {
	// clientPool 底层 gRPC 连接池：按 target 复用 ClientConn，
	// Channel 流则在同一条 HTTP/2 连接上按会话多路复用新建
	clientPool *rpcpkg.ClientPool
	logger     *log.Logger
	// onFrame 下行帧回调，构造时注入、运行期只读，允许为 nil
	onFrame FrameHandler

	// mu 保护 streams：锁粒度到单条 map 操作，绝不在持锁期间执行
	// Send/Recv 等可能阻塞的 IO，否则读锁会被长时间占死
	mu      sync.RWMutex
	streams map[uint64]*streamConn // session_id -> stream
}

// streamConn 一条会话级 gRPC 流的句柄。
type streamConn struct {
	// client 双向流对象。gRPC 保证同一流上 Send 与 Recv 可并发，
	// 但多个 Send 并发调用不安全；现状同一会话的上行只有一个写方
	// （router 读循环），因此未再加写锁
	client internalv2.GameService_ChannelClient
	// cancel 注销流的 context func：调用后 stream 的 Recv 立即返回
	// 错误、recvLoop 退出，是防止 goroutine 泄漏的收尾手段
	cancel context.CancelFunc
	// target 建流时的 game 节点（etcd:// scheme），仅用于日志排查
	target string
}

// NewPool 创建 Pool。
// 契约：onFrame 可为 nil（此时下行帧被静默丢弃）；clientPool 的 target
// 解析依赖上层注册的 gRPC resolver（server.go 注入 etcd resolver）。
func NewPool(clientPool *rpcpkg.ClientPool, logger *log.Logger, onFrame FrameHandler) *Pool {
	return &Pool{
		clientPool: clientPool,
		logger:     logger,
		onFrame:    onFrame,
		streams:    make(map[uint64]*streamConn),
	}
}

// GetOrConnect 获取或创建指定会话的 stream（firstFrame 通常为 BIND）。
//
// 双检锁（double-checked locking）：先在读锁下快路径命中——稳态零写锁
// 竞争；未命中再升级写锁并二次确认。二次确认不可省：否则两个 goroutine
// 同时未命中时会各建一条流，后写的覆盖 map，先建的那条失去引用，
// 其 recvLoop 泄漏成孤儿 goroutine。
// streamCtx 从 context.Background() 派生而非复用请求 ctx：流的生命周期
// 属于会话而非单次请求，请求返回时不能把流一起取消。
// 失败路径必须调 cancel()：WithCancel 派生的 ctx 若无人取消，父 context
// 会一直持有该子节点造成泄漏（go vet 的 lostcancel 检查项）。
func (p *Pool) GetOrConnect(ctx context.Context, target string, sessionID uint64, firstFrame *internalv2.StreamFrame) (internalv2.GameService_ChannelClient, error) {
	p.mu.RLock()
	sc, ok := p.streams[sessionID]
	p.mu.RUnlock()
	if ok && sc != nil {
		return sc.client, nil
	}

	// 慢路径：升级为写锁，并再次确认尚无其他 goroutine 已建好流
	p.mu.Lock()
	defer p.mu.Unlock()
	if sc, ok := p.streams[sessionID]; ok && sc != nil {
		return sc.client, nil
	}

	cc, err := p.clientPool.Get(target)
	if err != nil {
		return nil, err
	}
	client := internalv2.NewGameServiceClient(cc)
	// 流的 context 与单次请求解耦：会话存活则流存活，由 remove/Close 统一取消
	streamCtx, cancel := context.WithCancel(context.Background())
	stream, err := client.Channel(streamCtx)
	if err != nil {
		// 建流失败：立即 cancel 防 context 泄漏，并记 open 类错误指标
		cancel()
		metrics.ChannelError("open")
		return nil, fmt.Errorf("channel %s: %w", target, err)
	}

	if firstFrame != nil {
		// 首帧（BIND）随流一起发出：游戏服收到首个帧即完成会话绑定；
		// 发送失败必须取消流，不能留下「已登记但未绑定」的假流
		if err := stream.Send(firstFrame); err != nil {
			cancel()
			return nil, fmt.Errorf("send bind: %w", err)
		}
	}

	p.streams[sessionID] = &streamConn{client: stream, cancel: cancel, target: target}
	if metrics.Enabled() {
		metrics.ChannelStreams(len(p.streams))
	}
	// 收包循环即 goroutine per stream 模式：唯一泄漏防护点是 Recv 出错即退
	go p.recvLoop(sessionID, stream)
	return stream, nil
}

// Send 向指定会话的 stream 发送帧。
// 流不存在说明会话已消亡或尚未绑定：记 send_no_stream 指标并报错，
// 由调用方（server.forwardPacket）决定是否升级为断开客户端连接。
func (p *Pool) Send(sessionID uint64, frame *internalv2.StreamFrame) error {
	p.mu.RLock()
	sc, ok := p.streams[sessionID]
	p.mu.RUnlock()
	if !ok || sc == nil {
		metrics.ChannelError("send_no_stream")
		return fmt.Errorf("session %d stream not found", sessionID)
	}
	if err := sc.client.Send(frame); err != nil {
		metrics.ChannelError("send")
		return err
	}
	return nil
}

// recvLoop 会话级下行接收循环（goroutine per stream）。
// 退出条件唯一：stream.Recv 出错（对端关闭 / 流被 cancel / 网络故障）。
// 退出前必须 remove——同时清理 map 条目与 cancel func，否则 map 持有
// 已死流，后续 Send 全走 send_no_stream，且本 goroutine 无法退出。
func (p *Pool) recvLoop(sessionID uint64, stream internalv2.GameService_ChannelClient) {
	for {
		frame, err := stream.Recv()
		if err != nil {
			p.logger.Debug("backend stream closed", "session_id", sessionID, "err", err)
			p.remove(sessionID)
			return
		}
		if cp := frame.GetClientPacket(); cp != nil && p.onFrame != nil {
			p.onFrame(cp.SessionId, cp.MsgId, cp.Body)
		}
	}
}

// remove 关闭并移除会话 stream。
// 持写锁完成 cancel+delete，与 GetOrConnect 的建流互斥：否则可能出现
// 「remove 刚 delete、并发路径又重新插入」的竞态，留下野流条目。
func (p *Pool) remove(sessionID uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if sc, ok := p.streams[sessionID]; ok {
		sc.cancel()
		delete(p.streams, sessionID)
	}
}

// CloseSession 主动关闭某会话的 stream（会话断开时调用）。
// 幂等：对不存在的 session 只是空 delete；server.go 的 SetOnClosed
// 回调与 forwardPacket 失败路径都会调用，重复触发安全。
func (p *Pool) CloseSession(sessionID uint64) {
	p.remove(sessionID)
}

// Close 关闭所有 stream（进程优雅停机用）。
// 持写锁批量 cancel+delete；不等待各 recvLoop 退出——停机时上游 ctx
// 已取消，recvLoop 会随 Recv 报错自然退出，这里只负责解除 map 引用。
func (p *Pool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for id, sc := range p.streams {
		sc.cancel()
		delete(p.streams, id)
	}
	return nil
}
