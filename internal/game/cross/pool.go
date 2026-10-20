// internal/game/cross/pool.go —— 游戏服侧跨服连接池 + battle.CrossCaller 实现
//
// 架构职责：三张表 + 一条等待通道构成全部状态——
//
//	clients  addr → Client（每个跨服节点一条常驻流）
//	conns    addr → grpc.ClientConn（流的底层连接，Stop 时统一关闭）
//	rooms    room_id → addr（建房时记录，结算/透传必须回到同一节点）
//	pending  reqID → chan（建房响应的「请求-响应」配对）
//
// 协作关系：battle 模块经 Pool 的 CreateRoom/SendSnapshot/SendOp/SendResult
// 完成跨服战斗；下行帧由 pool 自身实现 cross.Handler 接住并回填 pending。
//
// 设计取舍：
//   - 为什么 rooms 路由表必须有：同一场战斗的快照/操作/结算必须落在同一
//     个跨服节点（房间状态在它内存里），负载到别的节点就是「无房间」错误；
//   - 为什么用 reqID → chan 而不是共享流上的 broadcast：多个 battle
//     goroutine 并发建房，channel 把响应精确交回请求方，无需全局锁着
//     回调表逐个比对；容量 1 + defer 删除保证迟到响应不阻塞、不泄漏；
//   - 为什么 ensure 用「先建后检」的双检：grpc.NewClient 与 map 插入
//     之间有两个并发调用时，后到者关闭自己多建的 conn、复用先到者的，
//     比全局锁住整个建连过程更省等待。
package cross

import (
	"context"
	"fmt"
	"sync"
	"time"

	"game_study/internal/game/module/battle"
	"game_study/pkg/log"

	internalv2 "game_study/pkg/pb/internalv2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// RoomRoute 房间 → 承载它的跨服节点（结算/透传要回到同一节点）。
type RoomRoute struct {
	RoomID uint64
	Addr   string
}

// Pool 到全部跨服节点的连接池。
type Pool struct {
	selfID string
	h      Handler
	logger *log.Logger

	// mu 保护三张主表；pending 单独用 pendingMu——两者的临界区没有重叠，
	// 分开可以减小锁竞争（建房等待不阻塞节点查找）
	mu      sync.RWMutex
	clients map[string]*Client          // addr -> client
	conns   map[string]*grpc.ClientConn // addr -> 连接（Stop 时关闭，否则对端 GracefulStop 会一直等）
	rooms   map[uint64]string           // room_id -> addr（建房时记录）

	pendingMu sync.Mutex
	pending   map[string]chan *internalv2.RoomCreateResp

	// reqSeq 只被 nextReqID 在 pendingMu 内自增，无需额外原子操作
	reqSeq uint64
}

// NewPool 创建客户端池。
func NewPool(selfID string, h Handler, logger *log.Logger) *Pool {
	return &Pool{
		selfID:  selfID,
		h:       h,
		logger:  logger,
		clients: make(map[string]*Client),
		conns:   make(map[string]*grpc.ClientConn),
		rooms:   make(map[uint64]string),
		pending: make(map[string]chan *internalv2.RoomCreateResp),
	}
}

// Start 按地址列表建立连接（每节点一条流，常驻重连）。
func (p *Pool) Start(ctx context.Context, addrs []string) {
	for _, addr := range addrs {
		if addr == "" {
			continue
		}
		if _, err := p.ensure(ctx, addr); err != nil {
			p.logger.Warn("cross node connect failed", "addr", addr, "err", err)
		}
	}
}

// ensure 不存在则建连并后台起流。
//
// 双检锁的第二半（写锁内再查一次）处理并发 Start：后到者发现已存在时，
// 关掉自己刚建的 conn 再返回既有 client——多建的那条 TCP 连接不会泄漏。
func (p *Pool) ensure(ctx context.Context, addr string) (*Client, error) {
	p.mu.RLock()
	c, ok := p.clients[addr]
	p.mu.RUnlock()
	if ok {
		return c, nil
	}

	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(4<<20)),
	)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}

	c = NewClient(p.selfID, addr, p.h, p.logger)
	p.mu.Lock()
	if old, dup := p.clients[addr]; dup {
		p.mu.Unlock()
		_ = conn.Close()
		return old, nil
	}
	p.clients[addr] = c
	p.conns[addr] = conn
	p.mu.Unlock()

	// 流的自维持重连在独立 goroutine：Pool 的调用方（Start/OnGroupUpdate）
	// 不被阻塞；ctx 取消时 Start 内部循环退出
	go c.Start(ctx, conn)
	return c, nil
}

// Available 是否有可用跨服节点。
func (p *Pool) Available() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.clients) > 0
}

// ReadyCount 已 BIND 成功的节点数（指标用）。
func (p *Pool) ReadyCount() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	n := 0
	for _, c := range p.clients {
		if c.Ready() {
			n++
		}
	}
	return n
}

// CreateRoom 申请建房（轮询选一个就绪节点）。
//
// req.RoomID 非 0 时表示使用中心服分配的房间号（多参战方各自请求同一号，CrossServer 幂等）。
//
// 等待策略：先按地址顺序找已就绪节点；全未就绪时给每个节点一次最多 3s
// 的 WaitReady——首个 BIND 完成的节点胜出，而不是死等某一个。
func (p *Pool) CreateRoom(ctx context.Context, req battle.CreateRoomReq) (uint64, map[int64]string, error) {
	c, addr, err := p.pickReady(ctx)
	if err != nil {
		return 0, nil, err
	}

	reqID := p.nextReqID()
	// 容量 1：OnCreateResp 的发送永不阻塞（见其 select default），
	// 本函数超时离开后 channel 被 GC，无 goroutine 泄漏
	ch := make(chan *internalv2.RoomCreateResp, 1)
	p.pendingMu.Lock()
	p.pending[reqID] = ch
	p.pendingMu.Unlock()
	// defer 删除是幂等收尾：无论成功/失败/超时，pending 表都不留残项
	defer func() {
		p.pendingMu.Lock()
		delete(p.pending, reqID)
		p.pendingMu.Unlock()
	}()

	if err := c.Send(&internalv2.CrossFrame{
		Data: &internalv2.CrossFrame_CreateReq{CreateReq: &internalv2.RoomCreateReq{
			ReqId:         reqID,
			Mode:          req.Mode,
			ExpectPlayers: int32(matchPlayersOf(req)),
			TimeoutSec:    int32(req.Timeout.Seconds()),
			PlayerIds:     req.PlayerIDs,
			RoomId:        req.RoomID,
			PresetTokens:  req.PresetTokens,
		}},
	}); err != nil {
		return 0, nil, fmt.Errorf("send create req: %w", err)
	}

	select {
	case resp := <-ch:
		if resp.GetCode() != 0 {
			return 0, nil, fmt.Errorf("cross rejected room creation (code=%d)", resp.GetCode())
		}
		// 建房成功才记录路由：失败房间不占 rooms 表
		p.mu.Lock()
		p.rooms[resp.GetRoomId()] = addr
		p.mu.Unlock()
		return resp.GetRoomId(), resp.GetTokens(), nil
	case <-ctx.Done():
		return 0, nil, ctx.Err()
	case <-time.After(5 * time.Second):
		return 0, nil, fmt.Errorf("create room timeout")
	}
}

// OnCreateResp 实现 Handler：建房响应回填 pending。
func (p *Pool) OnCreateResp(resp *internalv2.RoomCreateResp) {
	if resp == nil {
		return
	}
	p.pendingMu.Lock()
	ch, ok := p.pending[resp.GetReqId()]
	p.pendingMu.Unlock()
	if !ok {
		return
	}
	select {
	case ch <- resp:
	default:
	}
}

// clientOfRoom 找房间所在节点。
func (p *Pool) clientOfRoom(roomID uint64) (*Client, bool) {
	p.mu.RLock()
	addr, ok := p.rooms[roomID]
	if !ok {
		p.mu.RUnlock()
		return nil, false
	}
	c := p.clients[addr]
	p.mu.RUnlock()
	return c, c != nil
}

// SendSnapshot 送玩家快照入房。
func (p *Pool) SendSnapshot(roomID uint64, playerID int64, token string, snapshot []byte) error {
	c, ok := p.clientOfRoom(roomID)
	if !ok {
		return fmt.Errorf("room %d has no cross node", roomID)
	}
	return c.SendSnapshot(roomID, playerID, token, snapshot)
}

// SendOp 转发战斗操作。
func (p *Pool) SendOp(roomID uint64, playerID int64, msgID uint32, body []byte) error {
	c, ok := p.clientOfRoom(roomID)
	if !ok {
		return fmt.Errorf("room %d has no cross node", roomID)
	}
	return c.SendBattle(roomID, playerID, msgID, body)
}

// SendResult 转发结算上报。
func (p *Pool) SendResult(roomID uint64, playerID int64, token string, result int32, rewards []byte) error {
	c, ok := p.clientOfRoom(roomID)
	if !ok {
		return fmt.Errorf("room %d has no cross node", roomID)
	}
	return c.SendResult(&internalv2.BattleResult{
		RoomId:      roomID,
		PlayerId:    playerID,
		Result:      result,
		Rewards:     rewards,
		BattleToken: token,
		FinishedAt:  time.Now().Unix(),
	})
}

// OnRoomClosed 房间结算后清理路由。
//
// 由 crossHandler.OnBattleResult 触发；漏清理的后果是 rooms 表无限增长
// （每场战斗永久占一项）。
func (p *Pool) OnRoomClosed(roomID uint64) {
	p.mu.Lock()
	delete(p.rooms, roomID)
	p.mu.Unlock()
}

// Stop 关闭全部连接。
//
// 必须同时关 grpc.ClientConn：只关 stopCh 的话流不会断，
// 对端 CrossServer 的 GracefulStop 会一直等这条流（曾导致优雅关闭卡 5 分钟）。
func (p *Pool) Stop() {
	// 先在锁内快照再锁外关闭：Close 可能阻塞，持锁关会把并发查询堵死
	p.mu.Lock()
	clients := make([]*Client, 0, len(p.clients))
	conns := make([]*grpc.ClientConn, 0, len(p.conns))
	for _, c := range p.clients {
		clients = append(clients, c)
	}
	for _, conn := range p.conns {
		conns = append(conns, conn)
	}
	p.mu.Unlock()

	for _, c := range clients {
		c.Stop()
	}
	for _, conn := range conns {
		_ = conn.Close()
	}
}

// pickReady 选一个已就绪的节点。
func (p *Pool) pickReady(ctx context.Context) (*Client, string, error) {
	p.mu.RLock()
	type cand struct {
		c    *Client
		addr string
	}
	cands := make([]cand, 0, len(p.clients))
	for addr, c := range p.clients {
		cands = append(cands, cand{c: c, addr: addr})
	}
	p.mu.RUnlock()

	if len(cands) == 0 {
		return nil, "", fmt.Errorf("no cross node configured")
	}
	for _, cd := range cands {
		if cd.c.Ready() {
			return cd.c, cd.addr, nil
		}
	}
	// 没有就绪的：等第一个就绪（最多 3s）
	waitCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	for _, cd := range cands {
		if err := cd.c.WaitReady(waitCtx); err == nil {
			return cd.c, cd.addr, nil
		}
	}
	return nil, "", fmt.Errorf("no cross node ready")
}

// nextReqID 生成建房请求 ID：时间戳 + 序号，保证跨重启不重复
// （CrossServer 侧按 reqID 回填，重复 ID 会串响应）。
func (p *Pool) nextReqID() string {
	p.pendingMu.Lock()
	defer p.pendingMu.Unlock()
	p.reqSeq++
	return fmt.Sprintf("cr-%d-%d", time.Now().UnixNano(), p.reqSeq)
}

// matchPlayersOf 推断房间人数。
func matchPlayersOf(req battle.CreateRoomReq) int {
	if req.ExpectPlayers > 0 {
		return int(req.ExpectPlayers)
	}
	if n := len(req.PlayerIDs); n > 0 {
		return n
	}
	return 2
}
