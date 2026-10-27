// internal/cross/rpc/cross_server.go —— CrossService.Channel 实现（设计文档 4.5 / 10.3）
//
// 一条流 = 一个游戏服节点，由游戏服主动建立（地址来自中心服 GroupUpdate）。
// 首帧必须是 CrossCtrl{BIND, node_id}，之后 CrossServer 才能把结算精确回推到该节点
// （不做全网广播 —— 那样会把玩家 A 的结算发给 B 所在的所有游戏服）。
//
// CrossServer 不注册 etcd、不连网关，只接受游戏服连接。
//
// 协作关系：对上实现 battle.Sink（SendToGame/NodeOf/RoomClosed）供房间
// Actor 回推帧；对下管理 peer（每个游戏服节点的下行通道）。玩家归属表
// （playerNode）是「精确回推」的地基：结算帧知道要找哪个节点，
// 而不是全网广播。
package rpc

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"sync"
	"time"

	"game_study/internal/cross/battle"
	"game_study/internal/cross/seat"
	"game_study/pkg/log"

	internalv2 "game_study/pkg/pb/internalv2"
)

// Server CrossService 服务端。
//
// 三种并发结构对应三类访问模式：mu+RWMutex 保护 peers map（连接/断开/
// 查找）；peer 内部用 channel+once（单写 goroutine 的所有权模型）；
// playerNode 用 sync.Map（玩家归属的读远多于写、key 分散无热 key——
// 正是 sync.Map 的甜区，避免全局锁）。
type Server struct {
	internalv2.UnimplementedCrossServiceServer

	logger *log.Logger
	rooms  *battle.Manager
	seats  *seat.Manager
	nodeID string // 本跨服节点 ID（日志/指标）

	// tokenKey 准入令牌签名密钥（进程内随机：房间纯内存，重启后房间与令牌同时失效）
	tokenKey []byte

	mu    sync.RWMutex
	peers map[string]*peer // node_id -> 该节点的下行通道
	// playerNode 玩家 → 游戏服节点（结算时定位回推目标）
	playerNode sync.Map
}

// peer 一个游戏服节点的下行通道。
//
// 与 center/rpc 的 session 同构：有界 channel + done + once。
// 差异在容量（256 vs 512）：跨服帧比中心服指令更频（战斗消息透传），
// 但丢弃代价相对低（丢一帧战斗帧客户端可插值/重传），容量取小一半。
type peer struct {
	nodeID string
	sendCh chan *internalv2.CrossFrame
	done   chan struct{}
	once   sync.Once
}

func newPeer(nodeID string) *peer {
	return &peer{
		nodeID: nodeID,
		sendCh: make(chan *internalv2.CrossFrame, 256),
		done:   make(chan struct{}),
	}
}

func (p *peer) send(frame *internalv2.CrossFrame) error {
	select {
	case p.sendCh <- frame:
		return nil
	case <-p.done:
		return fmt.Errorf("peer %s closed", p.nodeID)
	default:
		// 背压：下行队列满说明该游戏服消费不过来，丢帧比阻塞房间 Actor 好
		// 阻塞的代价：房间 Actor 被一个慢节点卡住，同房间其他玩家全卡——
		// 扇出场景下「部分丢失」远优于「整体停顿」
		return fmt.Errorf("peer %s send queue full", p.nodeID)
	}
}

func (p *peer) close() {
	p.once.Do(func() { close(p.done) })
}

// AttachRooms 注入房间管理器。
//
// 存在构造环：Manager 需要 Server 作为 Sink，Server 又需要 Manager 收发房间消息。
// 用两段式初始化打破 —— 先 NewServer（rooms=nil），再 NewManager，最后 AttachRooms。
//
// 替代方案是在 NewServer 里传 Manager：但 Manager 的构造又需要 Server，
// 谁先谁后都死锁——把环切成两段是 Go 里处理相互依赖的常用手法
// （另一种是引入第三个 mediator，这里没必要）。
func (s *Server) AttachRooms(rooms *battle.Manager) { s.rooms = rooms }

// AttachSeats 注入座位管理器（与 AttachRooms 同款两段式初始化：
// seat.Manager 需要本 Server 作 Sink，本 Server 又需要它分发 SeatOp）。
func (s *Server) AttachSeats(seats *seat.Manager) { s.seats = seats }

// NewServer 创建 CrossService 服务端。
// tokenKey 进程内随机生成（32 字节）：房间纯内存，重启后房间与令牌
// 同时失效——不需要持久化密钥，随机反而是最小暴露面。rand 失败时
// 退回时间派生：仍比固定常量强，且不阻断启动。
func NewServer(nodeID string, rooms *battle.Manager, logger *log.Logger) *Server {
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		// 极端情况：退回时间派生密钥（仍比固定密钥强）
		binary.BigEndian.PutUint64(key, uint64(time.Now().UnixNano()))
	}
	return &Server{
		nodeID:   nodeID,
		rooms:    rooms,
		logger:   logger,
		tokenKey: key,
		peers:    make(map[string]*peer),
	}
}

// Channel 实现双向流：游戏服 ↔ 跨服节点。
//
// 生命周期与 center 的 Report 同构：首帧 BIND → 注册 peer → 起下行泵 →
// 循环收帧分派 → 断开清理。差异：BIND 帧校验更严（类型+nodeID 缺一不可），
// 且断线要清理 playerNode（该节点的全部玩家归属）。
func (s *Server) Channel(stream internalv2.CrossService_ChannelServer) error {
	// 首帧必须是 BIND
	// 没有 BIND 就没有 nodeID：后续所有帧无法归属到节点，
	// SendToGame 的精确回推也无从谈起
	first, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("recv bind frame: %w", err)
	}
	ctrl := first.GetCtrl()
	if ctrl == nil || ctrl.Type != internalv2.CrossCtrl_BIND || ctrl.NodeId == "" {
		return fmt.Errorf("first frame must be CrossCtrl{BIND, node_id}, got %T", first.Data)
	}

	nodeID := ctrl.NodeId
	p := newPeer(nodeID)

	s.mu.Lock()
	if old, ok := s.peers[nodeID]; ok {
		old.close() // 同节点重连：关掉旧通道
		// 先关旧再换新：旧通道的 writeLoop 见 done 即退出，
		// 不会有两个 goroutine 同时往同一条流上写
	}
	s.peers[nodeID] = p
	s.mu.Unlock()

	s.logger.Info("cross stream bound", "node", nodeID, "cross", s.nodeID)

	// 下行泵：把房间推来的帧写到流里（Send 不能并发调用，必须单 goroutine）
	writeErr := make(chan error, 1)
	go func() { writeErr <- s.writeLoop(p, stream) }()

	// 新连上的游戏服先拿一份当前座位状态：游戏服本地缓存据此回答查询。
	// 放在 writeLoop 启动之后——先有消费者再入队，避免多占一格队列。
	if s.seats != nil {
		_ = p.send(&internalv2.CrossFrame{
			Data: &internalv2.CrossFrame_SeatState{SeatState: s.seats.BuildState()},
		})
	}

	defer func() {
		// defer 里做四件收尾：关通道 → 摘 peer（带身份比对）→
		// 清玩家归属 → 记日志。defer 保证 panic 路径也执行
		p.close()
		s.mu.Lock()
		if s.peers[nodeID] == p { // 只清理自己，避免误删新连接
			// 身份比对是关键：重连场景下旧连接的 defer 后跑，
			// 不比对会把新 peer 从表里删掉——之后 SendToGame 全部找不到
			delete(s.peers, nodeID)
		}
		s.mu.Unlock()
		s.cleanupNode(nodeID)
		s.logger.Info("cross stream closed", "node", nodeID)
	}()

	for {
		frame, err := stream.Recv()
		if err != nil {
			if err != io.EOF && stream.Context().Err() == nil {
				s.logger.Warn("cross stream recv error", "node", nodeID, "err", err)
			}
			return nil
		}

		switch data := frame.Data.(type) {
		case *internalv2.CrossFrame_CreateReq:
			s.onCreateReq(nodeID, p, data.CreateReq)

		case *internalv2.CrossFrame_Snapshot:
			snap := data.Snapshot
			if snap == nil || snap.PlayerId == 0 {
				continue
			}
			// 先登记归属再入房：顺序不能反——入房触发 start 时会
			// NodeOf(pid) 找节点发 ROOM_READY，先入房会让第一次
			// ROOM_READY 因找不到归属而丢（该玩家收不到开战通知）
			s.playerNode.Store(snap.PlayerId, nodeID)
			if err := s.rooms.Join(nodeID, snap); err != nil {
				s.logger.Warn("join room failed",
					"room_id", snap.RoomId, "player_id", snap.PlayerId, "err", err)
			}

		case *internalv2.CrossFrame_Battle:
			if data.Battle == nil {
				continue
			}
			// 归属校验：消息里的 player_id 必须属于本节点，防止跨节点伪造
			// 伪造场景：A 节点冒充 B 节点的玩家发战斗消息，在 B 的房间里
			// 扇出——没有这道校验，任何人都能往任意房间注入消息
			if err := s.assertPlayerOwner(nodeID, data.Battle.PlayerId); err != nil {
				s.logger.Warn("battle msg rejected", "node", nodeID, "err", err)
				continue
			}
			if err := s.rooms.RelayBattle(nodeID, data.Battle); err != nil {
				s.logger.Debug("relay battle failed", "room_id", data.Battle.RoomId, "err", err)
			}

		case *internalv2.CrossFrame_Result:
			if data.Result == nil {
				continue
			}
			if err := s.assertPlayerOwner(nodeID, data.Result.PlayerId); err != nil {
				s.logger.Warn("result rejected", "node", nodeID, "err", err)
				continue
			}
			if err := s.rooms.Report(nodeID, data.Result); err != nil {
				s.logger.Debug("report result failed", "room_id", data.Result.RoomId, "err", err)
			}

		case *internalv2.CrossFrame_Seat:
			if data.Seat != nil {
				s.onSeatOp(nodeID, data.Seat)
			}

		case *internalv2.CrossFrame_Ctrl:
			if data.Ctrl == nil {
				continue
			}
			switch data.Ctrl.Type {
			case internalv2.CrossCtrl_ROOM_CLOSED:
				// 游戏服要求关房：仅记日志——真正触发关房的权威在
				// battle 侧（RoomClosed 回调），这里不能绕过 Manager 直接操作
				s.logger.Debug("game asked close room", "node", nodeID, "room_id", data.Ctrl.RoomId)
			case internalv2.CrossCtrl_HEALTH:
				// 心跳，无需处理
			}
		}

		// 下行泵异常退出 → 结束本流：每帧分派后非阻塞探一次写错误。
		// 写失败说明流已坏（对端断开/网络错误），继续 Recv 只会
		// 反复报错；不能阻塞在这里等——收帧循环不能停，
		// 否则满速涌入的上行帧会拖垮整个节点
		select {
		case werr := <-writeErr:
			return werr
		default:
		}
	}
}

// writeLoop 单 goroutine 写流（gRPC stream 不允许并发 Send）。
func (s *Server) writeLoop(p *peer, stream internalv2.CrossService_ChannelServer) error {
	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case <-p.done:
			return nil
		case frame := <-p.sendCh:
			if err := stream.Send(frame); err != nil {
				return fmt.Errorf("send to %s: %w", p.nodeID, err)
			}
		}
	}
}

// assertPlayerOwner 校验玩家确实属于该节点（防跨节点伪造）。
//
// sync.Map 的 Load 适合这种读多写少的表（玩家绑定只写入一次）；
// 类型断言 v.(string) 安全的前提是 Store 处也只写 string——
// 本文件是唯一写入方，否则断言失败会直接 panic。
// 两种拒绝：未绑定（该节点从未上报过该玩家快照/建房请求）
// 与绑错节点（属于别的游戏服）。
func (s *Server) assertPlayerOwner(nodeID string, playerID int64) error {
	v, ok := s.playerNode.Load(playerID)
	if !ok {
		return fmt.Errorf("player %d not bound to any node", playerID)
	}
	if v.(string) != nodeID {
		return fmt.Errorf("player %d belongs to %s, not %s", playerID, v.(string), nodeID)
	}
	return nil
}

// cleanupNode 节点断流：清理该节点登记的玩家映射。
//
// 不清理的后果：玩家重连到别的节点后，旧的绑定还在，
// 结算帧会被精确回推到已经死掉的连接上（SendToGame 报
// not connected）。Range 中 Delete 是安全的——文档明确
// Range 不保证跳过后续新增的 key，但删除已遍历项无影响。
func (s *Server) cleanupNode(nodeID string) {
	removed := 0
	s.playerNode.Range(func(k, v any) bool {
		if v.(string) == nodeID {
			s.playerNode.Delete(k)
			removed++
		}
		return true
	})
	if removed > 0 {
		s.logger.Info("cleared player bindings", "node", nodeID, "count", removed)
	}
}

// SendToGame 实现 battle.Sink：把帧精确推给指定游戏服节点。
//
// 房间 Actor 在结算时调用：凭 NodeOf 拿到的 nodeID 精确投递，
// 而不是遍历所有 peer 广播（后者会把 A 的结算推给无关节点）。
// 只读一次 map：RLock 取指针后立刻放锁，send 的背压阻塞
// 不放锁内——房间 Actor 只容忍很短的投递耗时。
func (s *Server) SendToGame(nodeID string, frame *internalv2.CrossFrame) error {
	if nodeID == "" {
		return fmt.Errorf("empty node id")
	}
	s.mu.RLock()
	p := s.peers[nodeID]
	s.mu.RUnlock()
	if p == nil {
		return fmt.Errorf("node %s not connected", nodeID)
	}
	return p.send(frame)
}

// NodeOf 实现 battle.Sink：查询玩家所在游戏服节点。
//
// 同步 Map 读路径，房间 Actor 每推一帧结算都可能调用；
// ok=false 表示玩家未绑定（还没发过快照就结算了——
// 正常不会发生，兜底返回让调用方丢弃而非 panic）。
func (s *Server) NodeOf(playerID int64) (string, bool) {
	v, ok := s.playerNode.Load(playerID)
	if !ok {
		return "", false
	}
	return v.(string), true
}

// RoomClosed 实现 battle.Sink：房间关闭时由 Manager 回调
// （Sink 依赖环的另一半：Manager → Sink → Manager，
// 但回调只经已注入的指针，不构成构造期循环）。
// 当前仅作钩子存在：清理类动作已由 peer 断开时的
// cleanupNode 兜底，这里留给后续扩展（如通知中心服回收房间号）。
func (s *Server) RoomClosed(roomID uint64) { s.rooms.RoomClosed(roomID) }

// onSeatOp 处理座位操作帧（游戏服 → 跨服节点）。
//
// 先登记 playerNode 再投递：奖励回推必须知道玩家属于哪个游戏服；漏登记
// 的后果是座位释放时找不到节点、奖励被丢弃。这里与 battle 的 Snapshot
// 预登记同一思路。
func (s *Server) onSeatOp(nodeID string, op *internalv2.SeatOp) {
	if s.seats == nil || op == nil || op.GetPlayerId() == 0 {
		return
	}
	s.playerNode.Store(op.GetPlayerId(), nodeID)

	switch op.GetAction() {
	case internalv2.SeatOp_OCCUPY:
		s.seats.Occupy(op.GetTier(), int(op.GetRoomIndex()), int(op.GetSeatIndex()), op.GetPlayerId(), nodeID)
	case internalv2.SeatOp_LEAVE:
		s.seats.Leave(op.GetTier(), int(op.GetRoomIndex()), int(op.GetSeatIndex()), op.GetPlayerId())
	}
}

// SendReward 实现 seat.Sink：把座位奖励精确推回玩家所在游戏服节点。
//
// 复用 SendToGame 的精确投递（按 BIND 的 node_id 找 peer），不做全网广播——
// 广播会把 A 玩家的奖励发给所有游戏服。投递失败不重试：座位奖励由离线
// 挂起表兜底（游戏服收不到就落库，登录补发）。
func (s *Server) SendReward(nodeID string, playerID int64, tier string, minutes, amount, seq int64) error {
	return s.SendToGame(nodeID, &internalv2.CrossFrame{
		Data: &internalv2.CrossFrame_SeatReward{SeatReward: &internalv2.SeatReward{
			PlayerId: playerID,
			Tier:     tier,
			Minutes:  minutes,
			Amount:   amount,
			Seq:      seq,
		}},
	})
}

// BroadcastState 实现 seat.Sink：把全量座位快照广播给所有已连接游戏服。
//
// 为什么广播而不是按需请求：座位总数固定 100、变更低频，全量快照很小；
// 各游戏服缓存后即可本地回答查询，省掉一条「请求 ID + pending 等待」链路。
// 投递是尽力而为（peer 队列满丢帧），下一次房间变更会带来新的全量快照，
// 丢一帧只会让某个游戏服的缓存短暂滞后。
func (s *Server) BroadcastState(st *internalv2.SeatState) error {
	if st == nil {
		return nil
	}
	s.mu.RLock()
	peers := make([]*peer, 0, len(s.peers))
	for _, p := range s.peers {
		peers = append(peers, p)
	}
	s.mu.RUnlock()

	frame := &internalv2.CrossFrame{Data: &internalv2.CrossFrame_SeatState{SeatState: st}}
	for _, p := range peers {
		_ = p.send(frame)
	}
	return nil
}

// CloseAllPeers 主动断开所有游戏服流（优雅关闭用）。
//
// 不主动断的话，gRPC 的 GracefulStop 会一直等这些双向流结束 —— 而流永远不会自己结束。
func (s *Server) CloseAllPeers() {
	s.mu.Lock()
	// 先快照再清表：close 放在锁外——close 本身只触发 done，
	// 但持锁做 I/O 相关操作是坏味道（这里 close 很轻，
	// 统一放锁外保持「锁内只做 map 操作」的一致性）
	peers := make([]*peer, 0, len(s.peers))
	for _, p := range s.peers {
		peers = append(peers, p)
	}
	// 直接换新 map 而非逐个 delete：语义是「全部作废」，
	// 且此后 Channel defer 里的身份比对必然失败（旧 peer != 新 nil），
	// 不会把 map 重新写脏
	s.peers = make(map[string]*peer)
	s.mu.Unlock()

	for _, p := range peers {
		p.close()
	}
	if len(peers) > 0 {
		s.logger.Info("closed all cross peers", "count", len(peers))
	}
}

// PeerCount 已连接游戏服节点数（指标用）。
// 只读 len：map 的 len 在持锁下读取是 O(1)，
// 不值得为它引入 atomic 计数器。
func (s *Server) PeerCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.peers)
}

// onCreateReq 处理建房请求（P13 直连路径）。
//
// 令牌用 HMAC(nodeID|roomID|playerID) 生成，CrossServer 入房时比对；
// 这样即使 room_id 泄漏，没有密钥也造不出有效令牌。
//
// 两条建房路径：room_id 非 0 → 中心服匹配已分配号（P8，权威在中心服）；
// 为 0 → 本地自增号（P13，游戏服绕过中心服直连跨服建房）。
// expect/timeout 给兜底默认值：协议字段缺失时按双人 10 分钟量级处理，
// 而不是创建出一个永远配不齐的房间。
func (s *Server) onCreateReq(nodeID string, p *peer, req *internalv2.RoomCreateReq) {
	resp := &internalv2.RoomCreateResp{ReqId: req.GetReqId()}

	expect := int(req.GetExpectPlayers())
	if expect <= 0 {
		expect = 2
	}
	timeout := time.Duration(req.GetTimeoutSec()) * time.Second

	// room_id 非 0 → 用中心服分配的号（P8）；否则本地自增（P13 直连）
	var (
		roomID uint64
		err    error
	)
	if req.GetRoomId() != 0 {
		roomID = req.GetRoomId()
		err = s.rooms.CreateWithID(roomID, req.GetMode(), expect, timeout)
	} else {
		roomID, err = s.rooms.Create(req.GetMode(), expect, timeout)
	}
	if err != nil {
		resp.Code = 1
		s.logger.Warn("create room failed", "node", nodeID, "req", req.GetReqId(),
			"room_id", req.GetRoomId(), "err", err)
		_ = p.send(&internalv2.CrossFrame{
			Data: &internalv2.CrossFrame_CreateResp{CreateResp: resp},
		})
		return
	}

	resp.Code = 0
	resp.RoomId = roomID
	resp.Tokens = make(map[int64]string, len(req.GetPlayerIds()))
	for _, pid := range req.GetPlayerIds() {
		// 中心服已签发令牌则直接采用（令牌权威在中心服）；否则本地签名
		token := req.GetPresetTokens()[pid]
		if token == "" {
			token = s.signToken(nodeID, roomID, pid)
		}
		resp.Tokens[pid] = token
		// 预登记玩家归属：快照到达前就要能路由。
		// 建房成功到首个 Snapshot 之间有时间窗，期间若发生结算
		// （不该发生，但 Actor 时序上可能），NodeOf 找不到人只能丢弃——
		// 预登记把这个窗口从「有」缩到「接近无」
		s.playerNode.Store(pid, nodeID)
	}

	s.logger.Info("room created by game request",
		"node", nodeID, "room_id", roomID, "players", len(resp.Tokens), "mode", req.GetMode())

	_ = p.send(&internalv2.CrossFrame{
		Data: &internalv2.CrossFrame_CreateResp{CreateResp: resp},
	})
}

// signToken 生成玩家准入令牌（HMAC-SHA256 前 16 字节 hex）。
//
// 取前 16 字节而非完整 32 字节：令牌只需抗碰撞到「猜不中」，
// 128bit 截断后仍是 2^128 空间，但帧更小（令牌随每个入房请求传播）。
// 输入拼接 nodeID|roomID|playerID：令牌与「哪个节点、哪个房间、
// 哪个玩家」三元组绑定，拿到 A 房间的令牌进不了 B 房间。
func (s *Server) signToken(nodeID string, roomID uint64, playerID int64) string {
	h := hmac.New(sha256.New, s.tokenKey)
	fmt.Fprintf(h, "%s|%d|%d", nodeID, roomID, playerID)
	return hex.EncodeToString(h.Sum(nil)[:16])
}
