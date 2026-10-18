// internal/cross/rpc/cross_server.go —— CrossService.Channel 实现（设计文档 4.5 / 10.3）
//
// 一条流 = 一个游戏服节点，由游戏服主动建立（地址来自中心服 GroupUpdate）。
// 首帧必须是 CrossCtrl{BIND, node_id}，之后 CrossServer 才能把结算精确回推到该节点
// （不做全网广播 —— 那样会把玩家 A 的结算发给 B 所在的所有游戏服）。
//
// CrossServer 不注册 etcd、不连网关，只接受游戏服连接。
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
	"game_study/pkg/log"

	internalv2 "game_study/pkg/pb/internalv2"
)

// Server CrossService 服务端。
type Server struct {
	internalv2.UnimplementedCrossServiceServer

	logger *log.Logger
	rooms  *battle.Manager
	nodeID string // 本跨服节点 ID（日志/指标）

	// tokenKey 准入令牌签名密钥（进程内随机：房间纯内存，重启后房间与令牌同时失效）
	tokenKey []byte

	mu    sync.RWMutex
	peers map[string]*peer // node_id -> 该节点的下行通道
	// playerNode 玩家 → 游戏服节点（结算时定位回推目标）
	playerNode sync.Map
}

// peer 一个游戏服节点的下行通道。
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
func (s *Server) AttachRooms(rooms *battle.Manager) { s.rooms = rooms }

// NewServer 创建 CrossService 服务端。
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
func (s *Server) Channel(stream internalv2.CrossService_ChannelServer) error {
	// 首帧必须是 BIND
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
	}
	s.peers[nodeID] = p
	s.mu.Unlock()

	s.logger.Info("cross stream bound", "node", nodeID, "cross", s.nodeID)

	// 下行泵：把房间推来的帧写到流里（Send 不能并发调用，必须单 goroutine）
	writeErr := make(chan error, 1)
	go func() { writeErr <- s.writeLoop(p, stream) }()

	defer func() {
		p.close()
		s.mu.Lock()
		if s.peers[nodeID] == p { // 只清理自己，避免误删新连接
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

		case *internalv2.CrossFrame_Ctrl:
			if data.Ctrl == nil {
				continue
			}
			switch data.Ctrl.Type {
			case internalv2.CrossCtrl_ROOM_CLOSED:
				s.logger.Debug("game asked close room", "node", nodeID, "room_id", data.Ctrl.RoomId)
			case internalv2.CrossCtrl_HEALTH:
				// 心跳，无需处理
			}
		}

		// 下行泵异常退出 → 结束本流
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
func (s *Server) NodeOf(playerID int64) (string, bool) {
	v, ok := s.playerNode.Load(playerID)
	if !ok {
		return "", false
	}
	return v.(string), true
}

// RoomClosed 实现 battle.Sink。
func (s *Server) RoomClosed(roomID uint64) { s.rooms.RoomClosed(roomID) }

// CloseAllPeers 主动断开所有游戏服流（优雅关闭用）。
//
// 不主动断的话，gRPC 的 GracefulStop 会一直等这些双向流结束 —— 而流永远不会自己结束。
func (s *Server) CloseAllPeers() {
	s.mu.Lock()
	peers := make([]*peer, 0, len(s.peers))
	for _, p := range s.peers {
		peers = append(peers, p)
	}
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
func (s *Server) PeerCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.peers)
}

// onCreateReq 处理建房请求（P13 直连路径）。
//
// 令牌用 HMAC(nodeID|roomID|playerID) 生成，CrossServer 入房时比对；
// 这样即使 room_id 泄漏，没有密钥也造不出有效令牌。
func (s *Server) onCreateReq(nodeID string, p *peer, req *internalv2.RoomCreateReq) {
	resp := &internalv2.RoomCreateResp{ReqId: req.GetReqId()}

	expect := int(req.GetExpectPlayers())
	if expect <= 0 {
		expect = 2
	}
	timeout := time.Duration(req.GetTimeoutSec()) * time.Second

	roomID, err := s.rooms.Create(req.GetMode(), expect, timeout)
	if err != nil {
		resp.Code = 1
		s.logger.Warn("create room failed", "node", nodeID, "req", req.GetReqId(), "err", err)
		_ = p.send(&internalv2.CrossFrame{
			Data: &internalv2.CrossFrame_CreateResp{CreateResp: resp},
		})
		return
	}

	resp.Code = 0
	resp.RoomId = roomID
	resp.Tokens = make(map[int64]string, len(req.GetPlayerIds()))
	for _, pid := range req.GetPlayerIds() {
		token := s.signToken(nodeID, roomID, pid)
		resp.Tokens[pid] = token
		// 预登记玩家归属：快照到达前就要能路由
		s.playerNode.Store(pid, nodeID)
	}

	s.logger.Info("room created by game request",
		"node", nodeID, "room_id", roomID, "players", len(resp.Tokens), "mode", req.GetMode())

	_ = p.send(&internalv2.CrossFrame{
		Data: &internalv2.CrossFrame_CreateResp{CreateResp: resp},
	})
}

// signToken 生成玩家准入令牌（HMAC-SHA256 前 16 字节 hex）。
func (s *Server) signToken(nodeID string, roomID uint64, playerID int64) string {
	h := hmac.New(sha256.New, s.tokenKey)
	fmt.Fprintf(h, "%s|%d|%d", nodeID, roomID, playerID)
	return hex.EncodeToString(h.Sum(nil)[:16])
}
