// internal/game/rpc/game_server.go —— GameService.Channel 双向流实现
package rpc

import (
	"context"
	"fmt"
	"io"
	"sync"

	"game_study/pkg/log"

	internalv2 "game_study/pkg/pb/internalv2"
)

// StreamHandler 处理 stream 上的数据。
type StreamHandler interface {
	OnBind(ctx context.Context, sessionID uint64, accountID, playerID int64) error
	OnUnbind(ctx context.Context, sessionID uint64) error
	OnClientPacket(ctx context.Context, sessionID uint64, msgID uint32, seq uint32, body []byte) error
}

// GameServer GameService 实现。
type GameServer struct {
	internalv2.UnimplementedGameServiceServer
	handler StreamHandler
	logger  *log.Logger

	mu      sync.RWMutex
	streams map[uint64]internalv2.GameService_ChannelServer
}

// NewGameServer 创建 GameServer。
func NewGameServer(handler StreamHandler, logger *log.Logger) *GameServer {
	return &GameServer{
		handler: handler,
		logger:  logger,
		streams: make(map[uint64]internalv2.GameService_ChannelServer),
	}
}

// Channel 实现双向流。
func (s *GameServer) Channel(stream internalv2.GameService_ChannelServer) error {
	ctx := stream.Context()
	// 等待第一条 BIND 控制消息建立会话映射
	frame, err := stream.Recv()
	if err != nil {
		return err
	}
	ctrl := frame.GetCtrl()
	if ctrl == nil || ctrl.Type != internalv2.CtrlMsg_BIND {
		return fmt.Errorf("first frame must be BIND")
	}
	sessionID := ctrl.SessionId
	s.bindStream(sessionID, stream)
	defer s.unbindStream(sessionID)

	if err := s.handler.OnBind(ctx, sessionID, ctrl.AccountId, ctrl.PlayerId); err != nil {
		return err
	}

	for {
		frame, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			_ = s.handler.OnUnbind(ctx, sessionID)
			return err
		}

		switch d := frame.Data.(type) {
		case *internalv2.StreamFrame_ClientPacket:
			cp := d.ClientPacket
			if err := s.handler.OnClientPacket(ctx, cp.SessionId, cp.MsgId, cp.Seq, cp.Body); err != nil {
				s.logger.Warn("handle client packet", "session_id", sessionID, "err", err)
			}
		case *internalv2.StreamFrame_Ctrl:
			s.handleCtrl(ctx, d.Ctrl)
		}
	}
}

func (s *GameServer) handleCtrl(ctx context.Context, ctrl *internalv2.CtrlMsg) {
	switch ctrl.Type {
	case internalv2.CtrlMsg_UNBIND:
		_ = s.handler.OnUnbind(ctx, ctrl.SessionId)
	case internalv2.CtrlMsg_KICK:
		// TODO(P3): 踢掉本地玩家
	}
}

func (s *GameServer) bindStream(sessionID uint64, stream internalv2.GameService_ChannelServer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.streams[sessionID] = stream
}

func (s *GameServer) unbindStream(sessionID uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.streams, sessionID)
}

// SendToSession 向指定 session 的 stream 发送下行包。
func (s *GameServer) SendToSession(sessionID uint64, frame *internalv2.StreamFrame) error {
	s.mu.RLock()
	stream, ok := s.streams[sessionID]
	s.mu.RUnlock()
	if !ok {
		return fmt.Errorf("session %d stream not found", sessionID)
	}
	return stream.Send(frame)
}
