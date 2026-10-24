// internal/game/rpc/game_server.go —— GameService.Channel 双向流实现
//
// 架构职责：game 进程对网关暴露的唯一入口——一条 gRPC 双向流 = 一个客户端
// 会话。上行（BIND/ClientPacket/Ctrl）翻译成 handler 调用，下行经
// SendToSession 写回同一条流。
//
// 协作关系：rpc.StreamHandler 由 game.Server 实现（server.go 的 OnBind/
// OnUnbind/OnClientPacket）；本文件只管流的生命周期与会话映射，
// 不懂任何业务。
//
// 设计取舍：
//   - 为什么下行必须走 GameServer.SendToSession 而不是让业务直接持有
//     stream：stream.Send 不是并发安全的（HTTP/2 多路复用下乱序会直接
//     被对端拒绝），统一经 streams map + RWMutex 串行化写出口；
//   - 为什么首帧强制 BIND：没有 sessionID 就无法建立「流 → 会话」映射，
//     后续所有下行都找不到目标；先校验再进循环，避免半开连接占资源。
package rpc

import (
	"context"
	"fmt"
	"io"
	"sync"

	"game_study/pkg/log"

	internalv2 "game_study/pkg/pb/internalv2"
)

// AdminSessionID 管理流专用的会话号。
//
// 网关除了「每个玩家一条流」之外，还需要一条「谁来都行、只用来下指令」的
// 通道（GM 踢人、广播）。这条流不绑定任何玩家，因此不能占用真实会话号——
// 真实会话号来自网关侧连接 ID，必然 > 0，用 0 做哨兵不会冲突，
// 也不会被误当成某个玩家。
const AdminSessionID uint64 = 0

// StreamHandler 处理 stream 上的数据。
//
// 三个方法的错误只记日志不中断流（见 Channel）：单包处理失败不该掐掉
// 整个会话，客户端可重试或走重连。
type StreamHandler interface {
	OnBind(ctx context.Context, sessionID uint64, accountID, playerID int64) error
	OnUnbind(ctx context.Context, sessionID uint64) error
	OnClientPacket(ctx context.Context, sessionID uint64, msgID uint32, seq uint32, body []byte) error
	// OnKick 网关要求强制下线（GM 踢人/封禁生效）。
	//
	// 参数是**角色 ID** 而不是会话号：网关不知道「玩家在哪个会话」，
	// 那份映射属于游戏服（players2sess 是唯一权威）。
	// reason 是给客户端看的文案（KickNotify.Reason），可为空。
	OnKick(ctx context.Context, playerID int64, reason string) error
}

// GameServer GameService 实现。
type GameServer struct {
	internalv2.UnimplementedGameServiceServer
	handler StreamHandler
	logger  *log.Logger

	// mu 保护 streams。gRPC 每个 Channel 调用一个独立 goroutine，多个
	// 会话并发 Recv/Send，普通 map 的并发读写必须显式加锁
	// （Go 的 map 并发写会触发不可恢复的 fatal error）。
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
//
// 生命周期：等首帧 BIND → 注册 streams（defer 反注册）→ 循环 Recv 分发。
// 阻塞至流断开：io.EOF 是对端正常关闭，其余错误在触发 OnUnbind 后上报。
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
	if sessionID == AdminSessionID {
		// 管理流：不绑定玩家、不进 streams 表（它没有下行需求）。
		// 多个网关实例可以各建一条，彼此不覆盖——这正是「管理流不注册到
		// streams」的原因：用 sessionID=0 当 key 会互相顶掉。
		return s.serveAdminStream(ctx, stream)
	}
	s.bindStream(sessionID, stream)
	// defer 保证任何返回路径（EOF/错误/panic 后的正常返回）都摘掉映射，
	// 否则已断开的流会留在 streams 里，下行持续静默失败
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
			// 对端异常断开（非 EOF）：补一次 OnUnbind，让会话编排层有机会
			// 清理映射与在线状态；之后才向上返回错误
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

// serveAdminStream 处理管理流：只接受控制帧，忽略业务包。
//
// 管理流没有玩家上下文，收到 ClientPacket 说明对端用错了流，丢弃即可
// （不能走 OnClientPacket——那需要 sessionID 对应的会话）。
func (s *GameServer) serveAdminStream(ctx context.Context, stream internalv2.GameService_ChannelServer) error {
	for {
		frame, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if d, ok := frame.Data.(*internalv2.StreamFrame_Ctrl); ok {
			s.handleCtrl(ctx, d.Ctrl)
			continue
		}
		s.logger.Warn("unexpected frame on admin stream")
	}
}

// handleCtrl 处理会话级控制帧（UNBIND/KICK）。
func (s *GameServer) handleCtrl(ctx context.Context, ctrl *internalv2.CtrlMsg) {
	switch ctrl.Type {
	case internalv2.CtrlMsg_UNBIND:
		_ = s.handler.OnUnbind(ctx, ctrl.SessionId)
	case internalv2.CtrlMsg_KICK:
		// 强制下线：GM 踢人、封禁生效。与玩家主动登出/掉线不同，
		// 这里由服务端发起，客户端收到 KickNotify 后断开。
		//
		// 按角色号路由：网关只知道 player_id，会话由游戏服自己查
		// （广播给所有节点，只有持有该玩家的那份会命中）。
		if ctrl.PlayerId == 0 {
			s.logger.Warn("kick without player_id ignored", "session_id", ctrl.SessionId)
			return
		}
		if err := s.handler.OnKick(ctx, ctrl.PlayerId, string(ctrl.Payload)); err != nil {
			s.logger.Warn("handle kick", "player_id", ctrl.PlayerId, "err", err)
		}
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
//
// 先 RLock 取 stream 再在锁外 Send：Send 可能阻塞（对端不读时流控背压），
// 持锁发送会把 bindStream/unbindStream 全部堵死。
func (s *GameServer) SendToSession(sessionID uint64, frame *internalv2.StreamFrame) error {
	s.mu.RLock()
	stream, ok := s.streams[sessionID]
	s.mu.RUnlock()
	if !ok {
		return fmt.Errorf("session %d stream not found", sessionID)
	}
	return stream.Send(frame)
}
