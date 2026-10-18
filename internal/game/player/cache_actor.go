package player

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"game_study/pkg/writebehind"
	"time"

	"game_study/pkg/actor"
	gamev1 "game_study/pkg/pb/game/v1"
	gatev1 "game_study/pkg/pb/gate/v1"
	"google.golang.org/protobuf/proto"
)

type cachedReply struct {
	id   uint32
	body []byte
}

func (a *Actor) receiveCached(ctx *actor.Context) {
	if msg, ok := ctx.Message().(*BindMsg); ok {
		// 会话路由独立于热状态加载，即使 Redis 故障也要能向当前连接回报错误。
		a.sessionID = msg.SessionID
		a.online = true
	}
	if a.cachePending != nil {
		if err := a.finishPending(); err != nil {
			a.cacheFailure(ctx, err)
			return
		}
	}
	if !a.loaded {
		switch ctx.Message().(type) {
		case *SaveMsg, *UnbindMsg:
			return
		}
		if err := a.preload(ctx); err != nil {
			a.cacheFailure(ctx, err)
			return
		}
	}
	if a.cacheReload {
		loadCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		state, version, err := a.repo.LoadCached(loadCtx, a.playerID)
		cancel()
		if err != nil {
			a.cacheFailure(ctx, err)
			return
		}
		// 保留在线派生加成，Redis 只保存持久化基础属性。
		state.bonus = a.state.bonus
		a.setState(state)
		a.cacheVersion = version
		a.cacheReload = false
	}
	previous := a.state.clone()
	before, _ := json.Marshal(previous.snapshot())
	a.bufferReplies = true
	a.queuedReplies = nil
	completed := false
	defer func() {
		a.bufferReplies = false
		a.queuedReplies = nil
		if !completed {
			a.setState(previous)
			a.cacheReload = true
		}
	}()
	a.receive(ctx)
	after, err := json.Marshal(a.state.snapshot())
	if err == nil && !bytes.Equal(before, after) {
		saveCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		var version int64
		version, err = a.repo.cache.Commit(saveCtx, a.state.snapshot(), a.cacheVersion)
		cancel()
		if err == nil {
			a.cacheVersion = version
			a.state.ClearItemsDirty()
		}
	}
	a.bufferReplies = false
	if err != nil {
		if canRollbackMessage(ctx.Message()) {
			a.setState(previous)
			a.cacheReload = true
		} else {
			// 旧模块可能已经提交 SQL 领取事实，不能再撤销其资产。
			// 保留内存中的待提交快照，后续消息先恢复提交再进入业务。
			a.cachePending = &writebehind.Record{Version: a.cacheVersion, State: a.state.snapshot()}
			a.cacheReload = false
		}
		completed = true
		a.queuedReplies = nil
		a.cacheFailure(ctx, err)
		return
	}
	completed = true
	for _, reply := range a.queuedReplies {
		if err := a.send(reply.id, reply.body); err != nil {
			a.logger.Warn("cached reply failed", "err", err)
		}
	}
	if msg, ok := ctx.Message().(*GrantItemsMsg); ok && msg.Done != nil {
		msg.Done <- nil
	}
}

func canRollbackMessage(message any) bool {
	switch msg := message.(type) {
	case *GrantItemsMsg:
		return true // 与资产同快照的 BizID 负责幂等重放。
	case *PacketMsg:
		switch msg.MsgID {
		case 6601, 6603, 3003, 2001:
			return true
		}
	}
	return false
}

func (a *Actor) finishPending() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pending := a.cachePending
	current, err := a.repo.cache.Load(ctx, a.playerID)
	if err != nil {
		return err
	}
	version := current.Version
	switch {
	case current.Version == pending.Version:
		version, err = a.repo.cache.Commit(ctx, pending.State, pending.Version)
		if err != nil {
			return err
		}
	case current.Version == pending.Version+1:
		expected, _ := json.Marshal(pending.State)
		actual, _ := json.Marshal(current.State)
		if !bytes.Equal(expected, actual) {
			return fmt.Errorf("pending legacy snapshot conflicts with cache")
		}
	default:
		return fmt.Errorf("pending legacy snapshot version conflict")
	}
	// 保留 SQL 已提交的资产，核对未知提交后才恢复后续业务。
	a.cacheVersion = version
	a.cachePending = nil
	a.state.ClearItemsDirty()
	return nil
}

func (a *Actor) cacheFailure(ctx *actor.Context, err error) {
	a.logger.Warn("player cache operation failed", "err", err)
	switch msg := ctx.Message().(type) {
	case *GrantItemsMsg:
		if msg.Done != nil {
			msg.Done <- err
		}
	case *BindMsg:
		_ = ctx.Respond(&BindResult{Code: 1, Err: err.Error()})
	case *PacketMsg:
		var response proto.Message
		var id uint32
		switch msg.MsgID {
		case 6601:
			id = 6602
			response = &gamev1.SigninInfoResp{Code: 1}
		case 6603:
			id = 6604
			response = &gamev1.SigninClaimResp{Code: 1}
		default:
			id = 20
			response = &gatev1.ErrorNotify{Code: 1005, Msg: "缓存暂不可用，请重试", Context: fmt.Sprintf("related_msg_id=%d", msg.MsgID)}
		}
		body, _ := proto.Marshal(response)
		_ = a.send(id, body)
	}
}
