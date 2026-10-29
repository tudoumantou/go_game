// handler.go —— 协议处理器：列表、申请、处理、删除。
//
// 只做「校验参数 → 调 store → 填 resp」。事务、行锁、唯一键冲突全在 store.go。
package friend

import (
	"fmt"
	"game_study/internal/common/errcode"
	"game_study/internal/game/module"

	gamev1 "game_study/pkg/pb/game/v1"
)

// onList 好友 + 待处理申请列表；在线状态只作展示，不影响关系判定。
func (m *Module) onList(c *module.Ctx[module.NoState], _ *gamev1.FriendListReq) error {
	// module.QueryCtx 是「Background + 模块统一上限」，defer cancel() 释放
	// 定时器并让取消沿 context 树传到 database/sql 驱动层，中断慢查询。
	// 上限值集中在 module.QueryTimeout，本文件不再出现 3s 字面量。
	ctxDB, cancel := module.QueryCtx()
	defer cancel()

	friends, applies, err := m.repo.List(ctxDB, c.State.PlayerID())
	if err != nil {
		return fmt.Errorf("list friends: %w", err)
	}

	resp := &gamev1.FriendListResp{Code: int32(errcode.OK), Limit: friendLimit()}
	for _, f := range friends {
		online := false
		if m.online != nil {
			if _, ok := m.online.IsOnline(ctxDB, f.PlayerID); ok {
				online = true
			}
		}
		resp.Friends = append(resp.Friends, &gamev1.FriendEntry{
			PlayerId:  f.PlayerID,
			Name:      f.Name,
			Level:     f.Level,
			Online:    online,
			CreatedAt: f.CreatedAt.Unix(),
		})
	}
	for _, a := range applies {
		resp.Applies = append(resp.Applies, &gamev1.FriendApplyEntry{
			PlayerId:  a.FromID,
			Name:      a.Name,
			Level:     a.Level,
			CreatedAt: a.CreatedAt.Unix(),
		})
	}

	// 黑名单单独一段查：它与好友/申请不是同一张表，合进 List 会让那条
	// 已经够长的函数更难读。在线状态对黑名单没意义，Online 恒为 false。
	blocks, err := m.repo.Blocks(ctxDB, c.State.PlayerID())
	if err != nil {
		return fmt.Errorf("list blocked: %w", err)
	}
	for _, b := range blocks {
		resp.Blocks = append(resp.Blocks, &gamev1.FriendEntry{
			PlayerId:  b.PlayerID,
			Name:      b.Name,
			Level:     b.Level,
			CreatedAt: b.CreatedAt.Unix(),
		})
	}
	return c.Reply(resp)
}

// onBlock 拉黑：写黑名单并解除双向好友。
func (m *Module) onBlock(c *module.Ctx[module.NoState], req *gamev1.FriendBlockReq) error {
	resp := &gamev1.FriendBlockResp{}
	if req.PlayerId <= 0 {
		return c.FailCode(resp, errcode.ErrFriendTargetNotFound)
	}
	me := c.State.PlayerID()
	if req.PlayerId == me {
		return c.FailCode(resp, errcode.ErrFriendBlockSelf)
	}

	ctxDB, cancel := module.QueryCtx()
	defer cancel()
	if err := m.repo.Block(ctxDB, me, req.PlayerId); err != nil {
		return fmt.Errorf("block player %d: %w", req.PlayerId, err)
	}
	return c.Reply(resp)
}

// onUnblock 取消拉黑（幂等）。
func (m *Module) onUnblock(c *module.Ctx[module.NoState], req *gamev1.FriendUnblockReq) error {
	resp := &gamev1.FriendUnblockResp{}
	if req.PlayerId <= 0 {
		return c.FailCode(resp, errcode.ErrFriendTargetNotFound)
	}

	ctxDB, cancel := module.QueryCtx()
	defer cancel()
	if err := m.repo.Unblock(ctxDB, c.State.PlayerID(), req.PlayerId); err != nil {
		return fmt.Errorf("unblock player %d: %w", req.PlayerId, err)
	}
	return c.Reply(resp)
}

// onApply 发起好友申请；自加/非法目标在模块层拦截，不发 SQL。
//
// 参数校验前置：非法的请求（PlayerId<=0、加自己）连库都不查，
// 既省一次往返，也避免把无意义查询打到 DB。
func (m *Module) onApply(c *module.Ctx[module.NoState], req *gamev1.FriendApplyReq) error {
	resp := &gamev1.FriendApplyResp{}
	if req.PlayerId <= 0 {
		return c.FailCode(resp, errcode.ErrFriendTargetNotFound)
	}
	if req.PlayerId == c.State.PlayerID() {
		return c.FailCode(resp, errcode.ErrFriendSelf)
	}

	ctxDB, cancel := module.QueryCtx()
	defer cancel()
	code, err := m.repo.Apply(ctxDB, c.State.PlayerID(), req.PlayerId)
	if err != nil {
		return fmt.Errorf("apply friend: %w", err)
	}
	// code 是仓储按业务场景算出的动态码（已存在/已申请/成功），
	// 不是编译期常量，所以直接写字段而不走 FailCode。
	resp.Code = code
	return c.Reply(resp)
}

// onHandle 同意/拒绝申请；过期在仓储层按 TTL 判定并顺手置为已拒绝。
func (m *Module) onHandle(c *module.Ctx[module.NoState], req *gamev1.FriendHandleReq) error {
	resp := &gamev1.FriendHandleResp{}
	if req.PlayerId <= 0 || req.PlayerId == c.State.PlayerID() {
		return c.FailCode(resp, errcode.ErrFriendApplyNotFound)
	}

	ctxDB, cancel := module.QueryCtx()
	defer cancel()
	code, err := m.repo.Handle(ctxDB, c.State.PlayerID(), req.PlayerId, req.Accept, applyTTL)
	if err != nil {
		return fmt.Errorf("handle friend apply: %w", err)
	}
	resp.Code = code
	return c.Reply(resp)
}

// onDelete 删除好友；幂等——本来不是好友也回成功（双向两行一起删）。
func (m *Module) onDelete(c *module.Ctx[module.NoState], req *gamev1.FriendDeleteReq) error {
	resp := &gamev1.FriendDeleteResp{}
	if req.PlayerId <= 0 {
		return c.FailCode(resp, errcode.ErrFriendTargetNotFound)
	}

	ctxDB, cancel := module.QueryCtx()
	defer cancel()
	if err := m.repo.Delete(ctxDB, c.State.PlayerID(), req.PlayerId); err != nil {
		return fmt.Errorf("delete friend: %w", err)
	}
	resp.Code = int32(errcode.OK) // 删除幂等：本来不是好友也视为成功
	return c.Reply(resp)
}
