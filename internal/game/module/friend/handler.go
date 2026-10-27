// handler.go —— 协议处理器：列表、申请、处理、删除。
//
// 只做「校验参数 → 调 store → 填 resp」。事务、行锁、唯一键冲突全在 store.go。
package friend

import (
	"context"
	"fmt"
	"game_study/internal/common/errcode"
	"game_study/internal/game/module"

	gamev1 "game_study/pkg/pb/game/v1"
	"time"
)

// onList 好友 + 待处理申请列表；在线状态只作展示，不影响关系判定。
func (m *Module) onList(ctx *module.Context, _ *gamev1.FriendListReq, resp *gamev1.FriendListResp) error {
	// Go 知识点：context.WithTimeout 派生带超时的 ctx，
	// defer cancel() 保证函数返回时释放定时器资源（取消会沿 context
	// 树传播到 database/sql 的驱动层，中断慢查询）。
	ctxDB, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	friends, applies, err := m.repo.List(ctxDB, ctx.State.PlayerID())
	if err != nil {
		return fmt.Errorf("list friends: %w", err)
	}

	resp.Code = int32(errcode.OK)
	resp.Limit = friendLimit()
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
	return nil
}

// onApply 发起好友申请；自加/非法目标在模块层拦截，不发 SQL。
//
// 参数校验前置：非法的请求（PlayerId<=0、加自己）连库都不查，
// 既省一次往返，也避免把无意义查询打到 DB。

// onApply 发起好友申请；自加/非法目标在模块层拦截，不发 SQL。
//
// 参数校验前置：非法的请求（PlayerId<=0、加自己）连库都不查，
// 既省一次往返，也避免把无意义查询打到 DB。
func (m *Module) onApply(ctx *module.Context, req *gamev1.FriendApplyReq, resp *gamev1.FriendApplyResp) error {
	if req.PlayerId <= 0 {
		return ctx.FailCode(resp, errcode.ErrFriendTargetNotFound)
	}
	if req.PlayerId == ctx.State.PlayerID() {
		return ctx.FailCode(resp, errcode.ErrFriendSelf)
	}

	ctxDB, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	code, err := m.repo.Apply(ctxDB, ctx.State.PlayerID(), req.PlayerId)
	if err != nil {
		return fmt.Errorf("apply friend: %w", err)
	}
	resp.Code = code
	return nil
}

// onHandle 同意/拒绝申请；过期在仓储层按 TTL 判定并顺手置为已拒绝。

// onHandle 同意/拒绝申请；过期在仓储层按 TTL 判定并顺手置为已拒绝。
func (m *Module) onHandle(ctx *module.Context, req *gamev1.FriendHandleReq, resp *gamev1.FriendHandleResp) error {
	if req.PlayerId <= 0 || req.PlayerId == ctx.State.PlayerID() {
		return ctx.FailCode(resp, errcode.ErrFriendApplyNotFound)
	}

	ctxDB, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	code, err := m.repo.Handle(ctxDB, ctx.State.PlayerID(), req.PlayerId, req.Accept, applyTTL)
	if err != nil {
		return fmt.Errorf("handle friend apply: %w", err)
	}
	resp.Code = code
	return nil
}

// onDelete 删除好友；幂等——本来不是好友也回成功（双向两行一起删）。

// onDelete 删除好友；幂等——本来不是好友也回成功（双向两行一起删）。
func (m *Module) onDelete(ctx *module.Context, req *gamev1.FriendDeleteReq, resp *gamev1.FriendDeleteResp) error {
	if req.PlayerId <= 0 {
		return ctx.FailCode(resp, errcode.ErrFriendTargetNotFound)
	}

	ctxDB, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := m.repo.Delete(ctxDB, ctx.State.PlayerID(), req.PlayerId); err != nil {
		return fmt.Errorf("delete friend: %w", err)
	}
	resp.Code = int32(errcode.OK) // 删除幂等：本来不是好友也视为成功
	return nil
}

// friendLimit 好友上限（friend_limit.xml col1），缺配置回退 50。
