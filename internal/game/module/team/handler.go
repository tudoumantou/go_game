// handler.go —— 协议处理器：队伍信息、创建、加入、离开。
package team

import (
	"context"
	"errors"

	"game_study/internal/common/errcode"
	"game_study/internal/game/module"

	gamev1 "game_study/pkg/pb/game/v1"
)

// onInfo 查询当前队伍信息；不在队回 int32(errcode.ErrTeamNotIn)。
//
// 两次查询（队伍 → 成员）：先查队伍，t==nil 时省掉第二次；
// 队伍上限 5 人，分两次查的代价可接受（合并成一条 JOIN 会把
// 「不在队」和「在队无成员」两种语义搅在一起）。
func (m *Module) onInfo(ctx *module.Context, _ *gamev1.TeamInfoReq, resp *gamev1.TeamInfoResp) error {
	if !m.ready() {
		// 无 DB 环境按「不在队」回：与 (nil, nil) 同码
		return ctx.FailCode(resp, errcode.ErrTeamNotIn)
	}
	t, err := m.repo.FindByPlayer(context.Background(), ctx.State.PlayerID())
	if err != nil {
		return err
	}
	if t == nil {
		return ctx.FailCode(resp, errcode.ErrTeamNotIn)
	}
	members, err := m.repo.Members(context.Background(), t.ID)
	if err != nil {
		return err
	}
	resp.Code = int32(errcode.OK)
	resp.TeamId = t.ID
	resp.CopyId = t.CopyID
	for _, mb := range members {
		resp.Members = append(resp.Members, &gamev1.TeamMemberInfo{
			PlayerId: mb.PlayerID,
			Name:     mb.Name,
			Ready:    mb.Ready,
			IsLeader: mb.IsLeader,
		})
	}
	return nil
}

// onCreate 创建队伍；repo 缺失（单机/测试）时按成功降级。
//
// 前置 FindByPlayer 挡「已有队伍再创建」：仓储层只管建队，
// 「一个人只能在一队」的约束在模块层显式声明（与 guild 同构）。
func (m *Module) onCreate(ctx *module.Context, req *gamev1.TeamCreateReq, resp *gamev1.TeamCreateResp) error {
	if !m.ready() {
		resp.Code = int32(errcode.OK)
		return nil
	}
	if t, err := m.repo.FindByPlayer(context.Background(), ctx.State.PlayerID()); err != nil {
		return err
	} else if t != nil {
		return ctx.FailCode(resp, errcode.ErrTeamAlreadyIn)
	}
	teamID, err := m.repo.Create(context.Background(), ctx.State.PlayerID(), req.CopyId)
	if err != nil {
		return err
	}
	resp.Code = int32(errcode.OK)
	resp.TeamId = teamID
	return nil
}

// onJoin 加入指定队伍；已在别的队先拒绝，超员/不存在由仓储错误码转换。
//
// 前置查询的意义：Join 仓储层只保证「进这个队」的原子性，
// 「你原来在别的队」需要先退再进——模块层显式拒绝跨队直加，
// demo 期不做自动换队（避免两个队伍的成员行竞态）。
func (m *Module) onJoin(ctx *module.Context, req *gamev1.TeamJoinReq, resp *gamev1.TeamJoinResp) error {
	if !m.ready() {
		resp.Code = int32(errcode.OK)
		return nil
	}
	if t, err := m.repo.FindByPlayer(context.Background(), ctx.State.PlayerID()); err != nil {
		return err
	} else if t != nil {
		return ctx.FailCode(resp, errcode.ErrTeamAlreadyIn)
	}
	if err := m.repo.Join(context.Background(), req.TeamId, ctx.State.PlayerID()); err != nil {
		switch {
		case errors.Is(err, ErrTeamNotExist):
			return ctx.FailCode(resp, errcode.ErrTeamNotExist)
		case errors.Is(err, ErrTeamFull):
			return ctx.FailCode(resp, errcode.ErrTeamFull)
		case errors.Is(err, ErrAlreadyIn):
			return ctx.FailCode(resp, errcode.ErrTeamAlreadyIn)
		default:
			return err
		}
	}
	resp.Code = int32(errcode.OK)
	return nil
}

// onLeave 主动退队；队长退队 = 解散（仓储层保证）。
//
// 解散的原子性在仓储事务内：删队长行 → 删全员 → 软删队伍，
// 任一步失败整体回滚，不会出现「队长退了、队伍还在」的半解散态。
func (m *Module) onLeave(ctx *module.Context, _ *gamev1.TeamLeaveReq, resp *gamev1.TeamLeaveResp) error {
	if !m.ready() {
		resp.Code = int32(errcode.OK)
		return nil
	}
	if err := m.repo.Leave(context.Background(), ctx.State.PlayerID()); err != nil {
		if errors.Is(err, ErrNotInTeam) {
			return ctx.FailCode(resp, errcode.ErrTeamNotIn)
		}
		return err
	}
	resp.Code = int32(errcode.OK)
	return nil
}
