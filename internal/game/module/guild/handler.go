// handler.go —— 协议处理器：公会信息、创建、加入、退出。
package guild

import (
	"context"
	"errors"
	"unicode/utf8"

	"game_study/internal/common/errcode"
	"game_study/internal/game/module"

	gamev1 "game_study/pkg/pb/game/v1"
	"strings"
)

// onInfo 查询所在公会；IsLeader 由 LeaderID 与本人比较得出。
//
// IsLeader 用公会行的 leader_id 内存态比较，不再查成员表 title：
// 会长身份以 leader_id 为权威（Quit 解散逻辑也认它），少一次查询
// 且两个判断口径一致，不会出现「title 是会长但 leader_id 是别人」的分歧。
func (m *Module) onInfo(ctx *module.Context, _ *gamev1.GuildInfoReq, resp *gamev1.GuildInfoResp) error {
	// 无 DB 环境按「未入会」回：与 (nil, nil) 同码，客户端行为一致
	if m.repo == nil {
		return ctx.FailCode(resp, errcode.ErrGuildNotIn)
	}
	g, err := m.repo.FindByPlayer(context.Background(), ctx.State.PlayerID())
	if err != nil {
		return err
	}
	if g == nil {
		return ctx.FailCode(resp, errcode.ErrGuildNotIn)
	}
	resp.Code = int32(errcode.OK)
	resp.GuildId = g.ID
	resp.Name = g.Name
	resp.Level = g.Level
	resp.MemberCount = g.MemberCount
	resp.IsLeader = g.LeaderID == ctx.State.PlayerID()
	return nil
}

// onCreate 创建公会：名字按字符（而非字节）校验 2-16，中文名不会被误判。
//
// utf8.RuneCountInString 而非 len：中文名 2 字 = 6 字节，按字节校验会把
// 合法中文名误判超短/超长；TrimSpace 先去首尾空白再计数，
// "  龙  " 这类输入应按 1 字判非法而不是 4 字放行。

// onCreate 创建公会：名字按字符（而非字节）校验 2-16，中文名不会被误判。
//
// utf8.RuneCountInString 而非 len：中文名 2 字 = 6 字节，按字节校验会把
// 合法中文名误判超短/超长；TrimSpace 先去首尾空白再计数，
// "  龙  " 这类输入应按 1 字判非法而不是 4 字放行。
func (m *Module) onCreate(ctx *module.Context, req *gamev1.GuildCreateReq, resp *gamev1.GuildCreateResp) error {
	name := strings.TrimSpace(req.Name)
	if utf8.RuneCountInString(name) < 2 || utf8.RuneCountInString(name) > 16 {
		return ctx.FailCode(resp, errcode.ErrGuildNameIllegal)
	}
	if m.repo == nil {
		// 无 DB 环境：降级直接成功（演示模式，guild_id 留 0）
		resp.Code = int32(errcode.OK)
		return nil
	}
	if g, err := m.repo.FindByPlayer(context.Background(), ctx.State.PlayerID()); err != nil {
		return err
	} else if g != nil {
		// 已有公会再创建：int32(errcode.ErrGuildAlreadyIn)（一个人只能在一个公会）
		return ctx.FailCode(resp, errcode.ErrGuildAlreadyIn)
	}
	guildID, err := m.repo.Create(context.Background(), name, ctx.State.PlayerID())
	if err != nil {
		// 哨兵错误转业务码；其余 err 是 DB 真故障，走 error 通道
		if errors.Is(err, ErrNameUsed) {
			return ctx.FailCode(resp, errcode.ErrGuildNameUsed)
		}
		return err
	}
	resp.Code = int32(errcode.OK)
	resp.GuildId = guildID
	return nil
}

// onJoin 加入公会；仓储错误（不存在/满员/已加入）转业务码。
//
// 前置 FindByPlayer 挡掉「已有公会」的常见情况；switch 把三种哨兵
// 分别转码，default 是 DB 真故障走 error 通道。
// ErrGuildNotExist 复用 int32(errcode.ErrGuildNotIn)：协议未给「目标公会不存在」
// 定义单独码，复用 6101 避免客户端遇到不认识的码。

// onJoin 加入公会；仓储错误（不存在/满员/已加入）转业务码。
//
// 前置 FindByPlayer 挡掉「已有公会」的常见情况；switch 把三种哨兵
// 分别转码，default 是 DB 真故障走 error 通道。
// ErrGuildNotExist 复用 int32(errcode.ErrGuildNotIn)：协议未给「目标公会不存在」
// 定义单独码，复用 6101 避免客户端遇到不认识的码。
func (m *Module) onJoin(ctx *module.Context, req *gamev1.GuildJoinReq, resp *gamev1.GuildJoinResp) error {
	if m.repo == nil {
		resp.Code = int32(errcode.OK)
		return nil
	}
	if g, err := m.repo.FindByPlayer(context.Background(), ctx.State.PlayerID()); err != nil {
		return err
	} else if g != nil {
		return ctx.FailCode(resp, errcode.ErrGuildAlreadyIn)
	}
	if err := m.repo.Join(context.Background(), req.GuildId, ctx.State.PlayerID()); err != nil {
		switch {
		case errors.Is(err, ErrGuildNotExist):
			return ctx.FailCode(resp, errcode.ErrGuildNotIn)
		case errors.Is(err, ErrGuildFull):
			return ctx.FailCode(resp, errcode.ErrGuildFull)
		case errors.Is(err, ErrAlreadyIn):
			return ctx.FailCode(resp, errcode.ErrGuildAlreadyIn)
		default:
			return err
		}
	}
	resp.Code = int32(errcode.OK)
	return nil
}

// onQuit 退出公会；会长退出即解散（仓储层删全员 + 删公会行）。
//
// 「解散」发生在仓储事务内：会长行删除后若后续 DELETE 失败，
// 整个事务回滚，不会出现「会长退了、公会还在」的半解散状态。

// onQuit 退出公会；会长退出即解散（仓储层删全员 + 删公会行）。
//
// 「解散」发生在仓储事务内：会长行删除后若后续 DELETE 失败，
// 整个事务回滚，不会出现「会长退了、公会还在」的半解散状态。
func (m *Module) onQuit(ctx *module.Context, _ *gamev1.GuildQuitReq, resp *gamev1.GuildQuitResp) error {
	if m.repo == nil {
		resp.Code = int32(errcode.OK)
		return nil
	}
	if err := m.repo.Quit(context.Background(), ctx.State.PlayerID()); err != nil {
		if errors.Is(err, ErrNotInGuild) {
			return ctx.FailCode(resp, errcode.ErrGuildNotIn)
		}
		return err
	}
	resp.Code = int32(errcode.OK)
	return nil
}
