// internal/game/module/guild/guild_test.go —— 公会模块单测
//
// 公会与队伍的分工差异值得在这里钉死：公会掉线不退会（持久社交关系），
// 会长退会即解散；名字按字符校验而不是字节（中文名不能被误判）。
// SQL 事务（人数上限加锁、解散原子性）由集成测试兜底。
package guild

import (
	"context"
	"strings"
	"testing"

	"game_study/internal/common/errcode"
	"game_study/internal/game/module"
	"game_study/internal/game/module/moduletest"
	"game_study/pkg/codec"
	"game_study/pkg/log"
	gamev1 "game_study/pkg/pb/game/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// ============================================================================
// 测试替身
// ============================================================================

type fakeRepo struct {
	guild     *Guild
	findErr   error
	createID  int64
	createErr error
	joinErr   error
	quitErr   error

	createdWith struct {
		name     string
		leaderID int64
	}
	joinedWith struct {
		guildID  int64
		playerID int64
	}
	quitCalls int
}

func (f *fakeRepo) FindByPlayer(context.Context, int64) (*Guild, error) {
	return f.guild, f.findErr
}

func (f *fakeRepo) Create(_ context.Context, name string, leaderID int64) (int64, error) {
	f.createdWith.name = name
	f.createdWith.leaderID = leaderID
	return f.createID, f.createErr
}

func (f *fakeRepo) Join(_ context.Context, guildID, playerID int64) error {
	f.joinedWith.guildID = guildID
	f.joinedWith.playerID = playerID
	return f.joinErr
}

func (f *fakeRepo) Quit(context.Context, int64) error {
	f.quitCalls++
	return f.quitErr
}

// ============================================================================
// 装配
// ============================================================================

func newTestModule(repo guildRepo) (*Module, *moduletest.State, *moduletest.Recorder, *module.Context) {
	st := moduletest.NewState(100)
	rec := moduletest.NewRecorder()
	m := New(repo, log.New("guild-test", log.WithLevel("error")))
	return m, st, rec, moduletest.NewContext(st, rec)
}

func call(t *testing.T, m *Module, ctx *module.Context, rec *moduletest.Recorder, reqID, respID uint32, req, resp proto.Message) {
	t.Helper()
	before := rec.Count(respID)
	raw, err := moduletest.Call(m, ctx, rec, reqID, respID, req)
	require.NoError(t, err)
	require.NotNil(t, raw, "没有回包: req=%d", reqID)
	require.NoError(t, proto.Unmarshal(raw, resp))
	require.Equal(t, before+1, rec.Count(respID), "一次请求只能回一个包")
}

func create(t *testing.T, m *Module, ctx *module.Context, rec *moduletest.Recorder, name string) *gamev1.GuildCreateResp {
	t.Helper()
	resp := &gamev1.GuildCreateResp{}
	call(t, m, ctx, rec, codec.MsgGuildCreateReq, codec.MsgGuildCreateResp, &gamev1.GuildCreateReq{Name: name}, resp)
	return resp
}

// ============================================================================
// 查询
// ============================================================================

// TestInfoNotInGuild 未入会回 6101；无库环境同码降级。
func TestInfoNotInGuild(t *testing.T) {
	m, _, rec, ctx := newTestModule(&fakeRepo{})
	resp := &gamev1.GuildInfoResp{}
	call(t, m, ctx, rec, codec.MsgGuildInfoReq, codec.MsgGuildInfoResp, &gamev1.GuildInfoReq{}, resp)
	require.EqualValues(t, int32(errcode.ErrGuildNotIn), resp.Code)

	m2, _, rec2, ctx2 := newTestModule(nil)
	resp2 := &gamev1.GuildInfoResp{}
	call(t, m2, ctx2, rec2, codec.MsgGuildInfoReq, codec.MsgGuildInfoResp, &gamev1.GuildInfoReq{}, resp2)
	require.EqualValues(t, int32(errcode.ErrGuildNotIn), resp2.Code)
}

// TestInfoIsLeaderFromLeaderID 会长身份以 leader_id 为准：
// 同一条公会行，本人是会长时 IsLeader=true，是成员时 false。
func TestInfoIsLeaderFromLeaderID(t *testing.T) {
	g := &Guild{ID: 5, Name: "龙腾", LeaderID: 100, Level: 2, MemberCount: 13}
	m, _, rec, ctx := newTestModule(&fakeRepo{guild: g})

	resp := &gamev1.GuildInfoResp{}
	call(t, m, ctx, rec, codec.MsgGuildInfoReq, codec.MsgGuildInfoResp, &gamev1.GuildInfoReq{}, resp)
	require.EqualValues(t, int32(errcode.OK), resp.Code)
	require.EqualValues(t, 5, resp.GuildId)
	require.EqualValues(t, "龙腾", resp.Name)
	require.EqualValues(t, 2, resp.Level)
	require.EqualValues(t, 13, resp.MemberCount)
	require.True(t, resp.IsLeader, "leader_id == 本人 → 会长")

	// 同一条公会行，换一个非会长玩家查询
	m2, _, rec2, ctx2 := newTestModule(&fakeRepo{guild: g})
	ctx2.State = moduletest.NewState(200)
	resp2 := &gamev1.GuildInfoResp{}
	call(t, m2, ctx2, rec2, codec.MsgGuildInfoReq, codec.MsgGuildInfoResp, &gamev1.GuildInfoReq{}, resp2)
	require.False(t, resp2.IsLeader, "不是 leader_id 就不是会长")
}

// TestInfoPropagatesError 查库真故障上抛（不能伪装成「未入会」）。
func TestInfoPropagatesError(t *testing.T) {
	m, _, rec, ctx := newTestModule(&fakeRepo{findErr: context.DeadlineExceeded})
	_, err := moduletest.Call(m, ctx, rec, codec.MsgGuildInfoReq, codec.MsgGuildInfoResp, &gamev1.GuildInfoReq{})
	require.Error(t, err)
}

// ============================================================================
// 创建 —— 名字校验（按字符而非字节）
// ============================================================================

// TestCreateNameValidation 名字 2-16 个字符，先 TrimSpace 再计数。
func TestCreateNameValidation(t *testing.T) {
	cases := []struct {
		name  string
		input string
		want  int32
	}{
		{"empty", "", int32(errcode.ErrGuildNameIllegal)},
		{"one char", "龙", int32(errcode.ErrGuildNameIllegal)},
		{"only spaces", "    ", int32(errcode.ErrGuildNameIllegal)},
		{"spaces around one char", "  龙  ", int32(errcode.ErrGuildNameIllegal)},
		{"two chinese chars", "龙腾", int32(errcode.OK)},
		{"sixteen chinese chars", strings.Repeat("龙", 16), int32(errcode.OK)},
		{"seventeen chinese chars", strings.Repeat("龙", 17), int32(errcode.ErrGuildNameIllegal)},
		{"ascii two chars", "ab", int32(errcode.OK)},
		{"sixteen ascii", "abcdefghijklmnop", int32(errcode.OK)},
		{"seventeen ascii", "abcdefghijklmnopq", int32(errcode.ErrGuildNameIllegal)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			m, _, rec, ctx := newTestModule(&fakeRepo{createID: 9})
			resp := create(t, m, ctx, rec, c.input)
			require.EqualValues(t, c.want, resp.Code, "input=%q", c.input)
		})
	}
}

// TestCreateNameIsTrimmed 建会时用的是去掉首尾空白后的名字，空格不能进库。
func TestCreateNameIsTrimmed(t *testing.T) {
	repo := &fakeRepo{createID: 9}
	m, _, rec, ctx := newTestModule(repo)

	resp := create(t, m, ctx, rec, "  龙腾  ")
	require.EqualValues(t, int32(errcode.OK), resp.Code)
	require.EqualValues(t, "龙腾", repo.createdWith.name, "入库名字必须已去空白")
}

// TestCreateRejectsWhenAlreadyIn 已有公会再创建：6104，且不调仓储。
func TestCreateRejectsWhenAlreadyIn(t *testing.T) {
	repo := &fakeRepo{guild: &Guild{ID: 1}}
	m, _, rec, ctx := newTestModule(repo)

	resp := create(t, m, ctx, rec, "龙腾")
	require.EqualValues(t, int32(errcode.ErrGuildAlreadyIn), resp.Code)
	require.Zero(t, repo.createdWith.leaderID, "已在会时不能调 Create")
}

// TestCreateNameUsed 名字重复（仓储 ErrNameUsed）→ 6103。
func TestCreateNameUsed(t *testing.T) {
	repo := &fakeRepo{createErr: ErrNameUsed}
	m, _, rec, ctx := newTestModule(repo)

	resp := create(t, m, ctx, rec, "龙腾")
	require.EqualValues(t, int32(errcode.ErrGuildNameUsed), resp.Code)
}

// TestCreatePassesLeader 建会：会长取会话身份。
func TestCreatePassesLeader(t *testing.T) {
	repo := &fakeRepo{createID: 42}
	m, _, rec, ctx := newTestModule(repo)

	resp := create(t, m, ctx, rec, "龙腾")
	require.EqualValues(t, int32(errcode.OK), resp.Code)
	require.EqualValues(t, 42, resp.GuildId)
	require.EqualValues(t, 100, repo.createdWith.leaderID)
}

// TestCreateNoRepoDegrades 无库环境：名字仍需校验，合法名字按成功降级。
func TestCreateNoRepoDegrades(t *testing.T) {
	m, _, rec, ctx := newTestModule(nil)
	require.EqualValues(t, int32(errcode.OK), create(t, m, ctx, rec, "龙腾").Code)
	require.EqualValues(t, int32(errcode.ErrGuildNameIllegal), create(t, m, ctx, rec, "龙").Code,
		"降级模式下名字校验不能一起被跳过")
}

// TestCreatePropagatesErrors 前置查询失败与建会失败都要上抛。
func TestCreatePropagatesErrors(t *testing.T) {
	m, _, rec, ctx := newTestModule(&fakeRepo{findErr: context.DeadlineExceeded})
	_, err := moduletest.Call(m, ctx, rec, codec.MsgGuildCreateReq, codec.MsgGuildCreateResp, &gamev1.GuildCreateReq{Name: "龙腾"})
	require.Error(t, err)

	m2, _, rec2, ctx2 := newTestModule(&fakeRepo{createErr: context.DeadlineExceeded})
	_, err = moduletest.Call(m2, ctx2, rec2, codec.MsgGuildCreateReq, codec.MsgGuildCreateResp, &gamev1.GuildCreateReq{Name: "龙腾"})
	require.Error(t, err)
}

// ============================================================================
// 加入
// ============================================================================

// TestJoinTranslatesRepoErrors 三个哨兵错误 → 业务码；未知错误上抛。
func TestJoinTranslatesRepoErrors(t *testing.T) {
	cases := []struct {
		err  error
		want int32
	}{
		{ErrGuildNotExist, int32(errcode.ErrGuildNotIn)}, // 协议没给「目标不存在」单独码，复用 6101
		{ErrGuildFull, int32(errcode.ErrGuildFull)},
		{ErrAlreadyIn, int32(errcode.ErrGuildAlreadyIn)},
	}
	for _, c := range cases {
		m, _, rec, ctx := newTestModule(&fakeRepo{joinErr: c.err})
		resp := &gamev1.GuildJoinResp{}
		call(t, m, ctx, rec, codec.MsgGuildJoinReq, codec.MsgGuildJoinResp, &gamev1.GuildJoinReq{GuildId: 7}, resp)
		require.EqualValues(t, c.want, resp.Code, "%v", c.err)
	}

	m, _, rec, ctx := newTestModule(&fakeRepo{joinErr: context.DeadlineExceeded})
	_, err := moduletest.Call(m, ctx, rec, codec.MsgGuildJoinReq, codec.MsgGuildJoinResp, &gamev1.GuildJoinReq{GuildId: 7})
	require.Error(t, err)
}

// TestJoinRejectsWhenAlreadyIn 已有公会时拒绝加入别的（不做自动转会）。
func TestJoinRejectsWhenAlreadyIn(t *testing.T) {
	repo := &fakeRepo{guild: &Guild{ID: 1}}
	m, _, rec, ctx := newTestModule(repo)

	resp := &gamev1.GuildJoinResp{}
	call(t, m, ctx, rec, codec.MsgGuildJoinReq, codec.MsgGuildJoinResp, &gamev1.GuildJoinReq{GuildId: 7}, resp)
	require.EqualValues(t, int32(errcode.ErrGuildAlreadyIn), resp.Code)
	require.Zero(t, repo.joinedWith.guildID, "已在会时不能调 Join")
}

// TestJoinSucceeds 正常加入：传对公会 ID 与会话身份。
func TestJoinSucceeds(t *testing.T) {
	repo := &fakeRepo{}
	m, _, rec, ctx := newTestModule(repo)

	resp := &gamev1.GuildJoinResp{}
	call(t, m, ctx, rec, codec.MsgGuildJoinReq, codec.MsgGuildJoinResp, &gamev1.GuildJoinReq{GuildId: 7}, resp)
	require.EqualValues(t, int32(errcode.OK), resp.Code)
	require.EqualValues(t, 7, repo.joinedWith.guildID)
	require.EqualValues(t, 100, repo.joinedWith.playerID)
}

// TestJoinNoRepoDegrades 无库环境按成功降级。
func TestJoinNoRepoDegrades(t *testing.T) {
	m, _, rec, ctx := newTestModule(nil)
	resp := &gamev1.GuildJoinResp{}
	call(t, m, ctx, rec, codec.MsgGuildJoinReq, codec.MsgGuildJoinResp, &gamev1.GuildJoinReq{GuildId: 7}, resp)
	require.EqualValues(t, int32(errcode.OK), resp.Code)
}

// ============================================================================
// 退出
// ============================================================================

// TestQuit 正常退出 / 不在会 6101 / 其他错误上抛 / 无库降级。
func TestQuit(t *testing.T) {
	m, _, rec, ctx := newTestModule(&fakeRepo{})
	resp := &gamev1.GuildQuitResp{}
	call(t, m, ctx, rec, codec.MsgGuildQuitReq, codec.MsgGuildQuitResp, &gamev1.GuildQuitReq{}, resp)
	require.EqualValues(t, int32(errcode.OK), resp.Code)

	m2, _, rec2, ctx2 := newTestModule(&fakeRepo{quitErr: ErrNotInGuild})
	resp2 := &gamev1.GuildQuitResp{}
	call(t, m2, ctx2, rec2, codec.MsgGuildQuitReq, codec.MsgGuildQuitResp, &gamev1.GuildQuitReq{}, resp2)
	require.EqualValues(t, int32(errcode.ErrGuildNotIn), resp2.Code)

	m3, _, rec3, ctx3 := newTestModule(&fakeRepo{quitErr: context.DeadlineExceeded})
	_, err := moduletest.Call(m3, ctx3, rec3, codec.MsgGuildQuitReq, codec.MsgGuildQuitResp, &gamev1.GuildQuitReq{})
	require.Error(t, err)

	m4, _, rec4, ctx4 := newTestModule(nil)
	resp4 := &gamev1.GuildQuitResp{}
	call(t, m4, ctx4, rec4, codec.MsgGuildQuitReq, codec.MsgGuildQuitResp, &gamev1.GuildQuitReq{}, resp4)
	require.EqualValues(t, int32(errcode.OK), resp4.Code)
}

// ============================================================================
// 生命周期与配置
// ============================================================================

// TestLogoutDoesNotQuitGuild 公会掉线不退会——这是与队伍刻意的相反决策。
//
// 队伍是临时关系（掉线退队防挂机占位），公会是持久社交关系
// （掉线退会等于每次断线都被踢）。两个模块共用同一个 OnLogout 钩子，
// 语义却相反，所以这条必须锁住，避免以后「统一一下」把公会也改了。
func TestLogoutDoesNotQuitGuild(t *testing.T) {
	repo := &fakeRepo{}
	m, _, _, ctx := newTestModule(repo)

	require.NoError(t, m.OnLogout(ctx))
	require.Zero(t, repo.quitCalls, "公会掉线不能退会")
	require.NoError(t, m.OnLogin(ctx), "登录钩子应为空操作")
	require.NoError(t, m.OnSave(context.Background()), "无内存态，OnSave 应为空操作")
}

// TestMaxMembersByLevel 人数上限按等级查表；未知等级回退 50。
func TestMaxMembersByLevel(t *testing.T) {
	require.EqualValues(t, 20, maxMembers(1), "guild_level 1 级 → 20 人")
	require.EqualValues(t, 40, maxMembers(2), "guild_level 2 级 → 40 人")
	require.EqualValues(t, 50, maxMembers(99), "未知等级回退 50，不能是 0")
	require.Greater(t, maxMembers(0), 0, "等级 0（脏数据）也要能收人")
}
