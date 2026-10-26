// internal/game/module/mail/mail_test.go —— 邮件模块单测
//
// 覆盖两条最容易出事的规则：
//  1. 领取幂等：同一封邮件只能发一次奖（业务键 mail:<id> 与道具同快照提交）；
//  2. 一请求一响应：模块用了 module.Typed，就不能再自己回包——多回一条会让
//     下一次请求读到上一条的响应，客户端表现为「业务码莫名丢失」。
//
// 全部用例不碰数据库：mailRepo 由假实现顶上，纯内存运行。
package mail

import (
	"context"
	"fmt"
	"testing"
	"time"

	"game_study/internal/common/errcode"
	"game_study/internal/game/module"
	"game_study/internal/game/module/moduletest"
	"game_study/pkg/codec"
	"game_study/pkg/gamedata"
	"game_study/pkg/log"
	"game_study/pkg/model"
	pkgmq "game_study/pkg/mq"
	gamev1 "game_study/pkg/pb/game/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// fakeRepo 是 mailRepo 的内存实现：只模拟模块真正用到的行为。
type fakeRepo struct {
	rows      []MailRow
	atts      map[int64][]Attachment
	listErr   error
	findErr   error
	markErr   error
	unreadErr error
	insertErr error

	marked    []int64
	deleted   []int64
	deleteErr error
	findHits  int
	insertID  int64
	insAtts   []Attachment
	inserted  *model.Mail // 最后一次插入的主邮件（断言模板填充用）
}

func (f *fakeRepo) UnreadCount(context.Context, int64) (int64, error) {
	return int64(len(f.rows)), f.unreadErr
}

func (f *fakeRepo) Insert(_ context.Context, m *model.Mail) (int64, error) {
	if f.insertErr != nil {
		return 0, f.insertErr
	}
	f.inserted = m
	return f.insertID, nil
}

func (f *fakeRepo) InsertAttachment(_ context.Context, _ int64, itemID, count int32) error {
	f.insAtts = append(f.insAtts, Attachment{ItemID: itemID, Count: count})
	return nil
}

func (f *fakeRepo) ListByPlayer(context.Context, int64, int) ([]MailRow, error) {
	return f.rows, f.listErr
}

func (f *fakeRepo) FindAttachments(_ context.Context, _, mailID int64) ([]Attachment, error) {
	f.findHits++
	if f.findErr != nil {
		return nil, f.findErr
	}
	return f.atts[mailID], nil
}

func (f *fakeRepo) Delete(_ context.Context, _, mailID int64) error {
	if f.deleteErr != nil {
		return f.deleteErr
	}
	f.deleted = append(f.deleted, mailID)
	return nil
}

func (f *fakeRepo) MarkTaken(_ context.Context, mailID int64) error {
	if f.markErr != nil {
		return f.markErr
	}
	f.marked = append(f.marked, mailID)
	return nil
}

func (f *fakeRepo) DeliverOffline(context.Context, int64, string, string, string, []Attachment) (int64, bool, error) {
	return 0, false, nil
}

func (f *fakeRepo) HasAttachments(context.Context, []int64) (map[int64]bool, error) {
	out := make(map[int64]bool, len(f.atts))
	for id, atts := range f.atts {
		if len(atts) > 0 {
			out[id] = true
		}
	}
	return out, nil
}

// newTestModule 组装一个内存版邮件模块。
func newTestModule(repo mailRepo) (*Module, *moduletest.State, *moduletest.Recorder, *module.Context) {
	st := moduletest.NewState(1)
	rec := moduletest.NewRecorder()
	m := New(repo, log.New("mail-test", log.WithLevel("error")))
	return m, st, rec, moduletest.NewContext(st, rec)
}

// take 发一次领取请求并解出响应。
func take(t *testing.T, m *Module, ctx *module.Context, rec *moduletest.Recorder, mailID int64) *gamev1.MailTakeAttachmentResp {
	t.Helper()
	raw, err := moduletest.Call(m, ctx, rec, codec.MsgMailTakeAttachmentReq, codec.MsgMailTakeAttachmentResp, &gamev1.MailTakeAttachmentReq{MailId: mailID})
	require.NoError(t, err, "call take")
	require.NotNil(t, raw, "没有回包")
	resp := &gamev1.MailTakeAttachmentResp{}
	require.NoError(t, proto.Unmarshal(raw, resp))
	return resp
}

// TestTakeGrantsAndMarksTaken 首次领取：发奖、占业务键、尽力标记已领。
func TestTakeGrantsAndMarksTaken(t *testing.T) {
	repo := &fakeRepo{atts: map[int64][]Attachment{7: {{ItemID: 1001, Count: 5}, {ItemID: 1002, Count: 3}}}}
	m, st, rec, ctx := newTestModule(repo)

	resp := take(t, m, ctx, rec, 7)
	require.EqualValues(t, int32(errcode.OK), resp.Code, fmt.Sprintf("take: %+v", resp))
	require.EqualValues(t, 5, st.GetItemCount(1001))
	require.EqualValues(t, 3, st.GetItemCount(1002))
	require.True(t, st.HasBiz("mail:7"), "领取事实必须落到业务键上")
	require.Equal(t, []int64{7}, repo.marked, "SQL 标记是尽力更新，正常路径要调")
	require.NotEmpty(t, resp.Rewards, "成功领取要带道具增量明细")
}

// TestTakeIsIdempotent 重复领取：回 5002 且不再发奖。
//
// 这是回归用例：曾经因为「Typed 自动回包 + handler 自己又回一次」，
// 客户端读到的是上一条响应的 code=0，幂等判定形同虚设。
func TestTakeIsIdempotent(t *testing.T) {
	repo := &fakeRepo{atts: map[int64][]Attachment{7: {{ItemID: 1001, Count: 5}}}}
	m, st, rec, ctx := newTestModule(repo)

	first := take(t, m, ctx, rec, 7)
	require.EqualValues(t, int32(errcode.OK), first.Code)
	require.EqualValues(t, 1, rec.Count(codec.MsgMailTakeAttachmentResp), "一次请求只能回一个包")

	second := take(t, m, ctx, rec, 7)
	require.EqualValues(t, int32(errcode.ErrMailTaken), second.Code, fmt.Sprintf("重复领取应回 ErrMailTaken, got %+v", second))
	require.EqualValues(t, 5, st.GetItemCount(1001), "重复领取不能重复发奖")
	require.Equal(t, 2, rec.Count(codec.MsgMailTakeAttachmentResp), "第二次请求同样只回一个包")
	require.Equal(t, 1, repo.findHits, "命中业务键时不该再查附件表")
}

// TestTakeBagFullKeepsClaimable 满包：回 5004，且不占业务键——清包后能重领。
func TestTakeBagFullKeepsClaimable(t *testing.T) {
	repo := &fakeRepo{atts: map[int64][]Attachment{9: {{ItemID: 1001, Count: 1}}}}
	m, st, rec, ctx := newTestModule(repo)
	st.SetCanHold(false)

	resp := take(t, m, ctx, rec, 9)
	require.EqualValues(t, int32(errcode.ErrBagFull), resp.Code, fmt.Sprintf("满包应回 ErrBagFull, got %+v", resp))
	require.EqualValues(t, 0, st.GetItemCount(1001), "满包时一件都不该入包")
	require.False(t, st.HasBiz("mail:9"), "发奖失败不能占业务键，否则清包后永远领不了")

	// 清出空间后重领应当成功。
	st.SetCanHold(true)
	again := take(t, m, ctx, rec, 9)
	require.EqualValues(t, int32(errcode.OK), again.Code, fmt.Sprintf("清包后应能重领, got %+v", again))
	require.EqualValues(t, 1, st.GetItemCount(1001))
}

// TestTakeErrorCodes 仓储错误映射到业务码。
func TestTakeErrorCodes(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want int32
	}{
		{"邮件不存在", ErrMailNotExist, int32(errcode.ErrMailNotFound)},
		{"没有附件", ErrNoAttachment, int32(errcode.ErrNoAttachment)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			repo := &fakeRepo{findErr: c.err}
			m, st, rec, ctx := newTestModule(repo)
			resp := take(t, m, ctx, rec, 3)
			require.EqualValues(t, c.want, resp.Code, fmt.Sprintf("got %+v", resp))
			require.False(t, st.HasBiz("mail:3"), "失败请求不能占业务键")
		})
	}
}

// TestTakeRejectsNonPositiveID 非法 id 直接挡掉，不查库。
func TestTakeRejectsNonPositiveID(t *testing.T) {
	repo := &fakeRepo{}
	m, _, rec, ctx := newTestModule(repo)

	resp := take(t, m, ctx, rec, 0)
	require.EqualValues(t, int32(errcode.ErrMailNotFound), resp.Code)
	require.Equal(t, 0, repo.findHits, "非法 id 不该查附件表")
}

// TestTakeSurvivesMarkFailure MarkTaken 失败不影响领取结果（快照才是权威）。
func TestTakeSurvivesMarkFailure(t *testing.T) {
	repo := &fakeRepo{
		atts:    map[int64][]Attachment{5: {{ItemID: 1001, Count: 2}}},
		markErr: context.DeadlineExceeded,
	}
	m, st, rec, ctx := newTestModule(repo)

	resp := take(t, m, ctx, rec, 5)
	require.EqualValues(t, int32(errcode.OK), resp.Code, "标记失败只影响列表展示，不能让领取失败")
	require.EqualValues(t, 2, st.GetItemCount(1001))
	require.True(t, st.HasBiz("mail:5"))
}

// TestListProjectsRows 列表：附件标记、已读折算、空过期时间不能变成 1970。
func TestListProjectsRows(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	repo := &fakeRepo{rows: []MailRow{
		{ID: 1, Title: "带附件", Status: 0, CreatedAt: now, HasAttachment: true},
		{ID: 2, Title: "已领", Status: 2, CreatedAt: now},
	}}
	m, _, rec, ctx := newTestModule(repo)

	raw, err := moduletest.Call(m, ctx, rec, codec.MsgMailListReq, codec.MsgMailListResp, &gamev1.MailListReq{})
	require.NoError(t, err)
	resp := &gamev1.MailListResp{}
	require.NoError(t, proto.Unmarshal(raw, resp))
	require.EqualValues(t, int32(errcode.OK), resp.Code)
	require.Len(t, resp.Mails, 2)

	require.EqualValues(t, 1, resp.Mails[0].MailId)
	require.True(t, resp.Mails[0].HasAttachment)
	require.False(t, resp.Mails[0].Read, "Status=0 是未读")
	require.EqualValues(t, 0, resp.Mails[0].ExpireAt, "没有过期时间应为 0，而不是 1970")

	require.EqualValues(t, 2, resp.Mails[1].MailId)
	require.True(t, resp.Mails[1].Read, "Status>=1 都算已读（含已领）")
	require.False(t, resp.Mails[1].HasAttachment)
	require.Equal(t, 1, rec.Count(codec.MsgMailListResp), "一次请求只能回一个包")
}

// TestListPropagatesRepoError 仓储错误向上抛，由框架记日志，不能假装空列表。
func TestListPropagatesRepoError(t *testing.T) {
	repo := &fakeRepo{listErr: context.DeadlineExceeded}
	m, _, rec, ctx := newTestModule(repo)

	_, err := moduletest.Call(m, ctx, rec, codec.MsgMailListReq, codec.MsgMailListResp, &gamev1.MailListReq{})
	require.Error(t, err, "查库失败必须上抛")
	require.Equal(t, 0, rec.Count(codec.MsgMailListResp), "失败请求不回业务包")
}

// TestDeliverOfflineRequiresRepo 无 DB 环境时离线发放必须报错，不能假装成功。
func TestDeliverOfflineRequiresRepo(t *testing.T) {
	m := New(nil, log.New("mail-test", log.WithLevel("error")))
	ok, err := m.DeliverOffline(context.Background(), 1, "biz", "t", "c", nil)
	require.Error(t, err, "缺 DB 时不能静默成功")
	require.False(t, ok)
}

// TestOnLoginQueriesUnread 登录钩子拉一次未读数；查库失败要上抛（登录失败可见）。
func TestOnLoginQueriesUnread(t *testing.T) {
	repo := &fakeRepo{rows: []MailRow{{ID: 1}, {ID: 2}}}
	m, _, _, ctx := newTestModule(repo)
	require.NoError(t, m.OnLogin(ctx), "查询成功时不应报错")

	repo.unreadErr = context.DeadlineExceeded
	require.Error(t, m.OnLogin(ctx), "查库失败必须上抛")
}

// TestOnLogoutMarksDirty 下线标脏，触发一次常规 flush 周期。
func TestOnLogoutMarksDirty(t *testing.T) {
	m, st, _, ctx := newTestModule(&fakeRepo{})
	require.NoError(t, m.OnLogout(ctx))
	require.Contains(t, st.DirtyModules(), "mail")
}

// TestDeliverSkipsNonPositiveAttachments 附件数量非正的脏数据直接跳过，
// 不能插进去让玩家领到 0 个甚至负数道具。
func TestDeliverSkipsNonPositiveAttachments(t *testing.T) {
	repo := &fakeRepo{insertID: 42}
	m, _, _, _ := newTestModule(repo)

	id, err := m.Deliver(context.Background(), &pkgmq.MailMsg{
		PlayerID:   1,
		TemplateID: 10,
		Title:      "补偿",
		Items: []pkgmq.Item{
			{ItemID: 1001, Count: 5},
			{ItemID: 1002, Count: 0},
			{ItemID: 1003, Count: -3},
			{ItemID: 1004, Count: 2},
		},
	})
	require.NoError(t, err)
	require.EqualValues(t, 42, id, "应返回主邮件 ID")
	require.Len(t, repo.insAtts, 2, fmt.Sprintf("只有正数附件该落库: %+v", repo.insAtts))
	require.EqualValues(t, 1001, repo.insAtts[0].ItemID)
	require.EqualValues(t, 1004, repo.insAtts[1].ItemID)
}

// TestDeliverFailsWhenMailInsertFails 主邮件插入失败必须上抛，
// 让 Kafka 重投——静默丢邮件等于丢奖励。
func TestDeliverFailsWhenMailInsertFails(t *testing.T) {
	repo := &fakeRepo{insertErr: context.DeadlineExceeded}
	m, _, _, _ := newTestModule(repo)

	_, err := m.Deliver(context.Background(), &pkgmq.MailMsg{PlayerID: 1})
	require.Error(t, err, "主邮件插入失败必须上抛")
	require.Empty(t, repo.insAtts, "主邮件没建成时不该插附件")
}

// TestDeliverUsesTemplate 只给模板号时，标题/正文来自 mail_template 表。
//
// 旧实现忽略这张表：运营改模板不生效，改文案必须改发送端代码。
func TestDeliverUsesTemplate(t *testing.T) {
	var tpl *gamedata.MailTemplateCfg
	for _, cfg := range gamedata.MailTemplateCfgList() {
		c := cfg
		tpl = &c
		break
	}
	require.NotNil(t, tpl, "mail_template 配置为空，测试前提不成立")

	repo := &fakeRepo{}
	m, _, _, _ := newTestModule(repo)

	_, err := m.Deliver(context.Background(), &pkgmq.MailMsg{
		PlayerID:   1,
		TemplateID: int32(tpl.Id),
	})
	require.NoError(t, err)
	require.NotNil(t, repo.inserted)
	require.Equal(t, tpl.Col1, repo.inserted.Title, "标题必须来自模板")
	require.Equal(t, tpl.Col2, repo.inserted.Content, "正文必须来自模板")
}

// TestDeliverMessageOverridesTemplate 消息里写了就用消息的（个性化文案）。
func TestDeliverMessageOverridesTemplate(t *testing.T) {
	var tplID int32
	for _, cfg := range gamedata.MailTemplateCfgList() {
		tplID = int32(cfg.Id)
		break
	}

	repo := &fakeRepo{}
	m, _, _, _ := newTestModule(repo)

	_, err := m.Deliver(context.Background(), &pkgmq.MailMsg{
		PlayerID:   1,
		TemplateID: tplID,
		Title:      "工单已处理",
	})
	require.NoError(t, err)
	require.Equal(t, "工单已处理", repo.inserted.Title, "消息里的标题优先")
	require.NotEmpty(t, repo.inserted.Content, "正文没覆盖，应回落到模板")
}

// TestDeliverUnknownTemplateKeepsMessage 模板号查不到时不能清空文案。
func TestDeliverUnknownTemplateKeepsMessage(t *testing.T) {
	repo := &fakeRepo{}
	m, _, _, _ := newTestModule(repo)

	_, err := m.Deliver(context.Background(), &pkgmq.MailMsg{
		PlayerID:   1,
		TemplateID: 999999,
		Title:      "补偿",
		Content:    "请查收",
	})
	require.NoError(t, err)
	require.Equal(t, "补偿", repo.inserted.Title)
	require.Equal(t, "请查收", repo.inserted.Content)
}
