// internal/game/module/mail/mail_index_test.go —— 归属索引接入的验证
//
// 邮件是第一个真正消费 by_owner:{table} 的模块（缓存架构设计 §4.2）：
//   - 投递：先落库再 ZADD（构建期只从库里取材，反过来会漏行）；
//   - 列表：Expand（ZRANGEBYSCORE → pipeline 批量取 → 残留自愈），
//     未配索引/展开失败回落 ListByPlayer。
//
// 这里用真 miniredis 而不是 mock：要验证的正是「ZADD 落到了哪个 key、
// 谁被摘掉了」，换成 mock 只能验证「调了哪个方法」，验证不了语义。
package mail

import (
	"context"
	"errors"
	"testing"
	"time"

	"game_study/internal/common/errcode"
	"game_study/internal/game/module"
	"game_study/internal/game/module/moduletest"
	"game_study/pkg/cache"
	"game_study/pkg/codec"
	"game_study/pkg/log"
	pkgmq "game_study/pkg/mq"
	gamev1 "game_study/pkg/pb/game/v1"
	"google.golang.org/protobuf/proto"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// newIndexedModule 组装一个挂了归属索引的模块（db 为 nil：不打库）。
func newIndexedModule(t *testing.T, repo mailRepo) (*Module, *cache.OwnerIndex, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	oi := cache.NewOwnerIndex(rdb, nil, "mail", "player_id", "id")
	m := New(repo, log.New("mail-test", log.WithLevel("error")))
	m.SetOwnerIndex(oi)
	return m, oi, mr
}

// TestDeliverRegistersOwnerIndex 投递成功后索引里能按玩家捞出这封邮件。
func TestDeliverRegistersOwnerIndex(t *testing.T) {
	repo := &fakeRepo{insertID: 42}
	m, _, mr := newIndexedModule(t, repo)

	id, err := m.Deliver(context.Background(), &pkgmq.MailMsg{PlayerID: 7, Title: "补偿"})
	require.NoError(t, err)
	require.EqualValues(t, 42, id)

	score, err := mr.ZScore(cache.OwnerIndexKey("mail"), "42")
	require.NoError(t, err, "投递后索引里必须有这封邮件")
	require.EqualValues(t, 7, score, "score 是归属玩家 id")
}

// TestDeliverIndexFailureDoesNotFailDelivery 索引写失败不能连累投递。
//
// 索引是加速层：漏了这一条，下次 Reconcile 重建补上，期间列表回落 SQL
// 仍能读到。反过来「因为缓存写失败就不发邮件」才是真的丢数据。
func TestDeliverIndexFailureDoesNotFailDelivery(t *testing.T) {
	repo := &fakeRepo{insertID: 42}
	m, _, mr := newIndexedModule(t, repo)
	mr.Close() // Redis 挂掉

	id, err := m.Deliver(context.Background(), &pkgmq.MailMsg{PlayerID: 7, Title: "补偿"})
	require.NoError(t, err, "缓存故障不该让发邮件失败")
	require.EqualValues(t, 42, id)
}

// TestListReadsFromOwnerIndex 索引就绪时列表走缓存，不打 ListByPlayer。
//
// listErr 是哨兵：如果实现退回落 SQL，这条测试会直接失败。
func TestListReadsFromOwnerIndex(t *testing.T) {
	repo := &fakeRepo{
		listErr: errors.New("SQL path must not be used"),
		atts:    map[int64][]Attachment{102: {{ItemID: 1001, Count: 1}}},
	}
	m, _, mr := newIndexedModule(t, repo)

	// 索引就绪 + 两条实体（101 无附件、102 有附件）
	mustSetReady(t, mr)
	mustZAdd(t, mr, 7, "101")
	mustZAdd(t, mr, 7, "102")
	created := "2026-10-26T10:00:00+08:00"
	mr.HSet(cache.EntityKey("mail", 101), "id", "101", "title", "旧", "channel", "0",
		"status", "1", "created_at", created)
	mr.HSet(cache.EntityKey("mail", 102), "id", "102", "title", "新", "channel", "0",
		"status", "0", "created_at", created)

	rows, err := m.listRows(context.Background(), 7)
	require.NoError(t, err)
	require.Len(t, rows, 2, "应走缓存路径（SQL 路径被 listErr 设成必失败）")

	// 倒序：id 大的在前
	require.EqualValues(t, 102, rows[0].ID)
	require.EqualValues(t, 101, rows[1].ID)
	require.EqualValues(t, 0, rows[0].Status, "status=0 未读")
	require.EqualValues(t, 1, rows[1].Status, "status=1 已读")
	require.True(t, rows[0].HasAttachment, "102 有附件")
	require.False(t, rows[1].HasAttachment, "101 无附件")
	require.Equal(t, "旧", rows[1].Title)
	// 时间列经 toString 转 RFC3339 后必须能还原，不能变成 1970
	require.EqualValues(t, 1792980000, rows[0].CreatedAt.Unix())
}

// TestListSelfHealsStaleIndex 索引里有、库里没有 → 顺手 ZREM。
//
// 这是「增删两次写不原子」的兜底（§4.3）：删行成功但 ZREM 失败时，
// 索引会留下永远取不到数据的 id。读路径顺手清掉，成本近乎为零。
func TestListSelfHealsStaleIndex(t *testing.T) {
	repo := &fakeRepo{}
	m, _, mr := newIndexedModule(t, repo)

	mustSetReady(t, mr)
	mustZAdd(t, mr, 7, "999") // 库与缓存都没有

	rows, err := m.listRows(context.Background(), 7)
	require.NoError(t, err)
	require.Empty(t, rows)

	_, err = mr.ZScore(cache.OwnerIndexKey("mail"), "999")
	require.Error(t, err, "残留索引应被读时自愈摘掉")
}

// TestListFallsBackWhenIndexUnavailable 索引答不上来时回落 SQL。
//
// 邮件是读路径：任何缓存故障都不该让玩家看不到邮件——顶多多打一次库。
func TestListFallsBackWhenIndexUnavailable(t *testing.T) {
	repo := &fakeRepo{rows: []MailRow{{ID: 5, Title: "sql", CreatedAt: time.Unix(1700000000, 0)}}}
	m, _, mr := newIndexedModule(t, repo)
	mr.Close() // Redis 挂掉 → Members 回落 DB → db 为 nil → Expand 报错

	rows, err := m.listRows(context.Background(), 7)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.EqualValues(t, 5, rows[0].ID)
}

// TestListEmptyIndexMeansNoMail 索引里没有该玩家 → 空列表，且不打库。
//
// 与「索引答不上来」严格区分：前者是确定的「没有邮件」，后者必须回落。
func TestListEmptyIndexMeansNoMail(t *testing.T) {
	repo := &fakeRepo{listErr: errors.New("SQL path must not be used")}
	m, _, mr := newIndexedModule(t, repo)
	mustSetReady(t, mr)

	rows, err := m.listRows(context.Background(), 7)
	require.NoError(t, err)
	require.Empty(t, rows)
}

// mustSetReady 把索引标记为就绪（ready 是「可以信任 ZSET」的提交点）。
func mustSetReady(t *testing.T, mr *miniredis.Miniredis) {
	t.Helper()
	require.NoError(t, mr.Set(cache.OwnerIndexReadyKey("mail"), "1"))
}

// mustZAdd 登记一条索引成员。
func mustZAdd(t *testing.T, mr *miniredis.Miniredis, score float64, member string) {
	t.Helper()
	_, err := mr.ZAdd(cache.OwnerIndexKey("mail"), score, member)
	require.NoError(t, err)
}

// TestTakeInvalidatesCachedRow 领取附件后必须作废缓存里的这一行。
//
// 这是接入缓存读取之后新增的一致性要求：onTake 只改 MySQL（status 0→2），
// 缓存里的 e:mail:{id} 还是领取前的状态，而且没有 TTL——不删的话玩家
// 领到东西后列表仍显示「未读 + 有附件」，且不会自己恢复。
func TestTakeInvalidatesCachedRow(t *testing.T) {
	repo := &fakeRepo{atts: map[int64][]Attachment{7: {{ItemID: 1001, Count: 5}}}}
	m, _, mr := newIndexedModule(t, repo)

	st := moduletest.NewState(1)
	rec := moduletest.NewRecorder()
	ctx := moduletest.NewContext(st, rec)

	// 预置：之前 onList 回填过这封邮件（未读、有附件）
	mr.HSet(cache.EntityKey("mail", 7), "id", "7", "title", "t", "status", "0")
	require.True(t, mr.Exists(cache.EntityKey("mail", 7)))

	resp := take(t, m, ctx, rec, 7)
	require.EqualValues(t, 0, resp.Code, "领取应成功")
	require.False(t, mr.Exists(cache.EntityKey("mail", 7)),
		"领取后缓存行必须失效，否则列表读到领取前的 status")
}

// TestTakeWithoutIndexStillWorks 未注入索引时领取照常（不依赖缓存）。
func TestTakeWithoutIndexStillWorks(t *testing.T) {
	repo := &fakeRepo{atts: map[int64][]Attachment{8: {{ItemID: 1001, Count: 1}}}}
	m, st, rec, ctx := newTestModule(repo)
	_ = st

	resp := take(t, m, ctx, rec, 8)
	require.EqualValues(t, 0, resp.Code)
}

// deleteMail 发一次删除请求并解出响应。
func deleteMail(t *testing.T, m *Module, ctx *module.Context, rec *moduletest.Recorder, mailID int64) *gamev1.MailDeleteResp {
	t.Helper()
	raw, err := moduletest.Call(m, ctx, rec, codec.MsgMailDeleteReq, codec.MsgMailDeleteResp,
		&gamev1.MailDeleteReq{MailId: mailID})
	require.NoError(t, err, "call delete")
	require.NotNil(t, raw, "没有回包")
	resp := &gamev1.MailDeleteResp{}
	require.NoError(t, proto.Unmarshal(raw, resp))
	return resp
}

// TestDeleteRemovesRowIndexAndCache 删除成功：库、索引、缓存三处一起清。
//
// 这是 RemoveEntity 的第一个真实调用方——之前它只有单测，没有任何业务
// 在调用（邮件没有删除协议）。三处必须同时清：只删库会留下「索引说存在、
// 展开时查不到」的残留（靠读时自愈兜，但那是兜底不是主路径）。
func TestDeleteRemovesRowIndexAndCache(t *testing.T) {
	repo := &fakeRepo{atts: map[int64][]Attachment{}} // 无附件 → 可删
	m, _, mr := newIndexedModule(t, repo)

	st := moduletest.NewState(1)
	rec := moduletest.NewRecorder()
	ctx := moduletest.NewContext(st, rec)

	mustZAdd(t, mr, 1, "77")
	mr.HSet(cache.EntityKey("mail", 77), "id", "77", "status", "2")

	resp := deleteMail(t, m, ctx, rec, 77)
	require.EqualValues(t, int32(errcode.OK), resp.Code, "无附件应允许删除")
	require.Equal(t, []int64{77}, repo.deleted, "库里要删")

	members, err := mr.ZMembers(cache.OwnerIndexKey("mail"))
	if err == nil {
		require.NotContains(t, members, "77", "索引必须摘掉")
	}
	require.False(t, mr.Exists(cache.EntityKey("mail", 77)), "缓存行必须清掉")
}

// TestDeleteRejectsUnclaimedAttachment 有未领附件时拒绝。
//
// 删了玩家就再也领不到——这是不可逆的损失，比「多一个错误提示」贵得多。
func TestDeleteRejectsUnclaimedAttachment(t *testing.T) {
	repo := &fakeRepo{atts: map[int64][]Attachment{78: {{ItemID: 1001, Count: 5}}}}
	m, _, mr := newIndexedModule(t, repo)

	st := moduletest.NewState(1)
	rec := moduletest.NewRecorder()
	ctx := moduletest.NewContext(st, rec)
	mustZAdd(t, mr, 1, "78")

	resp := deleteMail(t, m, ctx, rec, 78)
	require.EqualValues(t, int32(errcode.ErrMailHasUnclaimed), resp.Code)
	require.Empty(t, repo.deleted, "有未领附件时不能删库")
	require.True(t, mr.Exists(cache.OwnerIndexKey("mail")), "索引也不能动")
}

// TestDeleteNotFound 邮件不存在（或不是自己的）→ 统一回「不存在」。
func TestDeleteNotFound(t *testing.T) {
	repo := &fakeRepo{findErr: ErrMailNotExist}
	m, _, _ := newIndexedModule(t, repo)

	st := moduletest.NewState(1)
	rec := moduletest.NewRecorder()
	ctx := moduletest.NewContext(st, rec)

	resp := deleteMail(t, m, ctx, rec, 79)
	require.EqualValues(t, int32(errcode.ErrMailNotFound), resp.Code)
	require.Empty(t, repo.deleted)
}

// TestDeleteMalformedID 非正 id 在入口就挡掉，不打库。
func TestDeleteMalformedID(t *testing.T) {
	repo := &fakeRepo{}
	m, _, _ := newIndexedModule(t, repo)

	st := moduletest.NewState(1)
	rec := moduletest.NewRecorder()
	ctx := moduletest.NewContext(st, rec)

	for _, id := range []int64{0, -1} {
		resp := deleteMail(t, m, ctx, rec, id)
		require.EqualValues(t, int32(errcode.ErrMailNotFound), resp.Code)
	}
	require.Zero(t, repo.findHits, "非正 id 不该触发任何查询")
}

// TestDeleteWithoutIndexStillWorks 未注入索引时删除照常（不依赖缓存）。
func TestDeleteWithoutIndexStillWorks(t *testing.T) {
	repo := &fakeRepo{}
	m, _, rec, ctx := newTestModule(repo)

	resp := deleteMail(t, m, ctx, rec, 80)
	require.EqualValues(t, int32(errcode.OK), resp.Code)
	require.Equal(t, []int64{80}, repo.deleted)
}
