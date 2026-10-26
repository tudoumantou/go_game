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

	"game_study/pkg/cache"
	"game_study/pkg/log"
	pkgmq "game_study/pkg/mq"

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
