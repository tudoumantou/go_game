// internal/login/mq/role_created_consumer_test.go —— 角色索引消费者的语义测试
//
// 关注点不是「写库成功」（那是 UserRoleRepo 的事），而是消息边界的三种
// 失败形态：脏 payload、缺定位字段、写库报错。前两种必须进死信留痕
// （静默跳过 = 某个玩家永远看不到角色），第三种必须上抛让 Consumer 重试。
package mq

import (
	"context"
	"errors"
	"strconv"
	"testing"

	"game_study/pkg/log"
	"game_study/pkg/model"
	pkgmq "game_study/pkg/mq"

	"github.com/stretchr/testify/require"
)

// fakeRoleIndex 内存实现 RoleIndexWriter。
type fakeRoleIndex struct {
	upserts []*model.UserRole
	err     error
}

func (f *fakeRoleIndex) Upsert(_ context.Context, m *model.UserRole) error {
	if f.err != nil {
		return f.err
	}
	f.upserts = append(f.upserts, m)
	return nil
}

func newTestConsumer(repo RoleIndexWriter) *RoleCreatedConsumer {
	return NewRoleCreatedConsumer(nil, repo, log.New("role-idx-test", log.WithLevel("error")))
}

// roleEnv 构造一条合法消息。
func roleEnv(t *testing.T, msg pkgmq.RoleCreatedMsg) *pkgmq.Envelope {
	t.Helper()
	env, err := pkgmq.NewEnvelope(
		"role:"+strconv.FormatInt(msg.PlayerID, 10),
		pkgmq.TopicRoleCreated,
		strconv.FormatInt(msg.AccountID, 10),
		msg)
	require.NoError(t, err)
	return env
}

// TestRoleCreatedUpsertsIndex 正常路径：字段逐一对齐。
// server_id 尤其重要——它是选角界面区分「这个角色在哪个服」的唯一依据，
// 传错会让玩家在 A 服看到 B 服的角色。
func TestRoleCreatedUpsertsIndex(t *testing.T) {
	repo := &fakeRoleIndex{}
	c := newTestConsumer(repo)

	err := c.handle(context.Background(), roleEnv(t, pkgmq.RoleCreatedMsg{
		PlayerID: 10001, AccountID: 1001, ServerID: 7,
		Nickname: "张三", Level: 1, CreatedAt: 1700000000000,
	}))
	require.NoError(t, err)

	require.Len(t, repo.upserts, 1)
	got := repo.upserts[0]
	require.EqualValues(t, 1001, got.AccountId)
	require.EqualValues(t, 10001, got.PlayerId)
	require.EqualValues(t, 7, got.ServerId)
	require.Equal(t, "张三", got.Nickname)
	require.EqualValues(t, 1, got.Level)
}

// TestRoleCreatedRejectsMissingKey 缺 player_id/account_id 必须报错进死信。
// 这两种消息带着「写不进去」的语义，静默 return nil 会让 offset 推进、
// 消息消失，且没有任何日志指出是哪个角色丢了索引。
func TestRoleCreatedRejectsMissingKey(t *testing.T) {
	cases := []struct {
		name string
		msg  pkgmq.RoleCreatedMsg
	}{
		{"missing player", pkgmq.RoleCreatedMsg{AccountID: 1001, ServerID: 7}},
		{"missing account", pkgmq.RoleCreatedMsg{PlayerID: 10001, ServerID: 7}},
		{"both missing", pkgmq.RoleCreatedMsg{ServerID: 7}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			repo := &fakeRoleIndex{}
			err := newTestConsumer(repo).handle(context.Background(), roleEnv(t, tc.msg))
			require.Error(t, err)
			require.Empty(t, repo.upserts, "非法消息不得写库")
		})
	}
}

// TestRoleCreatedPropagatesRepoError 写库失败必须上抛，交给 Consumer 重试。
// 吞掉错误等于放弃这个角色——而重试是零成本的（唯一键幂等）。
func TestRoleCreatedPropagatesRepoError(t *testing.T) {
	repo := &fakeRoleIndex{err: errors.New("mysql: connection refused")}
	err := newTestConsumer(repo).handle(context.Background(), roleEnv(t, pkgmq.RoleCreatedMsg{
		PlayerID: 10001, AccountID: 1001, ServerID: 7, Nickname: "张三", Level: 1,
	}))
	require.Error(t, err)
	require.Contains(t, err.Error(), "upsert user_role")
}

// TestRoleCreatedBadPayload payload 不是合法 JSON → 报错（进死信）。
func TestRoleCreatedBadPayload(t *testing.T) {
	env := &pkgmq.Envelope{BizID: "role:1", Topic: pkgmq.TopicRoleCreated, Payload: []byte("{not json")}
	err := newTestConsumer(&fakeRoleIndex{}).handle(context.Background(), env)
	require.Error(t, err)
	require.Contains(t, err.Error(), "decode role created")
}
