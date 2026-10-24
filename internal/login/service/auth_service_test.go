// internal/login/service/auth_service_test.go —— 登录的账号状态拦截
//
// 封禁的语义是「不能进游戏」，而进程内判定的唯一入口就在这里：
// 网关只校验 token，不查账号状态。所以「冻结账号必须拿不到新 token」
// 是这条链路上唯一有效的闸门——漏了它，封禁就只是「下次登录失败」，
// 手上那张 token 还能用到 TTL 结束。
package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type fakeTokenStore struct {
	setCalls []string
	setErr   error
	tokens   map[string]int64
}

func newFakeTokenStore() *fakeTokenStore {
	return &fakeTokenStore{tokens: map[string]int64{}}
}

func (f *fakeTokenStore) Set(_ context.Context, token string, playerID int64, _ time.Duration) error {
	if f.setErr != nil {
		return f.setErr
	}
	f.setCalls = append(f.setCalls, token)
	f.tokens[token] = playerID
	return nil
}

func (f *fakeTokenStore) Get(_ context.Context, token string) (int64, error) {
	id, ok := f.tokens[token]
	if !ok {
		return 0, errors.New("token not found")
	}
	return id, nil
}

type fakeAccountStore struct {
	accountID int64
	status    int32
	findErr   error
	touched   int
}

func (f *fakeAccountStore) FindOrCreateByDevice(context.Context, string) (int64, int32, error) {
	return f.accountID, f.status, f.findErr
}

func (f *fakeAccountStore) TouchLogin(context.Context, int64) error {
	f.touched++
	return nil
}

func newService(accounts *fakeAccountStore) (*AuthService, *fakeTokenStore) {
	tokens := newFakeTokenStore()
	return NewAuthService(tokens, accounts, time.Hour), tokens
}

// TestGuestLoginIssuesToken 正常账号：签发 token 并落库。
func TestGuestLoginIssuesToken(t *testing.T) {
	svc, tokens := newService(&fakeAccountStore{accountID: 1001, status: statusNormal})

	token, accountID, err := svc.GuestLogin(context.Background(), "dev-1")
	require.NoError(t, err)
	require.NotEmpty(t, token)
	require.EqualValues(t, 1001, accountID)
	require.Equal(t, []string{token}, tokens.setCalls, "token 必须写进存储，否则网关认不出")
	require.EqualValues(t, 1001, tokens.tokens[token])
}

// TestGuestLoginRejectsFrozen 冻结账号：不签发 token。
func TestGuestLoginRejectsFrozen(t *testing.T) {
	svc, tokens := newService(&fakeAccountStore{accountID: 1001, status: statusFrozen})

	token, accountID, err := svc.GuestLogin(context.Background(), "dev-1")
	require.ErrorIs(t, err, ErrAccountFrozen)
	require.Empty(t, token, "被冻结的账号绝不能拿到新凭证")
	require.EqualValues(t, 1001, accountID, "accountID 要回给上层：登录流水需要它定位账号")
	require.Empty(t, tokens.setCalls)
}

// TestGuestLoginRejectsDeleted 注销账号：同样不签发 token。
func TestGuestLoginRejectsDeleted(t *testing.T) {
	svc, tokens := newService(&fakeAccountStore{accountID: 1001, status: statusDeleted})

	token, _, err := svc.GuestLogin(context.Background(), "dev-1")
	require.ErrorIs(t, err, ErrAccountDeleted)
	require.Empty(t, token)
	require.Empty(t, tokens.setCalls)
}

// TestGuestLoginUnknownStatusFailClosed 未知状态按拒绝处理（脏数据不能成为后门）。
func TestGuestLoginUnknownStatusFailClosed(t *testing.T) {
	svc, tokens := newService(&fakeAccountStore{accountID: 1001, status: 99})

	token, _, err := svc.GuestLogin(context.Background(), "dev-1")
	require.Error(t, err)
	require.Empty(t, token)
	require.Empty(t, tokens.setCalls, "fail-closed：未知状态一律不发凭证")
}

// TestGuestLoginPropagatesLookupError 查库失败原样上抛（不是「账号被拒」）。
func TestGuestLoginPropagatesLookupError(t *testing.T) {
	svc, _ := newService(&fakeAccountStore{findErr: context.DeadlineExceeded})

	_, _, err := svc.GuestLogin(context.Background(), "dev-1")
	require.Error(t, err)
	require.False(t, errors.Is(err, ErrAccountFrozen), "DB 故障不能被当成封禁")
}

// TestGuestLoginFailsWhenTokenStoreFails token 写失败：不返回 token。
//
// 返回 token 而没写进存储，客户端会拿着一个网关永久认不出的凭证去连接。
func TestGuestLoginFailsWhenTokenStoreFails(t *testing.T) {
	tokens := newFakeTokenStore()
	tokens.setErr = context.DeadlineExceeded
	svc := NewAuthService(tokens, &fakeAccountStore{accountID: 1001}, time.Hour)

	token, _, err := svc.GuestLogin(context.Background(), "dev-1")
	require.Error(t, err)
	require.Empty(t, token)
}

// TestGuestLoginWithoutAccountRepo 无 MySQL 环境：降级为固定账号，仍发 token。
func TestGuestLoginWithoutAccountRepo(t *testing.T) {
	tokens := newFakeTokenStore()
	svc := NewAuthService(tokens, nil, time.Hour)

	token, accountID, err := svc.GuestLogin(context.Background(), "dev-1")
	require.NoError(t, err)
	require.NotEmpty(t, token)
	require.EqualValues(t, 1000001, accountID, "固定账号避开 0（0 是「未鉴权」哨兵）")
}

// TestValidateToken 有效 token 返回账号，无效返回错误。
func TestValidateToken(t *testing.T) {
	svc, tokens := newService(&fakeAccountStore{accountID: 1001})
	token, _, err := svc.GuestLogin(context.Background(), "dev-1")
	require.NoError(t, err)

	id, err := svc.ValidateToken(context.Background(), token)
	require.NoError(t, err)
	require.EqualValues(t, 1001, id)

	_, err = svc.ValidateToken(context.Background(), "nope")
	require.Error(t, err)

	// 登出（删 token）后校验必须失败
	delete(tokens.tokens, token)
	_, err = svc.ValidateToken(context.Background(), token)
	require.Error(t, err, "登出后 token 必须立刻失效")
}
