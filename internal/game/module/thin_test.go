// internal/game/module/thin_test.go —— Thin 基座单测：分发、守卫、失败响应、可回滚声明。
package module

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	gamev1 "game_study/pkg/pb/game/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// thinState 是 PlayerState 的最小内存实现（Thin 只用 PlayerID）。
type thinState struct{ id int64 }

func (s *thinState) PlayerID() int64                { return s.id }
func (s *thinState) Profile() Profile               { return Profile{} }
func (s *thinState) ExpMax() int64                  { return 0 }
func (s *thinState) GetAttr(string) int64           { return 0 }
func (s *thinState) SetAttr(string, int64)          {}
func (s *thinState) AddBonus(string, int64)         {}
func (s *thinState) ClearBonus()                    {}
func (s *thinState) GetItemCount(int32) int64       { return 0 }
func (s *thinState) Items() map[int32]int64         { return nil }
func (s *thinState) AddItem(int32, int64)           {}
func (s *thinState) RemoveItem(int32, int64) bool   { return false }
func (s *thinState) MarkDirty(string)               {}
func (s *thinState) SetSceneID(int32)               {}
func (s *thinState) Ext(string) json.RawMessage     { return nil }
func (s *thinState) SetExt(string, json.RawMessage) {}
func (s *thinState) CanHold([]ItemGrant) bool       { return true }
func (s *thinState) ClaimBiz(string) bool           { return true }
func (s *thinState) HasBiz(string) bool             { return false }

// respOf 演示协议：req=1001 → resp=1002，错误码走 Code 字段。
func respOf(msgID uint32) (uint32, RespFactory, bool) {
	if msgID != 1001 {
		return 0, nil, false
	}
	return 1002, func(code int32) proto.Message {
		return &gamev1.SigninInfoResp{Code: code}
	}, true
}

func newThinFixture(t *Thin) (*Context, *map[uint32][]byte) {
	replies := map[uint32][]byte{}
	ctx := &Context{State: &thinState{id: 42}, CacheEnabled: true,
		Send: func(id uint32, body []byte) error { replies[id] = body; return nil }}
	return ctx, &replies
}

// TestThinDispatchesRegisteredHandlers 验证注册的协议被分发、未注册的报错。
func TestThinDispatchesRegisteredHandlers(t *testing.T) {
	var got uint32
	th := NewThin("demo")
	th.HandleR(1001, 1002, &gamev1.SigninInfoResp{}, func(_ *Context, msgID uint32, _ []byte) error {
		got = msgID
		return nil
	})
	ctx, _ := newThinFixture(th)
	if err := th.OnMessage(ctx, 1001, nil); err != nil {
		t.Fatal(err)
	}
	require.EqualValues(t, 1001, got)
	if err := th.OnMessage(ctx, 9999, nil); err == nil {
		t.Fatal("unexpected message should error")
	}
	if ids := th.MsgIDs(); len(ids) != 1 || ids[0] != 1001 {
		t.Fatalf("MsgIDs = %v", ids)
	}
}

// TestThinGuardRejectsWithoutCache 验证 guard 拒绝时按 errCode 回失败响应。
func TestThinGuardRejectsWithoutCache(t *testing.T) {
	th := NewThin("demo", ThinErrCode(7), ThinGuard(RequireCache))
	called := false
	th.HandleR(1001, 1002, &gamev1.SigninInfoResp{}, func(*Context, uint32, []byte) error { called = true; return nil })

	ctx, replies := newThinFixture(th)
	ctx.CacheEnabled = false
	if err := th.OnMessage(ctx, 1001, nil); err != nil {
		t.Fatal(err)
	}
	if called {
		t.Fatal("handler must not run when guard rejects")
	}
	raw, ok := (*replies)[1002]
	require.True(t, ok, "expected failure reply 1002")
	var resp gamev1.SigninInfoResp
	if err := proto.Unmarshal(raw, &resp); err != nil {
		t.Fatal(err)
	}
	require.EqualValues(t, 7, resp.Code)
}

// TestThinCacheFailureReply 验证缓存不可用响应的协议号与错误码。
func TestThinCacheFailureReply(t *testing.T) {
	th := NewThin("demo", ThinErrCode(7))
	th.HandleR(1001, 1002, &gamev1.SigninInfoResp{}, func(*Context, uint32, []byte) error { return nil })
	respID, raw, ok := th.CacheFailureReply(1001)
	if !ok || respID != 1002 || len(raw) == 0 {
		t.Fatalf("CacheFailureReply = %d, %v, %v", respID, raw, ok)
	}
	if _, _, ok := th.CacheFailureReply(9999); ok {
		t.Fatal("unknown msg should not produce reply")
	}
	// 未设置 respOf 的 Thin 不实现缓存失败响应（走网关通用错误）。
	bare := NewThin("bare")
	if _, _, ok := bare.CacheFailureReply(1001); ok {
		t.Fatal("bare thin should not implement CacheFailureAware")
	}
}

// TestThinDiscardableOnlyForRegistered 验证只有注册过的协议声明可回滚。
func TestThinDiscardableOnlyForRegistered(t *testing.T) {
	th := NewThin("demo")
	th.HandleR(1001, 1002, &gamev1.SigninInfoResp{}, func(*Context, uint32, []byte) error { return nil })
	if !th.Discardable(1001) {
		t.Fatal("registered msg should be rollbackable")
	}
	if th.Discardable(9999) {
		t.Fatal("unregistered msg must not be rollbackable")
	}
}

// TestThinFailLogsAndReplies 验证 Fail 回业务错误码且处理器错误可上传。
func TestThinFailLogsAndReplies(t *testing.T) {
	th := NewThin("demo")
	boom := errors.New("boom")
	th.HandleR(1001, 1002, &gamev1.SigninInfoResp{}, func(*Context, uint32, []byte) error { return boom })
	ctx, replies := newThinFixture(th)
	if err := th.OnMessage(ctx, 1001, nil); !errors.Is(err, boom) {
		t.Fatalf("handler error should propagate, got %v", err)
	}
	require.Len(t, (*replies)[1002], 0, "plain handler error should not auto-reply (module decides)")
	if err := th.Fail(ctx, 1001, 6602, boom); err != nil {
		t.Fatal(err)
	}
	var resp gamev1.SigninInfoResp
	if err := proto.Unmarshal((*replies)[1002], &resp); err != nil {
		t.Fatal(err)
	}
	require.EqualValues(t, 6602, resp.Code)
}

// TestThinLifecycleHooks 验证 Init/Login/Logout 钩子可选、默认无操作。
func TestThinLifecycleHooks(t *testing.T) {
	var initN, loginN, logoutN int
	th := NewThin("demo",
		ThinInit(func(*Context) error { initN++; return nil }),
		ThinLogin(func(*Context) error { loginN++; return nil }),
		ThinLogout(func(*Context) error { logoutN++; return nil }),
	)
	ctx, _ := newThinFixture(th)
	if err := th.Init(ctx); err != nil {
		t.Fatal(err)
	}
	if err := th.OnLogin(ctx); err != nil {
		t.Fatal(err)
	}
	if err := th.OnLogout(ctx); err != nil {
		t.Fatal(err)
	}
	if err := th.OnSave(context.Background()); err != nil {
		t.Fatal(err)
	}
	if initN != 1 || loginN != 1 || logoutN != 1 {
		t.Fatalf("hooks = %d/%d/%d", initN, loginN, logoutN)
	}
	// 未挂钩子的 Thin：全部无操作。
	bare := NewThin("bare")
	if err := bare.Init(ctx); err != nil {
		t.Fatal(err)
	}
	if err := bare.OnLogin(ctx); err != nil {
		t.Fatal(err)
	}
	if err := bare.OnLogout(ctx); err != nil {
		t.Fatal(err)
	}
}

// TestThinDuplicateHandlePanics 验证重复注册协议启动期 panic。
func TestThinDuplicateHandlePanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("duplicate Handle should panic")
		}
	}()
	th := NewThin("demo")
	th.Handle(1001, func(*Context, uint32, []byte) error { return nil })
	th.Handle(1001, func(*Context, uint32, []byte) error { return nil })
}

// TestThinOnEvent 事件钩子被转发；未挂时为空操作（模块无需为不订阅事件写代码）。
func TestThinOnEvent(t *testing.T) {
	var got []Event
	th := NewThin("evt", ThinOnEvent(func(_ *Context, evt Event) { got = append(got, evt) }))
	th.OnEvent(&Context{}, Event{Type: "kill", Key: "1001", Delta: 1})
	th.OnEvent(&Context{}, Event{Type: "use_item", Key: "2001", Delta: 3})
	if len(got) != 2 || got[0].Type != "kill" || got[1].Delta != 3 {
		t.Fatalf("事件未按序转发: %#v", got)
	}

	// 未挂 ThinOnEvent：不 panic、无副作用。
	bare := NewThin("bare_evt")
	bare.OnEvent(&Context{}, Event{Type: "kill"})

	var _ EventAware = th
}

// TestHandleRBuildsErrorReply 验证 HandleR 声明的原型能自动构造错误包。
//
// 这是「respOf 收进基座」的核心：模块只写「请求号 + 响应号 + 空原型」，
// 不再写按 msgID 分支的工厂函数。
func TestHandleRBuildsErrorReply(t *testing.T) {
	th := NewThin("demo", ThinErrCode(9))
	th.HandleR(2001, 2002, &gamev1.SigninClaimResp{}, func(*Context, uint32, []byte) error { return nil })
	ctx, replies := newThinFixture(th)

	if err := th.Fail(ctx, 2001, 6603, errors.New("bag full")); err != nil {
		t.Fatal(err)
	}
	var resp gamev1.SigninClaimResp
	if err := proto.Unmarshal((*replies)[2002], &resp); err != nil {
		t.Fatalf("错误包解析失败: %v", err)
	}
	require.EqualValues(t, 6603, resp.Code)

	// CacheFailureReply 用模块级 errCode，不是 Fail 的入参。
	respID, raw, ok := th.CacheFailureReply(2001)
	if !ok || respID != 2002 || len(raw) == 0 {
		t.Fatalf("CacheFailureReply = %d/%v/%v", respID, raw, ok)
	}
	var cached gamev1.SigninClaimResp
	if err := proto.Unmarshal(raw, &cached); err != nil || cached.Code != 9 {
		t.Fatalf("缓存失败包 code=%d err=%v, want 9", cached.Code, err)
	}
}

// TestHandleRWithoutCodeField 响应没有 code 字段时仍回包（客户端至少知道失败）。
func TestHandleRWithoutCodeField(t *testing.T) {
	th := NewThin("demo")
	// SigninReward 没有 code 字段，用它验证降级路径。
	th.HandleR(3001, 3002, &gamev1.SigninReward{}, func(*Context, uint32, []byte) error { return nil })
	respID, raw, ok := th.CacheFailureReply(3001)
	if !ok || respID != 3002 || len(raw) != 0 {
		t.Fatalf("无 code 字段响应应为空包: %d/%v/%v", respID, raw, ok)
	}
}

// TestHandleRNilPrototypePanics 原型为 nil 是编码错误，启动期暴露。
func TestHandleRNilPrototypePanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("nil prototype should panic")
		}
	}()
	th := NewThin("demo")
	th.HandleR(4001, 4002, nil, func(*Context, uint32, []byte) error { return nil })
}

// TestRespOfOverridesHandleR ThinRespOf 优先于 HandleR 的自动推导
// （留给「一个请求回多种响应」的少数场景）。
func TestRespOfOverridesHandleR(t *testing.T) {
	th := NewThin("demo", ThinRespOf(respOf))
	th.HandleR(1001, 9999, &gamev1.SigninClaimResp{}, func(*Context, uint32, []byte) error { return nil })
	ctx, replies := newThinFixture(th)
	if err := th.Fail(ctx, 1001, 5, errors.New("x")); err != nil {
		t.Fatal(err)
	}
	if _, ok := (*replies)[9999]; ok {
		t.Fatal("HandleR 的响应号不应生效（ThinRespOf 优先）")
	}
	if _, ok := (*replies)[1002]; !ok {
		t.Fatal("应以 respOf 声明的 1002 回包")
	}
}
