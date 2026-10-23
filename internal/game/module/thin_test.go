// internal/game/module/thin_test.go —— Thin 基座单测：分发、守卫、失败响应、可回滚声明。
package module

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	gamev1 "game_study/pkg/pb/game/v1"
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
	th := NewThin("demo", ThinRespOf(respOf))
	th.Handle(1001, func(_ *Context, msgID uint32, _ []byte) error {
		got = msgID
		return nil
	})
	ctx, _ := newThinFixture(th)
	if err := th.OnMessage(ctx, 1001, nil); err != nil {
		t.Fatal(err)
	}
	if got != 1001 {
		t.Fatalf("handler got %d", got)
	}
	if err := th.OnMessage(ctx, 9999, nil); err == nil {
		t.Fatal("unexpected message should error")
	}
	if ids := th.MsgIDs(); len(ids) != 1 || ids[0] != 1001 {
		t.Fatalf("MsgIDs = %v", ids)
	}
}

// TestThinGuardRejectsWithoutCache 验证 guard 拒绝时按 errCode 回失败响应。
func TestThinGuardRejectsWithoutCache(t *testing.T) {
	th := NewThin("demo", ThinErrCode(7), ThinRespOf(respOf), ThinGuard(RequireCache))
	called := false
	th.Handle(1001, func(*Context, uint32, []byte) error { called = true; return nil })

	ctx, replies := newThinFixture(th)
	ctx.CacheEnabled = false
	if err := th.OnMessage(ctx, 1001, nil); err != nil {
		t.Fatal(err)
	}
	if called {
		t.Fatal("handler must not run when guard rejects")
	}
	raw, ok := (*replies)[1002]
	if !ok {
		t.Fatal("expected failure reply 1002")
	}
	var resp gamev1.SigninInfoResp
	if err := proto.Unmarshal(raw, &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Code != 7 {
		t.Fatalf("code = %d, want 7", resp.Code)
	}
}

// TestThinCacheFailureReply 验证缓存不可用响应的协议号与错误码。
func TestThinCacheFailureReply(t *testing.T) {
	th := NewThin("demo", ThinErrCode(7), ThinRespOf(respOf))
	th.Handle(1001, func(*Context, uint32, []byte) error { return nil })
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

// TestThinRollbackableOnlyForRegistered 验证只有注册过的协议声明可回滚。
func TestThinRollbackableOnlyForRegistered(t *testing.T) {
	th := NewThin("demo", ThinRespOf(respOf))
	th.Handle(1001, func(*Context, uint32, []byte) error { return nil })
	if !th.Rollbackable(1001) {
		t.Fatal("registered msg should be rollbackable")
	}
	if th.Rollbackable(9999) {
		t.Fatal("unregistered msg must not be rollbackable")
	}
}

// TestThinFailLogsAndReplies 验证 Fail 回业务错误码且处理器错误可上传。
func TestThinFailLogsAndReplies(t *testing.T) {
	th := NewThin("demo", ThinRespOf(respOf))
	boom := errors.New("boom")
	th.Handle(1001, func(*Context, uint32, []byte) error { return boom })
	ctx, replies := newThinFixture(th)
	if err := th.OnMessage(ctx, 1001, nil); !errors.Is(err, boom) {
		t.Fatalf("handler error should propagate, got %v", err)
	}
	if len((*replies)[1002]) != 0 {
		t.Fatal("plain handler error should not auto-reply (module decides)")
	}
	if err := th.Fail(ctx, 1001, 6602, boom); err != nil {
		t.Fatal(err)
	}
	var resp gamev1.SigninInfoResp
	if err := proto.Unmarshal((*replies)[1002], &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Code != 6602 {
		t.Fatalf("code = %d, want 6602", resp.Code)
	}
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
