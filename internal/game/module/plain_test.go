// internal/game/module/plain_test.go —— 无状态模块基座与注册表声明
package module

import (
	"context"
	"errors"
	"testing"

	"game_study/pkg/log"
	gamev1 "game_study/pkg/pb/game/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// 测试用协议号（不占用正式段位）。
const (
	testPlainMsgA uint32 = 990001
	testPlainMsgB uint32 = 990002
)

// testPlainModule 嵌入 Plain 的典型用法。
type testPlainModule struct {
	*Plain
	calls  []uint32
	inited bool
	login  bool
}

func newTestPlainModule(t *testing.T) *testPlainModule {
	t.Helper()
	m := &testPlainModule{}
	m.Plain = NewPlain("test_plain",
		PlainLogger(log.New("test")),
		PlainInit(func(*Context) error { m.inited = true; return nil }),
		PlainLogin(func(*Context) error { m.login = true; return nil }),
	)
	m.Handle(testPlainMsgA, func(ctx *Context, msgID uint32, _ []byte) error {
		m.calls = append(m.calls, msgID)
		return nil
	})
	m.Handle(testPlainMsgB, func(ctx *Context, msgID uint32, body []byte) error {
		if string(body) == "boom" {
			return errors.New("injected")
		}
		m.calls = append(m.calls, msgID)
		return nil
	})
	return m
}

// TestPlainDispatch 注册的协议按 msgID 分发，处理器被调用。
func TestPlainDispatch(t *testing.T) {
	m := newTestPlainModule(t)
	if err := m.OnMessage(&Context{}, testPlainMsgA, nil); err != nil {
		t.Fatalf("OnMessage A: %v", err)
	}
	if err := m.OnMessage(&Context{}, testPlainMsgB, []byte("ok")); err != nil {
		t.Fatalf("OnMessage B: %v", err)
	}
	if len(m.calls) != 2 || m.calls[0] != testPlainMsgA || m.calls[1] != testPlainMsgB {
		t.Fatalf("分发顺序不符: %v", m.calls)
	}
}

// TestPlainUnknownMsg 未注册的协议号返回错误而不是静默忽略。
func TestPlainUnknownMsg(t *testing.T) {
	m := newTestPlainModule(t)
	if err := m.OnMessage(&Context{}, 999999, nil); err == nil {
		t.Fatal("未注册的 msgID 应返回错误")
	}
}

// TestPlainHandlerError 处理器错误原样上抛（由玩家层决定回包策略）。
func TestPlainHandlerError(t *testing.T) {
	m := newTestPlainModule(t)
	if err := m.OnMessage(&Context{}, testPlainMsgB, []byte("boom")); err == nil {
		t.Fatal("处理器错误应上抛")
	}
}

// TestPlainDuplicateHandlePanics 重复注册同一协议号属于编码错误，构造期 panic。
func TestPlainDuplicateHandlePanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("重复注册应 panic")
		}
	}()
	_ = NewPlain("dup", func(p *Plain) {})
	p := NewPlain("dup2")
	p.Handle(testPlainMsgA, func(*Context, uint32, []byte) error { return nil })
	p.Handle(testPlainMsgA, func(*Context, uint32, []byte) error { return nil })
}

// TestPlainLifecycleHooks 可选钩子被调用；未挂的钩子返回 nil。
func TestPlainLifecycleHooks(t *testing.T) {
	m := newTestPlainModule(t)
	if err := m.Init(&Context{}); err != nil || !m.inited {
		t.Fatalf("Init 钩子未生效: err=%v inited=%v", err, m.inited)
	}
	if err := m.OnLogin(&Context{}); err != nil || !m.login {
		t.Fatalf("OnLogin 钩子未生效: err=%v login=%v", err, m.login)
	}
	if err := m.OnLogout(&Context{}); err != nil {
		t.Fatalf("未挂 OnLogout 时应返回 nil: %v", err)
	}
	if err := m.OnSave(context.Background()); err != nil {
		t.Fatalf("无状态模块 OnSave 应返回 nil: %v", err)
	}
}

// TestPlainMsgIDsCopy MsgIDs 返回副本，调用方改动不影响内部状态。
func TestPlainMsgIDsCopy(t *testing.T) {
	m := newTestPlainModule(t)
	ids := m.MsgIDs()
	if len(ids) != 2 {
		t.Fatalf("应注册 2 个协议，got %v", ids)
	}
	ids[0] = 1
	if m.MsgIDs()[0] == 1 {
		t.Fatal("MsgIDs 应返回副本")
	}
}

// TestPlainIsStateless Plain 声明所有协议无状态。
func TestPlainIsStateless(t *testing.T) {
	m := newTestPlainModule(t)
	if !m.IsStateless(testPlainMsgA) || !m.IsStateless(testPlainMsgB) {
		t.Fatal("Plain 模块应声明所有协议无状态")
	}
}

// TestPlainReply 回包经 proto 序列化后交给 Sender。
func TestPlainReply(t *testing.T) {
	m := newTestPlainModule(t)
	var got uint32
	var gotBody []byte
	ctx := &Context{Send: func(msgID uint32, body []byte) error {
		got, gotBody = msgID, body
		return nil
	}}
	if err := m.Reply(ctx, 999, wrapperspb.String("x")); err != nil {
		t.Fatalf("Reply: %v", err)
	}
	if got != 999 || len(gotBody) == 0 {
		t.Fatalf("Reply 未送达: msgID=%d len=%d", got, len(gotBody))
	}
}

// TestRegistryIsStateless 注册表按 msgID 判断无状态声明；
// 未实现 StatelessAware 的模块默认 false（保守：多付开销，不丢数据）。
func TestRegistryIsStateless(t *testing.T) {
	r := NewRegistry()
	plain := newTestPlainModule(t)
	r.Register(plain)

	if !r.IsStateless(testPlainMsgA) {
		t.Fatal("Plain 模块的协议应判定为无状态")
	}
	if r.IsStateless(999999) {
		t.Fatal("未注册的协议应返回 false")
	}

	// 普通模块（只实现 Module，不实现 StatelessAware）
	legacy := &legacyModule{}
	r.Register(legacy)
	if r.IsStateless(legacyID) {
		t.Fatal("未声明 StatelessAware 的模块应返回 false")
	}
}

const legacyID uint32 = 990003

// legacyModule 只实现 Module 接口（模拟存量有状态模块）。
type legacyModule struct{}

func (*legacyModule) Name() string                             { return "legacy" }
func (*legacyModule) MsgIDs() []uint32                         { return []uint32{legacyID} }
func (*legacyModule) Init(*Context) error                      { return nil }
func (*legacyModule) OnSave(context.Context) error             { return nil }
func (*legacyModule) OnLogin(*Context) error                   { return nil }
func (*legacyModule) OnLogout(*Context) error                  { return nil }
func (*legacyModule) OnMessage(*Context, uint32, []byte) error { return nil }

var _ Module = (*legacyModule)(nil)
var _ Module = (*testPlainModule)(nil)
var _ StatelessAware = (*testPlainModule)(nil)

// TestPlainHandleRRespondsOnCacheFailure Plain 的 HandleR + PlainErrCode
// 让零状态模块也能给出业务错误包（否则只能收网关通用错误）。
func TestPlainHandleRRespondsOnCacheFailure(t *testing.T) {
	p := NewPlain("rank", PlainErrCode(6702))
	p.HandleR(8101, 8102, &gamev1.RankListResp{}, func(*Context, uint32, []byte) error { return nil })

	respID, raw, ok := p.CacheFailureReply(8101)
	if !ok || respID != 8102 || len(raw) == 0 {
		t.Fatalf("CacheFailureReply = %d/%v/%v", respID, raw, ok)
	}
	var resp gamev1.RankListResp
	require.NoError(t, proto.Unmarshal(raw, &resp))
	require.EqualValues(t, 6702, resp.Code)

	// 未声明的协议 → ok=false（走网关通用错误）
	if _, _, ok := p.CacheFailureReply(9999); ok {
		t.Fatal("未声明响应的协议不应回包")
	}

	// 未设 errCode → 即使声明了响应也不回（Plain 不该凭空造业务码）
	bare := NewPlain("bare_rank")
	bare.HandleR(8101, 8102, &gamev1.RankListResp{}, func(*Context, uint32, []byte) error { return nil })
	if _, _, ok := bare.CacheFailureReply(8101); ok {
		t.Fatal("未设 errCode 时不应回包")
	}
}

// TestPlainHandleRNilPrototypePanics nil 原型是编码错误，启动期暴露。
func TestPlainHandleRNilPrototypePanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("nil prototype should panic")
		}
	}()
	p := NewPlain("x")
	p.HandleR(1, 2, nil, func(*Context, uint32, []byte) error { return nil })
}
