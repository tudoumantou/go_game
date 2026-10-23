// internal/game/module/plain.go —— 无状态模块基座（Plain）
//
// 解决什么问题：一部分玩法的处理器根本不碰玩家状态——榜单查询只是代理一次
// 中心服调用，聊天广播只是转发，技能表只是读配置。但它们仍被要求实现完整的
// Module 接口（7 个方法），而且在缓存权威模式下，每条消息都要走一遍
// 「clone 整个 State → 业务 → JSON 比对 → CAS」的流程：即使 diff 为空、
// 不产生 Redis 写，clone 与两次 Marshal 的开销也白付了。
//
// Plain 把这类模块收敛成「声明协议 + 写处理器」两件事，并向框架声明
// 「这条消息不修改玩家状态」——player.Actor.Receive 据此直接分发，
// 完全跳过快照流程（见 registry.IsStateless）。
//
// 三档基座的分工（新增功能先选档，再写代码）：
//
//	Plain     零状态：不读写 State、不落库（榜单/聊天转发/静态表查询）
//	Thin      单玩家状态：改 Ext 槽位，随快照 CAS（签到/任务/背包）
//	（无基座） 关系型事务：直写 MySQL 事务（好友/公会/队伍）
//
// 判据：处理器里出现 ctx.State 的写方法（AddItem/SetExt/MarkDirty），
// 就不该用 Plain——用错了不会静默出错（状态照常随快照提交），但那意味着
// 你放弃了 Plain 的快路径，应该改用 Thin。
package module

import (
	"context"
	"fmt"

	"game_study/pkg/log"
	"google.golang.org/protobuf/proto"
)

// PlainHandler 无状态协议处理函数。
//
// body 是原始协议字节：空消息体可直接忽略；需要字段的处理器自行
// proto.Unmarshal——框架不替业务猜消息类型。
type PlainHandler func(ctx *Context, msgID uint32, body []byte) error

// Plain 无状态模块基座（Module 的预装配实现）。
//
// 用法：
//
//	m := &Module{}
//	m.Plain = module.NewPlain("rank",
//		module.PlainLogger(logger),
//	)
//	m.HandleR(codec.MsgRankListReq, codec.MsgRankListResp, &gamev1.RankListResp{}, m.onList)
type Plain struct {
	name     string
	logger   *log.Logger
	handlers map[uint32]PlainHandler
	ids      []uint32
	// resp 请求协议 → 响应声明（HandleR 注册时填入）。
	resp map[uint32]respSpec
	// errCode 缓存不可用时的默认错误码；0 表示不自定义（走网关通用错误）。
	errCode int32

	initFn   func(ctx *Context) error
	loginFn  func(ctx *Context) error
	logoutFn func(ctx *Context) error
}

// PlainOption 构造选项。
type PlainOption func(*Plain)

// PlainLogger 注入日志器（处理器里打印业务日志用）。
func PlainLogger(l *log.Logger) PlainOption {
	return func(p *Plain) { p.logger = l }
}

// PlainInit 挂初始化钩子（加载配置表等，一次）。
func PlainInit(fn func(ctx *Context) error) PlainOption {
	return func(p *Plain) { p.initFn = fn }
}

// PlainLogin 挂登录钩子（需要预热缓存等场景；不挂即空实现）。
func PlainLogin(fn func(ctx *Context) error) PlainOption {
	return func(p *Plain) { p.loginFn = fn }
}

// PlainLogout 挂下线钩子（清理跨模块引用等；不挂即空实现）。
func PlainLogout(fn func(ctx *Context) error) PlainOption {
	return func(p *Plain) { p.logoutFn = fn }
}

// PlainErrCode 设置缓存不可用时的默认错误码。
//
// 不设置时 Plain 不实现 CacheFailureAware（Registry 探测不到，走网关通用错误）。
func PlainErrCode(code int32) PlainOption {
	return func(p *Plain) { p.errCode = code }
}

// NewPlain 创建无状态模块基座。
func NewPlain(name string, opts ...PlainOption) *Plain {
	p := &Plain{
		name:     name,
		handlers: make(map[uint32]PlainHandler),
		resp:     make(map[uint32]respSpec),
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// Handle 注册协议处理器（同一 msgID 重复注册 panic——编码错误）。
func (p *Plain) Handle(msgID uint32, h PlainHandler) {
	if _, ok := p.handlers[msgID]; ok {
		panic(fmt.Sprintf("plain module %s: msgID %d registered twice", p.name, msgID))
	}
	p.handlers[msgID] = h
	p.ids = append(p.ids, msgID)
}

// HandleR 注册处理器并声明响应协议（与 Thin.HandleR 同语义）。
//
//	m.HandleR(codec.MsgRankListReq, codec.MsgRankListResp, &gamev1.RankListResp{}, m.onList)
func (p *Plain) HandleR(reqID, respID uint32, prototype proto.Message, h PlainHandler) {
	if prototype == nil {
		panic(fmt.Sprintf("plain module %s: HandleR(%d) nil prototype", p.name, reqID))
	}
	p.Handle(reqID, h)
	p.resp[reqID] = respSpec{respID: respID, prototype: prototype}
}

// CacheFailureReply 实现 module.CacheFailureAware。
//
// 未声明响应或未设 errCode 时 ok=false，由网关回通用错误通知——
// 零状态模块本来就不该在缓存不可用时给出业务语义的回包。
func (p *Plain) CacheFailureReply(msgID uint32) (uint32, []byte, bool) {
	spec, ok := p.resp[msgID]
	if !ok || p.errCode == 0 {
		return 0, nil, false
	}
	raw, err := proto.Marshal(withCode(spec.prototype, p.errCode))
	if err != nil {
		return 0, nil, false
	}
	return spec.respID, raw, true
}

// Reply 序列化并回包（marshal 失败原样返回，调用方通常直接 return）。
func (p *Plain) Reply(ctx *Context, msgID uint32, m proto.Message) error {
	raw, err := proto.Marshal(m)
	if err != nil {
		return err
	}
	return ctx.Reply(msgID, raw)
}

// Logger 返回注入的日志器（可能为 nil，调用方自行判空）。
func (p *Plain) Logger() *log.Logger { return p.logger }

// Name 实现 Module。
func (p *Plain) Name() string { return p.name }

// MsgIDs 实现 Module。返回副本，防止调用方改动内部切片。
func (p *Plain) MsgIDs() []uint32 {
	out := make([]uint32, len(p.ids))
	copy(out, p.ids)
	return out
}

// Init 实现 Module。
func (p *Plain) Init(ctx *Context) error {
	if p.initFn != nil {
		return p.initFn(ctx)
	}
	return nil
}

// OnMessage 实现 Module：按 msgID 分发到处理器。
func (p *Plain) OnMessage(ctx *Context, msgID uint32, body []byte) error {
	h, ok := p.handlers[msgID]
	if !ok {
		return fmt.Errorf("plain module %s: no handler for msgID %d", p.name, msgID)
	}
	return h(ctx, msgID, body)
}

// OnSave 实现 Module：无状态模块没有需要落库的私有数据。
func (p *Plain) OnSave(context.Context) error { return nil }

// OnLogin 实现 Module。
func (p *Plain) OnLogin(ctx *Context) error {
	if p.loginFn != nil {
		return p.loginFn(ctx)
	}
	return nil
}

// OnLogout 实现 Module。
func (p *Plain) OnLogout(ctx *Context) error {
	if p.logoutFn != nil {
		return p.logoutFn(ctx)
	}
	return nil
}

// IsStateless 实现 StatelessAware：本模块处理的消息不修改玩家状态，
// 玩家层据此跳过「clone → 业务 → JSON 比对 → CAS」全流程。
func (p *Plain) IsStateless(uint32) bool { return true }
