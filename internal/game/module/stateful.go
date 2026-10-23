// internal/game/module/stateful.go —— 单结构体模块基座（R2.6）
//
// 解决什么问题：像签到、任务、商店这类模块，业务状态就是一个结构体，
// 但每个模块都要自己写「内存结构 ↔ Ext JSON ↔ MySQL 行」的往返：
//
//	load(state)   → json/行 解码成 *Data
//	save(state,d) → *Data 编码回 Ext
//	decode(row)   → 行转 Data
//	validate(row) → 落库前校验
//
// 其中前两个纯属样板（signin 里 29 行），而且要求业务开发者理解 Ext 的
// 存在。Stateful 把它们收进基座：业务只声明结构体，读写用 Read/Mutate
// 闭包，编解码与标脏由基座完成。
//
// 与 Thin 的关系：Stateful 内嵌 Thin，Module 七件套（Name/MsgIDs/分发/
// OnSave/OnLogin/OnLogout/回滚声明）仍由 Thin 提供；Stateful 只多管一件事——
// 状态结构体的编解码。
//
// 适用边界：
//   - 状态是「单个结构体」（签到/任务/商店/副本进度）；
//   - 多表关联或需要跨玩家事务的模块（好友/公会/队伍）不适用，保持手写。
package module

import (
	"encoding/json"
	"fmt"
)

// Stateful 单结构体模块基座。
//
// 用法：
//
//	type Data struct { Days []int32; Rewards map[int32]int64 }
//
//	m.Stateful = module.NewStateful("signin", newData,
//		module.ThinLogger(logger),
//		module.ThinErrCode(codeSystem),
//	)
//	m.HandleR(MsgClaimReq, MsgClaimResp, &gamev1.SigninClaimResp{}, m.onClaim)
//
//	func (m *Module) onClaim(ctx *module.Context, _ uint32, _ []byte) error {
//		return m.Mutate(ctx, func(d *Data) bool {
//			d.Days = append(d.Days, today)
//			return true          // 有改动 → 写回 Ext 并标脏
//		})
//	}
//
// Go 知识点：泛型参数 T 通过 newFn 的签名推断（func() *T），调用处不必写
// 显式的类型实参；基座本身不做反射，序列化走 encoding/json 的常规路径。
type Stateful[T any] struct {
	*Thin
	// newFn 首次访问时构造空状态（可为 nil，此时用 T 的零值）。
	newFn func() *T
}

// NewStateful 创建单结构体模块基座。
//
// newFn 用于给新玩家一个合理的空状态（比如空切片而不是 nil），
// 避免业务闭包里到处判空。opts 直接复用 Thin 的选项（日志/错误码/回包工厂…）。
func NewStateful[T any](name string, newFn func() *T, opts ...ThinOption) *Stateful[T] {
	return &Stateful[T]{
		Thin:  NewThin(name, opts...),
		newFn: newFn,
	}
}

// Read 以只读方式访问状态：解码后交给 fn，不回写。
func (s *Stateful[T]) Read(ctx *Context, fn func(*T)) error {
	d, err := s.decode(ctx)
	if err != nil {
		return err
	}
	fn(d)
	return nil
}

// Mutate 以读写方式访问状态：fn 返回 true 表示有改动，基座负责写回 Ext 并标脏。
//
// 为什么让 fn 返回 bool 而不是无条件写回：很多请求是「只读 + 早退」
// （已领取、配置缺失、背包已满），无权改状态却触发一次序列化与标脏，
// 会让每条消息都产生一次提交。fn 的返回值就是「我到底改没改」的答案。
func (s *Stateful[T]) Mutate(ctx *Context, fn func(*T) bool) error {
	d, err := s.decode(ctx)
	if err != nil {
		return err
	}
	if !fn(d) {
		return nil
	}
	return s.encode(ctx, d)
}

// State 返回当前状态的解码副本（只读场景的便捷写法）。
//
// 每次调用都会解码一次，循环里调它要自己缓存。
func (s *Stateful[T]) State(ctx *Context) (*T, error) { return s.decode(ctx) }

// decode 从玩家快照的 Ext 槽位解出状态结构体。
func (s *Stateful[T]) decode(ctx *Context) (*T, error) {
	raw := ctx.State.Ext(s.Name())
	if len(raw) == 0 {
		if s.newFn != nil {
			return s.newFn(), nil
		}
		return new(T), nil
	}
	d := new(T)
	if err := json.Unmarshal(raw, d); err != nil {
		return nil, fmt.Errorf("module %s: decode state: %w", s.Name(), err)
	}
	return d, nil
}

// encode 把状态结构体写回 Ext 槽位（SetExt 内部会标脏）。
func (s *Stateful[T]) encode(ctx *Context, d *T) error {
	raw, err := json.Marshal(d)
	if err != nil {
		return fmt.Errorf("module %s: encode state: %w", s.Name(), err)
	}
	ctx.State.SetExt(s.Name(), raw)
	return nil
}

// 注：曾有 WithLogger 便于构造后补日志器，已删除——它重建一个全新 Thin，
// 会把已注册的 handler 与响应声明一起丢掉。需要日志器请在 NewStateful 时
// 用 ThinLogger 选项传入。
