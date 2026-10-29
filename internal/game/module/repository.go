// internal/game/module/repository.go —— 模块状态的读写契约（Repository）
//
// 解决什么问题：在此之前，一个业务处理器要写四行才能改一次状态——
//
//	d, err := c.Repo.Get()        // 解码
//	if err != nil { ... }
//	d.Xxx = ...                   // 改
//	if err := c.Repo.Save(d); err != nil { ... }   // 编码 + 标脏
//
// 四行里有两行是纯样板，而且**漏写 Save 不会报错**——状态改了、没落库、
// 单元测试还是绿的（测试读的是同一份内存）。全仓 30 处 Get / 19 处 Save
// 里，任何一处漏写都是一次静默数据丢失。
//
// Repository 把「读 → 改 → 写 → 标脏」收敛成一次调用：业务只描述
// 「拿到状态之后做什么」，编解码、脏标记、幂等键全部由契约负责。
//
// 三层职责（这是本文件存在的第二个理由）：
//
//	业务 handler   只认 Repository 的方法，不知道 Ext 槽位、不知道表结构
//	Stateful       内存结构 ↔ Ext JSON 的编解码（基座）
//	writebehind    快照 → 表行的落库（persist.go / persist_gen.go）
//
// 三者之间只有 Repository 一个接口，换缓存实现（比如状态改存 Redis 独立
// key）时业务代码一行不用改。
package module

import (
	"errors"
)

// ErrApplied 幂等命中：Once 的键已经应用过，本次请求没有执行 fn。
//
// 业务用 errors.Is(err, module.ErrApplied) 判断，与「系统错误」区分开——
// 前者是对重放请求的正常应答（直接回当前状态即可），后者要回错误码。
var ErrApplied = errors.New("module: biz key already applied")

// Repository 模块状态的读写契约。
//
// 四个方法对应四种真实场景，没有第五种：
//
//	View    只读（列表/详情查询）
//	Mutate  读改写（接取/领取/推进进度）
//	Once    带幂等键的读改写（支付回调、跨服消息重投）
//	Touch   只标脏不写（下线时保证最后一次内存改动落库）
//
// 为什么 Mutate 要让 fn 返回 changed 而不是无条件写回：大量请求是
// 「只读 + 早退」（今天已领、配置缺失、背包已满），它们有权进闭包但无权
// 改状态。无条件写回会让每条这种请求都产生一次序列化 + 一次提交。
// fn 的返回值就是「我到底改没改」的答案，基座据此决定是否编码。
type Repository[T any] interface {
	// View 只读访问状态：解码后交给 fn，不回写、不标脏。
	//
	// fn 里对 *T 的修改会被丢弃（下次 Get 重新解码得到的是旧值）。
	// 需要改状态用 Mutate——这个约束是有意的：View 的语义是查询。
	View(fn func(*T) error) error

	// Mutate 读改写：fn 返回 changed=true 才编码回写并标脏。
	//
	// fn 返回 error 时状态不回写（内存里的改动随 *T 一起丢弃），
	// error 原样抛给调用方。
	Mutate(fn func(*T) (changed bool, err error)) error

	// Once 带幂等键的 Mutate：key 已应用过时直接返回 ErrApplied，不执行 fn。
	//
	// 键与状态在同一次快照提交里落库（ClaimBiz 内部标脏），所以不存在
	// 「键写进了库、状态没写」的中间态。key 为空串等价于 Mutate。
	//
	// 顺序是「先查键 → 执行 fn → 成功后才占键」：fn 失败时键不被占用，
	// 重投的请求还能重试。反过来（先占键再执行）会把一次失败变成永久丢失。
	//
	// 当前还没有业务用到它（充值/跨服重投的幂等走的是 player 层）。保留是
	// 因为它是 State.ClaimBiz 的唯一替代入口——ClaimBiz 自带落库链路
	// （applied_grants 列），绕过它手写一遍就会漏掉「只发奖也要标脏」这一步，
	// 而那正是键丢失、重投二次发奖的根因。
	Once(key string, fn func(*T) (changed bool, err error)) error

	// Touch 槽位已存在时标脏，保证内存改动随下一轮 flush 落库。
	//
	// 与 Mutate 的区别：不编码、不修改状态，只「催一次提交」。用于下线钩子
	// 这类「改动早就在内存里、只差一次提交」的场景。
	//
	// 槽位为空时什么也不做——为从没用过本模块的玩家写一条空状态，除了
	// 让表变胖没有任何意义。
	Touch() error

	// Reset 清空状态：槽位删除并标脏，下次 View 回到 newFn 的空状态。
	//
	// 当前没有业务调用，保留是因为它是「清空某个模块进度」的唯一合法入口
	// （重置活动、运维补偿脚本）。没有它，那类代码会去写 State.SetExt(name, nil)，
	// 而 Ext 槽位是基座的实现细节——`convention_test.go` 也禁止模块直接碰它。
	Reset() error
}

// Repo 是 Repository 的默认实现：状态存玩家快照的 Ext 槽位。
//
// 只被 module 包内部与测试构造，业务通过 Ctx.Repo 拿到它。
//
// 生命周期：每次请求由 newCtx 新建一个（便宜，两个指针），
// 因此它绑定「本次请求的 Context」——不要跨请求保存引用。
type Repo[D any] struct {
	s   *Stateful[D]
	ctx *Context
}

// 编译期断言：默认实现必须满足契约。
var _ Repository[struct{}] = (*Repo[struct{}])(nil)

// View 实现 Repository。
func (r *Repo[D]) View(fn func(*D) error) error {
	d, err := r.s.decode(r.ctx)
	if err != nil {
		return err
	}
	return fn(d)
}

// Mutate 实现 Repository。
func (r *Repo[D]) Mutate(fn func(*D) (bool, error)) error {
	d, err := r.s.decode(r.ctx)
	if err != nil {
		return err
	}
	changed, err := fn(d)
	if err != nil || !changed {
		return err
	}
	return r.s.encode(r.ctx, d)
}

// Once 实现 Repository。
func (r *Repo[D]) Once(key string, fn func(*D) (bool, error)) error {
	if key != "" && r.ctx.State.HasBiz(key) {
		return ErrApplied
	}
	return r.Mutate(func(d *D) (bool, error) {
		changed, err := fn(d)
		if err != nil {
			return false, err
		}
		if key != "" {
			// ClaimBiz 在内部标脏（"grants"），键随快照一起 CAS 落库。
			// 这里不再判断返回值：上面的 HasBiz 已经查过，中间不会有别的
			// 协程改同一个玩家的 State（同玩家消息在 Actor 内串行）。
			r.ctx.State.ClaimBiz(key)
		}
		return changed, nil
	})
}

// Touch 实现 Repository。
func (r *Repo[D]) Touch() error {
	if len(r.ctx.State.Ext(r.s.Name())) == 0 {
		return nil
	}
	r.ctx.State.MarkDirty(r.s.Name())
	return nil
}

// Reset 实现 Repository。
func (r *Repo[D]) Reset() error {
	r.ctx.State.SetExt(r.s.Name(), nil)
	return nil
}

// 说明：曾经还有 Get / Save / Delete 三个方法（等价于「手动拆开 Mutate」
// 和「Reset 的别名」），是 Repository 之前的写法。迁移期保留了一段时间，
// 全部 24 个模块迁完后已删除——留着它们等于给「漏写 Save」留一条后路，
// 而这个抽象的全部价值就在堵死那条路。
