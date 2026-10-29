// pkg/writebehind/flat.go —— RegisterFlat：把单行状态表注册成模块持久化
//
// 大多数玩法模块的持久化需求止于「一个玩家一行」：读出来、改一改、
// 整行 UPSERT 回去。为每个这样的模块手写 Persister（四方法）是重复劳动，
// RegisterFlat 用 flush.FlatOps[T]（gendb 生成的 Upsert/Load 函数对）
// 注册一个标准 Persister，落库编排与快照层一行不改。
//
// 与 RegisterStateful 的关系（本文件曾经有第二份实现，已删除）：
//
//	RegisterFlat[T]         Ext 的 JSON 就是表行 T
//	RegisterStateful[R, D]  Ext 存业务结构 D，落库时经 Codec 转成行 R
//
// 前者是后者在 Data == Row 且转换函数为恒等时的特例。此前两者各有一份
// flatPersister / statefulPersister，四方法（Name/Validate/PersistBatch/
// Load）逐行同构，只差中间一次 ToRow/FromRow；连注册参数校验的 panic
// 都是同一段抄两遍、文案只差一个词。那份重复的代价不在行数——在于
// 「Validate 该不该判 nil」「Load 遇到 ErrNoRows 返回什么」这类判断
// 有两处答案，改一处就会让 flat 与 stateful 的模块在坏数据上表现不同。
//
// 现在 flat 只是「恒等 codec 的 stateful」，骨架唯一。新增能力（比如
// 批量预取、按行分表）只改 statefulPersister 一处，两种注册同时受益。
package writebehind

import (
	"game_study/pkg/flush"
)

// RegisterFlat 把「玩家级单行状态表」注册为模块持久化实现。
//
// 它是 RegisterStateful 的语法糖：内部用恒等转换构造 StateCodec 注册进
// 同一张表，落库编排 / 快照层一行不改。将来状态变复杂时，删掉注册改
// 手写 Persister 即可，JSON 槽位格式不变（T 就是 gendb 生成的表结构体）。
//
// validate 是可选业务校验（语义约束）；不传则只做结构校验（可解码且非 nil）。
// 校验失败与手写 Persister 语义一致：整批回滚，不静默跳过坏数据。
//
// Go 细节：validate ...func(*T) error 是可变参数模拟「可选参数」——
// Go 没有默认参数值，惯用做法是尾附可变槽位；这里只取 validate[0]，
// 传多个是调用方错误（静默忽略后续）。
func RegisterFlat[T any](name string, ops flush.FlatOps[T], validate ...func(*T) error) {
	var v func(*T) error
	if len(validate) > 0 {
		v = validate[0]
	}
	RegisterStateful(name, ops, StateCodec[T, T]{
		ToRow:   flatToRow[T],
		FromRow: flatFromRow[T],
	}, v)
}

// flatToRow 恒等转换：Ext 的 JSON 解出来就是表行，不需要字段映射。
//
// playerID 用不上——表行 T 自己带主键（gendb 生成的 Upsert 从行里取）。
// 形参必须保留：它要实现 StateCodec.ToRow 的签名。
func flatToRow[T any](_ int64, d *T) (*T, error) { return d, nil }

// flatFromRow 恒等转换：表行直接当业务结构返回。
func flatFromRow[T any](r *T) (*T, error) { return r, nil }
