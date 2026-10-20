// pkg/writebehind/flat.go —— RegisterFlat：把单行状态表注册成模块持久化
//
// 大多数玩法模块的持久化需求止于「一个玩家一行」：读出来、改一改、
// 整行 UPSERT 回去。为每个这样的模块手写 Persister（四方法）是重复劳动，
// RegisterFlat 用 flush.FlatOps[T]（gendb 生成的 Upsert/Load 函数对）
// 现场构造一个标准 Persister 注册进同一张表，worker/snapshot/Redis
// 提交路径一行不改。
//
// 设计取舍：先覆盖「单行表」这个 80% 场景；复杂模块（分表、增量删、
// 跨表事务）仍可随时改写 Persister 接管——JSON 槽位格式不变，线上
// 数据无需迁移。这是「用泛型 + 函数注入换掉模板代码」的典型案例。
package writebehind

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"game_study/pkg/flush"
)

// RegisterFlat 把「玩家级单行状态表」注册为模块持久化实现。
//
// 它是 Persister 扩展点的语法糖：内部构造标准 Persister 注册进同一张表，
// worker / 快照 / Redis 提交一行不改。将来状态变复杂时，删掉注册改手写
// Persister 即可，JSON 槽位格式不变（T 就是 gendb 生成的表结构体）。
//
// validate 是可选业务校验（语义约束）；不传则只做结构校验（可解码且非 nil）。
// 校验失败与手写 Persister 语义一致：整批回滚，不静默跳过坏数据。
//
// Go 细节：validate ...func(*T) error 是可变参数模拟「可选参数」——
// Go 没有默认参数值，惯用做法是尾附可变槽位；这里只取 validate[0]，
// 传多个是调用方错误（静默忽略后续）。
func RegisterFlat[T any](name string, ops flush.FlatOps[T], validate ...func(*T) error) {
	RegisterPersister(newFlatPersister(name, ops, validate...))
}

// flatPersister 是 RegisterFlat 的产物，行为等价于一份手写 Persister。
//
// 泛型类型持有 T 的两个生成函数；validate 为 nil 表示只做结构校验。
// 方法用值接收者（p flatPersister[T]）：结构体只有三个字级字段，
// 值拷贝比指针多一次解引更便宜，且杜绝注册后被外部改字段。
type flatPersister[T any] struct {
	// name 模块名；同时是 Snapshot.Ext 的键（Persister.Name 契约）。
	name string
	// ops gendb 生成的读写函数对（注入的持久化策略）。
	ops flush.FlatOps[T]
	// validate 可选业务校验；nil = 仅结构校验。
	validate func(*T) error
}

// newFlatPersister 校验注册参数；缺字段属于编码错误，启动期 panic 暴露。
//
// panic 语义与 RegisterPersister 一致：模块漏传 Upsert/Load 或名字为空，
// 都是「接了一半」的开发期错误，启动即炸好过运行到落库才发现
// （那时线上玩家数据已经开始堆积）。
//
// Go 细节：validate... 在函数内就是切片，取 [0] 前判 len>0——
// 空切片下标会 panic（index out of range），这是可变参数惯用的判空方式。
func newFlatPersister[T any](name string, ops flush.FlatOps[T], validate ...func(*T) error) flatPersister[T] {
	if name == "" {
		panic("writebehind: flat persister with empty name")
	}
	if ops.Upsert == nil || ops.Load == nil {
		panic(fmt.Sprintf("writebehind: flat persister %q requires both Upsert and Load", name))
	}
	p := flatPersister[T]{name: name, ops: ops}
	if len(validate) > 0 {
		p.validate = validate[0]
	}
	return p
}

// Name 实现 Persister.Name（模块名 = Ext 键）。
func (p flatPersister[T]) Name() string { return p.name }

// Validate 做结构校验（解码成功且非 nil）与可选业务校验。
//
// 调用方（Snapshot.Validate）已保证 raw 非空，这里再判 nil 只是防御——
// 双保险：即使将来有调用方绕过 Snapshot.Validate，这里也不会把
// nil 状态当真数据写库。
func (p flatPersister[T]) Validate(raw json.RawMessage) error {
	d, err := Decode[T](raw)
	if err != nil {
		return fmt.Errorf("decode %s state: %w", p.name, err)
	}
	if d == nil {
		return fmt.Errorf("empty %s state", p.name)
	}
	if p.validate != nil {
		return p.validate(d)
	}
	return nil
}

// PersistBatch 把本批玩家的状态解码成表行，交给生成的批量 UPSERT。
//
// 流程：逐行 Decode → 跳过 nil（该玩家无此模块数据）→ 汇总成 []T →
// ops.Upsert 合批写（内部按 flush.DefaultBatchRows 切批）。
// 任一行解码失败整体返回错误：坏数据宁可整批回滚，也不静默丢人——
// 丢一个玩家的数据比一次回滚难发现得多。
//
// Go 细节：out := make([]T, 0, len(rows)) 按上界预分配，append 零扩容；
// Decode 返回 *T，这里解引用拷贝进 out（T 是表结构体，值拷贝语义，
// 之后与原 JSON 再无关联）。
func (p flatPersister[T]) PersistBatch(ctx context.Context, tx *sql.Tx, rows []ModuleRow) error {
	out := make([]T, 0, len(rows))
	for _, row := range rows {
		d, err := Decode[T](row.Data)
		if err != nil {
			return fmt.Errorf("player %d: %w", row.PlayerID, err)
		}
		if d == nil {
			continue
		}
		out = append(out, *d)
	}
	if len(out) == 0 {
		return nil
	}
	return p.ops.Upsert(ctx, tx, out)
}

// Load 冷启动读取；没有行等价于「从未写过该模块」，返回 (nil, nil)。
//
// Go 细节：errors.Is(err, sql.ErrNoRows) 是 database/sql 哨兵错误的
// 标准判定——驱动可能包装 ErrNoRows（如带上下文的包装错误），
// 只能用 Is 不能 ==；未命中转成 (nil, nil) 让上层走「新建默认状态」
// 路径，而非报错。
func (p flatPersister[T]) Load(ctx context.Context, tx *sql.Tx, playerID int64) (json.RawMessage, error) {
	d, err := p.ops.Load(ctx, tx, playerID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return Encode(d)
}
