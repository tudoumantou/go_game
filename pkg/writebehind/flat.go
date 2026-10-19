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
func RegisterFlat[T any](name string, ops flush.FlatOps[T], validate ...func(*T) error) {
	RegisterPersister(newFlatPersister(name, ops, validate...))
}

// flatPersister 是 RegisterFlat 的产物，行为等价于一份手写 Persister。
type flatPersister[T any] struct {
	name     string
	ops      flush.FlatOps[T]
	validate func(*T) error
}

// newFlatPersister 校验注册参数；缺字段属于编码错误，启动期 panic 暴露。
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

func (p flatPersister[T]) Name() string { return p.name }

// Validate 做结构校验（解码成功且非 nil）与可选业务校验。
//
// 调用方（Snapshot.Validate）已保证 raw 非空，这里再判 nil 只是防御。
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
