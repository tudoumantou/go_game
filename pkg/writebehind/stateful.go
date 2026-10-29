// pkg/writebehind/stateful.go —— RegisterStateful：Ext 存业务结构，落库存表行
//
// 与 RegisterFlat 的差别：
//
//	RegisterFlat[T]         Ext 的 JSON 就是表行 T（业务结构被迫迁就行字段）
//	RegisterStateful[R, D]  Ext 存业务结构 D，落库时经 Codec 转成行 R
//
// 后者让业务结构可以自由设计（比如签到用 {Days, Rewards}，而表里是
// claimed_days/claimed_rewards 两个 JSON 字符串列），代价是多一次转换。
// 转换代码集中在模块的 persist.go 里，业务文件不再出现持久化细节。
package writebehind

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"game_study/pkg/flush"
)

// StateCodec 业务结构与表行之间的双向转换。
//
// ToRow 在落库时调用（每行一次）；FromRow 在冷启动读取时调用。
// 任一侧返回错误都会让整批回滚——坏数据宁可失败，也不静默丢玩家。
type StateCodec[Row, Data any] struct {
	// ToRow 转成表行；playerID 由落库批次提供（业务结构不必自己带主键）。
	ToRow   func(playerID int64, d *Data) (*Row, error)
	FromRow func(*Row) (*Data, error)
}

// RegisterStateful 注册「Ext 存业务结构、落库存表行」的模块持久化。
//
// 用法（模块的 persist.go）：
//
//	func init() {
//		writebehind.RegisterStateful(stateKey, model.PlayerSigninFlat,
//			writebehind.StateCodec[model.PlayerSignin, Data]{
//				ToRow: toRow, FromRow: fromRow,
//			},
//			validate,
//		)
//	}
func RegisterStateful[Row, Data any](
	name string,
	ops flush.FlatOps[Row],
	codec StateCodec[Row, Data],
	validate func(*Data) error,
) {
	if name == "" {
		panic("writebehind: persister with empty name")
	}
	if ops.Upsert == nil || ops.Load == nil {
		panic(fmt.Sprintf("writebehind: persister %q requires both Upsert and Load", name))
	}
	if codec.ToRow == nil || codec.FromRow == nil {
		panic(fmt.Sprintf("writebehind: persister %q requires ToRow and FromRow", name))
	}
	RegisterPersister(statefulPersister[Row, Data]{
		name: name, ops: ops, codec: codec, validate: validate,
	})
}

type statefulPersister[Row, Data any] struct {
	name     string
	ops      flush.FlatOps[Row]
	codec    StateCodec[Row, Data]
	validate func(*Data) error
}

func (p statefulPersister[Row, Data]) Name() string { return p.name }

// Validate 解出业务结构后做结构校验与可选业务校验。
func (p statefulPersister[Row, Data]) Validate(raw json.RawMessage) error {
	d, err := Decode[Data](raw)
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

// PersistBatch 把本批玩家的业务结构逐个转成表行后合批 UPSERT。
func (p statefulPersister[Row, Data]) PersistBatch(ctx context.Context, tx *sql.Tx, rows []ModuleRow) error {
	out := make([]Row, 0, len(rows))
	for _, row := range rows {
		d, err := Decode[Data](row.Data)
		if err != nil {
			return fmt.Errorf("player %d: %w", row.PlayerID, err)
		}
		if d == nil {
			continue
		}
		r, err := p.codec.ToRow(row.PlayerID, d)
		if err != nil {
			return fmt.Errorf("player %d: %w", row.PlayerID, err)
		}
		if r == nil {
			continue
		}
		out = append(out, *r)
	}
	if len(out) == 0 {
		return nil
	}
	return p.ops.Upsert(ctx, tx, out)
}

// Load 冷启动读行并转回业务结构；没有行等价于「从未写过」，返回 (nil, nil)。
func (p statefulPersister[Row, Data]) Load(ctx context.Context, tx *sql.Tx, playerID int64) (json.RawMessage, error) {
	row, err := p.ops.Load(ctx, tx, playerID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	if row == nil {
		return nil, nil
	}
	d, err := p.codec.FromRow(row)
	if err != nil {
		return nil, fmt.Errorf("%s: player %d: %w", p.name, playerID, err)
	}
	return Encode(d)
}
