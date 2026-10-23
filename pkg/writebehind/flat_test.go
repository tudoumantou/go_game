package writebehind

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"game_study/pkg/flush"
	"github.com/stretchr/testify/require"
)

// flatRow 模拟一张玩家级单行状态表（PK = player_id）。
type flatRow struct {
	PlayerID int64 `json:"player_id"`
	Anchor   int64 `json:"anchor_day"`
}

// okOps 返回一份行为固定的读写适配；tx/querier 均不使用，便于脱离数据库测语义。
func okOps(upserted *[]flatRow) flush.FlatOps[flatRow] {
	return flush.FlatOps[flatRow]{
		Upsert: func(_ context.Context, _ *sql.Tx, rows []flatRow) error {
			*upserted = append(*upserted, rows...)
			return nil
		},
		Load: func(_ context.Context, _ flush.Querier, playerID int64) (*flatRow, error) {
			if playerID == 7 {
				return &flatRow{PlayerID: 7, Anchor: 100}, nil
			}
			return nil, sql.ErrNoRows
		},
	}
}

func TestRegisterFlatRoundTrip(t *testing.T) {
	resetPersisters()
	t.Cleanup(resetPersisters)

	var upserted []flatRow
	RegisterFlat("flat", okOps(&upserted))
	p, ok := LookupPersister("flat")
	require.True(t, ok, "persister not registered")
	require.EqualValues(t, "flat", p.Name())

	// 结构校验：合法、零值合法（结构校验不管业务语义）、损坏必须报错。
	good := json.RawMessage(`{"player_id":1,"anchor_day":20000}`)
	err := p.Validate(good)
	require.NoError(t, err, fmt.Sprintf("valid state rejected: %v", err))
	err = p.Validate(json.RawMessage(`{"player_id":1}`))
	require.NoError(t, err, fmt.Sprintf("zero value must pass structural check: %v", err))
	err = p.Validate(json.RawMessage(`{`))
	require.Error(t, err, "broken json must fail validation")

	// PersistBatch：解码每行后一次性交给生成的批量 UPSERT（tx 由假实现忽略）。
	err = p.PersistBatch(context.Background(), nil, []ModuleRow{
		{PlayerID: 1, Data: good},
		{PlayerID: 2, Data: json.RawMessage(`{"player_id":2,"anchor_day":3}`)},
	})
	require.NoError(t, err)
	require.Len(t, upserted, 2, fmt.Sprintf("upserted=%+v", upserted))
	require.EqualValues(t, 3, upserted[1].Anchor)

	// decode 失败必须整批报错，不允许静默跳过坏玩家。
	err = p.PersistBatch(context.Background(), nil, []ModuleRow{{PlayerID: 3, Data: json.RawMessage(`{`)}})
	require.Error(t, err, "broken row must fail the batch")

	// Load：命中返回 JSON；无行等价于「从未写过」，必须是 (nil, nil)。
	raw, err := p.Load(context.Background(), nil, 7)
	require.NoError(t, err, fmt.Sprintf("load hit: %v %v", raw, err))
	require.NotNil(t, raw)
	var d flatRow
	err = json.Unmarshal(raw, &d)
	require.NoError(t, err, fmt.Sprintf("%+v %v", d, err))
	require.EqualValues(t, 100, d.Anchor)
	miss, err := p.Load(context.Background(), nil, 8)
	require.NoError(t, err, fmt.Sprintf("missing row must be (nil,nil): %v %v", miss, err))
	require.Nil(t, miss)
}

func TestRegisterFlatValidateHook(t *testing.T) {
	resetPersisters()
	t.Cleanup(resetPersisters)

	var upserted []flatRow
	RegisterFlat("flat", okOps(&upserted), func(d *flatRow) error {
		if d.Anchor <= 0 {
			return errors.New("anchor must be positive")
		}
		return nil
	})
	p, _ := LookupPersister("flat")
	err := p.Validate(json.RawMessage(`{"player_id":1}`))
	require.Error(t, err, "hook must reject zero anchor")
	err = p.Validate(json.RawMessage(`{"player_id":1,"anchor_day":5}`))
	require.NoError(t, err)
}

func TestRegisterFlatPanicsOnIncompleteOps(t *testing.T) {
	resetPersisters()
	t.Cleanup(resetPersisters)

	defer func() {
		require.NotNil(t, recover(), "missing ops must panic")
	}()
	RegisterFlat[flatRow]("flat", flush.FlatOps[flatRow]{})
}

func TestRegisterFlatDuplicatePanics(t *testing.T) {
	resetPersisters()
	t.Cleanup(resetPersisters)

	var upserted []flatRow
	RegisterFlat("flat", okOps(&upserted))
	defer func() {
		require.NotNil(t, recover(), "duplicate registration must panic")
	}()
	RegisterFlat("flat", okOps(&upserted))
}
