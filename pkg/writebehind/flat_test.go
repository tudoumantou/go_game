package writebehind

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
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
	if err := p.Validate(good); err != nil {
		t.Fatalf("valid state rejected: %v", err)
	}
	if err := p.Validate(json.RawMessage(`{"player_id":1}`)); err != nil {
		t.Fatalf("zero value must pass structural check: %v", err)
	}
	if err := p.Validate(json.RawMessage(`{`)); err == nil {
		t.Fatal("broken json must fail validation")
	}

	// PersistBatch：解码每行后一次性交给生成的批量 UPSERT（tx 由假实现忽略）。
	err := p.PersistBatch(context.Background(), nil, []ModuleRow{
		{PlayerID: 1, Data: good},
		{PlayerID: 2, Data: json.RawMessage(`{"player_id":2,"anchor_day":3}`)},
	})
	require.NoError(t, err)
	if len(upserted) != 2 || upserted[1].Anchor != 3 {
		t.Fatalf("upserted=%+v", upserted)
	}

	// decode 失败必须整批报错，不允许静默跳过坏玩家。
	if err := p.PersistBatch(context.Background(), nil, []ModuleRow{{PlayerID: 3, Data: json.RawMessage(`{`)}}); err == nil {
		t.Fatal("broken row must fail the batch")
	}

	// Load：命中返回 JSON；无行等价于「从未写过」，必须是 (nil, nil)。
	raw, err := p.Load(context.Background(), nil, 7)
	if err != nil || raw == nil {
		t.Fatalf("load hit: %v %v", raw, err)
	}
	var d flatRow
	if err := json.Unmarshal(raw, &d); err != nil || d.Anchor != 100 {
		t.Fatalf("%+v %v", d, err)
	}
	miss, err := p.Load(context.Background(), nil, 8)
	if err != nil || miss != nil {
		t.Fatalf("missing row must be (nil,nil): %v %v", miss, err)
	}
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
	if err := p.Validate(json.RawMessage(`{"player_id":1}`)); err == nil {
		t.Fatal("hook must reject zero anchor")
	}
	if err := p.Validate(json.RawMessage(`{"player_id":1,"anchor_day":5}`)); err != nil {
		t.Fatal(err)
	}
}

func TestRegisterFlatPanicsOnIncompleteOps(t *testing.T) {
	resetPersisters()
	t.Cleanup(resetPersisters)

	defer func() {
		if recover() == nil {
			t.Fatal("missing ops must panic")
		}
	}()
	RegisterFlat[flatRow]("flat", flush.FlatOps[flatRow]{})
}

func TestRegisterFlatDuplicatePanics(t *testing.T) {
	resetPersisters()
	t.Cleanup(resetPersisters)

	var upserted []flatRow
	RegisterFlat("flat", okOps(&upserted))
	defer func() {
		if recover() == nil {
			t.Fatal("duplicate registration must panic")
		}
	}()
	RegisterFlat("flat", okOps(&upserted))
}
