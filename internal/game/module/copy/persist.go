// internal/game/module/copy/persist.go —— 副本模块的持久化适配
package copy

import (
	"encoding/json"
	"fmt"

	"game_study/pkg/model"
	"game_study/pkg/writebehind"
)

func init() {
	writebehind.RegisterStateful(stateKey, model.CopyRecordFlat,
		writebehind.StateCodec[model.CopyRecord, Data]{
			ToRow:   toRow,
			FromRow: fromRow,
		},
		validate,
	)
}

// newData 新玩家的空状态；map 非 nil 是硬约束（onSettle 会写入）。
func newData() *Data { return &Data{Records: map[int32]Record{}} }

// toRow 整份进度存一个 JSON 列。
func toRow(playerID int64, d *Data) (*model.CopyRecord, error) {
	raw, err := json.Marshal(d.Records)
	if err != nil {
		return nil, fmt.Errorf("encode copy state: %w", err)
	}
	return &model.CopyRecord{PlayerId: playerID, Records: string(raw)}, nil
}

// fromRow 冷启动读回进度；空列等价于「一次都没通关」。
func fromRow(row *model.CopyRecord) (*Data, error) {
	records := map[int32]Record{}
	if row.Records != "" {
		if err := json.Unmarshal([]byte(row.Records), &records); err != nil {
			return nil, fmt.Errorf("decode copy state: %w", err)
		}
	}
	if records == nil {
		records = map[int32]Record{}
	}
	return &Data{Records: records}, nil
}

// validate 落库前校验：星级 0-3、次数非负。
//
// 星级上界 3 是协议约定（面板只画 3 颗星），写死在校验里而非配置，
// 因为它同时约束客户端展示与服务端存储。
func validate(d *Data) error {
	for id, rec := range d.Records {
		if id <= 0 || rec.Star < 0 || rec.Star > 3 || rec.PassCount < 0 {
			return fmt.Errorf("invalid copy %d record %+v", id, rec)
		}
	}
	return nil
}
