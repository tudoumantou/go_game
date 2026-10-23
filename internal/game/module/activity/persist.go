// internal/game/module/activity/persist.go —— 活动模块的持久化适配
//
// 业务文件只关心 Data 与本模块规则；「Data 怎么变成 activity_record 表的一行」
// 集中在这里。业务代码里不再出现 json.Marshal / SetExt / model.ActivityRecord。
package activity

import (
	"encoding/json"
	"fmt"

	"game_study/pkg/model"
	"game_study/pkg/writebehind"
)

// init 注册持久化：Ext 槽位存 Data（业务结构），落库时转成 activity_record 行。
func init() {
	writebehind.RegisterStateful(stateKey, model.ActivityRecordFlat,
		writebehind.StateCodec[model.ActivityRecord, Data]{
			ToRow:   toRow,
			FromRow: fromRow,
		},
		validate,
	)
}

// newData 新玩家的空状态：map 必须初始化。
func newData() *Data { return &Data{Records: map[int32]Record{}} }

// toRow 把业务结构转成表行；活动状态整体存一个 JSON 列。
func toRow(playerID int64, d *Data) (*model.ActivityRecord, error) {
	raw, err := json.Marshal(d.Records)
	if err != nil {
		return nil, fmt.Errorf("encode activity state: %w", err)
	}
	return &model.ActivityRecord{PlayerId: playerID, Records: string(raw)}, nil
}

// fromRow 把表行还原成业务结构；空列等价于「一个活动都没参与」。
func fromRow(row *model.ActivityRecord) (*Data, error) {
	records := map[int32]Record{}
	if row.Records != "" {
		if err := json.Unmarshal([]byte(row.Records), &records); err != nil {
			return nil, fmt.Errorf("decode activity state: %w", err)
		}
	}
	if records == nil {
		records = map[int32]Record{}
	}
	return &Data{Records: records}, nil
}

// validate 落库前校验：进度非负、活动 ID 合法。
func validate(d *Data) error {
	for id, rec := range d.Records {
		if id <= 0 {
			return fmt.Errorf("invalid activity id %d", id)
		}
		if rec.Progress < 0 {
			return fmt.Errorf("invalid activity progress %d (id=%d)", rec.Progress, id)
		}
	}
	return nil
}
