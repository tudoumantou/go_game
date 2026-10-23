// internal/game/module/shop/persist.go —— 商店模块的持久化适配
package shop

import (
	"encoding/json"
	"fmt"

	"game_study/pkg/model"
	"game_study/pkg/writebehind"
)

func init() {
	writebehind.RegisterStateful(stateKey, model.ShopRecordFlat,
		writebehind.StateCodec[model.ShopRecord, Data]{
			ToRow:   toRow,
			FromRow: fromRow,
		},
		validate,
	)
}

// newData 新玩家空状态；Day 由 normalize 在读取时对齐。
func newData() *Data { return &Data{Bought: map[string]int32{}} }

// toRow：Day 存独立列，购买次数存 JSON 列。
func toRow(playerID int64, d *Data) (*model.ShopRecord, error) {
	raw, err := json.Marshal(d.Bought)
	if err != nil {
		return nil, fmt.Errorf("encode shop state: %w", err)
	}
	return &model.ShopRecord{PlayerId: playerID, Day: d.Day, Bought: string(raw)}, nil
}

// fromRow 冷启动读回业务结构；空列等价于「一次都没买过」。
func fromRow(row *model.ShopRecord) (*Data, error) {
	d := newData()
	d.Day = row.Day
	if row.Bought != "" {
		if err := json.Unmarshal([]byte(row.Bought), &d.Bought); err != nil {
			return nil, fmt.Errorf("decode shop state: %w", err)
		}
	}
	if d.Bought == nil {
		d.Bought = map[string]int32{}
	}
	return d, nil
}

// validate 落库前校验：次数非负、键非空。
//
// 空键也拒绝：键格式由 goodsKey 生成，空键说明状态被外部改坏。
func validate(d *Data) error {
	for key, n := range d.Bought {
		if key == "" || n < 0 {
			return fmt.Errorf("invalid shop record %q = %d", key, n)
		}
	}
	return nil
}

// normalize 按业务日重置购买次数，返回是否有改动。
//
// 放在读取路径上是原实现的语义：跨天后第一次访问就看到 0，而不是等买家
// 触发一次写入。列表请求用 Read（不写回），下次读仍会重算，行为一致；
// 购买请求用 Mutate，改动随这次购买一起落库。
func normalize(d *Data, today int64) bool {
	if !dailyRefresh() || d.Day == today {
		return false
	}
	d.Bought = map[string]int32{}
	d.Day = today
	return true
}
