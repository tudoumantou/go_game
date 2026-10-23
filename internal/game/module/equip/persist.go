// internal/game/module/equip/persist.go —— 装备模块的持久化适配
package equip

import (
	"encoding/json"
	"fmt"

	"game_study/pkg/model"
	"game_study/pkg/writebehind"
)

func init() {
	writebehind.RegisterStateful(stateKey, model.PlayerEquipFlat,
		writebehind.StateCodec[model.PlayerEquip, Data]{
			ToRow:   toRow,
			FromRow: fromRow,
		},
		validate,
	)
}

// newData 新玩家的空状态；Equips 给空切片，JSON 序列化才是 [] 而不是 null。
func newData() *Data { return &Data{Equips: []Entry{}} }

// toRow：装备列表整体存一个 JSON 数组列（与 schema 注释一致）。
func toRow(playerID int64, d *Data) (*model.PlayerEquip, error) {
	raw, err := json.Marshal(d.Equips)
	if err != nil {
		return nil, fmt.Errorf("encode equip state: %w", err)
	}
	return &model.PlayerEquip{PlayerId: playerID, Equips: string(raw)}, nil
}

// fromRow 冷启动读回装备列表；空列等价于「一件都没有」。
func fromRow(row *model.PlayerEquip) (*Data, error) {
	d := newData()
	if row.Equips != "" {
		if err := json.Unmarshal([]byte(row.Equips), &d.Equips); err != nil {
			return nil, fmt.Errorf("decode equip state: %w", err)
		}
	}
	if d.Equips == nil {
		d.Equips = []Entry{}
	}
	return d, nil
}

// validate 落库前校验：UID 唯一、强化等级非负。
//
// 这是快照落库前的最后一道闸：校验失败会让整批写回失败（宁可丢一批
// 也不写坏数据），因此只查「结构性不变量」，不查业务规则。
func validate(d *Data) error {
	seen := make(map[int64]bool, len(d.Equips))
	for _, e := range d.Equips {
		if e.UID <= 0 {
			return fmt.Errorf("invalid equip uid %d", e.UID)
		}
		if e.EnhanceLevel < 0 {
			return fmt.Errorf("invalid enhance level %d (uid=%d)", e.EnhanceLevel, e.UID)
		}
		if seen[e.UID] {
			return fmt.Errorf("duplicate equip uid %d", e.UID)
		}
		seen[e.UID] = true
	}
	return nil
}
