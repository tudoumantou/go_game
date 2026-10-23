// internal/game/module/skill/persist.go —— 技能模块的持久化适配
//
// 业务文件只关心 Data；「Data 怎么变成 player_skill 表的一行」集中在这里。
package skill

import (
	"encoding/json"
	"fmt"

	"game_study/pkg/model"
	"game_study/pkg/writebehind"
)

func init() {
	writebehind.RegisterStateful(stateKey, model.PlayerSkillFlat,
		writebehind.StateCodec[model.PlayerSkill, Data]{
			ToRow:   toRow,
			FromRow: fromRow,
		},
		validate,
	)
}

// newData 新玩家的空状态：map 必须初始化，nil map 写会 panic。
func newData() *Data { return &Data{Levels: map[int32]int32{}} }

// toRow 把技能等级编码成表行；整份存一个 JSON 列。
func toRow(playerID int64, d *Data) (*model.PlayerSkill, error) {
	raw, err := json.Marshal(d.Levels)
	if err != nil {
		return nil, fmt.Errorf("encode skill state: %w", err)
	}
	return &model.PlayerSkill{PlayerId: playerID, Skills: string(raw)}, nil
}

// fromRow 把表行还原成业务结构；空列等价于「一个技能都没学」。
func fromRow(row *model.PlayerSkill) (*Data, error) {
	levels := map[int32]int32{}
	if row.Skills != "" {
		if err := json.Unmarshal([]byte(row.Skills), &levels); err != nil {
			return nil, fmt.Errorf("decode skill state: %w", err)
		}
	}
	if levels == nil {
		levels = map[int32]int32{}
	}
	return &Data{Levels: levels}, nil
}

// validate 落库前校验：等级必须为正，技能 ID 合法。
//
// 等级 <=0 一律拒绝：0 级没有语义（未学习应该没有键），负数说明数据被改坏。
func validate(d *Data) error {
	for id, level := range d.Levels {
		if id <= 0 || level <= 0 {
			return fmt.Errorf("invalid skill %d level %d", id, level)
		}
	}
	return nil
}
