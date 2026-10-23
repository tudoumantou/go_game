// internal/game/module/quest/persist.go —— 任务模块的持久化适配
//
// 业务文件（quest.go）只关心 Data 与本模块规则；「Data 怎么变成
// player_quest 表的一行」集中在这里。这是 R2.6 的落点：业务代码里不再出现
// json.Marshal / SetExt / model.PlayerQuest 这些词。
package quest

import (
	"encoding/json"
	"fmt"

	"game_study/pkg/model"
	"game_study/pkg/writebehind"
)

// init 注册持久化：Ext 槽位存 Data（业务结构），落库时转成 player_quest 行。
func init() {
	writebehind.RegisterStateful(stateKey, model.PlayerQuestFlat,
		writebehind.StateCodec[model.PlayerQuest, Data]{
			ToRow:   toRow,
			FromRow: fromRow,
		},
		validate,
	)
}

// toRow 把业务结构转成表行；任务状态整体存一个 JSON 列。
func toRow(playerID int64, d *Data) (*model.PlayerQuest, error) {
	raw, err := json.Marshal(d.Quests)
	if err != nil {
		return nil, fmt.Errorf("encode quest state: %w", err)
	}
	return &model.PlayerQuest{PlayerId: playerID, Quests: string(raw)}, nil
}

// fromRow 把表行还原成业务结构；空列等价于「一个任务都没接」。
func fromRow(row *model.PlayerQuest) (*Data, error) {
	quests := map[int32]Record{}
	if row.Quests != "" {
		if err := json.Unmarshal([]byte(row.Quests), &quests); err != nil {
			return nil, fmt.Errorf("decode quest state: %w", err)
		}
	}
	if quests == nil {
		quests = map[int32]Record{}
	}
	return &Data{Quests: quests}, nil
}

// validate 落库前校验状态取值与进度范围，坏数据整批回滚。
//
// Status 上界是 statusClaimed：超范围说明快照被改坏，
// 宁可这批写回失败，也不把非法状态带进数据库。
func validate(d *Data) error {
	for id, rec := range d.Quests {
		if id <= 0 {
			return fmt.Errorf("invalid quest id %d", id)
		}
		if rec.Status < statusDoing || rec.Status > statusClaimed {
			return fmt.Errorf("invalid quest status %d", rec.Status)
		}
		if rec.Progress < 0 {
			return fmt.Errorf("invalid quest progress %d", rec.Progress)
		}
	}
	return nil
}
