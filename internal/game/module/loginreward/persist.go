// persist.go —— Ext 槽位 ↔ player_login 行 的转换。
//
// 业务文件只关心 Data；「Data 怎么变成 player_login 表的一行」集中在这里。
// 迁移前这里是手工 LoadExt/SaveExt + RegisterFlat（Ext 的 JSON 就是表行），
// 现在与 quest/equip 一致：Ext 存 Data，落库时经 Codec 转成行。
package loginreward

import (
	"fmt"

	"game_study/pkg/model"
	"game_study/pkg/writebehind"
)

// init 注册持久化：player_login 主键为 (player_id)。
func init() {
	writebehind.RegisterStateful(stateKey, model.PlayerLoginFlat,
		writebehind.StateCodec[model.PlayerLogin, Data]{
			ToRow:   toRow,
			FromRow: fromRow,
		},
		validate,
	)
}

// toRow 把业务结构转成表行；player_id 由落库层传入，Data 里不存冗余主键。
func toRow(playerID int64, d *Data) (*model.PlayerLogin, error) {
	return &model.PlayerLogin{
		PlayerId: playerID,
		LastDay:  d.LastDay,
		Streak:   d.Streak,
	}, nil
}

// fromRow 把表行还原成业务结构。
func fromRow(row *model.PlayerLogin) (*Data, error) {
	return &Data{LastDay: row.LastDay, Streak: row.Streak}, nil
}

// validate 落库前校验：天数与连续计数不能为负。
//
// 零值合法（未发放过），负数一定是数据损坏。
func validate(d *Data) error {
	if d.LastDay < 0 || d.Streak < 0 {
		return fmt.Errorf("invalid login state: day=%d streak=%d", d.LastDay, d.Streak)
	}
	return nil
}
