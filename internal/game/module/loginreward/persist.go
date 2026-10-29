// persist.go —— Ext 槽位 ↔ player_login 行 的转换。
//
// 本模块没有客户端协议，走 Thin + 手工 RegisterFlat（gencodec 的 @flat
// 生成只覆盖「单结构体整体落库」的场景，这里的 load/save 需要零值语义，
// 手写更直接）。
package loginreward

import (
	"fmt"

	"game_study/pkg/model"
	"game_study/pkg/writebehind"
)

// init 注册持久化：player_login 主键为 (player_id)。
func init() {
	writebehind.RegisterFlat[model.PlayerLogin](stateKey, model.PlayerLoginFlat, validate)
}

// validate 落库前校验：天数与连续计数不能为负。
//
// 零值合法（未发放过），负数一定是数据损坏。
func validate(row *model.PlayerLogin) error {
	if row.LastDay < 0 || row.Streak < 0 {
		return fmt.Errorf("invalid login state: day=%d streak=%d", row.LastDay, row.Streak)
	}
	return nil
}

// load 读取登录状态；从未登录过返回零值（LastDay=0 不等于任何自然日序号）。
func load(state writebehind.ExtState) (*Data, error) {
	row, err := writebehind.LoadExt[model.PlayerLogin](state, stateKey)
	if err != nil {
		return nil, err
	}
	if row == nil {
		return &Data{}, nil
	}
	return &Data{LastDay: row.LastDay, Streak: row.Streak}, nil
}

// save 把发放事实写回 Ext 槽位；落库调度由 Write-Behind 负责。
func save(state writebehind.ExtState, playerID int64, d *Data) error {
	return writebehind.SaveExt(state, stateKey, &model.PlayerLogin{
		PlayerId: playerID,
		LastDay:  d.LastDay,
		Streak:   d.Streak,
	})
}
