package signin

import (
	"fmt"

	"game_study/internal/game/module"
	"game_study/pkg/writebehind"
)

// Repo 是签到的 Actor 状态访问器，不持有 MySQL 或 Redis 客户端。
// 持久化由玩家统一缓存提交和独立 writebehind 进程负责（见 persist.go）。
type Repo struct{}

func NewRepo() *Repo { return &Repo{} }

type view struct {
	Start  int64
	Day    int32
	Claims map[int64][]module.ItemGrant
}

// Load 解码当前签到状态并投影出本周期视图；从未签到过时按第一天处理。
//
// 状态读写一律走 writebehind.LoadExt / SaveExt：模块只声明依赖
// writebehind.ExtState 窄接口，不重复定义同形接口，也不手写编解码。
func (*Repo) Load(state writebehind.ExtState, today int64) (*view, error) {
	data, err := writebehind.LoadExt[Data](state, stateKey)
	if err != nil {
		return nil, err
	}
	if data == nil {
		return &view{Start: today, Day: 1, Claims: make(map[int64][]module.ItemGrant)}, nil
	}
	start, day := cycleFor(data.AnchorDay, today)
	if day == 0 {
		return nil, fmt.Errorf("business date precedes signin anchor")
	}
	claims := make(map[int64][]module.ItemGrant)
	for date, items := range data.Claims {
		if date >= start && date < start+7 {
			claims[date] = items
		}
	}
	return &view{Start: start, Day: day, Claims: claims}, nil
}

// Claim 只在内存里登记领取事实；返回 already=true 时 grants 是持久化的历史快照。
func (*Repo) Claim(state writebehind.ExtState, today int64, grants []module.ItemGrant) ([]module.ItemGrant, bool, error) {
	data, err := writebehind.LoadExt[Data](state, stateKey)
	if err != nil {
		return nil, false, err
	}
	if data == nil {
		data = &Data{AnchorDay: today, Claims: make(map[int64][]module.ItemGrant)}
	}
	if today < data.AnchorDay {
		return nil, false, fmt.Errorf("business date precedes signin anchor")
	}
	if stored, ok := data.Claims[today]; ok {
		return stored, true, nil
	}
	if data.Claims == nil {
		data.Claims = make(map[int64][]module.ItemGrant)
	}
	data.Claims[today] = append([]module.ItemGrant(nil), grants...)
	if err := writebehind.SaveExt(state, stateKey, data); err != nil {
		return nil, false, err
	}
	return grants, false, nil
}
