package signin

import (
	"fmt"
	"game_study/internal/game/module"
	"game_study/pkg/writebehind"
)

// Repo 是签到的 Actor 状态访问器，不持有 MySQL 或 Redis 客户端。
// 持久化由玩家统一缓存提交和独立 writebehind 进程负责。
type Repo struct{}

func NewRepo() *Repo { return &Repo{} }

type view struct {
	Start  int64
	Day    int32
	Claims map[int64][]module.ItemGrant
}

func (*Repo) Load(state module.SigninState, today int64) (*view, error) {
	data := state.SigninData()
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

func (*Repo) Claim(state module.SigninState, today int64, grants []module.ItemGrant) ([]module.ItemGrant, bool, error) {
	data := state.SigninData()
	if data == nil {
		data = &writebehind.SigninData{AnchorDay: today, Claims: make(map[int64][]module.ItemGrant)}
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
	state.SetSigninData(data)
	return grants, false, nil
}
