package signin

import (
	"encoding/json"
	"fmt"

	"game_study/internal/game/module"
	"game_study/pkg/writebehind"
)

// stateAccess 是签到需要的最小状态能力。
//
// 刻意不依赖整个 module.PlayerState：模块只用到「读自己那段、写自己那段」，
// 窄接口让单测可以用几行内存实现，也约束模块不去碰别人的数据。
type stateAccess interface {
	Ext(name string) json.RawMessage
	SetExt(name string, raw json.RawMessage)
}

// Repo 是签到的 Actor 状态访问器，不持有 MySQL 或 Redis 客户端。
// 持久化由玩家统一缓存提交和独立 writebehind 进程负责（见 persist.go）。
type Repo struct{}

func NewRepo() *Repo { return &Repo{} }

type view struct {
	Start  int64
	Day    int32
	Claims map[int64][]module.ItemGrant
}

// read 解码当前签到状态；从未签到过时返回 nil。
func read(state stateAccess) (*Data, error) {
	return writebehind.Decode[Data](state.Ext(stateKey))
}

// write 把签到状态写回模块私有槽位（会标脏并触发落库）。
func write(state stateAccess, d *Data) error {
	raw, err := writebehind.Encode(d)
	if err != nil {
		return err
	}
	state.SetExt(stateKey, raw)
	return nil
}

func (*Repo) Load(state stateAccess, today int64) (*view, error) {
	data, err := read(state)
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
func (*Repo) Claim(state stateAccess, today int64, grants []module.ItemGrant) ([]module.ItemGrant, bool, error) {
	data, err := read(state)
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
	if err := write(state, data); err != nil {
		return nil, false, err
	}
	return grants, false, nil
}
