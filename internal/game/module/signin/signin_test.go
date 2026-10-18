package signin

import (
	"testing"

	"game_study/pkg/gamedata"
)

func TestRewardConfiguration(t *testing.T) {
	for day := int32(1); day <= 7; day++ {
		items, err := rewardsForDay(day, gamedata.SigninRewardCfgList)
		if err != nil || len(items) == 0 {
			t.Fatalf("day %d: %v", day, err)
		}
	}
	for _, cfg := range [][]gamedata.SigninRewardCfg{
		nil,
		{{Id: 1, Day: 1, ItemId: 999999, Count: 1}},
		{{Id: 1, Day: 1, ItemId: 1001, Count: 0}},
		{{Id: 1, Day: 1, ItemId: 1001, Count: 1 << 40}},
	} {
		if _, err := rewardsForDay(1, cfg); err == nil {
			t.Fatalf("invalid configuration accepted: %v", cfg)
		}
	}
}
