// internal/game/module/signin/persist.go —— 签到的持久化适配
//
// 业务文件（signin.go）只关心 SignIn 结构与本周期规则；「SignIn 怎么变成
// player_signin 表的一行」集中在这里。这是 R2.6「业务只处理内存」的落点：
// 业务代码里不出现 json.Marshal、SetExt、model.PlayerSignin 这些词。
package signin

import (
	"encoding/json"
	"fmt"

	"game_study/pkg/model"
	"game_study/pkg/writebehind"
)

// init 注册持久化：Ext 槽位存 SignIn（业务结构），落库时转成 player_signin 行。
func init() {
	writebehind.RegisterStateful(stateKey, model.PlayerSigninFlat,
		writebehind.StateCodec[model.PlayerSignin, SignIn]{
			ToRow:   toRow,
			FromRow: fromRow,
		},
		validate,
	)
}

// toRow 把业务结构转成表行。
//
// 表里 days/rewards 各存一个 JSON 字符串列（便于 SQL 侧做局部审计），
// 所以这里要再序列化一次；两者都是小结构，成本可忽略。
func toRow(playerID int64, d *SignIn) (*model.PlayerSignin, error) {
	days, err := json.Marshal(d.Days)
	if err != nil {
		return nil, fmt.Errorf("encode signin days: %w", err)
	}
	rewards, err := json.Marshal(d.Rewards)
	if err != nil {
		return nil, fmt.Errorf("encode signin rewards: %w", err)
	}
	return &model.PlayerSignin{
		PlayerId:       playerID,
		ClaimedDays:    string(days),
		ClaimedRewards: string(rewards),
	}, nil
}

// fromRow 把表行还原成业务结构；空列按「尚未领取」处理。
func fromRow(row *model.PlayerSignin) (*SignIn, error) {
	days := []int32{}
	if row.ClaimedDays != "" {
		if err := json.Unmarshal([]byte(row.ClaimedDays), &days); err != nil {
			return nil, fmt.Errorf("decode signin days: %w", err)
		}
	}
	rewards := map[int32]int64{}
	if row.ClaimedRewards != "" {
		if err := json.Unmarshal([]byte(row.ClaimedRewards), &rewards); err != nil {
			return nil, fmt.Errorf("decode signin rewards: %w", err)
		}
	}
	return &SignIn{Days: days, Rewards: rewards}, nil
}

// validate 落库前校验：天数在 1-7，奖励数量为正，坏数据整批回滚。
//
// 天数上界 weekDays 同时约束「周序号」语义——8 号不存在于任何一周。
func validate(d *SignIn) error {
	for _, day := range d.Days {
		if day < 1 || day > weekDays {
			return fmt.Errorf("invalid signin day %d", day)
		}
	}
	for id, count := range d.Rewards {
		if id <= 0 || count <= 0 {
			return fmt.Errorf("invalid signin reward %d x %d", id, count)
		}
	}
	return nil
}
