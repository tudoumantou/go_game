package signin

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"game_study/pkg/model"
	"game_study/pkg/writebehind"
)

// stateKey 同时是 Snapshot.Ext 的键与落库归属的模块名。
const stateKey = "signin"

// Data 是签到的玩家私有状态：周期锚点 + 每个业务日的领取快照。
//
// JSON 字段与旧版内建结构完全一致，因此 Redis 中旧快照可以直接迁移进
// Ext["signin"]，升级不需要停机刷新热数据。
type Data struct {
	AnchorDay int64                             `json:"anchor_day"`
	Claims    map[int64][]writebehind.ItemGrant `json:"claims"`
}

// persister 把签到状态接入统一的快照、Redis 提交与独立落库。
type persister struct{}

// init 注册扩展点：只要 signin 包被业务代码导入，冷启动读取与落库就同时生效，
// 不会出现「协议已注册但数据不落库」的半接入状态。
func init() { writebehind.RegisterPersister(persister{}) }

func (persister) Name() string { return stateKey }

func (persister) Validate(raw json.RawMessage) error {
	d, err := writebehind.Decode[Data](raw)
	if err != nil {
		return fmt.Errorf("decode signin state: %w", err)
	}
	if d == nil {
		return fmt.Errorf("empty signin state")
	}
	if d.AnchorDay <= 0 {
		return fmt.Errorf("invalid signin anchor %d", d.AnchorDay)
	}
	for day, rewards := range d.Claims {
		if day < d.AnchorDay || len(rewards) == 0 {
			return fmt.Errorf("invalid signin day %d", day)
		}
		for _, g := range rewards {
			if g.ItemID <= 0 || g.Count <= 0 {
				return fmt.Errorf("invalid signin reward on day %d", day)
			}
		}
	}
	return nil
}

// PersistBatch 在一个事务内写入本批玩家的锚点与领取流水。
//
// 表名、列名、占位符全部由 pkg/model 的生成代码提供，这里只做业务字段组装。
func (persister) PersistBatch(ctx context.Context, tx *sql.Tx, rows []writebehind.ModuleRow) error {
	anchors := make([]model.PlayerSignin, 0, len(rows))
	claims := make([]model.SigninClaim, 0, len(rows))
	for _, row := range rows {
		d, err := writebehind.Decode[Data](row.Data)
		if err != nil {
			return fmt.Errorf("player %d: %w", row.PlayerID, err)
		}
		if d == nil {
			continue
		}
		anchors = append(anchors, model.PlayerSignin{PlayerId: row.PlayerID, AnchorDay: d.AnchorDay})
		claims = append(claims, d.rows(row.PlayerID)...)
	}
	if err := model.UpsertPlayerSigninBatch(ctx, tx, anchors); err != nil {
		return err
	}
	return model.UpsertSigninClaimBatch(ctx, tx, claims)
}

// rows 把本玩家的领取记录摊平成表行，按日期升序保证 SQL 可复现。
func (d *Data) rows(playerID int64) []model.SigninClaim {
	days := make([]int64, 0, len(d.Claims))
	for day := range d.Claims {
		days = append(days, day)
	}
	sort.Slice(days, func(i, j int) bool { return days[i] < days[j] })
	out := make([]model.SigninClaim, 0, len(days))
	for _, day := range days {
		raw, err := json.Marshal(d.Claims[day])
		if err != nil {
			continue // 奖励快照一定能编码（结构固定），失败说明数据已被破坏，跳过交由校验拦截
		}
		start, _ := cycleFor(d.AnchorDay, day)
		out = append(out, model.SigninClaim{
			PlayerId:      playerID,
			SignDay:       day,
			CycleStartDay: start,
			DayIndex:      int32(day - start + 1),
			Rewards:       string(raw),
		})
	}
	return out
}

// Load 冷启动读取：没有锚点行说明该角色从未签到。
func (persister) Load(ctx context.Context, tx *sql.Tx, playerID int64) (json.RawMessage, error) {
	anchor, err := model.FindPlayerSigninByPK(ctx, tx, playerID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	claims, err := model.ListSigninClaimByPlayer(ctx, tx, playerID)
	if err != nil {
		return nil, err
	}
	data := Data{AnchorDay: anchor.AnchorDay, Claims: make(map[int64][]writebehind.ItemGrant, len(claims))}
	for _, c := range claims {
		var items []writebehind.ItemGrant
		if err := json.Unmarshal([]byte(c.Rewards), &items); err != nil {
			return nil, fmt.Errorf("decode signin rewards on day %d: %w", c.SignDay, err)
		}
		data.Claims[c.SignDay] = items
	}
	return writebehind.Encode(&data)
}
