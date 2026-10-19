// internal/game/module/loginreward/loginreward.go —— 登录礼包（R3.2）
//
// 规则：按北京时间自然日，每天首次登录发放一次；连续登录天数对应
// login_reward.xml 的档位（col1），断签重置为 1，超出最大档按最大档发。
//
// 幂等靠状态本身：LastDay 与奖励在同一次快照 CAS 里提交，重连、换服、
// 进程重启都不会重复发。不占用 AppliedGrants —— 那是订单级业务键的地盘，
// 每天一个键会让快照无限膨胀（见 docs/R2.1-缓存原子变更契约.md）。
package loginreward

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"game_study/internal/game/module"
	"game_study/pkg/gamedata"
	"game_study/pkg/log"
	"game_study/pkg/model"
	"game_study/pkg/writebehind"

	gamev1 "game_study/pkg/pb/game/v1"
	"google.golang.org/protobuf/proto"
)

const stateKey = "login"

// businessZone 固定 UTC+8：与签到共用同一套业务日口径。
var businessZone = time.FixedZone("Asia/Shanghai", 8*60*60)

// Data 是登录礼包的私有状态。
type Data struct {
	LastDay int64 `json:"last_day"`
	Streak  int32 `json:"streak"`
}

// init 注册持久化：player_login 主键为 (player_id)。
func init() {
	writebehind.RegisterFlat[model.PlayerLogin](stateKey, model.PlayerLoginFlat, validate)
}

// validate 落库前校验：天数与连续计数不能为负。
func validate(row *model.PlayerLogin) error {
	if row.LastDay < 0 || row.Streak < 0 {
		return fmt.Errorf("invalid login state: day=%d streak=%d", row.LastDay, row.Streak)
	}
	return nil
}

// Module 登录礼包模块（无协议：登录时自动发放）。
type Module struct {
	logger *log.Logger
	now    func() time.Time
}

// New 的可选时钟用于测试跨日与断签；生产默认服务器时间。
func New(logger *log.Logger, clock ...func() time.Time) *Module {
	m := &Module{logger: logger, now: time.Now}
	if len(clock) > 0 && clock[0] != nil {
		m.now = clock[0]
	}
	return m
}

// Name 实现 module.Module。
func (m *Module) Name() string { return stateKey }

// MsgIDs 实现 module.Module：登录礼包没有客户端协议。
func (m *Module) MsgIDs() []uint32 { return nil }

// Init 启动期校验配置可用。
func (m *Module) Init(*module.Context) error {
	if len(gamedata.LoginRewardCfgList) == 0 {
		return fmt.Errorf("login_reward.xml is empty")
	}
	for _, cfg := range gamedata.LoginRewardCfgList {
		if cfg.Col1 <= 0 {
			return fmt.Errorf("invalid login reward day %d", cfg.Col1)
		}
		if _, err := parseReward(cfg.Col2); err != nil {
			return fmt.Errorf("login reward %d: %w", cfg.Id, err)
		}
	}
	return nil
}

// OnMessage 实现 module.Module：不处理任何协议。
func (m *Module) OnMessage(*module.Context, uint32, []byte) error {
	return fmt.Errorf("loginreward: no client protocol expected")
}

// OnSave 实现 module.Module：状态随快照提交。
func (m *Module) OnSave(context.Context) error { return nil }

// OnLogin 发放当日登录礼包。
//
// 重连会再次触发 OnLogin，但 LastDay 已是今天时直接返回，不会重复发奖。
func (m *Module) OnLogin(ctx *module.Context) error {
	day := businessDay(m.now())
	d, err := load(ctx.State)
	if err != nil {
		return err
	}
	if d.LastDay == day {
		return nil // 今天已经领过
	}
	if d.LastDay == day-1 {
		d.Streak++
	} else {
		d.Streak = 1 // 断签或首次登录
	}
	d.LastDay = day

	grants, err := rewardForStreak(d.Streak)
	if err != nil {
		// 配置缺失不阻断登录，但状态不前进，下次登录会重试。
		m.warn("login reward config missing", ctx, err)
		return nil
	}
	// 满包时不前进状态：下次登录会重新发放，不会吞掉当天奖励。
	if !ctx.State.CanHold(grants) {
		m.warn("login reward deferred: bag full", ctx, module.ErrBagFull)
		return nil
	}
	if err := save(ctx.State, ctx.State.PlayerID(), d); err != nil {
		return err
	}
	for _, g := range grants {
		ctx.State.AddItem(g.ItemID, g.Count)
	}
	ctx.State.MarkDirty("bag")

	if m.logger != nil {
		m.logger.Info("login reward granted",
			"player_id", ctx.State.PlayerID(), "day", day, "streak", d.Streak, "items", len(grants))
	}
	// 登录阶段客户端可能还没进场景，推送失败不影响正确性（背包以下次拉取为准）。
	if ctx.Send != nil {
		changes := make([]*gamev1.ItemChange, 0, len(grants))
		for _, g := range grants {
			changes = append(changes, &gamev1.ItemChange{
				ItemUid: int64(g.ItemID),
				ItemId:  g.ItemID,
				Count:   int32(ctx.State.GetItemCount(g.ItemID)),
				Change:  1,
			})
		}
		if raw, err := proto.Marshal(&gamev1.BagChangeNotify{Changes: changes}); err == nil {
			_ = ctx.Send(3010, raw)
		}
	}
	return nil
}

// OnLogout 实现 module.Module。
func (m *Module) OnLogout(*module.Context) error { return nil }

// rewardForStreak 取连续第 streak 天的奖励；超过最大档按最后一档发。
func rewardForStreak(streak int32) ([]module.ItemGrant, error) {
	cfgs := make([]gamedata.LoginRewardCfg, len(gamedata.LoginRewardCfgList))
	copy(cfgs, gamedata.LoginRewardCfgList)
	sort.Slice(cfgs, func(i, j int) bool { return cfgs[i].Col1 < cfgs[j].Col1 })
	if len(cfgs) == 0 {
		return nil, fmt.Errorf("login_reward.xml is empty")
	}
	pick := cfgs[len(cfgs)-1]
	for _, cfg := range cfgs {
		if cfg.Col1 >= int64(streak) {
			pick = cfg
			break
		}
	}
	g, err := parseReward(pick.Col2)
	if err != nil {
		return nil, fmt.Errorf("login reward %d: %w", pick.Id, err)
	}
	return []module.ItemGrant{g}, nil
}

// parseReward 解析 "item_id,count"。
func parseReward(s string) (module.ItemGrant, error) {
	parts := strings.Split(s, ",")
	if len(parts) != 2 {
		return module.ItemGrant{}, fmt.Errorf("expect \"item_id,count\", got %q", s)
	}
	id, err := strconv.ParseInt(strings.TrimSpace(parts[0]), 10, 32)
	if err != nil || id <= 0 {
		return module.ItemGrant{}, fmt.Errorf("invalid item id %q", parts[0])
	}
	count, err := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64)
	if err != nil || count <= 0 {
		return module.ItemGrant{}, fmt.Errorf("invalid count %q", parts[1])
	}
	if _, ok := gamedata.GetItemCfg(id); !ok {
		return module.ItemGrant{}, fmt.Errorf("item %d not found", id)
	}
	return module.ItemGrant{ItemID: int32(id), Count: count}, nil
}

// businessDay 北京时间自然日序号。
func businessDay(now time.Time) int64 { return (now.Unix() + 8*60*60) / 86400 }

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

func save(state writebehind.ExtState, playerID int64, d *Data) error {
	return writebehind.SaveExt(state, stateKey, &model.PlayerLogin{
		PlayerId: playerID,
		LastDay:  d.LastDay,
		Streak:   d.Streak,
	})
}

func (m *Module) warn(msg string, ctx *module.Context, err error) {
	if m.logger != nil {
		m.logger.Warn(msg, "player_id", ctx.State.PlayerID(), "err", err)
	}
}
