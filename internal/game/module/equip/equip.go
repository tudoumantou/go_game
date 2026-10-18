// internal/game/module/equip/equip.go —— 装备模块（3101-3106）
package equip

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"game_study/internal/game/module"
	"game_study/pkg/cache"
	"game_study/pkg/db"
	"game_study/pkg/gamedata"
	"game_study/pkg/log"

	gamev1 "game_study/pkg/pb/game/v1"
	"google.golang.org/protobuf/proto"
)

// 协议号（4.9：3101/3102 列表，3103/3104 穿戴，3105/3106 脱下）。
const (
	msgIDListReq    uint32 = 3101
	msgIDListResp   uint32 = 3102
	msgIDWearReq    uint32 = 3103
	msgIDWearResp   uint32 = 3104
	msgIDTakeOffReq uint32 = 3105
	msgIDTakeOffResp uint32 = 3106
)

// 业务码（4.10）。
const (
	codeOK          int32 = 0
	codeNotExist    int32 = 3101
	codeLevelLow    int32 = 3102
	codeSlotConflict int32 = 3103
	codeNotEquipped int32 = 3104
)

// ErrNotExist 装备实例不存在。
var ErrNotExist = errors.New("equip not exist")

// Entry 装备实例。
type Entry struct {
	UID          int64
	EquipID      int32
	EnhanceLevel int32
	Quality      int32
	Equipped     bool
}

// Repo 装备仓储。
type Repo struct {
	db      *db.DB
	idTable *cache.IDTable // 可选：按实例 uid 查库前先判存在性（12.5）
}

// NewRepo 创建仓储。
func NewRepo(database *db.DB) *Repo {
	return &Repo{db: database}
}

// SetIDTable 注入 ID 表（12.5）。
//
// 价值：穿戴/脱下按客户端上报的实例 uid 查库，伪造或过期的 uid 在 Redis 一次
// SISMEMBER 就能判定不存在，不必打一次库。真实存在的 uid 仍走原查询（校验归属）。
func (r *Repo) SetIDTable(t *cache.IDTable) { r.idTable = t }

// ListByPlayer 查询玩家装备。
func (r *Repo) ListByPlayer(ctx context.Context, playerID int64) ([]Entry, error) {
	const q = `SELECT id, equip_id, enhance_level, quality, equipped
	           FROM player_equip WHERE player_id = ? ORDER BY id`
	rows, err := r.db.QueryContext(ctx, q, playerID)
	if err != nil {
		return nil, fmt.Errorf("list player_equip: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []Entry
	for rows.Next() {
		var e Entry
		var eq int32
		if err := rows.Scan(&e.UID, &e.EquipID, &e.EnhanceLevel, &e.Quality, &eq); err != nil {
			return nil, err
		}
		e.Equipped = eq == 1
		out = append(out, e)
	}
	return out, rows.Err()
}

// Find 按实例 ID 查询（校验归属）。
//
// 这里**不能**用 idtab 做前置存在性判断：登录重算属性、GM 修复数据等路径会读到
// idtab 尚未登记的行（集合靠启动构建 + 增量登记维护，直接写库的记录要等 10 分钟对账），
// 一旦误判「不存在」，玩家会看到属性少算——比多打一次库严重得多。
// 需要对客户端上报的 uid 做廉价校验时用 Exists()。
func (r *Repo) Find(ctx context.Context, playerID, uid int64) (*Entry, error) {
	const q = `SELECT id, equip_id, enhance_level, quality, equipped
	           FROM player_equip WHERE id = ? AND player_id = ?`
	var e Entry
	var eq int32
	err := r.db.QueryRowContext(ctx, q, uid, playerID).Scan(&e.UID, &e.EquipID, &e.EnhanceLevel, &e.Quality, &eq)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotExist
	}
	if err != nil {
		return nil, err
	}
	e.Equipped = eq == 1
	return &e, nil
}

// CountEquippedSlot 统计某部位已穿数量（部位由配置 Col3 标识）。
func (r *Repo) CountEquippedSlot(ctx context.Context, playerID int64, equipIDs []int32) (int, error) {
	if len(equipIDs) == 0 {
		return 0, nil
	}
	q := `SELECT COUNT(*) FROM player_equip WHERE player_id = ? AND equipped = 1 AND equip_id IN (`
	args := []any{playerID}
	for i, id := range equipIDs {
		if i > 0 {
			q += ","
		}
		q += "?"
		args = append(args, id)
	}
	q += ")"
	var n int
	if err := r.db.QueryRowContext(ctx, q, args...).Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}

// Insert 插入装备实例（equipped=1）。
func (r *Repo) Insert(ctx context.Context, playerID int64, equipID int32) (int64, error) {
	const q = `INSERT INTO player_equip (player_id, equip_id, enhance_level, quality, equipped)
	           VALUES (?, ?, 0, 0, 1)`
	res, err := r.db.ExecContext(ctx, q, playerID, equipID)
	if err != nil {
		return 0, fmt.Errorf("insert player_equip: %w", err)
	}
	uid, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	// 登记 idtab：否则到下轮对账（10 分钟）之前，集合里都没有这个实例。
	// 失败只影响存在性判断的加速效果，不影响业务，忽略。
	if r.idTable != nil {
		_ = r.idTable.Add(ctx, "player_equip", uid)
	}
	return uid, nil
}

// MarkEquipped 设置实例的穿戴状态。
func (r *Repo) MarkEquipped(ctx context.Context, playerID, uid int64, equipped bool) error {
	v := 0
	if equipped {
		v = 1
	}
	const q = `UPDATE player_equip SET equipped = ? WHERE id = ? AND player_id = ?`
	if _, err := r.db.ExecContext(ctx, q, v, uid, playerID); err != nil {
		return fmt.Errorf("mark equipped: %w", err)
	}
	return nil
}

// Delete 删除装备实例。
func (r *Repo) Delete(ctx context.Context, playerID, uid int64) error {
	const q = `DELETE FROM player_equip WHERE id = ? AND player_id = ?`
	_, err := r.db.ExecContext(ctx, q, uid, playerID)
	return err
}

// Module 装备模块。
type Module struct {
	repo   *Repo
	logger *log.Logger
}

// New 创建装备模块。
func New(repo *Repo, logger *log.Logger) *Module {
	return &Module{repo: repo, logger: logger}
}

// Name 实现 module.Module。
func (m *Module) Name() string { return "equip" }

// MsgIDs 实现 module.Module。
func (m *Module) MsgIDs() []uint32 { return []uint32{msgIDListReq, msgIDWearReq, msgIDTakeOffReq} }

// Init 实现 module.Module。
func (m *Module) Init(ctx *module.Context) error { return nil }

// OnMessage 实现 module.Module。
func (m *Module) OnMessage(ctx *module.Context, msgID uint32, body []byte) error {
	switch msgID {
	case msgIDListReq:
		return m.onList(ctx)
	case msgIDWearReq:
		return m.onWear(ctx, body)
	case msgIDTakeOffReq:
		return m.onTakeOff(ctx, body)
	default:
		return fmt.Errorf("equip: unexpected msg %d", msgID)
	}
}

// OnSave 实现 module.Module。
func (m *Module) OnSave(ctx context.Context) error { return nil }

// OnLogin 实现 module.Module：按已穿装备重算属性加成。
//
// P12：优先用登录预加载的装备快照（省一次查询）；未预取时才回源 DB。
func (m *Module) OnLogin(ctx *module.Context) error {
	var equippedIDs []int32

	if ctx.Prefetch != nil {
		for _, e := range ctx.Prefetch.Equips() {
			if e.Equipped {
				equippedIDs = append(equippedIDs, e.EquipID)
			}
		}
	} else if m.repo != nil {
		list, err := m.repo.ListByPlayer(context.Background(), ctx.State.PlayerID())
		if err != nil {
			return err
		}
		for _, e := range list {
			if e.Equipped {
				equippedIDs = append(equippedIDs, e.EquipID)
			}
		}
	}

	ctx.State.ClearBonus()
	for _, equipID := range equippedIDs {
		cfg, ok := gamedata.GetEquipCfg(int64(equipID))
		if !ok {
			continue
		}
		ctx.State.AddBonus("atk", cfg.Col4)
	}
	return nil
}

// OnLogout 实现 module.Module。
func (m *Module) OnLogout(ctx *module.Context) error {
	ctx.State.MarkDirty(m.Name())
	return nil
}

func (m *Module) onList(ctx *module.Context) error {
	resp := &gamev1.EquipListResp{Code: codeOK}
	if m.repo == nil {
		return m.reply(ctx, msgIDListResp, resp)
	}
	list, err := m.repo.ListByPlayer(context.Background(), ctx.State.PlayerID())
	if err != nil {
		return err
	}
	for _, e := range list {
		resp.Equips = append(resp.Equips, &gamev1.EquipEntry{
			Uid:          e.UID,
			EquipId:      e.EquipID,
			EnhanceLevel: e.EnhanceLevel,
			Quality:      e.Quality,
			Equipped:     e.Equipped,
		})
	}
	return m.reply(ctx, msgIDListResp, resp)
}

func (m *Module) onWear(ctx *module.Context, body []byte) error {
	var req gamev1.EquipWearReq
	if err := proto.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("decode EquipWearReq: %w", err)
	}
	resp := &gamev1.EquipWearResp{}
	if m.repo == nil {
		resp.Code = codeNotExist
		return m.reply(ctx, msgIDWearResp, resp)
	}

	// 按实例 UID 穿戴（与 EquipList 下发的 uid 一致）
	//
	// 这里刻意**不**用 idtab 前置判断：装备行由 GM 工具/补偿脚本直接写库时，
	// idtab 要等 10 分钟对账才包含新 uid，拿它判存在会让玩家穿不上装备。
	// 穿戴是低频操作，多打一次库的代价可以忽略。
	e, err := m.repo.Find(context.Background(), ctx.State.PlayerID(), req.Uid)
	if err != nil {
		resp.Code = codeNotExist
		return m.reply(ctx, msgIDWearResp, resp)
	}
	if e.Equipped {
		resp.Code = codeOK
		resp.Attrs = attrPairs(ctx)
		return m.reply(ctx, msgIDWearResp, resp)
	}

	cfg, ok := gamedata.GetEquipCfg(int64(e.EquipID))
	if !ok {
		resp.Code = codeNotExist
		return m.reply(ctx, msgIDWearResp, resp)
	}
	if ctx.State.Profile().Level < int32(cfg.Col2) {
		resp.Code = codeLevelLow
		return m.reply(ctx, msgIDWearResp, resp)
	}

	// 同部位冲突检查（Col3=部位）
	sameSlot := sameSlotEquipIDs(cfg.Col3)
	n, err := m.repo.CountEquippedSlot(context.Background(), ctx.State.PlayerID(), sameSlot)
	if err != nil {
		return err
	}
	if n > 0 {
		resp.Code = codeSlotConflict
		return m.reply(ctx, msgIDWearResp, resp)
	}
	if err := m.repo.MarkEquipped(context.Background(), ctx.State.PlayerID(), e.UID, true); err != nil {
		return err
	}

	// 加属性
	ctx.State.AddBonus("atk", cfg.Col4)
	ctx.State.MarkDirty(m.Name())

	resp.Code = codeOK
	resp.Attrs = attrPairs(ctx)
	return m.reply(ctx, msgIDWearResp, resp)
}

func (m *Module) onTakeOff(ctx *module.Context, body []byte) error {
	var req gamev1.EquipTakeOffReq
	if err := proto.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("decode EquipTakeOffReq: %w", err)
	}
	resp := &gamev1.EquipTakeOffResp{}
	if m.repo == nil {
		resp.Code = codeNotExist
		return m.reply(ctx, msgIDTakeOffResp, resp)
	}
	e, err := m.repo.Find(context.Background(), ctx.State.PlayerID(), req.Uid)
	if err != nil {
		resp.Code = codeNotExist
		return m.reply(ctx, msgIDTakeOffResp, resp)
	}
	if !e.Equipped {
		resp.Code = codeNotEquipped
		return m.reply(ctx, msgIDTakeOffResp, resp)
	}
	cfg, ok := gamedata.GetEquipCfg(int64(e.EquipID))
	if !ok {
		resp.Code = codeNotExist
		return m.reply(ctx, msgIDTakeOffResp, resp)
	}
	if err := m.repo.MarkEquipped(context.Background(), ctx.State.PlayerID(), req.Uid, false); err != nil {
		return err
	}
	// 扣属性（装备仍在 player_equip 中，仅置 equipped=0）
	ctx.State.AddBonus("atk", -cfg.Col4)
	ctx.State.MarkDirty(m.Name())

	resp.Code = codeOK
	resp.Attrs = attrPairs(ctx)
	return m.reply(ctx, msgIDTakeOffResp, resp)
}

// sameSlotEquipIDs 返回同部位的全部装备配置 ID。
func sameSlotEquipIDs(slot string) []int32 {
	var out []int32
	for _, cfg := range gamedata.EquipCfgList {
		if cfg.Col3 == slot {
			out = append(out, int32(cfg.Id))
		}
	}
	return out
}

func attrPairs(ctx *module.Context) []*gamev1.AttrPair {
	return []*gamev1.AttrPair{
		{AttrId: 1, Value: ctx.State.GetAttr("hp")},
		{AttrId: 2, Value: ctx.State.GetAttr("mp")},
		{AttrId: 3, Value: ctx.State.GetAttr("atk")},
		{AttrId: 4, Value: ctx.State.GetAttr("def")},
		{AttrId: 5, Value: ctx.State.GetAttr("spd")},
	}
}

func (m *Module) reply(ctx *module.Context, msgID uint32, msg proto.Message) error {
	body, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	return ctx.Reply(msgID, body)
}
