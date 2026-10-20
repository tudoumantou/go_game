package store

// object.go 负责 map_object 的读写与 payload 编解码。
//
// 设计文档 14.1 的硬性约束：payload 按 kind 分三种类型化 schema，
// 不允许 map[string]any 混写。原因是混写会让「字段是否必填」既无法在
// 编译期校验，也无法在 SQL 侧建立稳定索引；一旦某条记录缺字段，
// 只能在运行时 panic 或静默用零值——两种都是数据事故。
//
// 为什么本包定义 cityPayload/troopPayload/resourcePayload 镜像结构
// 而不是直接 json.Marshal(domain.City)：
//   - domain 结构没有 json tag，默认编码会得到 "Header"/"PlayerID"
//     这类 Go 字段名，等于把 Go 的命名习惯固化进数据库 schema；
//   - 镜像结构让「落库的字段名」成为 store 包内的显式契约，
//     domain 加字段时也不会悄悄改变已存 JSON 的形状；
//   - 任务书禁止修改 domain，镜像+双向转换是最小侵入的做法。
//
// 代价是每加一个字段要改两处。这是有意接受的：JSON 列的兼容性
// 本来就该显式管理（schema_version 就是为它准备的）。

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"game_study/internal/worldmap/domain"
)

// Execer 是「能执行语句」的最小接口。
//
// 用接口而不是直接写 *sql.Tx：AllocObjectID 既要在事务里调用
// （提交路径），也要能在事务外单独分配（工具/测试/预热）。
// *sql.DB 与 *sql.Tx 都实现了它，Go 的结构化类型让调用方无需适配。
//
// 知识点：接口应该由**使用方**定义并且尽量小（io.Writer 只有一个方法）。
// 这里只要 ExecContext，比直接依赖 *sql.DB 的庞大方法集更灵活。
type Execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// ---------- payload JSON 契约 ----------

// payloadEnvelope 是 map_object.payload 的统一外壳。
//
// 三个指针字段按 kind 只填一个：kind=1 填 City，2 填 Troop，3 填 Resource。
// 用指针而不是值是为了让「没填」在 JSON 里表现为 null，
// 解码时能明确区分「这个 kind 不该有该字段」而不是「字段是零值」。
type payloadEnvelope struct {
	Kind     int32            `json:"kind"`
	City     *cityPayload     `json:"city,omitempty"`
	Troop    *troopPayload    `json:"troop,omitempty"`
	Resource *resourcePayload `json:"resource,omitempty"`
}

// headerJSON 是 ObjectHeader 的落库形态。
// 冗余存 world_id：payload 单独被导出时仍然自解释，代价是每行几十字节。
type headerJSON struct {
	WorldID       uint64 `json:"world_id"`
	ObjectID      uint64 `json:"object_id"`
	Kind          int32  `json:"kind"`
	OwnerScene    int32  `json:"owner_scene"`
	Version       uint64 `json:"version"`
	SchemaVersion uint32 `json:"schema_version"`
	Deleted       bool   `json:"deleted"`
}

type posJSON struct {
	X int64 `json:"x"`
	Y int64 `json:"y"`
}

// cityPayload 对应 kind=CITY 的 payload。
type cityPayload struct {
	Header        headerJSON `json:"header"`
	PlayerID      int64      `json:"player_id"`
	GuildID       int64      `json:"guild_id"`
	Position      posJSON    `json:"position"`
	ConfigVersion string     `json:"config_version"`
}

type busyJSON struct {
	Reason     int32  `json:"reason"`
	OpID       string `json:"op_id"`
	SinceMS    int64  `json:"since_ms"`
	DeadlineMS int64  `json:"deadline_ms"`
}

type targetJSON struct {
	TargetID uint64 `json:"target_id"`
	Intent   int32  `json:"intent"`
}

type moveJSON struct {
	MoveID           string    `json:"move_id"`
	From             posJSON   `json:"from"`
	To               posJSON   `json:"to"`
	StartAtMS        int64     `json:"start_at_ms"`
	EndAtMS          int64     `json:"end_at_ms"`
	SpeedUnitsPerSec int64     `json:"speed_units_per_sec"`
	Path             []posJSON `json:"path,omitempty"`
	SegmentIndex     int32     `json:"segment_index"`
	PathVersion      uint64    `json:"path_version"`
}

type combatJSON struct {
	ConfigID    int32  `json:"config_id"`
	Level       int32  `json:"level"`
	Attack      int64  `json:"attack"`
	Defense     int64  `json:"defense"`
	HP          int64  `json:"hp"`
	Speed       int64  `json:"speed"`
	SnapshotMS  int64  `json:"snapshot_ms"`
	RuleVersion string `json:"rule_version"`
}

type deferredJSON struct {
	Kind      int32  `json:"kind"`
	RequestID string `json:"request_id"`
}

// troopPayload 对应 kind=TROOP 的 payload。
//
// Cargo 用 map[string]int64 而不是 map[int32]int64：JSON 的对象键
// 只能是字符串，直接编码 map[int32] 在标准库里能工作（键被转成十进制
// 字符串），但反过来解码会失败。显式用字符串键并在转换层做
// strconv，双向都可控。
type troopPayload struct {
	Header        headerJSON       `json:"header"`
	PlayerID      int64            `json:"player_id"`
	SlotID        int32            `json:"slot_id"`
	HomeCityID    uint64           `json:"home_city_id"`
	Phase         int32            `json:"phase"`
	Busy          busyJSON         `json:"busy"`
	Target        targetJSON       `json:"target"`
	Move          *moveJSON        `json:"move,omitempty"`
	Cargo         map[string]int64 `json:"cargo,omitempty"`
	CapacityUnits int64            `json:"capacity_units"`
	Stats         combatJSON       `json:"stats"`
	Deferred      *deferredJSON    `json:"deferred,omitempty"`
	DispatchID    string           `json:"dispatch_id,omitempty"`
	SettlementID  string           `json:"settlement_id,omitempty"`
	BattleID      string           `json:"battle_id,omitempty"`
}

// resourcePayload 对应 kind=RESOURCE 的 payload。
type resourcePayload struct {
	Header            headerJSON `json:"header"`
	Position          posJSON    `json:"position"`
	RichBlockID       int64      `json:"rich_block_id"`
	ResourceType      int32      `json:"resource_type"`
	ItemID            int32      `json:"item_id"`
	Total             int64      `json:"total"`
	Remaining         int64      `json:"remaining"`
	ActiveCollectID   string     `json:"active_collect_id,omitempty"`
	RefreshGeneration int64      `json:"refresh_generation"`
	PendingDestroy    bool       `json:"pending_destroy"`
	ConfigVersion     string     `json:"config_version"`
}

// ---------- domain → JSON ----------

func headerToJSON(h domain.ObjectHeader) headerJSON {
	return headerJSON{
		WorldID:       uint64(h.WorldID),
		ObjectID:      uint64(h.ID),
		Kind:          int32(h.Kind),
		OwnerScene:    int32(h.OwnerScene),
		Version:       h.Version,
		SchemaVersion: h.SchemaVersion,
		Deleted:       h.Deleted,
	}
}

func headerFromJSON(h headerJSON) domain.ObjectHeader {
	return domain.ObjectHeader{
		WorldID:       domain.WorldID(h.WorldID),
		ID:            domain.ObjectID(h.ObjectID),
		Kind:          domain.ObjectKind(h.Kind),
		OwnerScene:    domain.SceneID(h.OwnerScene),
		Version:       h.Version,
		SchemaVersion: h.SchemaVersion,
		Deleted:       h.Deleted,
	}
}

func posToJSON(p domain.Pos) posJSON { return posJSON{X: p.X, Y: p.Y} }

func posFromJSON(p posJSON) domain.Pos { return domain.Pos{X: p.X, Y: p.Y} }

func combatToJSON(c domain.CombatSnapshot) combatJSON {
	return combatJSON{
		ConfigID:    c.ConfigID,
		Level:       c.Level,
		Attack:      c.Attack,
		Defense:     c.Defense,
		HP:          c.HP,
		Speed:       c.Speed,
		SnapshotMS:  c.SnapshotMS,
		RuleVersion: c.RuleVersion,
	}
}

func combatFromJSON(c combatJSON) domain.CombatSnapshot {
	return domain.CombatSnapshot{
		ConfigID:    c.ConfigID,
		Level:       c.Level,
		Attack:      c.Attack,
		Defense:     c.Defense,
		HP:          c.HP,
		Speed:       c.Speed,
		SnapshotMS:  c.SnapshotMS,
		RuleVersion: c.RuleVersion,
	}
}

func cargoToJSON(cargo map[int32]int64) map[string]int64 {
	if cargo == nil {
		return nil
	}
	out := make(map[string]int64, len(cargo))
	for itemID, qty := range cargo {
		out[fmt.Sprintf("%d", itemID)] = qty
	}
	return out
}

func cargoFromJSON(cargo map[string]int64) (map[int32]int64, error) {
	if cargo == nil {
		return nil, nil
	}
	out := make(map[int32]int64, len(cargo))
	for key, qty := range cargo {
		// ParseInt 而不是 fmt.Sscanf：后者对 "12abc" 这类前缀数字会
		// 解析成功并留下尾巴，键被外部改坏时无法被发现。
		itemID, err := strconv.ParseInt(key, 10, 32)
		if err != nil {
			// 键不是十进制整数说明 JSON 被外部改坏了：报错而不是跳过，
			// 静默丢一行货物等于凭空吞掉玩家资产。
			return nil, fmt.Errorf("cargo 键 %q 不是合法物品 ID: %w", key, err)
		}
		out[int32(itemID)] = qty
	}
	return out, nil
}

func cityToPayload(c *domain.City) *cityPayload {
	if c == nil {
		return nil
	}
	return &cityPayload{
		Header:        headerToJSON(c.Header),
		PlayerID:      c.PlayerID,
		GuildID:       c.GuildID,
		Position:      posToJSON(c.Position),
		ConfigVersion: c.ConfigVersion,
	}
}

func cityFromPayload(p *cityPayload) *domain.City {
	if p == nil {
		return nil
	}
	return &domain.City{
		Header:        headerFromJSON(p.Header),
		PlayerID:      p.PlayerID,
		GuildID:       p.GuildID,
		Position:      posFromJSON(p.Position),
		ConfigVersion: p.ConfigVersion,
	}
}

func troopToPayload(t *domain.Troop) *troopPayload {
	if t == nil {
		return nil
	}
	p := &troopPayload{
		Header:     headerToJSON(t.Header),
		PlayerID:   t.PlayerID,
		SlotID:     t.SlotID,
		HomeCityID: uint64(t.HomeCityID),
		Phase:      int32(t.Phase),
		Busy: busyJSON{
			Reason:     int32(t.Busy.Reason),
			OpID:       t.Busy.OpID,
			SinceMS:    t.Busy.SinceMS,
			DeadlineMS: t.Busy.DeadlineMS,
		},
		Target: targetJSON{
			TargetID: uint64(t.Target.TargetID),
			Intent:   int32(t.Target.Intent),
		},
		Cargo:         cargoToJSON(t.Cargo),
		CapacityUnits: t.CapacityUnits,
		Stats:         combatToJSON(t.Stats),
		DispatchID:    t.DispatchID,
		SettlementID:  t.SettlementID,
		BattleID:      t.BattleID,
	}
	if t.Move != nil {
		m := &moveJSON{
			MoveID:           t.Move.MoveID,
			From:             posToJSON(t.Move.From),
			To:               posToJSON(t.Move.To),
			StartAtMS:        t.Move.StartAtMS,
			EndAtMS:          t.Move.EndAtMS,
			SpeedUnitsPerSec: t.Move.SpeedUnitsPerSec,
			SegmentIndex:     t.Move.SegmentIndex,
			PathVersion:      t.Move.PathVersion,
		}
		for _, p := range t.Move.Path {
			m.Path = append(m.Path, posToJSON(p))
		}
		p.Move = m
	}
	if t.Deferred != nil {
		p.Deferred = &deferredJSON{Kind: int32(t.Deferred.Kind), RequestID: t.Deferred.RequestID}
	}
	return p
}

func troopFromPayload(p *troopPayload) (*domain.Troop, error) {
	if p == nil {
		return nil, nil
	}
	cargo, err := cargoFromJSON(p.Cargo)
	if err != nil {
		return nil, err
	}
	t := &domain.Troop{
		Header:     headerFromJSON(p.Header),
		PlayerID:   p.PlayerID,
		SlotID:     p.SlotID,
		HomeCityID: domain.ObjectID(p.HomeCityID),
		Phase:      domain.TroopPhase(p.Phase),
		Busy: domain.BusyState{
			Reason:     domain.BusyReason(p.Busy.Reason),
			OpID:       p.Busy.OpID,
			SinceMS:    p.Busy.SinceMS,
			DeadlineMS: p.Busy.DeadlineMS,
		},
		Target: domain.TargetRef{
			TargetID: domain.ObjectID(p.Target.TargetID),
			Intent:   domain.MarchIntent(p.Target.Intent),
		},
		Cargo:         cargo,
		CapacityUnits: p.CapacityUnits,
		Stats:         combatFromJSON(p.Stats),
		DispatchID:    p.DispatchID,
		SettlementID:  p.SettlementID,
		BattleID:      p.BattleID,
	}
	if p.Move != nil {
		m := &domain.MoveSegment{
			MoveID:           p.Move.MoveID,
			From:             posFromJSON(p.Move.From),
			To:               posFromJSON(p.Move.To),
			StartAtMS:        p.Move.StartAtMS,
			EndAtMS:          p.Move.EndAtMS,
			SpeedUnitsPerSec: p.Move.SpeedUnitsPerSec,
			SegmentIndex:     p.Move.SegmentIndex,
			PathVersion:      p.Move.PathVersion,
		}
		for _, pp := range p.Move.Path {
			m.Path = append(m.Path, posFromJSON(pp))
		}
		t.Move = m
	}
	if p.Deferred != nil {
		t.Deferred = &domain.DeferredAction{
			Kind:      domain.DeferredKind(p.Deferred.Kind),
			RequestID: p.Deferred.RequestID,
		}
	}
	return t, nil
}

func resourceToPayload(r *domain.ResourceNode) *resourcePayload {
	if r == nil {
		return nil
	}
	return &resourcePayload{
		Header:            headerToJSON(r.Header),
		Position:          posToJSON(r.Position),
		RichBlockID:       r.RichBlockID,
		ResourceType:      r.ResourceType,
		ItemID:            r.ItemID,
		Total:             r.Total,
		Remaining:         r.Remaining,
		ActiveCollectID:   r.ActiveCollectID,
		RefreshGeneration: r.RefreshGeneration,
		PendingDestroy:    r.PendingDestroy,
		ConfigVersion:     r.ConfigVersion,
	}
}

func resourceFromPayload(p *resourcePayload) *domain.ResourceNode {
	if p == nil {
		return nil
	}
	return &domain.ResourceNode{
		Header:            headerFromJSON(p.Header),
		Position:          posFromJSON(p.Position),
		RichBlockID:       p.RichBlockID,
		ResourceType:      p.ResourceType,
		ItemID:            p.ItemID,
		Total:             p.Total,
		Remaining:         p.Remaining,
		ActiveCollectID:   p.ActiveCollectID,
		RefreshGeneration: p.RefreshGeneration,
		PendingDestroy:    p.PendingDestroy,
		ConfigVersion:     p.ConfigVersion,
	}
}

// encodePayload 把对象快照编码成 JSON 列内容。
//
// 只编码 kind 对应的那一种结构：同时塞三种会让 payload 体积膨胀，
// 更糟的是「kind 与实际字段不一致」这种情况无法被检测。
func encodePayload(snap *domain.ObjectSnapshot) ([]byte, error) {
	if snap == nil {
		return nil, fmt.Errorf("%w: 对象快照为空", domain.ErrInvalidArgument)
	}
	env := payloadEnvelope{Kind: int32(snap.Kind)}
	switch snap.Kind {
	case domain.ObjectKindCity:
		if snap.City == nil {
			return nil, fmt.Errorf("%w: kind=CITY 但 city 快照为空(object_id=%d)", domain.ErrInvalidArgument, snap.Key.ID)
		}
		env.City = cityToPayload(snap.City)
		// Header 以 ObjectSnapshot 为准覆盖：快照头是规划器刚算出的权威值，
		// City.Header 可能是它在内存里还没同步的旧副本。
		env.City.Header = snapshotHeader(snap)
	case domain.ObjectKindTroop:
		if snap.Troop == nil {
			return nil, fmt.Errorf("%w: kind=TROOP 但 troop 快照为空(object_id=%d)", domain.ErrInvalidArgument, snap.Key.ID)
		}
		env.Troop = troopToPayload(snap.Troop)
		env.Troop.Header = snapshotHeader(snap)
	case domain.ObjectKindResource:
		if snap.Resource == nil {
			return nil, fmt.Errorf("%w: kind=RESOURCE 但 resource 快照为空(object_id=%d)", domain.ErrInvalidArgument, snap.Key.ID)
		}
		env.Resource = resourceToPayload(snap.Resource)
		env.Resource.Header = snapshotHeader(snap)
	default:
		return nil, fmt.Errorf("%w: 未知对象种类 %d(object_id=%d)", domain.ErrInvalidArgument, snap.Kind, snap.Key.ID)
	}
	return json.Marshal(env)
}

// decodePayload 把 JSON 列内容还原成对象快照的 typed 字段。
//
// 未知 kind / JSON 损坏都返回错误而不是零值：文档 16.4 明确
// 「快照 JSON 损坏 → 对该 world 停」，静默跳过会让世界带着空洞变成 Ready。
func decodePayload(worldID domain.WorldID, objectID domain.ObjectID, kind domain.ObjectKind, raw []byte) (*domain.City, *domain.Troop, *domain.ResourceNode, error) {
	if len(raw) == 0 {
		return nil, nil, nil, fmt.Errorf("%w: payload 为空(object_id=%d)", domain.ErrInvalidState, objectID)
	}
	var env payloadEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return nil, nil, nil, fmt.Errorf("解析 payload object_id=%d: %w", objectID, err)
	}
	// 列上的 kind 是权威的；payload 里的 kind 只用于交叉校验。
	// 两者不一致说明有人绕过 store 改了库，属于数据损坏。
	if env.Kind != 0 && domain.ObjectKind(env.Kind) != kind {
		return nil, nil, nil, fmt.Errorf("%w: kind 列=%d payload=%d(object_id=%d)",
			domain.ErrInvalidState, kind, env.Kind, objectID)
	}
	switch kind {
	case domain.ObjectKindCity:
		city := cityFromPayload(env.City)
		if city == nil {
			return nil, nil, nil, fmt.Errorf("%w: payload 缺少 city(object_id=%d)", domain.ErrInvalidState, objectID)
		}
		fillHeader(&city.Header, worldID, objectID)
		return city, nil, nil, nil
	case domain.ObjectKindTroop:
		troop, err := troopFromPayload(env.Troop)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("解析部队 payload object_id=%d: %w", objectID, err)
		}
		if troop == nil {
			return nil, nil, nil, fmt.Errorf("%w: payload 缺少 troop(object_id=%d)", domain.ErrInvalidState, objectID)
		}
		fillHeader(&troop.Header, worldID, objectID)
		return nil, troop, nil, nil
	case domain.ObjectKindResource:
		res := resourceFromPayload(env.Resource)
		if res == nil {
			return nil, nil, nil, fmt.Errorf("%w: payload 缺少 resource(object_id=%d)", domain.ErrInvalidState, objectID)
		}
		fillHeader(&res.Header, worldID, objectID)
		return nil, nil, res, nil
	default:
		return nil, nil, nil, fmt.Errorf("%w: 未知对象种类 %d(object_id=%d)", domain.ErrInvalidState, kind, objectID)
	}
}

// snapshotHeader 由快照顶层字段构造 headerJSON。
func snapshotHeader(snap *domain.ObjectSnapshot) headerJSON {
	return headerJSON{
		WorldID:       uint64(snap.Key.WorldID),
		ObjectID:      uint64(snap.Key.ID),
		Kind:          int32(snap.Kind),
		OwnerScene:    int32(snap.OwnerScene),
		Version:       snap.Version,
		SchemaVersion: snap.SchemaVersion,
		Deleted:       snap.Deleted,
	}
}

// fillHeader 用列值补齐 header 的世界与主键。
//
// 为什么还要补：payload 里的 header 可能是历史版本写的（schema_version
// 没升级就加了字段），而 world_id/object_id 永远以列为准——列是主键，
// payload 只是附属数据。
func fillHeader(h *domain.ObjectHeader, worldID domain.WorldID, objectID domain.ObjectID) {
	if h.WorldID == 0 {
		h.WorldID = worldID
	}
	if h.ID == 0 {
		h.ID = objectID
	}
}

// ---------- 读取 ----------

// objectRow 是 map_object 的单行中间态。
type objectRow struct {
	worldID       uint64
	objectID      uint64
	kind          int32
	ownerScene    int32
	version       uint64
	schemaVersion uint32
	playerID      int64
	payload       []byte
	deleted       int32
}

// toSnapshot 把一行转成 domain.ObjectSnapshot。
func (row objectRow) toSnapshot() (domain.ObjectSnapshot, error) {
	kind := domain.ObjectKind(row.kind)
	city, troop, res, err := decodePayload(domain.WorldID(row.worldID), domain.ObjectID(row.objectID), kind, row.payload)
	if err != nil {
		return domain.ObjectSnapshot{}, err
	}
	return domain.ObjectSnapshot{
		Key:           domain.ObjectKey{WorldID: domain.WorldID(row.worldID), ID: domain.ObjectID(row.objectID)},
		Kind:          kind,
		OwnerScene:    domain.SceneID(row.ownerScene),
		Version:       row.version,
		SchemaVersion: row.schemaVersion,
		PlayerID:      row.playerID,
		City:          city,
		Troop:         troop,
		Resource:      res,
		Deleted:       row.deleted != 0,
	}, nil
}

const objectColumns = `object_id, kind, owner_scene, version, schema_version, player_id, payload, deleted`

// scanObjectRows 消费整个结果集。
//
// Go 知识点：rows 必须 Close（defer），否则连接不归还池；循环结束后必须
// 检查 rows.Err()——Next 返回 false 既可能是正常结束也可能是网络中断，
// 只看 Next 会把半个结果集当成完整状态加载进内存，那是最危险的恢复方式。
func scanObjectRows(rows *sql.Rows, worldID domain.WorldID) ([]domain.ObjectSnapshot, error) {
	defer func() { _ = rows.Close() }()
	var out []domain.ObjectSnapshot
	for rows.Next() {
		row := objectRow{worldID: uint64(worldID)}
		if err := rows.Scan(&row.objectID, &row.kind, &row.ownerScene, &row.version,
			&row.schemaVersion, &row.playerID, &row.payload, &row.deleted); err != nil {
			return nil, fmt.Errorf("扫描对象行: %w", err)
		}
		snap, err := row.toSnapshot()
		if err != nil {
			// 带 object_id 往外抛：恢复流程要靠它定位损坏行（文档 16.4）。
			return nil, fmt.Errorf("解析对象 %d: %w", row.objectID, err)
		}
		out = append(out, snap)
	}
	return out, rows.Err()
}

// LoadScene 加载一个场景内所有未删除对象，并返回其最大对象版本。
//
// maxVersion 的用途：场景内存镜像可以用它做「整体不落后于库」的粗校验；
// 对象级精确比对仍然要用每个对象自己的 Version。返回 0 表示场景为空。
func (r *Repository) LoadScene(ctx context.Context, worldID domain.WorldID, sceneID domain.SceneID) ([]domain.ObjectSnapshot, uint64, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT `+objectColumns+`
		 FROM map_object
		 WHERE world_id = ? AND owner_scene = ? AND deleted = 0
		 ORDER BY object_id ASC`,
		int64(worldID), int32(sceneID))
	if err != nil {
		return nil, 0, fmt.Errorf("查询场景对象 world=%d scene=%d: %w", worldID, sceneID, err)
	}
	objects, err := scanObjectRows(rows, worldID)
	if err != nil {
		return nil, 0, err
	}
	var maxVersion uint64
	for _, obj := range objects {
		if obj.Version > maxVersion {
			maxVersion = obj.Version
		}
	}
	return objects, maxVersion, nil
}

// LoadObject 按主键加载单个对象。
//
// 墓碑对象也会返回（Deleted=true），由调用方决定如何处理：
// 战斗/清理流程需要知道「这里曾经有个对象」，直接返回 NotFound
// 会丢掉这个信息。真正「不存在」才返回 ErrObjectNotFound。
func (r *Repository) LoadObject(ctx context.Context, worldID domain.WorldID, objectID domain.ObjectID) (domain.ObjectSnapshot, error) {
	var row objectRow
	err := r.db.QueryRowContext(ctx,
		`SELECT `+objectColumns+` FROM map_object WHERE world_id = ? AND object_id = ?`,
		int64(worldID), uint64(objectID)).Scan(&row.objectID, &row.kind, &row.ownerScene,
		&row.version, &row.schemaVersion, &row.playerID, &row.payload, &row.deleted)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.ObjectSnapshot{}, fmt.Errorf("%w: world=%d object_id=%d", domain.ErrObjectNotFound, worldID, objectID)
	}
	if err != nil {
		return domain.ObjectSnapshot{}, fmt.Errorf("查询对象 world=%d object_id=%d: %w", worldID, objectID, err)
	}
	row.worldID = uint64(worldID)
	return row.toSnapshot()
}

// LoadOccupancy 加载全世界的静态占位。
//
// 排序 (cell_x, cell_y, layer) 与唯一键顺序一致：内存索引构建时按这个
// 顺序插入，冲突检测的报错顺序就是确定的，测试与日志可复现。
func (r *Repository) LoadOccupancy(ctx context.Context, worldID domain.WorldID) ([]domain.OccupancyClaim, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT cell_x, cell_y, layer, object_id FROM map_occupancy
		 WHERE world_id = ? ORDER BY cell_x ASC, cell_y ASC, layer ASC`,
		int64(worldID))
	if err != nil {
		return nil, fmt.Errorf("查询静态占位 world=%d: %w", worldID, err)
	}
	defer func() { _ = rows.Close() }()
	var out []domain.OccupancyClaim
	for rows.Next() {
		var (
			cellX, cellY int64
			layer        int32
			objectID     uint64
		)
		if err := rows.Scan(&cellX, &cellY, &layer, &objectID); err != nil {
			return nil, fmt.Errorf("扫描占位行: %w", err)
		}
		out = append(out, domain.OccupancyClaim{
			CellX: cellX, CellY: cellY, Layer: layer,
			ObjectKey: domain.ObjectKey{WorldID: worldID, ID: domain.ObjectID(objectID)},
		})
	}
	return out, rows.Err()
}

// AllocObjectID 从 map_id_allocator 分配一个对象 ID。
//
// 必须是 SQL 自增：time.Now().UnixNano() 在多实例下会碰撞，进程内计数器
// 重启后重复。允许空洞——分配成功但事务回滚是正常现象，
// 「ID 存在」不等于「对象存在」（domain/ids.go / 文档 14.2）。
//
// 调用方约定：在**提交事务之外**先分配，然后把 ID 写进 plan。这样事务
// 重试时用的是同一个 ID，不会多创建一次对象。若在事务内分配又重试，
// 空洞会变多（可接受）但绝不能换成重新分配（会产生第二个对象）。
func (r *Repository) AllocObjectID(ctx context.Context, q Execer) (domain.ObjectID, error) {
	if q == nil {
		return 0, fmt.Errorf("%w: 分配器执行器为空", domain.ErrInvalidArgument)
	}
	// INSERT INTO map_id_allocator () VALUES () 是 MySQL 允许的空行插入写法：
	// 全部列都有默认值/自增，不需要虚构业务列。
	res, err := q.ExecContext(ctx, `INSERT INTO map_id_allocator () VALUES ()`)
	if err != nil {
		return 0, fmt.Errorf("分配对象 ID: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("读取对象 ID: %w", err)
	}
	return domain.ObjectID(id), nil
}

// loadAllObjects 读取一个世界的全部未删除对象（恢复流程用）。
//
// 与 LoadScene 的区别：这里跨全部场景，按 object_id 升序返回。
// 排序不是可有可无的：恢复流程要按对象顺序重建空间索引与引用校验，
// 稳定顺序让「同一份数据两次恢复得到同样的内存状态」成立，测试才能断言。
func loadAllObjects(ctx context.Context, q queryer, worldID domain.WorldID) ([]domain.ObjectSnapshot, error) {
	rows, err := q.QueryContext(ctx,
		`SELECT `+objectColumns+` FROM map_object
		 WHERE world_id = ? AND deleted = 0 ORDER BY object_id ASC`,
		int64(worldID))
	if err != nil {
		return nil, fmt.Errorf("查询世界对象 world=%d: %w", worldID, err)
	}
	return scanObjectRows(rows, worldID)
}
