// internal/game/player/preload.go —— 登录预加载（设计文档 12.6）
//
// 目标：把登录期需要的全部数据用**一次往返**拿完，而不是各模块 OnLogin 里逐个查库。
//
// 现状（P12 前）：player → player_attr → player_item 串行 3 次；
// 随后 equip.OnLogin 查 player_equip、activity.OnLogin 查 activity_record，再 2 次。
// 合计 5 次串行往返，本地约 10ms+，跨机房可达 50ms+。
//
// P12 后：查询全部并行，耗时 ≈ 单次最慢查询；结果存入 Prefetch，模块 OnLogin 直接读。
// v3.6 起多一路归属展开（Redis 命中时不碰库），见 ExpandOwners。
//
// 架构职责与协作：本文件主要服务**非缓存模式**（缓存模式的等价物是一次
// LoadCached）；ExpandOwners 是例外——归属展开在两种模式下都走这里
// （缓存模式由 actor.preload 直接调，非缓存模式并进 Load 的并行查询）。
// 产物 Prefetch 经 module.Context.Prefetch 被各模块消费，框架不替模块
// 解释数据格式。
//
// 设计取舍：为什么创角后不立即预加载/建 Actor（延迟到首个业务包），
// 见 server.go OnClientPacket 的懒加载注释——那是组装层的决策，这里只
// 提供「已决定加载时如何最快加载」的能力。
package player

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"game_study/internal/game/module"
	"game_study/pkg/cache"
	"game_study/pkg/db"
	"game_study/pkg/log"
	"game_study/pkg/metrics"
	"game_study/pkg/model"

	"golang.org/x/sync/errgroup"
)

// DefaultPreloadTimeout 预加载超时（12.6.2 第 4 条：超时则登录失败，避免半成品 Actor 上线）。
const DefaultPreloadTimeout = 5 * time.Second

// Prefetch 预加载结果（一次并行拉取的全部数据）。
//
// 字段分两组：Player/Attr/Items 由框架组装进 State；ownerRows 是归属展开
// 的实体行，原样透传给模块——框架不解析模块私有结构。
//
// 为什么没有装备/活动快照：P12 设计里它们各占一个并行查询，改成「状态随
// 玩家快照（Ext）加载」之后就没有第二个来源了，字段与 Equips/Activities
// 接口一并删除（v3.6）。模块要预取一对多实体，走 OwnerRows。
type Prefetch struct {
	Player *model.Player
	Attr   *model.PlayerAttr
	Items  map[int32]int64

	// ownerRows 归属展开的实体行（按表名分组，见 ExpandOwners）。
	ownerRows map[string][]cache.EntityRow
}

// OwnerRows 实现 module.PrefetchSource：某表归属展开的实体行。
//
// 未展开的表返回 nil——模块必须同时支持自行查库的路径（预加载是优化，
// 不是正确性前提）。返回的行按实体 id 升序（索引侧保证）。
func (p *Prefetch) OwnerRows(table string) []cache.EntityRow {
	if p == nil {
		return nil
	}
	return p.ownerRows[table]
}

// Preloader 预加载器。
type Preloader struct {
	db      *db.DB
	logger  *log.Logger
	timeout time.Duration
	// owners 归属索引（缓存架构设计 §3）：一对多表登录时按角色展开。
	// 空表示没有配 mappings，ExpandOwners 退化为空操作。
	owners []*cache.OwnerIndex
}

// NewPreloader 创建预加载器。
func NewPreloader(database *db.DB, logger *log.Logger, timeout time.Duration) *Preloader {
	if timeout <= 0 {
		timeout = DefaultPreloadTimeout
	}
	return &Preloader{db: database, logger: logger, timeout: timeout}
}

// SetOwnerIndexes 注入归属索引（由组装层按 idtab.mappings 配置注入）。
//
// 顺序即展开顺序：调用方按表名排序传入，保证日志与加载行为每次启动一致。
func (p *Preloader) SetOwnerIndexes(indexes ...*cache.OwnerIndex) {
	p.owners = indexes
}

// Load 并行拉取玩家全部登录数据并组装 State。
//
// 任一必需项（player）缺失视为错误；其余表缺失按空处理（新角色无装备/活动记录是正常的）。
//
// 用 errgroup 而不是手写 WaitGroup：Add/Done 必须手工配对，多一个少一个
// 都会让 Wait 永久阻塞（本函数曾因删掉两个查询却漏改 Add(5) 而死锁，且
// 只在非缓存降级路径上暴露）。errgroup 的 counter 由 Go/Wait 自动管理，
// 这类结构性错误从此不可能再出现；第一次错误还会 cancel 其余查询（快速失败），
// 不必等三个查询全部超时。
//
// 并发安全性：各 goroutine 只写 pf 的不同字段，无数据竞争；gctx 由
// database/sql 驱动层负责取消在途查询。
func (p *Preloader) Load(ctx context.Context, playerID int64) (*State, *Prefetch, error) {
	start := time.Now()
	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	pf := &Prefetch{Items: make(map[int32]int64)}
	g, gctx := errgroup.WithContext(ctx)

	// 并行拉取：基础三表 + 归属展开共 4 路并发，耗时 ≈ max(单路耗时)
	g.Go(func() error {
		row, err := model.FindPlayerByPK(gctx, p.db.DB, playerID)
		switch {
		case err == nil:
			pf.Player = row
		case errors.Is(err, sql.ErrNoRows):
			return ErrNoPlayer
		default:
			return fmt.Errorf("load player: %w", err)
		}
		return nil
	})

	g.Go(func() error {
		row, err := model.FindPlayerAttrByPK(gctx, p.db.DB, playerID)
		switch {
		case err == nil:
			pf.Attr = row
		case errors.Is(err, sql.ErrNoRows):
			// 无属性行：创角事务应保证存在，缺失按默认值处理
		default:
			return fmt.Errorf("load attr: %w", err)
		}
		return nil
	})

	g.Go(func() error {
		items, err := p.queryItems(gctx, playerID)
		if err != nil {
			return err
		}
		pf.Items = items
		return nil
	})

	// 归属展开（缓存架构设计 §4.2）：一对多表的实体按角色 id 批量加载。
	// 与另三个查询并行——它打的是 Redis（命中时完全不碰库）。
	g.Go(func() error {
		rows, err := p.ExpandOwners(gctx, playerID)
		if err != nil {
			return err
		}
		pf.ownerRows = rows
		return nil
	})

	err := g.Wait()

	// 记录耗时（20.1：登录 P99 < 2s）。
	// 与原实现的一点差异：原代码的成功判定漏掉了 attr 错误，现在四路任一
	// 失败即记失败——attr 失败本来就会让本次登录失败，指标应与真实结果一致。
	if metrics.Enabled() {
		metrics.LoginPreloadObserve(time.Since(start), err == nil)
	}
	if err != nil {
		return nil, nil, err
	}
	// 装备与活动状态已随玩家快照（Ext）加载，不再单独查库——
	// 原 P12 的两个快照字段与 Equips/Activities 接口已于 v3.6 删除。

	return assembleState(playerID, pf), pf, nil
}

// ExpandOwners 按归属索引展开登录实体（缓存架构设计 §4.2）。
//
// 对每个注入的索引：Members(playerID) 取 id 列表（索引未 ready 时自动
// 回落 MySQL），LoadBatch 批量加载——Redis hash 命中的直接用，未命中的
// 一次 SQL 捞回并回填数据表。
//
// 读时自愈（§4.3）：两处都没有的 id 是索引残留（删行时漏清索引），
// Remove 掉。清理失败只告警——它是加分项，定时 Reconcile 还会兜底，
// 不该让登录失败。
//
// 只收非空表：某表展开 0 行时不出现在 map 里——模块看到「没有这个表」
// 与「这个表 0 行」的后续动作一致（自行查库/展示空列表），没必要占 key。
//
// 错误即登录失败：能走到这里说明 MySQL 也答不上来（Redis 故障已在
// Members/LoadBatch 内部回落库）。实体数据缺失时放行等于「功能列表
// 为空且不报错」，那是这套设计里最难查的事故形态。
func (p *Preloader) ExpandOwners(ctx context.Context, playerID int64) (map[string][]cache.EntityRow, error) {
	if len(p.owners) == 0 {
		return nil, nil
	}
	out := make(map[string][]cache.EntityRow, len(p.owners))
	for _, oi := range p.owners {
		ids, err := oi.Members(ctx, playerID)
		if err != nil {
			return nil, fmt.Errorf("expand %s members: %w", oi.Table(), err)
		}
		if len(ids) == 0 {
			continue
		}
		rows, missing, err := oi.LoadBatch(ctx, ids)
		if err != nil {
			return nil, fmt.Errorf("expand %s rows: %w", oi.Table(), err)
		}
		if len(missing) > 0 {
			if err := oi.Index().Remove(ctx, missing...); err != nil {
				p.logger.Warn("owner index self-heal failed",
					"table", oi.Table(), "player_id", playerID, "ids", missing, "err", err)
			} else {
				p.logger.Info("owner index self-healed",
					"table", oi.Table(), "removed", len(missing))
			}
		}
		if len(rows) > 0 {
			out[oi.Table()] = rows
		}
	}
	return out, nil
}

// assembleState 把预加载结果组装成 State。
func assembleState(playerID int64, pf *Prefetch) *State {
	state := NewState(playerID)

	if pf.Player != nil {
		state.SetProfile(module.Profile{
			Name:    pf.Player.Name,
			Level:   pf.Player.Level,
			Exp:     pf.Player.Exp,
			Coin:    pf.Player.Coin,
			SceneID: pf.Player.SceneId,
		})
	}

	if pf.Attr != nil {
		state.SetAttrStruct(Attr{
			Career: pf.Attr.Career,
			Hp:     pf.Attr.Hp,
			Mp:     pf.Attr.Mp,
			Atk:    pf.Attr.Atk,
			Def:    pf.Attr.Def,
			Spd:    pf.Attr.Spd,
		})
	}

	for itemID, count := range pf.Items {
		if count > 0 {
			state.AddItem(itemID, count)
		}
	}
	return state
}

func (p *Preloader) queryItems(ctx context.Context, playerID int64) (map[int32]int64, error) {
	const q = `SELECT item_id, count FROM player_item WHERE player_id = ?`
	rows, err := p.db.DB.QueryContext(ctx, q, playerID)
	if err != nil {
		return nil, fmt.Errorf("query items: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := make(map[int32]int64)
	for rows.Next() {
		var itemID, count int32
		if err := rows.Scan(&itemID, &count); err != nil {
			return nil, err
		}
		out[itemID] = int64(count)
	}
	return out, rows.Err()
}

// ErrNoPlayer 角色不存在（登录时角色被删/数据异常）。
var ErrNoPlayer = errors.New("player not found")
