// internal/game/player/preload.go —— 登录预加载（设计文档 12.6）
//
// 目标：把登录期需要的全部数据用**一次往返**拿完，而不是各模块 OnLogin 里逐个查库。
//
// 现状（P12 前）：player → player_attr → player_item 串行 3 次；
// 随后 equip.OnLogin 查 player_equip、activity.OnLogin 查 activity_record，再 2 次。
// 合计 5 次串行往返，本地约 10ms+，跨机房可达 50ms+。
//
// P12 后：5 个查询全部并行，耗时 ≈ 单次最慢查询；结果存入 Prefetch，模块 OnLogin 直接读。
//
// 架构职责与协作：本文件只服务**非缓存模式**（缓存模式的等价物是一次
// LoadCached）；产物 Prefetch 经 module.Context.Prefetch 被各模块消费，
// 框架不替模块解释数据格式。
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
	"sync"
	"time"

	"game_study/internal/game/module"
	"game_study/pkg/db"
	"game_study/pkg/log"
	"game_study/pkg/metrics"
	"game_study/pkg/model"
)

// DefaultPreloadTimeout 预加载超时（12.6.2 第 4 条：超时则登录失败，避免半成品 Actor 上线）。
const DefaultPreloadTimeout = 5 * time.Second

// Prefetch 预加载结果（一次并行拉取的全部数据）。
//
// 字段按「框架资产 / 模块资产」分两组：Player/Attr/Items 由框架组装进
// State，EquipList/ActList 原样透传给模块——框架不解析模块私有结构。
type Prefetch struct {
	Player    *model.Player
	Attr      *model.PlayerAttr
	Items     map[int32]int64
	EquipList []module.EquipSnapshot
	ActList   []module.ActivitySnapshot
}

// Equips 实现 module.PrefetchSource。
func (p *Prefetch) Equips() []module.EquipSnapshot { return p.EquipList }

// Activities 实现 module.PrefetchSource。
func (p *Prefetch) Activities() []module.ActivitySnapshot { return p.ActList }

// Preloader 预加载器。
type Preloader struct {
	db      *db.DB
	logger  *log.Logger
	timeout time.Duration
}

// NewPreloader 创建预加载器。
func NewPreloader(database *db.DB, logger *log.Logger, timeout time.Duration) *Preloader {
	if timeout <= 0 {
		timeout = DefaultPreloadTimeout
	}
	return &Preloader{db: database, logger: logger, timeout: timeout}
}

// Load 并行拉取玩家全部登录数据并组装 State。
//
// 任一必需项（player）缺失视为错误；其余表缺失按空处理（新角色无装备/活动记录是正常的）。
//
// Go 知识点：
//   - wg.Add(N) 必须在所有 go 语句**之前**调用：Add 在 goroutine 里做会
//     有「Wait 已返回、Add 又来」的竞态；这里集中 Add(5) 再逐个放 goroutine；
//   - 每个 goroutine 只写自己的 err 变量、只读共享的 pf 的不同字段，
//     没有数据竞争；ctx 超时由 database/sql 的驱动层负责取消在途查询。
func (p *Preloader) Load(ctx context.Context, playerID int64) (*State, *Prefetch, error) {
	start := time.Now()
	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	pf := &Prefetch{Items: make(map[int32]int64)}

	// 并行拉取：5 个查询并发，耗时 ≈ max(单表耗时)
	var (
		wg        sync.WaitGroup
		playerErr error
		attrErr   error
		itemsErr  error
	)

	wg.Add(5)

	go func() {
		defer wg.Done()
		row, err := model.FindPlayerByPK(ctx, p.db.DB, playerID)
		switch {
		case err == nil:
			pf.Player = row
		case errors.Is(err, sql.ErrNoRows):
			playerErr = ErrNoPlayer
		default:
			playerErr = fmt.Errorf("load player: %w", err)
		}
	}()

	go func() {
		defer wg.Done()
		row, err := model.FindPlayerAttrByPK(ctx, p.db.DB, playerID)
		switch {
		case err == nil:
			pf.Attr = row
		case errors.Is(err, sql.ErrNoRows):
			// 无属性行：创角事务应保证存在，缺失按默认值处理
		default:
			attrErr = fmt.Errorf("load attr: %w", err)
		}
	}()

	go func() {
		defer wg.Done()
		items, err := p.queryItems(ctx, playerID)
		if err != nil {
			itemsErr = err
			return
		}
		pf.Items = items
	}()

	wg.Wait()

	// 记录耗时（20.1：登录 P99 < 2s）
	if metrics.Enabled() {
		metrics.LoginPreloadObserve(time.Since(start), playerErr == nil && itemsErr == nil)
	}

	if playerErr != nil {
		return nil, nil, playerErr
	}
	if attrErr != nil {
		return nil, nil, attrErr
	}
	if itemsErr != nil {
		return nil, nil, itemsErr
	}
	// 装备与活动状态已随玩家快照（Ext）加载，不再单独查库。

	return assembleState(playerID, pf), pf, nil
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
