// internal/game/player/preload.go —— 登录预加载（设计文档 12.6）
//
// 目标：把登录期需要的全部数据用**一次往返**拿完，而不是各模块 OnLogin 里逐个查库。
//
// 现状（P12 前）：player → player_attr → player_item 串行 3 次；
// 随后 equip.OnLogin 查 player_equip、activity.OnLogin 查 activity_record，再 2 次。
// 合计 5 次串行往返，本地约 10ms+，跨机房可达 50ms+。
//
// P12 后：三表并行，耗时 ≈ 单次最慢查询。
//
// 架构职责与协作：本文件只服务**非缓存模式**（缓存模式的等价物是一次
// LoadCached）。产物 Prefetch 只在组装 State 时用一次，不对外暴露——
// 「登录时把模块数据预取进 Actor，模块 OnLogin 直接读」这条链路
// （v3.6 的 Prefetch.OwnerRows）已于 v3.9 删除：它没有消费者，而且与
// 当前模块形态不匹配（见下）。
//
// 为什么删掉登录展开：现有有内存态的玩法（签到/装备/伙伴/任务/活动）
// 全是「player_id 单行 JSON」，不需要按归属者展开多行；而一对多的模块
// （邮件/好友/队伍/公会）都无内存态、直查 DB，登录展开的数据挂在
// Actor 上只对「登录那一刻」有效，之后新数据不会反映。真出现「有内存态
// + 一对多」的功能时，在它的 OnLogin 里调 OwnerIndex.Expand 即可（5 行），
// 不需要框架先预取一份。
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
	"game_study/pkg/db"
	"game_study/pkg/log"
	"game_study/pkg/metrics"
	"game_study/pkg/model"

	"golang.org/x/sync/errgroup"
)

// DefaultPreloadTimeout 预加载超时（12.6.2 第 4 条：超时则登录失败，避免半成品 Actor 上线）。
const DefaultPreloadTimeout = 5 * time.Second

// Prefetch 预加载结果（非缓存模式一次并行拉取的三表）。
//
// 只是 Load 的中间产物：组装成 State 之后就丢弃，不跨消息存活。
// 历史注记：v3.6 曾把归属展开的实体行也挂在这里（ownerRows + OwnerRows），
// 供模块 OnLogin 消费；v3.9 删除——没有消费者，且与当前模块形态不匹配
// （详见文件头的说明）。
type Prefetch struct {
	Player *model.Player
	Attr   *model.PlayerAttr
	Items  map[int32]int64
}

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

	// 并行拉取：三表并发，耗时 ≈ max(单表耗时)
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

	err := g.Wait()

	// 记录耗时（20.1：登录 P99 < 2s）。
	// 与原实现的一点差异：原代码的成功判定漏掉了 attr 错误，现在任一失败
	// 即记失败——attr 失败本来就会让本次登录失败，指标应与真实结果一致。
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
