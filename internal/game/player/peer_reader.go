// internal/game/player/peer_reader.go —— 跨玩家只读访问的实现
//
// 数据源优先级：Redis 快照 > MySQL。
//
// 为什么 Redis 优先：写路径是「先改内存 → 同步写 Redis 副本 → 异步落 MySQL」，
// 所以副本比库新（库里可能是 60 秒前的状态）。展示类读取要的就是「现在多大」，
// 拿库会显示落后一个落库周期。
//
// 为什么必须能回落库：副本只覆盖「有快照的玩家」。下面三种情况必然 miss，
// 不回落就等于「查无此人」：
//   - 从未进过游戏（建了角色但没登录过）；
//   - 快照被淘汰 / Redis 重启（副本不在持久化范围内）；
//   - A 类同步落库后短暂删除快照（重建完成前的窗口）。
//
// 不回填：读一眼就写回 Redis 会把「浏览别人资料」变成写放大，而且并发 Seed
// 还要处理版本冲突。跨玩家读的频率远低于本人读写，省下的复杂度比省下的
// 那次 SQL 更值。
package player

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"game_study/internal/game/module"
	"game_study/pkg/model"
	"game_study/pkg/writebehind"
)

// View 读单个玩家摘要；不存在返回 (nil, nil)。
//
// Redis 的任何失败（miss/损坏/连接故障）都回落库：展示路径不该因为缓存
// 不可用而整体失败，库是最终真相。
func (r *Repository) View(ctx context.Context, playerID int64) (*module.PeerView, error) {
	if playerID <= 0 {
		return nil, nil
	}
	if r.cache != nil {
		if rec, err := r.cache.Load(ctx, playerID); err == nil {
			return peerViewOf(rec.State), nil
		}
	}
	if !r.Ready() {
		return nil, nil
	}
	p, err := model.FindPlayerByPK(ctx, r.SQL(), playerID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("peer view %d: %w", playerID, err)
	}
	return &module.PeerView{PlayerID: p.Id, Name: p.Name, Level: p.Level}, nil
}

// ViewMany 批量读玩家摘要（Redis pipeline + 一条 IN 回源）。
//
// 为什么批量：好友列表 50 人逐个读就是 50 次 RTT；pipeline 合成一次，
// 回源也合成一条 IN。这是「只读走 Redis」唯一需要额外设计的地方。
//
// 返回的 map 只含找到的 id；输入里重复/非法的 id 会被去重或跳过。
func (r *Repository) ViewMany(ctx context.Context, ids []int64) (map[int64]*module.PeerView, error) {
	out := make(map[int64]*module.PeerView, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	if r.cache != nil {
		if recs, err := r.cache.LoadMany(ctx, ids); err == nil {
			for id, rec := range recs {
				out[id] = peerViewOf(rec.State)
			}
		}
	}
	// 去重后再决定哪些要回源（重复 id 会让 IN 里出现重复占位符）
	missing := make([]int64, 0, len(ids))
	seen := make(map[int64]struct{}, len(ids))
	for _, id := range ids {
		if id <= 0 {
			continue
		}
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		if _, ok := out[id]; !ok {
			missing = append(missing, id)
		}
	}
	if len(missing) == 0 || !r.Ready() {
		return out, nil
	}
	rows, err := model.ListPlayerByIds(ctx, r.SQL(), missing)
	if err != nil {
		return nil, fmt.Errorf("peer views: %w", err)
	}
	for _, p := range rows {
		out[p.Id] = &module.PeerView{PlayerID: p.Id, Name: p.Name, Level: p.Level}
	}
	return out, nil
}

// peerViewOf 快照 → 只读视图。
func peerViewOf(s writebehind.Snapshot) *module.PeerView {
	return &module.PeerView{
		PlayerID: s.PlayerID,
		Name:     s.Profile.Name,
		Level:    s.Profile.Level,
	}
}
