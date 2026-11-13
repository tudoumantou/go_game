// delivery.go —— 不由玩家请求驱动的写路径：积分上报、镜像落库。
//
// 对应模块约定的 delivery.go 槽位（「不是玩家点出来的数据写入」）。
// 两个循环都在独立 goroutine 里跑，与玩家 Actor、中心服回调并发，
// 读写的都是 Registry（自带锁）。
package crossplay

import (
	"context"
	"sort"

	"game_study/pkg/model"
	internalv2 "game_study/pkg/pb/internalv2"
)

// reportBatch 单个上报报文的最大条目数。
//
// 一条报文几千条也能发，但接收端要一次性过 oneof 解码 + 逐条合并，
// 卡住的是中心服那唯一一条 tickLoop。500 条约 12KB，处理时间在微秒级。
const reportBatch = 500

// reportTick 上报本服玩家的积分变化（60s 一拍）。
//
// 只报变化项：60s 内没人变就一个字节都不发。用绝对值而不是差值——
// 绝对值天然幂等，丢一包下一拍自然修复，不需要序号和补发队列。
//
// 上报失败时把这一批重新标脏：不重试的话，这批变化要等到这些玩家
// 下次被打才会再进队列，中间中心服一直拿着旧值。
func (m *Module) reportTick(context.Context) error {
	if m.center == nil || !m.center.Ready() {
		return nil // 没连上中心服：脏集合留着，连上之后下一拍自然发出去
	}
	batch := m.reg.TakeDirty(0)
	if len(batch) == 0 {
		return nil
	}

	for start := 0; start < len(batch); start += reportBatch {
		end := start + reportBatch
		if end > len(batch) {
			end = len(batch)
		}
		chunk := batch[start:end]
		entries := make([]*internalv2.CrossScoreEntry, 0, len(chunk))
		for _, e := range chunk {
			entries = append(entries, &internalv2.CrossScoreEntry{
				GroupId:   e.GroupID,
				PlayerId:  e.PlayerID,
				Score:     e.Score,
				UpdatedAt: e.UpdatedAt,
				ServerId:  e.ServerID,
			})
		}
		if err := m.center.SendCrossScore(entries); err != nil {
			// 整批失败就整批回填（不区分是哪一条坏的：中心服拒收整条报文）。
			m.reg.RestoreDirty(batch[start:])
			return err
		}
	}
	return nil
}

// persistTick 把有变化的组镜像落库（5 分钟一拍）。
//
// 只写「自上次落库后有变化的组」：1000 个组里一小时内可能只有十几个变过，
// 全量刷会让 binlog 白涨两个数量级。cross_group 在游戏服是恢复副本不是权威，
// 少写几次没有任何业务影响——真丢了等中心服 10 分钟校准推一份过来。
func (m *Module) persistTick(ctx context.Context) error {
	if m.repo == nil || !m.repo.Ready() {
		return nil
	}
	dirty := m.reg.GroupDirty()
	if len(dirty) == 0 {
		return nil
	}
	all := m.reg.AllGroups()
	subset := make(map[int32][]Entry, len(dirty))
	for _, gid := range dirty {
		if members, ok := all[gid]; ok {
			subset[gid] = members
		}
	}
	// 本服玩家一起刷：组里的本服玩家条目与 players 共享指针，
	// 光刷组表而玩家表是旧的，重启加载时会用玩家表的值覆盖组里的值。
	return m.saveGroups(ctx, subset, m.reg.AllPlayers())
}

// saveGroups 把组快照写成表行。
//
// own 是本服玩家列表，用来给组内条目补 ServerID：组表只存
// players / servers 两列 JSON，players 列里没有 server_id（那是用户定的表结构），
// 所以别服玩家的归属要从 servers 列恢复——写入时这两列必须在同一次调用里生成。
func (m *Module) saveGroups(ctx context.Context, groups map[int32][]Entry, own []Entry) error {
	if len(groups) == 0 {
		return nil
	}
	// 本服玩家 server_id 查表（组里的条目可能来自下发，ServerID 已有；
	// 但本服玩家从 players 表来的那条可能缺）。
	ownServer := make(map[int64]int32, len(own))
	for _, e := range own {
		ownServer[e.PlayerID] = e.ServerID
	}

	rows := make([]model.CrossGroup, 0, len(groups))
	for gid, members := range groups {
		if len(members) == 0 {
			continue // 空组不写：写了也只是占一行，重启读出来还是空
		}
		for i := range members {
			if members[i].ServerID == 0 {
				members[i].ServerID = ownServer[members[i].PlayerID]
			}
		}
		playersCol, serversCol, err := membersToJSON(members)
		if err != nil {
			m.logger.Warn("marshal cross_group failed, group skipped", "group", gid, "err", err)
			continue
		}
		rows = append(rows, model.CrossGroup{
			GroupId: gid,
			Players: playersCol,
			Servers: serversCol,
		})
	}
	// 稳定顺序：同一次刷盘产生的 binlog 在两次运行里一致，便于比对。
	sort.Slice(rows, func(i, j int) bool { return rows[i].GroupId < rows[j].GroupId })

	if err := m.repo.SaveGroups(ctx, rows); err != nil {
		// 落库失败不重试也不报错：内存里的镜像还在，下一拍会重写。
		// 但要把 dirty 标记补回去，否则下一拍不会选中这些组。
		m.logger.Warn("save cross_group failed", "groups", len(rows), "err", err)
		return err
	}
	return nil
}
