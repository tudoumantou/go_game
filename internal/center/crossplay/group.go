package crossplay

import "sort"

// Distribute 把参与者分配到组，每人落进唯一一组。
//
// 规则：
//  1. 按 Power 降序排（同 Power 按 PlayerID 升序，保证结果可重现）；
//  2. 组数 = ceil(n / GroupSize)，尾组不足也照样开班；
//  3. **蛇形分配**（第 1 名→G1、第 2 名→G2、第 N 名→GN，第 N+1 名回 GN …），
//     不是「排序后前 100 人一组」。
//
// 为什么必须蛇形：切片式分组的第 1 组是全场最强的 100 人、最后一组是最弱的 100 人，
// 而奖励按组内排名发放 —— 于是强者组里第 10 名的实力远高于弱者组第 1 名，
// 却拿更少的奖。蛇形保证每组实力总和接近、组内名次含金量一致。
//
// 人数均衡性：n 除不尽时各组最多差 1 人。
//
// 输入不被修改（内部复制后再排）。
func Distribute(members []Member) []Group {
	n := len(members)
	if n == 0 {
		return nil
	}

	sorted := make([]Member, n)
	copy(sorted, members)
	sort.SliceStable(sorted, func(i, j int) bool {
		if sorted[i].Power != sorted[j].Power {
			return sorted[i].Power > sorted[j].Power
		}
		// 同分按 ID 升序：同一批数据每次分组结果必须一致，
		// 否则重算会产出不同的分组，后续对账无从下手。
		return sorted[i].PlayerID < sorted[j].PlayerID
	})

	groupCount := (n + GroupSize - 1) / GroupSize
	groups := make([]Group, groupCount)
	for i := range groups {
		groups[i] = Group{ID: int32(i + 1), Members: make([]Member, 0, GroupSize)}
	}

	for i, m := range sorted {
		round, pos := i/groupCount, i%groupCount
		if round%2 == 1 {
			// 奇数轮反向：让每轮的头名落到上一轮的尾组，抵消「强人扎堆第一组」。
			pos = groupCount - 1 - pos
		}
		m.GroupID = groups[pos].ID
		groups[pos].Members = append(groups[pos].Members, m)
	}

	// 蛇形分配后组内已按 Power 降序（轮次越靠后 Power 越低），
	// 这里不再排序 —— 减去一次无意义的 O(n log n)。
	return groups
}

// ClampScore 应用下限：扣到 ScoreFloor 后不再下降。
//
// 攻击方仍然加分（已确认不堵这个口子），所以这里的语义只是「分数不低于下限」，
// 不是「到下限就免疫」。
func ClampScore(score int64) int64 {
	if score < ScoreFloor {
		return ScoreFloor
	}
	return score
}
