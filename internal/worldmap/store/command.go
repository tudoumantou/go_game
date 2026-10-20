package store

// command.go 承载幂等回执的查询与 actor_key 归一化。
//
// 设计文档 7.1/14.1 的幂等键是 (world_id, actor_key, request_id)。
// runtime 侧用的是 domain.CommandKey{WorldID, PlayerID, RequestID}，
// 本文件负责把二者对上——这是整个持久化层唯一一处「类型边界转换」，
// 集中在这里是为了让其余代码不用关心映射规则。

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"game_study/internal/worldmap/domain"
)

// ActorKey 把 player_id 归一化成 map_command.actor_key（"p:<id>"）。
//
// 为什么表里用字符串列而不是 player_id 列（设计文档 14.1）：
// 幂等键要同时覆盖「玩家命令」和「内部系统命令」两类。系统命令没有
// player_id（到达、采集结束、战斗结果、刷新批次都由定时器触发）。
// 如果用 (world_id, player_id, request_id) 做主键，系统命令只能伪造
// player_id=0，于是所有系统的 request_id 挤在同一个命名空间里，
// 一旦两个子系统用了相同的 request_id 就会互相覆盖回执——这种 bug
// 在测试里几乎不可能出现，上线后却表现为「某条命令莫名其妙地成功」。
//
// 用 actor_key 把命名空间显式分域："p:1001" 与 "sys:refresh" 永不碰撞，
// 而且字符串键与「谁是主体」这件事的耦合更弱：将来加 guild 命令
// （"g:88"）只是新增一个前缀，不需要改表。
func ActorKey(playerID int64) string {
	return fmt.Sprintf("p:%d", playerID)
}

// SystemActorKey 返回内部系统命令的 actor_key（"sys:<kind>"）。
//
// kind 必须是**稳定**的英文标识（arrive / collect-end / battle-result /
// refresh），绝不能拼进命令 ID 或时间戳：一旦带上每次不同的东西，
// 幂等键就退化成「每次都不同」，重试会重复执行（文档 7.1）。
func SystemActorKey(kind string) string {
	return "sys:" + kind
}

// actorKeyOf 从计划推导 actor_key。
//
// 已知局限：domain.MutationPlan 只有 Key.PlayerID，没有 actor kind 字段
// （本任务不允许改 domain）。因此玩家命令走 "p:<id>"，playerID<=0 的
// 计划退化为 "sys:playerless"。真正接入系统命令时需要在 domain 侧补一个
// ActorKey 字段——本包不擅自猜 kind，否则会把不同系统命令错误地
// 合并成一个幂等域，那比「暂时不支持」危险得多。
func actorKeyOf(plan *domain.MutationPlan) string {
	if plan.Key.PlayerID > 0 {
		return ActorKey(plan.Key.PlayerID)
	}
	return SystemActorKey("playerless")
}

// FindReceipt 按 (world, actor, request) 查首次命令回执。
//
// 返回 (nil, false, nil) 表示确实没有回执——注意这与「查询失败」是
// 两件完全不同的事：前者说明命令没执行（或在执行中），后者说明
// 我们**无法判断**（文档 7.3 步骤 3：未查到不能直接证明旧事务没提交，
// 要锁住相同 scene 行等旧事务结束后再查）。
func (r *Repository) FindReceipt(ctx context.Context, worldID domain.WorldID, actorKey, requestID string) (*domain.CommandReceipt, bool, error) {
	if worldID == 0 || actorKey == "" || requestID == "" {
		return nil, false, fmt.Errorf("%w: 回执查询参数不完整", domain.ErrInvalidArgument)
	}
	var (
		rc   domain.CommandReceipt
		hash []byte
	)
	err := r.db.QueryRowContext(ctx,
		`SELECT command_id, payload_hash, result_code, result_json, committed_at_ms
		 FROM map_command WHERE world_id = ? AND actor_key = ? AND request_id = ?`,
		int64(worldID), actorKey, requestID).Scan(
		&rc.CommandID, &hash, &rc.ResultCode, &rc.ResultJSON, &rc.CommittedAtMS)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("查询命令回执 world=%d actor=%s request=%s: %w",
			worldID, actorKey, requestID, err)
	}
	if len(hash) != len(rc.PayloadHash) {
		return nil, false, fmt.Errorf("%w: 回执 %s payload_hash 长度=%d 期望=32",
			domain.ErrInvalidState, rc.CommandID, len(hash))
	}
	copy(rc.PayloadHash[:], hash)
	rc.Key = domain.CommandKey{WorldID: worldID, RequestID: requestID}
	// actor_key → player_id 是有损映射（"sys:*" 没有玩家），所以 Key.PlayerID
	// 只在 "p:" 前缀时回填，否则保持 0。调用方比对幂等键时应比较
	// (WorldID, RequestID) 或直接用 actorKey，不要依赖 PlayerID。
	if pid, ok := parseActorKey(actorKey); ok {
		rc.Key.PlayerID = pid
	}
	return &rc, true, nil
}

// FindReceiptByKey 是按 domain.CommandKey 查询的便捷包装。
//
// 内部统一走 actor_key：这是 runtime（CommandKey）与表结构（actor_key）
// 之间的唯一桥梁。将来如果 CommandEnvelope 增加 ActorKey 字段，
// 只需要改这一个函数的实现，调用方无感。
func (r *Repository) FindReceiptByKey(ctx context.Context, key domain.CommandKey) (*domain.CommandReceipt, bool, error) {
	if key.PlayerID <= 0 {
		return nil, false, fmt.Errorf("%w: 玩家命令的 player_id 必须为正, got %d", domain.ErrInvalidArgument, key.PlayerID)
	}
	return r.FindReceipt(ctx, key.WorldID, ActorKey(key.PlayerID), key.RequestID)
}

// parseActorKey 从 "p:<id>" 还原 player_id。
// 手写解析而不是 fmt.Sscanf：后者会把 "p:12x" 解析成 12 而不报错。
func parseActorKey(actorKey string) (int64, bool) {
	const prefix = "p:"
	if len(actorKey) <= len(prefix) || actorKey[:len(prefix)] != prefix {
		return 0, false
	}
	var id int64
	for _, r := range actorKey[len(prefix):] {
		if r < '0' || r > '9' {
			return 0, false
		}
		id = id*10 + int64(r-'0')
		if id < 0 {
			// 溢出：非法 actor_key，按未知玩家处理。
			return 0, false
		}
	}
	return id, true
}
