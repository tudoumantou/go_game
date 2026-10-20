// pkg/cache/idtab.go —— ID 分配表（Redis INCR）
//
// 架构职责：全局唯一 ID 的发放（玩家 ID、订单号等），替代 MySQL
// AUTO_INCREMENT——发号不依赖单库，多进程/多分片并发申请不重复。
// 协作关系：与 idtable.go 是「分配 vs 存在性」的一对（见 IDTab 注释）；
// key 由 keygen.SeqKey 统一生成；Redis 故障时 Next 返回 error，由调用
// 方决定降级（创建类业务通常直接拒绝）。
// 关键取舍：INCR 单命令原子发号 + start 初始化用 SETNX——「初始化」与
// 「分配」两步都有并发竞争，分别用 SETNX/INCR 化解，无需 Lua。
package cache

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// IDTab 基于 Redis INCR 的 ID 分配器。
//
// 注意与 idtable.go 的 IDTable 区分：
//   - IDTab  —— **分配**新 ID（Redis String INCR，key = seq:{name}）
//   - IDTable —— **存在性**判断（Redis Set，key = idtab:{table}）
//
// 两者 key 前缀不同，避免 Redis 类型冲突（String vs Set）。
// 实例无状态（只存 client/name/start），可被多 goroutine 并发调用 Next。
type IDTab struct {
	client redis.UniversalClient
	name   string
	start  int64
}

// NewIDTab 创建 ID 分配器。name 是 ID 域（player/account/...）。
//
// start <= 0 表示不做初始化——沿用 Redis 现有计数继续发（用于已运行
// 的库，避免重启后把计数器重置回起点）。
func NewIDTab(client redis.UniversalClient, name string, start int64) *IDTab {
	return &IDTab{client: client, name: name, start: start}
}

// Next 分配下一个 ID。
//
// 返回的 ID 从 start 起严格递增、不复用（INCR 原子性保证）。
// 初始化语义：start > 0 时先把 key 置为 start-1，紧接着 INCR 得到
// start；SETNX 保证只有第一个到达者生效，后续并发调用看到已存在的
// key 直接 INCR 接力——不会把计数器重置。
// key 不过期（TTL=0）：ID 序列必须跨重启持久，一旦过期清零会重新
// 发出已落库的 ID，造成主键冲突/数据串号。
func (t *IDTab) Next(ctx context.Context) (int64, error) {
	key := SeqKey(t.name)
	// 首次分配时用 start 初始化（SETNX 保证并发安全）
	if t.start > 0 {
		// 错误故意忽略：并发下必然只有一个客户端初始化成功，其余的
		// 「失败」正说明 key 已存在，不是异常；硬报错反而会误伤。
		_ = t.client.SetNX(ctx, key, t.start-1, 0).Err()
	}
	// INCR 是 Redis 单命令，多进程并发调用也不会发出重复 ID
	id, err := t.client.Incr(ctx, key).Result()
	if err != nil {
		return 0, fmt.Errorf("idtab %s: %w", t.name, err)
	}
	return id, nil
}
