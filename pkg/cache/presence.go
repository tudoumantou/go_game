// pkg/cache/presence.go —— 在线状态与离线暂存（设计文档 13.3 模式① / ②）
//
// 架构职责：
//
//	模式① 在线归属：哪个游戏服节点持有某玩家的活跃连接（顶号/路由/推送）；
//	模式② 离线暂存：玩家不在线时先攒在 Redis List，登录时一次 drain。
//
// 协作关系：key 由 keygen.OnlineKey 与本文件 pendKey 生成；登录流程调
// TrySetOnline 抢占归属、PendDrain 拉取暂存；网关/游戏服心跳调
// RefreshOnline 续期。
// 关键取舍：状态带 TTL 而非永久——节点崩溃后最多 TTL 时间自动视为
// 离线，无需节点主动清理；抢占用 Lua 保证「判断+写入」原子，管道化
// 的 GET+SET 做不到（并发双登会双双成功）。
package cache

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Presence 在线状态与离线暂存。
//
// 无状态封装，可被网关与游戏服并发使用（Redis 命令原子）。
type Presence struct {
	client redis.UniversalClient
}

// NewPresence 创建 Presence。
func NewPresence(client redis.UniversalClient) *Presence {
	return &Presence{client: client}
}

// SetOnline 标记玩家在线（value = 所在节点 ID）。
//
// 强制覆盖语义（SET 而非 SETNX）：管理员踢下线后重新登录等场景需要
// 无条件写；普通登录请用 TrySetOnline（带归属冲突检测）。
// TTL 由调用方心跳续期（RefreshOnline）——崩溃节点最多占用 TTL 时间
// 的所有权，之后自动释放。
func (p *Presence) SetOnline(ctx context.Context, playerID int64, nodeID string, ttl time.Duration) error {
	// ttl <=0 兜底 5 分钟：与默认心跳周期匹配，同时防误配 0（永不过期）
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	return p.client.Set(ctx, OnlineKey(playerID), nodeID, ttl).Err()
}

// tryOnlineScript 原子占位：旧值存在且属于其它节点时拒绝，否则写入本节点。
//
// 用 GET + SET 的管道做不到「判断与写入」原子，两台游戏服同时登录同一角色时
// 会双双成功、各自认为是权威会话。这里用 Lua 把两步合成一次执行。
// Go 知识点：redis.NewScript 返回的 Script 会缓存 Lua 源码，Run 时优先
// EVALSHA（已加载则 O(1) 传输），NOSCRIPT 时自动回退 EVAL——比每次
// 手拼 Eval 少传一次完整脚本体。
// 返回 {code, owner}：code=1 抢占成功；code=0 被占（owner 是当前
// 持有者）。同节点重连时 cur == ARGV[1]，走「否则」分支放行，实现
// 「本节点顶掉自己的旧连接」。
var tryOnlineScript = redis.NewScript(`
local cur = redis.call('GET', KEYS[1])
if cur and cur ~= ARGV[1] then
  return {0, cur}
end
redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
return {1, ARGV[1]}
`)

// TrySetOnline 原子占用在线归属。
//
// 返回 ok=false 时 owner 是当前持有该角色的节点；同节点重连（旧会话未过期）
// 视为成功，允许本节点顶掉自己的旧连接。
// 结果解析：Lua 返回的 table 在 go-redis 里是 []any——整数为 int64，
// 字符串可能是 string 或 []byte（取决于 RESP 协议与解析路径），所以
// 对两种形态都做兼容。
// Go 知识点：res[0].(int64) 是**丢弃 ok 的类型断言**——失败时 code 取
// 零值 0，天然落到「抢占失败」分支，安全失败（fail-safe）；res[1] 用
// 类型 switch 分别处理 string/[]byte，比连环断言更清晰。
func (p *Presence) TrySetOnline(ctx context.Context, playerID int64, nodeID string, ttl time.Duration) (ok bool, owner string, err error) {
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	res, err := tryOnlineScript.Run(ctx, p.client, []string{OnlineKey(playerID)}, nodeID, ttl.Milliseconds()).Slice()
	if err != nil {
		return false, "", err
	}
	if len(res) != 2 {
		return false, "", fmt.Errorf("unexpected presence result: %v", res)
	}
	// ok 由 0/1 表示；owner 可能是 string 或 []byte（不同客户端实现）。
	code, _ := res[0].(int64)
	switch v := res[1].(type) {
	case string:
		owner = v
	case []byte:
		owner = string(v)
	}
	return code == 1, owner, nil
}

// RefreshOnline 续期在线状态。
//
// 心跳调用；key 不存在时 Expire 返回 false 但不报错——此时应按「已
// 掉线」处理（心跳发现 key 没了，说明被顶号或已清理）。
func (p *Presence) RefreshOnline(ctx context.Context, playerID int64, ttl time.Duration) error {
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	return p.client.Expire(ctx, OnlineKey(playerID), ttl).Err()
}

// IsOnline 查询玩家在线节点；ok=false 表示离线或状态过期。
//
// 容错取舍：Redis 任何错误（含键不存在的 redis.Nil）统一按离线返回，
// 不区分「真离线」与「查不到」——在线状态是路由提示而非权威数据，
// 调用方（推送/顶号）需容忍误判；权威校验走 TrySetOnline。
func (p *Presence) IsOnline(ctx context.Context, playerID int64) (nodeID string, ok bool) {
	val, err := p.client.Get(ctx, OnlineKey(playerID)).Result()
	if err != nil {
		return "", false
	}
	return val, val != ""
}

// SetOffline 清除在线状态。
//
// 主动下线（正常登出/踢线）调用；崩溃场景靠 TTL 自然过期，无需调用。
func (p *Presence) SetOffline(ctx context.Context, playerID int64) error {
	return p.client.Del(ctx, OnlineKey(playerID)).Err()
}

// PendPush 离线暂存（List，capped maxLen，TTL 7d）。
//
// 三步一条事务（TxPipeline）：RPush 追加 + LTrim 截断 + Expire 续期
// ——分开执行时中途失败会留下「超长且永不过期」的脏 key。
// LTrim -maxLen -1：只保留**最新** maxLen 条（负数索引从尾部算），
// 离线再久的玩家也不会撑爆内存。
// maxLen<=0 / ttl<=0 走默认（200 条 / 7 天）：调用方偷懒时行为可预期。
// Go 知识点：TxPipeline = MULTI/EXEC 事务，三条命令一次提交、原子
// 生效，同时把 3 个 RTT 压成 1 个。
func (p *Presence) PendPush(ctx context.Context, biz string, playerID int64, value []byte, maxLen int64, ttl time.Duration) error {
	if maxLen <= 0 {
		maxLen = 200
	}
	if ttl <= 0 {
		ttl = 7 * 24 * time.Hour
	}
	key := pendKey(biz, playerID)
	pipe := p.client.TxPipeline()
	pipe.RPush(ctx, key, value)
	pipe.LTrim(ctx, key, -maxLen, -1)
	pipe.Expire(ctx, key, ttl)
	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("pend push %s: %w", key, err)
	}
	return nil
}

// PendDrain 取出并清空暂存消息（登录 drain）。
//
// LRange + Del 放同一事务：要么取到并删掉，要么都没发生——避免
// 「取了没删」（重复推送）与「删了没取」（消息丢失）两个坏结局。
// Go 知识点：TxPipeline 的各命令结果要在 Exec **之后**通过命令对象
// 的 Result() 读取；Exec 报错时所有结果都不可信，直接返回 error。
func (p *Presence) PendDrain(ctx context.Context, biz string, playerID int64) ([][]byte, error) {
	key := pendKey(biz, playerID)
	pipe := p.client.TxPipeline()
	rangeCmd := pipe.LRange(ctx, key, 0, -1)
	pipe.Del(ctx, key)
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, fmt.Errorf("pend drain %s: %w", key, err)
	}
	vals, err := rangeCmd.Result()
	if err != nil {
		return nil, err
	}
	out := make([][]byte, 0, len(vals))
	for _, v := range vals {
		out = append(out, []byte(v))
	}
	return out, nil
}

// PendLen 暂存数量（运维/监控用）。
//
// 涨得太快说明消费端（登录 drain）链路有阻塞，或推送侧失控。
func (p *Presence) PendLen(ctx context.Context, biz string, playerID int64) (int64, error) {
	return p.client.LLen(ctx, pendKey(biz, playerID)).Result()
}

// pendKey 暂存 key：pend:{biz}:{playerID}。
//
// biz 区分暂存域（mail/chat/...），同玩家不同域各自一个 List。
func pendKey(biz string, playerID int64) string {
	return fmt.Sprintf("pend:%s:%d", biz, playerID)
}
