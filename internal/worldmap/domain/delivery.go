package domain

import (
	"context"
	"time"
)

// delivery.go 承载回城资产交接的领域契约（文档第 13、18.2 章）：
// DeliveryApplier 接口、投递游标、Ack 与回执查询、应用结果
// 状态，以及业务去重键 DeliveryKey 与重试退避 RetryBackoff。
//
// 背景（13.1）：地图 MySQL 事务与玩家 Redis CAS 是两个提交
// 边界，跨系统不宣称单事务“恰好一次”。方案是“持久化交接单
// + 至少一次投递 + 玩家原子去重应用”，业务上实现一次发奖
// （INV-07：同一交接单最多加一次背包；未确认的交接单可
// 持续重投）。domain 层只描述这套协议的类型与纯函数，
// 不持有任何连接。

// DeliveryKeyPrefix 是交接单业务去重键前缀（文档 13.2）。
// 玩家侧 Ext["mapdispatch"] 回执与 ClaimBiz 用它做原子去重。
// 前缀单独成常量：测试与日志引用同一处，避免两处各写一遍
// 字符串后悄悄漂移。
const DeliveryKeyPrefix = "slg:delivery:"

// DeliveryKey 返回交接单的业务去重键（文档 13.2）：
//
//	slg:delivery:{delivery_id}
//
// delivery_id 由服务端 UUID 分配、全局唯一；键只做拼接不
// 做校验——UUID 分配失败时根本不应该创建交接单，那是上游
// 的 bug，在这里返回错误只会把故障点藏得更深。
func DeliveryKey(deliveryID string) string {
	return DeliveryKeyPrefix + deliveryID
}

// DeliveryRetryMinMS / DeliveryRetryMaxMS 对应配置
// delivery_retry_min / delivery_retry_max（文档 17.1）。
// 以毫秒 int64 保存而不是 time.Duration：配置表、数据库
// next_retry_ms、告警年龄计算全部用毫秒，只在真正等待前
// 才转换成 Duration（见 RetryBackoff 关于溢出的说明）。
const (
	DeliveryRetryMinMS int64 = 1_000
	DeliveryRetryMaxMS int64 = 60_000
)

// RetryBackoff 计算第 attempt 次重试前的等待时间：指数退避，
// 下限 1s、上限 60s（文档 17.1）。
//
// attempt 从 1 开始计数：第 1 次重试等 1s，之后 2s、4s、
// 8s……60s 封顶。序列即 1s<<(attempt-1) 毫秒再夹紧。
//
// 两个 Go 知识点：
//
//   - 位运算翻倍：1s << n 等价于 ×2^n，比循环乘 2 更直白，
//     也更容易在注释里对上“指数退避”的定义。
//   - 移位前必须夹紧计数：Go 的移位计数若大于等于位宽，
//     结果不是报错而是 0—— attempt=100 时 1000<<99 会静默
//     变成 0 等待，重试立即重试形成忙循环。由于 1s<<6=64s
//     已超过 60s 上限，shift>=6 一律封顶，顺带消除了这个坑。
//
// time.Duration 与毫秒 int64 的边界：Duration 是 int64 纳秒。
// 把毫秒数转 Duration 必须显式乘 time.Millisecond（1e6）——
// 直接 time.Duration(ms) 会把毫秒当成纳秒，差六个数量级，
// 且编译期无警告。反过来，把持久化的毫秒时间戳（当前约
// 1.7e12）转 Duration 虽然到 2262 年之前不会溢出，但这个
// “刚好不出”没有任何安全边际，中间一旦再混入乘法就爆。
// 因此约定：持久层永远存毫秒 int64，只有确定是“时长”
// （如本函数 ≤60s 的退避）才转 Duration。
func RetryBackoff(attempt int32) time.Duration {
	if attempt < 1 {
		// 0/负 attempt 按首次重试处理，绝不返回 0——
		// 0 等待意味着调用方会立刻重投，把退避变成轮询。
		attempt = 1
	}
	// 1s<<6 = 64s > 60s，因此 shift>=6 的结果必被夹紧到上限。
	const maxShift = 6
	shift := attempt - 1
	if shift >= maxShift {
		return time.Duration(DeliveryRetryMaxMS) * time.Millisecond
	}
	ms := DeliveryRetryMinMS << uint(shift)
	if ms > DeliveryRetryMaxMS {
		ms = DeliveryRetryMaxMS
	}
	return time.Duration(ms) * time.Millisecond
}

// NextRetryAtMS 计算下次重试时刻（毫秒时间戳）。
// 调用方持有 nowMS（命令/扫描时一次获取，9.2），
// 加上本次退避得到 next_retry_ms 写入 Delivery。
func NextRetryAtMS(nowMS int64, attempt int32) int64 {
	return nowMS + RetryBackoff(attempt).Milliseconds()
}

// DeliveryApplyStatus 交接单应用结果状态（文档 13.2/13.5）。
// 数值会进日志与对账，不重排。
type DeliveryApplyStatus int32

const (
	// DeliveryApplyUnspecified 零值非法。
	DeliveryApplyUnspecified DeliveryApplyStatus = 0
	// DeliveryApplyApplied 本次新应用并提交成功。
	DeliveryApplyApplied DeliveryApplyStatus = 1
	// DeliveryApplyAlreadyApplied 玩家已有回执（同
	// delivery_id 同 hash），幂等返回，不重复加奖（INV-07）。
	DeliveryApplyAlreadyApplied DeliveryApplyStatus = 2
	// DeliveryApplyRetryableBlocked 可重试阻塞：背包满、
	// 数量超限、玩家提交结果未知、查询无回执。保留 Cargo
	// 和槽位，按退避重投（13.2：不先发邮件兜底）。
	DeliveryApplyRetryableBlocked DeliveryApplyStatus = 3
	// DeliveryApplyQuarantined 永久错误：同 delivery_id
	// 不同 payload（13.5）。隔离告警，不自动覆盖旧回执，
	// 交运维调查——自动“修复”会掩盖数据损坏。
	DeliveryApplyQuarantined DeliveryApplyStatus = 4
)

// Valid 报告是否为已知状态。
func (s DeliveryApplyStatus) Valid() bool {
	switch s {
	case DeliveryApplyApplied, DeliveryApplyAlreadyApplied,
		DeliveryApplyRetryableBlocked, DeliveryApplyQuarantined:
		return true
	default:
		return false
	}
}

// DeliveryApplyResult 是一次投递/查询的结果（文档 13.2）。
// Status 决定 Map 侧后续动作；Reason 是稳定文案，入日志与
// 对账；版本与摘要在成功路径上用于对账。
type DeliveryApplyResult struct {
	Status DeliveryApplyStatus
	// Reason 人类可读原因（稳定文案）。成功时可为空；
	// 失败时非空，便于告警定位是“背包满”还是“无回执”。
	Reason string
	// PlayerVersion 玩家侧提交版本（Applied/AlreadyApplied
	// 时有效）。Map 对账用：APPLIED 状态但玩家权威收据
	// 不存在时生成对账异常（19.6）。
	PlayerVersion int64
	// ReceiptHash 玩家侧回执摘要。与 Map 持久化的
	// payload_hash 不一致即 Quarantined（13.5）。
	ReceiptHash [32]byte
}

// DeliveryAck 是 Map 收到的应用确认（文档 13.2 步骤 6：
// “Map 收到 Applied（包含 delivery_id/hash 和玩家提交
// 版本）”）。hash 必须与持久单一致才置 APPLIED——
// 成功确认必须发生在玩家 CAS 成功之后（13.2 步骤 4）。
type DeliveryAck struct {
	DeliveryID    string
	PayloadHash   [32]byte
	PlayerVersion int64
	// AckAtMS Map 侧收到确认的时刻，用于 oldest_age
	// 告警与对账时间线（17.3）。
	AckAtMS int64
}

// Matches 校验 Ack 与交接单一致：同 delivery_id 且
// payload_hash 相同。hash 不同说明同 id 不同 payload，
// 按 13.5 隔离而不是覆盖旧回执。
func (a DeliveryAck) Matches(d *Delivery) bool {
	if d == nil {
		return false
	}
	return a.DeliveryID == d.DeliveryID && a.PayloadHash == d.PayloadHash
}

// DeliveryCursor 是 DeliveryWorker 扫描未完成交接单的
// keyset 游标（文档 13.2 步骤 2）。
//
// 为什么用 keyset 而不是 OFFSET 分页：积压增长时 OFFSET
// 会让“跳过前 N 条”的成本随 N 线性上升，且扫描途中新插入
// 的单据会导致漏行/重复行。按 (created_at_ms, delivery_id)
// 字典序推进，每批从上一批最后一条之后开始，成本只与批
// 大小相关。Limit 限制单批数量，防止一次拉取过多挤占
// 玩家资产应用配额（17.2）。
type DeliveryCursor struct {
	WorldID WorldID
	// AfterCreatedAtMS/AfterDeliveryID 组成 keyset 位置：
	// 只取 (created_at, delivery_id) 字典序更大的行。
	AfterCreatedAtMS int64
	AfterDeliveryID  string
	// Limit 单批上限；<=0 时由调用方按配置补默认值。
	Limit int32
}

// Advance 用本批最后一条推进游标。传入 nil 是调用方 bug
// （空批次不应推进），保持静默以免把扫描循环打断成 panic。
func (c *DeliveryCursor) Advance(d *Delivery) {
	if d == nil {
		return
	}
	c.AfterCreatedAtMS = d.CreatedAtMS
	c.AfterDeliveryID = d.DeliveryID
}

// DeliveryReceiptQuery 是回执查询（文档 13.2 步骤 5：
// 缓存提交未知时“从权威快照查回执并比较 hash，不能直接
// 再次加奖”）。PayloadHash 带 Map 侧持久化摘要，玩家侧
// 回执与它不一致即永久错误。
type DeliveryReceiptQuery struct {
	WorldID     WorldID
	DeliveryID  string
	PayloadHash [32]byte
}

// DeliveryApplier 是玩家侧交接应用接口（文档 18.2）。
//
//	Apply(ctx, Delivery) (DeliveryApplyResult, error)
//	Query(ctx, DeliveryReceiptQuery) (DeliveryApplyResult, error)
//
// 实现方在 Game 侧（内部 ApplyMapDelivery）。返回的 error
// 仅表示“这次调用本身失败”（网络/超时），调用方按退避
// 重投；业务结论一律看 DeliveryApplyResult.Status——
// 把“背包满”写成 error 会让调用方无法区分该重试还是该
// 换策略。
//
// Query 的语义：玩家无回执时返回 RetryableBlocked
// （Reason 说明无回执），Map 重新投递 Apply；有回执且
// hash 一致返回 AlreadyApplied；有回执 hash 不符返回
// Quarantined。
type DeliveryApplier interface {
	Apply(ctx context.Context, d Delivery) (DeliveryApplyResult, error)
	Query(ctx context.Context, q DeliveryReceiptQuery) (DeliveryApplyResult, error)
}

// NewRetryableBlocked 构造可重试阻塞结果。稳定文案单独
// 成构造函数，避免每个调用点各拼一句字符串后对账对不上。
func NewRetryableBlocked(reason string) DeliveryApplyResult {
	return DeliveryApplyResult{Status: DeliveryApplyRetryableBlocked, Reason: reason}
}

// NewQuarantined 构造隔离结果（payload 冲突等永久错误）。
func NewQuarantined(reason string) DeliveryApplyResult {
	return DeliveryApplyResult{Status: DeliveryApplyQuarantined, Reason: reason}
}
