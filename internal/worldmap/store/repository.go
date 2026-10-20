// Package store 是 SLG 大地图子系统的 MySQL 持久化层（设计文档第 7、14、16 章）。
//
// 职责边界（文档 18.1）：
//   - 只做「把 domain.MutationPlan 原子地落到 InnoDB」与「把权威状态读回内存」；
//   - 不持有业务规则，不回调 runtime，不产生内存状态；
//   - 所有读写都显式带 world_id：大世界按 world 分片，缺了它跨世界查询必然错。
//
// 依赖方向：store → domain（只依赖领域类型），不依赖 runtime。
//
// 为什么连 runtime.Committer 也不 import：Go 的接口是结构化的，
// 只要 Commit 的方法集逐字相同，*Repository 就自动满足 runtime.Committer。
// 用本地接口做编译期断言，既拿到了同样的保证，又不会让持久化层
// 被 runtime 的内部演化绑住（见下方 committer 与 Logger 的说明）。
// 装配处 server.go 可以再补一行 runtime.Committer 断言做跨包确认。
package store

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"game_study/internal/worldmap/domain"
)

// Options 是仓储的运行参数。零值字段由 NewRepository 补默认值，
// 调用方只需要填真正关心的项。
type Options struct {
	// OwnerID 是本进程的世界租约标识（map_world.owner_id）。
	// 提交事务时会与库里的 owner 比对：比对失败说明世界已被别的进程接管，
	// 本进程的写入必须被栅栏拦住（文档 16.2）。
	OwnerID string
	// LeaseTTL 是租约时长，默认 30s（文档 16.2 默认值）。
	LeaseTTL time.Duration
	// TxTimeout 是单个业务事务的超时，默认 5s。
	// 短事务是这套设计的硬要求：长事务会放大行锁等待、提高死锁概率，
	// 也会让「结果未知」的解析窗口变长。
	TxTimeout time.Duration
	// MaxRetries 是死锁/锁等待超时（1213/1205）的最大重试次数，默认 3。
	MaxRetries int
}

// 默认参数。写成常量而不是字面量：LeaseTTL 同时被 lease.go 与测试引用，
// 两处各写一遍 30*time.Second 迟早会漂移。
const (
	defaultLeaseTTL   = 30 * time.Second
	defaultTxTimeout  = 5 * time.Second
	defaultMaxRetries = 3
	// retryBaseBackoff 是重试退避的基数：第 n 次重试等待 base<<n
	// （5ms/10ms/20ms）。指数退避的意义在于把并发事务的相位错开，
	// 固定间隔重试会让两个互相死锁的事务每次都撞在同一个时刻。
	retryBaseBackoff = 5 * time.Millisecond
)

// map_world.status 取值（与 DDL 注释一致）。
//
// 导出它们是因为恢复流程需要调 SetWorldStatus 把世界置 READY，
// 让调用方自己写魔法数字 1/2/3 迟早会写错一个。
const (
	// WorldStatusReady 世界可读写，接受新命令。
	WorldStatusReady int32 = 1
	// WorldStatusRecovering 正在加载/补处理，拒绝新写。
	WorldStatusRecovering int32 = 2
	// WorldStatusNotReady 不可服务（未持有租约或已停服）。
	WorldStatusNotReady int32 = 3
)

// 仓储层哨兵错误。
//
// Go 知识点：哨兵错误是「可判定性」的载体——调用方用 errors.Is 判断
// 类别，用 %w 包装携带上下文。绝不能靠 err.Error() 字符串匹配：
// 文案随时会改，字符串匹配会在某个版本悄悄失效。
var (
	// ErrStaleEpoch 计划携带的 world epoch 已过期（世界被重新加载过）。
	// 这是栅栏的核心：旧代次的计划必须被拒绝，不能写进新世界。
	ErrStaleEpoch = errors.New("worldmap/store: 世界 epoch 已过期")
	// ErrWorldNotFound map_world 没有该世界的行。
	// 世界必须先由运维/初始化流程建行，store 不会隐式创建业务世界。
	ErrWorldNotFound = errors.New("worldmap/store: 世界行不存在")
	// ErrLeaseHeld 世界租约仍在有效期内且属于别的进程（文档 16.2）。
	ErrLeaseHeld = errors.New("worldmap/store: 世界租约被其他进程持有")
	// ErrLeaseLost 续租/释放时 owner 或 epoch 不匹配：本进程已经不是持有者。
	ErrLeaseLost = errors.New("worldmap/store: 世界租约已丢失")
	// ErrLeaseExpired 事务内发现租约已过期：即使 owner 还是自己，
	// 也不能再写——接管者可能正在同一个窗口里递增 epoch。
	ErrLeaseExpired = errors.New("worldmap/store: 世界租约已过期")
	// ErrSceneNotFound map_scene 缺少该场景行。
	ErrSceneNotFound = errors.New("worldmap/store: 场景行不存在")
	// ErrCollectNotFound map_collect 缺少该会话行。
	ErrCollectNotFound = errors.New("worldmap/store: 采集会话不存在")
	// ErrDeliveryNotFound map_delivery 缺少该交接单。
	ErrDeliveryNotFound = errors.New("worldmap/store: 交接单不存在")
	// ErrDeliveryNotPending 交接单已不是 PENDING，不能重复标记。
	ErrDeliveryNotPending = errors.New("worldmap/store: 交接单不是待投递状态")
	// ErrRefreshProgressNotFound 刷新进度不存在。
	ErrRefreshProgressNotFound = errors.New("worldmap/store: 刷新进度不存在")
)

// queryer 是「能查询」的最小接口，*sql.DB 与 *sql.Tx 都满足。
//
// 存在的理由：恢复流程必须在一个只读事务里跑完全部查询才能拿到
// 一致性快照；如果把加载函数写死成 r.db，恢复就只能逐表读，
// 每张表各自一个快照——那正是文档 16.1 要避免的「半新半旧」。
type queryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// Logger 是仓储使用的日志接口（与 runtime.Logger 方法集逐字相同）。
//
// 为什么不在 store 里直接 import runtime：文档 18.1 规定依赖方向是
// store → domain，runtime → store 的装配在 server.go 完成。如果 store
// 反过来 import runtime，两个包就绑成了一个编译单元——runtime 里任何
// 一次内部重构都会让持久化层跟着编译不过。
//
// Go 的接口是**结构化**的：*log.Logger 同时满足这里的 Logger 与
// runtime.Logger，调用方不需要任何适配代码。
type Logger interface {
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

// nopLogger 丢弃日志（logger 传 nil 时使用）。
type nopLogger struct{}

func (nopLogger) Info(string, ...any)  {}
func (nopLogger) Warn(string, ...any)  {}
func (nopLogger) Error(string, ...any) {}

// committer 是与 runtime.Committer 方法集逐字相同的本地接口。
//
// 为什么不写 var _ runtime.Committer = (*Repository)(nil)：见 Logger 的
// 说明——store 不导入 runtime。结构化类型让这个本地断言具备与导入
// 完全等价的保证：只要方法集相同，*Repository 就自动满足
// runtime.Committer。装配处（server.go）可以再补一行
// `var _ runtime.Committer = store.NewRepository(...)` 作为跨包确认。
type committer interface {
	Commit(ctx context.Context, plan *domain.MutationPlan) domain.CommitResult
}

// Repository 是 MySQL 仓储。并发安全：所有状态都在库里，
// 结构体字段构造后只读，可被多个事务 worker 共享（tx worker pool 就是
// 这么用的：N 个 goroutine 调用同一个 Committer）。
type Repository struct {
	db     *sql.DB
	logger Logger
	opts   Options
	// nowMS 是 UTC 毫秒时钟。抽成字段是为了测试注入假时钟——
	// 租约测试不能真的 sleep 30 秒。
	nowMS func() int64
}

// 编译期断言：Repository 必须满足 runtime.Committer（文档 18.2 的 Store 方向）。
//
// 这行代码没有运行时开销，却能在「Commit 签名被改坏」时立刻编译失败，
// 而不是等 server.go 装配时才发现。接口实现靠结构化类型（duck typing）
// 而不是显式声明，断言是 Go 里给隐式契约补一张「显式收据」的标准做法。
var _ committer = (*Repository)(nil)

// NewRepository 构造仓储。
//
// database 必须是 pkg/db 已配置好池参数的 *sql.DB（不要在这里另造连接池）；
// logger 可以为 nil，内部退化为 runtime.NopLogger，避免每个日志调用点
// 都写 if r.logger != nil。
func NewRepository(database *sql.DB, logger Logger, opts Options) *Repository {
	if logger == nil {
		logger = nopLogger{}
	}
	if opts.LeaseTTL <= 0 {
		opts.LeaseTTL = defaultLeaseTTL
	}
	if opts.TxTimeout <= 0 {
		opts.TxTimeout = defaultTxTimeout
	}
	if opts.MaxRetries < 0 {
		// 负数是配置错误，按「不重试」处理而不是放大成无限重试。
		opts.MaxRetries = 0
	}
	return &Repository{
		db:     database,
		logger: logger,
		opts:   opts,
		nowMS:  func() int64 { return time.Now().UTC().UnixMilli() },
	}
}

// OwnerID 返回本仓储配置的持有者标识（监控/日志用）。
func (r *Repository) OwnerID() string { return r.opts.OwnerID }

// now 返回当前 UTC 毫秒。持久化时间统一用毫秒 int64：
// TIMESTAMP 列受会话时区影响，BIGINT 毫秒不受，跨重启也更稳。
func (r *Repository) now() int64 {
	if r.nowMS == nil {
		return time.Now().UTC().UnixMilli()
	}
	return r.nowMS()
}

// newEventID 生成 32 位十六进制事件 ID（CHAR(36) 足够存放）。
//
// 用 crypto/rand 而不是 math/rand：事件 ID 会进 outbox / 资源流水，
// 碰撞意味着两条不同的业务事实共用一个主键——这是数据损坏，
// 不能用「概率很低」来搪塞。也不引第三方 uuid 包（当前是 indirect 依赖），
// 16 字节随机数转十六进制就是标准 UUIDv4 的熵，只是格式不同。
func newEventID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("生成事件 ID: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}

// sleepCtx 是可取消的退避等待。
//
// 不直接用 time.Sleep：停机时 ctx 取消必须立刻返回，否则关闭流程要
// 白等完退避；用 time.NewTimer + select 是 Go 里等待「超时或取消」的
// 标准写法（defer Stop 防止定时器泄漏）。
func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// retryBackoff 返回第 attempt 次（0-based）重试前的等待。
// 5ms/10ms/20ms...用移位表达翻倍，语义最直白。
func retryBackoff(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	if attempt > 6 {
		// 移位超过 6 次已远超预期（320ms），钳住防止溢出式无意义增长。
		attempt = 6
	}
	return retryBaseBackoff << uint(attempt)
}
