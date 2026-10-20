package runtime

import (
	"context"
	"errors"
	"sync"

	"game_study/internal/worldmap/application"
	"game_study/internal/worldmap/domain"
)

// ErrOverloaded 表示过载：邮箱满、回执名额耗尽、队列超限。
// 语义是「未接受」——上游可以稍后重试，且不需要写幂等回执
// （文档 15.3：格式非法与未接受都不写回执）。
var ErrOverloaded = errors.New("worldmap/runtime: 过载")

// ErrSceneClosed 表示场景已关闭或世界 NotReady。
var ErrSceneClosed = errors.New("worldmap/runtime: 场景已关闭")

// ErrPendingUnknown 表示上一个事务结果不可解析，场景已冻结。
// 调用方应提示客户端"操作处理中"，并在解析完成后重试（文档 7.3）。
var ErrPendingUnknown = errors.New("worldmap/runtime: 事务结果待解析")

// Committer 是持久化提交接口（文档 18.2 的 Store 方向）。
//
// 为什么 SceneLoop 只依赖这个窄接口，而不是具体仓储：
//   - M1 用 MemoryCommitter 跑通闭环，M2 换成 MySQL 实现时
//     场景循环一行都不用改；
//   - 测试要模拟"提交成功但回包丢失"（CommitResult.Unknown），
//     这只有替身能做到，真实数据库很难构造。
//
// 实现方必须在返回前完成深拷贝：计划里的切片/map 属于调用方，
// 异步落库时调用方可能已经复用它们。
type Committer interface {
	Commit(ctx context.Context, plan *domain.MutationPlan) domain.CommitResult
}

// ProjectionBatch 是一次提交后要广播的投影增量（文档 12.4）。
//
// 只携带键与版本，不携带完整对象：视图层需要详情时回查快照，
// 避免"投影消息里带业务字段"导致视图与权威状态长期不一致。
type ProjectionBatch struct {
	WorldID  domain.WorldID
	SceneID  domain.SceneID
	Revision uint64
	Enters   []domain.ObjectKey
	Updates  []domain.ObjectKey
	Leaves   []domain.ObjectKey
}

// ProjectionSink 是投影出口（文档 18.2）。
//
// 契约：TryPublish 返回错误不改变已提交业务，只表示"这一批没推出去"。
// 消费者通过 revision 缺口重新拉快照（文档 7.3），因此实现方
// 只需尽力而为，不做重试与阻塞。
type ProjectionSink interface {
	TryPublish(batch ProjectionBatch) error
}

// NoopSink 丢弃全部投影。M1 还没有 AOI 视图层，用它占位。
type NoopSink struct{}

// TryPublish 实现 ProjectionSink。
func (NoopSink) TryPublish(ProjectionBatch) error { return nil }

// MemorySink 是测试用投影收集器。
// 并发安全：SceneLoop 写、测试读，必须加锁。
type MemorySink struct {
	mu      sync.Mutex
	batches []ProjectionBatch
}

// TryPublish 记录一批投影。
func (s *MemorySink) TryPublish(batch ProjectionBatch) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.batches = append(s.batches, batch)
	return nil
}

// Batches 返回已记录的投影副本。
func (s *MemorySink) Batches() []ProjectionBatch {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]ProjectionBatch(nil), s.batches...)
}

// Reset 清空记录（测试用例之间复用 sink 时调用）。
func (s *MemorySink) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.batches = nil
}

// Logger 是运行时使用的日志接口。只依赖三个方法而不是具体 logger，
// 是为了让测试可以传一个把日志写进 slice 的假实现来断言
// "panic 时确实报了 fail-stop"。
type Logger interface {
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

// NopLogger 丢弃日志。
type NopLogger struct{}

func (NopLogger) Info(string, ...any)  {}
func (NopLogger) Warn(string, ...any)  {}
func (NopLogger) Error(string, ...any) {}

// PlannerFunc 把函数适配成 application.Planner，便于测试注入。
type PlannerFunc func(snap *application.SceneSnapshot, env domain.CommandEnvelope, cmd application.Command, nowMS int64) (*domain.MutationPlan, error)

// Plan 实现 application.Planner。
func (f PlannerFunc) Plan(snap *application.SceneSnapshot, env domain.CommandEnvelope, cmd application.Command, nowMS int64) (*domain.MutationPlan, error) {
	return f(snap, env, cmd, nowMS)
}
