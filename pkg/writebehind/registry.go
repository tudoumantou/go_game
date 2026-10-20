// pkg/writebehind/registry.go —— 模块持久化扩展点注册表
//
// 玩法模块通过实现 Persister 并 RegisterPersister 接入统一落库：
// 之后该模块的快照会自动进入 Redis 提交、冷启动装载与合批写，
// worker/snapshot 等横切文件零改动（开闭原则：对扩展开放、对修改关闭）。
//
// 注册时机：通常在模块包的 init 中调用——只要包被 import（协议注册
// 保证它会被 import），数据链路即生效，杜绝「协议通了但数据不落库」
// 的半接入状态。注册表读多写少，用 RWMutex 保护。
package writebehind

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
)

// ModuleRow 是某个玩家在某个模块下的持久化片段。
//
// Data 是模块自己的 JSON 表示，框架只负责搬运，不解释其结构——
// 这是框架与模块的边界：格式由 Persister 的编解码负责，框架只保证
// 「同一模块的多行一起送达 PersistBatch」。
type ModuleRow struct {
	// PlayerID 玩家 ID（分组与排序键，落库顺序确定）。
	PlayerID int64
	// Data 该玩家此模块的 JSON 快照（来自 Snapshot.Ext[name]）。
	Data json.RawMessage
}

// Persister 是模块自有持久化扩展点。
//
// 实现并注册它之后，模块状态会自动进入统一快照、Redis 提交队列和落库批次，
// 不需要再修改 worker / snapshot / cache_load 这些横切文件。
//
// 约定：
//   - Name 同时是 Snapshot.Ext 的键，必须全局唯一且在版本升级间保持稳定；
//   - Validate 只做语义校验，格式由 json.Unmarshal 保证；
//   - PersistBatch 必须只使用传入的事务，不得自己 Commit；
//   - Load 只读，返回 nil 表示该玩家没有这个模块的数据。
//
// Go 细节：接口方法全部显式接收 context 与资源（tx），实现不得
// 私自开连接/起事务——事务边界由 worker.Persist 统一控制，
// 模块越界提交会破坏「多模块同一事务」的原子性保证。
type Persister interface {
	Name() string
	Validate(data json.RawMessage) error
	PersistBatch(ctx context.Context, tx *sql.Tx, rows []ModuleRow) error
	Load(ctx context.Context, tx *sql.Tx, playerID int64) (json.RawMessage, error)
}

var (
	// persisterMu 保护 persisters。RWMutex 而非 Mutex：查找发生在
	// 每条快照的 Validate/Load 路径（读远多于写），读写锁让并发读
	// 不互斥；写只发生在启动注册与测试重置。
	persisterMu sync.RWMutex
	// persisters 模块名 → 实现；启动期写满后运行期只读。
	persisters = make(map[string]Persister)
)

// RegisterPersister 注册模块持久化实现。
//
// 通常在模块包的 init 中调用：只要包被导入，冷启动读取与落库就同时生效，
// 避免出现「已注册协议但数据不落库」的半接入状态。
//
// 重名是编码错误，直接 panic —— 与 module.Registry 的协议号冲突处理一致。
//
// Go 细节：先做 nil/空名校验（编程错误）再拿锁，缩小临界区；
// panic 文案带 name，崩溃日志可直接定位冲突模块。
func RegisterPersister(p Persister) {
	if p == nil {
		panic("writebehind: nil persister")
	}
	name := p.Name()
	if name == "" {
		panic("writebehind: persister with empty name")
	}
	persisterMu.Lock()
	defer persisterMu.Unlock()
	if _, ok := persisters[name]; ok {
		panic(fmt.Sprintf("writebehind: duplicate persister %q", name))
	}
	persisters[name] = p
}

// LookupPersister 按名字查询，未注册时返回 false。
//
// comma-ok 两返回值区分「未注册」与「零值实现」；RLock 允许多个
// 落库 goroutine 并发查表。热路径（每个 Ext 键一次），
// RLock 的读锁开销远低于互斥锁。
func LookupPersister(name string) (Persister, bool) {
	persisterMu.RLock()
	defer persisterMu.RUnlock()
	p, ok := persisters[name]
	return p, ok
}

// Persisters 返回按名字排序的副本，顺序稳定便于测试与日志比对。
//
// 返回副本而非内部 map/切片：外部遍历不影响注册表，也防止调用方
// 拿到可写视图。排序后顺序确定——Go 的 map 遍历顺序是随机的，
// 直接暴露会让日志/测试结果抖动。
//
// Go 细节：两次 make 分别按已知上界预分配 names 与 out 的容量，
// 避免 append 扩容；sort.Strings 是 O(n log n)，注册表规模在百级、
// 调用频率低，无需缓存排序结果。
func Persisters() []Persister {
	persisterMu.RLock()
	defer persisterMu.RUnlock()
	names := make([]string, 0, len(persisters))
	for name := range persisters {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([]Persister, 0, len(names))
	for _, name := range names {
		out = append(out, persisters[name])
	}
	return out
}

// resetPersisters 仅供包内测试清理注册表。
//
// 测试之间需要隔离注册状态（每个用例注册自己的假模块）；
// 小写开头 = 包外不可见，把「重置全局表」的能力关在包内，
// 防止生产代码误调把注册表清空。
func resetPersisters() {
	persisterMu.Lock()
	defer persisterMu.Unlock()
	persisters = make(map[string]Persister)
}
