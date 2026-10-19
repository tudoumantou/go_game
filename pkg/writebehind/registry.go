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
// Data 是模块自己的 JSON 表示，框架只负责搬运，不解释其结构。
type ModuleRow struct {
	PlayerID int64
	Data     json.RawMessage
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
type Persister interface {
	Name() string
	Validate(data json.RawMessage) error
	PersistBatch(ctx context.Context, tx *sql.Tx, rows []ModuleRow) error
	Load(ctx context.Context, tx *sql.Tx, playerID int64) (json.RawMessage, error)
}

var (
	persisterMu sync.RWMutex
	persisters  = make(map[string]Persister)
)

// RegisterPersister 注册模块持久化实现。
//
// 通常在模块包的 init 中调用：只要包被导入，冷启动读取与落库就同时生效，
// 避免出现「已注册协议但数据不落库」的半接入状态。
//
// 重名是编码错误，直接 panic —— 与 module.Registry 的协议号冲突处理一致。
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
func LookupPersister(name string) (Persister, bool) {
	persisterMu.RLock()
	defer persisterMu.RUnlock()
	p, ok := persisters[name]
	return p, ok
}

// Persisters 返回按名字排序的副本，顺序稳定便于测试与日志比对。
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
func resetPersisters() {
	persisterMu.Lock()
	defer persisterMu.Unlock()
	persisters = make(map[string]Persister)
}
