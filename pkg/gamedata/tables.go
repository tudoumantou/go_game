// pkg/gamedata/tables.go —— 配置快照的通用读写（R10.3/R10.4）
//
// 设计要点：
//
//  1. **快照不可变**：一份 Tables 构造好后不再修改，热更新是「构造新快照 + 原子指针替换」。
//     读取方拿到的是某一时刻自洽的一整套表，不会出现「item 已更新、quest 还是旧的」。
//  2. **读取无锁**：Cur() 只做一次 atomic.Load，模块每次读配置都从当前快照取，
//     不缓存跨消息的 *Cfg 指针（热更新后旧指针仍指向旧快照，语义上属于「本条消息内一致」）。
//  3. **失败保留旧版本**：任何一张表解析或校验失败，整批不生效；上一版本保留在 previous，
//     可用 Rollback 回滚。
//  4. **外部输入**：LoadDir 从磁盘目录读 `<table>.json`。重新读编译期 embed **永远不会**
//     改变配置内容，热更新必须有新的外部输入，这是本实现不提供「Reload from embed」的原因。
package gamedata

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
)

// table 通用只读表视图：list 与 byID 指向同一批行，构造后不再修改。
type table[T any] struct {
	list []T
	byID map[int64]*T
}

func newTable[T any](list []T, idOf func(*T) int64) (table[T], error) {
	byID := make(map[int64]*T, len(list))
	for i := range list {
		id := idOf(&list[i])
		if _, dup := byID[id]; dup {
			return table[T]{}, fmt.Errorf("duplicate primary key %d", id)
		}
		byID[id] = &list[i]
	}
	return table[T]{list: list, byID: byID}, nil
}

// List 全量行。返回的切片由快照持有，调用方只读。
func (t table[T]) List() []T { return t.list }

// Get 按主键查询。
func (t table[T]) Get(id int64) (*T, bool) {
	c, ok := t.byID[id]
	return c, ok
}

// Len 行数。
func (t table[T]) Len() int { return len(t.list) }

// validate 快照自检：空表通常意味着配置写错或文件名对不上。
func (t table[T]) validate(name string) error {
	if len(t.list) == 0 {
		return fmt.Errorf("%s: empty table", name)
	}
	if len(t.byID) != len(t.list) {
		return fmt.Errorf("%s: duplicate primary key", name)
	}
	return nil
}

var (
	current  atomic.Pointer[Tables]
	previous atomic.Pointer[Tables]
)

func init() { current.Store(mustEmbedded()) }

// Cur 返回当前配置快照。热路径只做一次原子读，不做拷贝。
func Cur() *Tables { return current.Load() }

// Version 当前快照版本（内嵌哈希，或热更新后的派生版本）。
func Version() string { return Cur().Version }

// MustEmbedded 用编译期内嵌数据构造一份快照。
// 热更新测试与「恢复出厂配置」用；正常读取请用 Cur()。
func MustEmbedded() *Tables { return mustEmbedded() }

// Replace 原子应用一批表覆盖。
//
// patch 的键是表名（与 XML 文件名一致，如 "item"），值是该表的 JSON 数组。
// 未出现在 patch 里的表沿用当前快照——线上改一张表不必把 33 张表全部重传。
//
// 任何一张表解析失败、主键重复或为空，整批不生效并返回错误，当前快照保持不变。
// 成功返回新版本号。
func Replace(patch map[string][]byte) (string, error) {
	if len(patch) == 0 {
		return "", fmt.Errorf("empty patch")
	}

	old := current.Load()
	next := *old // 浅拷贝：各表字段是值类型，逐个替换互不影响

	// 版本由「表名 + 内容」派生，与 patch 的遍历顺序无关，同一批内容得到同一版本。
	names := make([]string, 0, len(patch))
	for name := range patch {
		names = append(names, name)
	}
	sort.Strings(names)

	h := sha256.New()
	for _, name := range names {
		set, ok := setters[name]
		if !ok {
			return "", fmt.Errorf("unknown table %q（表名必须与 XML 文件名一致）", name)
		}
		if err := set(&next, patch[name]); err != nil {
			return "", fmt.Errorf("%s: %w", name, err)
		}
		h.Write([]byte(name))
		h.Write([]byte{0})
		h.Write(patch[name])
	}

	if err := next.validate(); err != nil {
		return "", err
	}

	next.Version = deriveVersion(old.Version, h.Sum(nil))
	current.Store(&next)
	previous.Store(old)
	return next.Version, nil
}

// LoadDir 从目录读取 `<table>.json` 并原子应用。
//
// 这是热更新的外部输入入口：目录内容来自运维/配置下发，而不是编译期 embed。
// 目录里存在未知表名会直接报错（拼错文件名不应该静默无效）。
func LoadDir(dir string) (string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return "", err
	}
	patch := make(map[string][]byte, len(entries))
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			return "", err
		}
		patch[strings.TrimSuffix(e.Name(), ".json")] = raw
	}
	if len(patch) == 0 {
		return "", fmt.Errorf("no *.json in %s", dir)
	}
	return Replace(patch)
}

// Rollback 回到上一个快照（只保留一层）。成功返回回滚后的版本。
func Rollback() (string, error) {
	prev := previous.Load()
	if prev == nil {
		return "", fmt.Errorf("no previous snapshot")
	}
	cur := current.Swap(prev)
	previous.Store(cur)
	return prev.Version, nil
}

// deriveVersion 由「旧版本主体 + 本次内容哈希」派生新版本，形如 abc123+9f2c1d4e。
// 只保留一层派生，避免反复热更新后版本串无限变长。
func deriveVersion(old string, sum []byte) string {
	base := old
	if i := strings.IndexByte(base, '+'); i > 0 {
		base = base[:i]
	}
	return fmt.Sprintf("%s+%x", base, sum[:8])
}
