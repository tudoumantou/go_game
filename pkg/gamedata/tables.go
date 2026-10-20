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
//
// 架构职责：提供与具体表无关的快照机制——泛型 table[T] 视图 + 原子指针 + 版本派生，
// data.go 里的生成代码只负责「每张表长什么样」。
// 协作关系：Cur() 被各模块在消息处理路径上高频调用（读路径零锁零拷贝）；
// Replace/LoadDir 是低频运维路径，两者互不阻塞。
// 设计取舍（并发）：未使用 CAS 循环——Replace 是低频外部触发操作，并发双写
// 属于运维误用；换取的是简单可读的 Load→拷贝→Store 三段式。
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
//
// 泛型 [T any]：一套索引发米给 33 张表复用，避免每张表手写 map + Get。
// list 与 byID 共享底层数组（byID 里存的是 &list[i]），因此二者永远自洽；
// 由于整表只在构造期写入，「构造后不可变」由约定保证而非锁。
type table[T any] struct {
	// list 全量行（保序）
	list []T
	// byID 主键 → 行指针（O(1) 查找）
	byID map[int64]*T
}

// newTable 用行切片构建视图。
// idOf 以函数注入而不是接口：行类型是生成的结构体，没有统一方法可取主键，
// 传闭包比给每张表定义 PrimaryKey() 更轻，也省掉一次接口动态分发。
func newTable[T any](list []T, idOf func(*T) int64) (table[T], error) {
	byID := make(map[int64]*T, len(list))
	for i := range list {
		id := idOf(&list[i])
		// 主键重复必须在构造期拦截：放进 map 后 Get 随机返回一行，
		// 表现为「配置看起来生效了但值不稳定」的灵异 bug。
		if _, dup := byID[id]; dup {
			return table[T]{}, fmt.Errorf("duplicate primary key %d", id)
		}
		byID[id] = &list[i]
	}
	return table[T]{list: list, byID: byID}, nil
}

// List 全量行。返回的切片由快照持有，调用方只读。
// 不返回拷贝：行数上千、调用频率高，拷贝成本不值得；只读契约靠注释约束。
func (t table[T]) List() []T { return t.list }

// Get 按主键查询。
// (*T, bool) 双返回值是 Go 惯例（comma-ok）：nil + false 表示不存在，
// 调用方必须显式处理，不会把零值当成合法配置。
func (t table[T]) Get(id int64) (*T, bool) {
	c, ok := t.byID[id]
	return c, ok
}

// Len 行数。
func (t table[T]) Len() int { return len(t.list) }

// validate 快照自检：空表通常意味着配置写错或文件名对不上。
// 双重校验（空 + 主键计数不一致）分别覆盖「整表丢失」与「构造期漏拦」两种事故。
func (t table[T]) validate(name string) error {
	if len(t.list) == 0 {
		return fmt.Errorf("%s: empty table", name)
	}
	if len(t.byID) != len(t.list) {
		return fmt.Errorf("%s: duplicate primary key", name)
	}
	return nil
}

// current/previous 用 atomic.Pointer[Tables]（Go 1.19 泛型原子指针）而非
// 裸指针 + Mutex：读路径（Cur）无锁无分配；Store 之前的全部写在 Load 后
// 对其他 goroutine 可见（atomic 提供 happens-before 边），快照发布安全。
var (
	current  atomic.Pointer[Tables]
	previous atomic.Pointer[Tables]
)

// init 包初始化即装载内嵌快照：保证 Cur() 在任何调用点都不会返回 nil，
// 调用方无需判空；代价是进程启动时为 33 张表建一次索引（毫秒级）。
func init() { current.Store(mustEmbedded()) }

// Cur 返回当前配置快照。热路径只做一次原子读，不做拷贝。
// 注意：返回值可能在下次热更新后失效，禁止跨消息缓存 *Tables 或其内部行指针，
// 应「每条消息进来时取一次」——这正是要点 2 的「本条消息内一致」。
func Cur() *Tables { return current.Load() }

// Version 当前快照版本（内嵌哈希，或热更新后的派生版本）。
// 日志打印它即可回答「这个进程跑的是哪份配置」。
func Version() string { return Cur().Version }

// MustEmbedded 用编译期内嵌数据构造一份快照。
// 热更新测试与「恢复出厂配置」用；正常读取请用 Cur()。
// 名字带 Must 是 Go 惯例：失败即 panic（见 mustEmbedded），调用点无需处理 error。
func MustEmbedded() *Tables { return mustEmbedded() }

// Replace 原子应用一批表覆盖。
//
// patch 的键是表名（与 XML 文件名一致，如 "item"），值是该表的 JSON 数组。
// 未出现在 patch 里的表沿用当前快照——线上改一张表不必把 33 张表全部重传。
//
// 任何一张表解析失败、主键重复或为空，整批不生效并返回错误，当前快照保持不变。
// 成功返回新版本号。
//
// 契约：patch 为空报错（防止误传空 map 把版本号空转一圈）；全部失败路径都
// 发生在 current.Store 之前，因此「要么全新、要么不动」。
func Replace(patch map[string][]byte) (string, error) {
	if len(patch) == 0 {
		return "", fmt.Errorf("empty patch")
	}

	old := current.Load()
	// 浅拷贝：Tables 里每个表字段都是值类型（table[T] 内含 slice/map 头），
	// 逐个替换字段互不影响；旧快照的底层数据被原样保留，Rollback 才成立。
	next := *old

	// 版本由「表名 + 内容」派生，与 patch 的遍历顺序无关，同一批内容得到同一版本。
	// 先排序再哈希：map 遍历顺序随机，不排序会让同样的配置算出不同版本号。
	names := make([]string, 0, len(patch))
	for name := range patch {
		names = append(names, name)
	}
	sort.Strings(names)

	h := sha256.New()
	for _, name := range names {
		// setters 是 map 查找（O(1)）而非 switch：33 个分支的 switch 每次
		// 热更新都要线性比较，注册表模式也方便生成代码统一维护。
		set, ok := setters[name]
		if !ok {
			// 表名与 XML 文件名不一致 = 下发方写错，必须显式失败：
			// 静默忽略会让运维以为配置生效了。
			return "", fmt.Errorf("unknown table %q（表名必须与 XML 文件名一致）", name)
		}
		if err := set(&next, patch[name]); err != nil {
			return "", fmt.Errorf("%s: %w", name, err)
		}
		h.Write([]byte(name))
		h.Write([]byte{0})
		h.Write(patch[name])
	}

	// 整批校验放在最后：单表 setters 只保证自己合法，validate 保证整套自洽
	// （例如某表被改空）。
	if err := next.validate(); err != nil {
		return "", err
	}

	next.Version = deriveVersion(old.Version, h.Sum(nil))
	// Store 是原子发布点：此后所有 Cur() 立即看到新快照；previous 保存旧快照
	// 供 Rollback 使用（只一层，再次 Replace 会覆盖 previous）。
	current.Store(&next)
	previous.Store(old)
	return next.Version, nil
}

// LoadDir 从目录读取 `<table>.json` 并原子应用。
//
// 这是热更新的外部输入入口：目录内容来自运维/配置下发，而不是编译期 embed。
// 目录里存在未知表名会直接报错（拼错文件名不应该静默无效）。
// 只收 *.json 且跳过子目录：目录里允许混放 README、.bak 等说明文件。
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
		// item.json → "item"：文件名（去后缀）即表名，与 setters 的键对齐
		patch[strings.TrimSuffix(e.Name(), ".json")] = raw
	}
	if len(patch) == 0 {
		return "", fmt.Errorf("no *.json in %s", dir)
	}
	return Replace(patch)
}

// Rollback 回到上一个快照（只保留一层）。成功返回回滚后的版本。
// Swap + 对调 previous：连续两次 Rollback 等于「取消上次回滚」，
// 语义直观；只保留一层是刻意的——多层历史需要链表/快照存储，收益不抵复杂度。
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
// 相同旧版本 + 相同内容 → 相同新版本：幂等，重复下发同一批配置版本号不变，
// 便于对账「这个进程到底吃没吃这次热更新」。
func deriveVersion(old string, sum []byte) string {
	base := old
	if i := strings.IndexByte(base, '+'); i > 0 {
		base = base[:i]
	}
	return fmt.Sprintf("%s+%x", base, sum[:8])
}
