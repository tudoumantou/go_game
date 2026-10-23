package aoi

import (
	"sync/atomic"

	"game_study/internal/scenemap"
)

// Brief 是实体的"对外可见"外观快照。
//
// 它是 AOI 与外界之间唯一的数据形态：玩家 Actor 拿它渲染，
// MGR 拿它做跨 Actor 的只读判定，协议层拿它序列化。
//
// 字段全部是值类型或深拷贝过的 map，所以 Brief 一旦进入 Snapshot
// 就冻结了——调用方可以随便读、随便传，不会和 AOI 的后续更新
// 产生数据竞争。
//
// 为什么把 Ext 做成 map 而不是泛型字段：Ext 承载"每种实体各不相同的
// 少量展示属性"（怪物的血条样式、宠物的主人名、子弹的速度），
// 用一个开放的 map 比在 Brief 上开 20 个可选字段更好维护。
// 代价是 map 必须深拷贝，见 Clone。
type Brief struct {
	UUID  scenemap.UUID
	Kind  scenemap.EntityKind
	CfgID int32
	Name  string

	// X/Y 是像素坐标（广播口径），不是行走格。
	// 权威坐标在 MGR 的格子索引里，这里只是"上一次同步给 AOI 的值"。
	X int32
	Y int32

	Dir        scenemap.Dir
	HP         int32
	MaxHP      int32
	State      int32         // 实体状态机（MGR 内部枚举的镜像，框架不解释）
	MasterUUID scenemap.UUID // 宠物/召唤的主人；0 表示无主

	// Ext 业务扩展字段。Clone 时深拷贝（只拷一层：值不再是指针/map 的约定
	// 由写入方保证，AOI 不做递归深拷贝，否则热路径成本不可控）。
	Ext map[string]any
}

// Clone 返回深拷贝。Ext 为 nil 时保持 nil（不分配空 map）。
func (b Brief) Clone() Brief {
	if b.Ext != nil {
		cp := make(map[string]any, len(b.Ext))
		for k, v := range b.Ext {
			cp[k] = v
		}
		b.Ext = cp
	}
	return b
}

// IsPlayer 报告是否为真人玩家（广播规则里最常用的判定）。
func (b Brief) IsPlayer() bool { return b.Kind == scenemap.EntityKindPlayer }

// IsMate 报告是否为"跟随型"实体（宠物/召唤），它们的移动只对主人做
// 额外直达同步。
func (b Brief) IsMate() bool { return b.MasterUUID.Valid() }

// Snapshot 是某个时刻的整图视野快照，**构造后不可变**。
//
// 三个 map 是同一次构建的产物，彼此自洽：
//
//	Entities 全部实体（uuid → 外观）
//	Views    视野格 → 该格实体列表（每个实体恰好出现一次）
//	CellOf   实体 → 视野格（与 Views 互为倒排）
//
// 不变量（测试会断言）：
//
//	len(CellOf) == len(Entities)
//	Σ len(Views[c]) == len(Entities)
//	∀u: Views[CellOf[u]] 包含 u
//
// 为什么整份替换而不是增量：读方（玩家 Actor / MGR）需要"一次读到
// 自洽的一屏"。如果发布增量补丁，读方在 apply 到一半时被调度，
// 就会看到"实体已出现在 Entities、但还在旧的 Views 里"的半更新状态；
// 要修就得在读方加锁或版本重试，等于把 AOI 的串行成本转移给所有读者。
// 整份替换的代价是 O(n) 拷贝，单图几百实体完全可接受（一次 malloc +
// 几百次 map 写入，微秒级）。
type Snapshot struct {
	Revision uint64
	Entities map[scenemap.UUID]Brief
	Views    map[CellKey][]scenemap.UUID
	CellOf   map[scenemap.UUID]CellKey
}

// EmptySnapshot 返回一个空快照（Revision 0）。
//
// 单独导出：池化复用与测试都需要一个"合法但空"的初值，
// 让 Load() 永远不返回 nil，读方少一层 nil 判断。
func EmptySnapshot() *Snapshot {
	return &Snapshot{
		Entities: make(map[scenemap.UUID]Brief),
		Views:    make(map[CellKey][]scenemap.UUID),
		CellOf:   make(map[scenemap.UUID]CellKey),
	}
}

// Entity 按 UUID 取外观。
func (s *Snapshot) Entity(u scenemap.UUID) (Brief, bool) {
	if s == nil {
		return Brief{}, false
	}
	b, ok := s.Entities[u]
	return b, ok
}

// Cell 返回实体所在的视野格。
func (s *Snapshot) Cell(u scenemap.UUID) (CellKey, bool) {
	if s == nil {
		return CellKey{}, false
	}
	c, ok := s.CellOf[u]
	return c, ok
}

// View 返回某个视野格内的实体列表（已排序，只读）。
//
// 调用方不得修改返回的切片：它和快照共享底层数组，
// 改了会影响所有持有同一快照的读者。
func (s *Snapshot) View(c CellKey) []scenemap.UUID {
	if s == nil {
		return nil
	}
	return s.Views[c]
}

// EntitiesIn 返回这些视野格内的实体（去重排序）。
// 读方的"拉一屏"接口，语义与 ViewGrid.EntitiesIn 一致。
func (s *Snapshot) EntitiesIn(cells []CellKey) []scenemap.UUID {
	if s == nil || len(cells) == 0 {
		return nil
	}
	seen := make(map[scenemap.UUID]struct{}, 16)
	for _, c := range cells {
		for _, u := range s.Views[c] {
			seen[u] = struct{}{}
		}
	}
	out := make([]scenemap.UUID, 0, len(seen))
	for u := range seen {
		out = append(out, u)
	}
	sortUUIDs(out)
	return out
}

// Len 返回实体数量。
func (s *Snapshot) Len() int {
	if s == nil {
		return 0
	}
	return len(s.Entities)
}

// Store 是不可变快照的原子发布点。
//
// 只有一条规则：**单写多读**。
//
//	写：AOI 的 goroutine（每次处理完一批命令后整份替换）
//	读：任意 goroutine（玩家 Actor / MGR / 监控），无锁
//
// atomic.Pointer 保证读者要么看到旧快照、要么看到新快照，
// 绝不会看到"指针换了但内容写了一半"——这正是 Go 内存模型里
// atomic 提供的 happens-before：Store 之前的所有写入对读到该指针
// 的 goroutine 可见。因此快照内部的 map 不需要任何锁。
//
// 不能换成 RWMutex：读方是高频路径（每个玩家每次拉视野都要读），
// 而写方每批命令只写一次；用锁会让几百个读者争一把互斥量，
// 与"AOI 不能拖慢玩家"的目标相反。
type Store struct {
	p atomic.Pointer[Snapshot]
}

// NewStore 返回已发布空快照的 Store。
func NewStore() *Store {
	s := &Store{}
	s.p.Store(EmptySnapshot())
	return s
}

// Load 返回当前快照（永远非 nil）。
func (s *Store) Load() *Snapshot {
	if s == nil {
		return EmptySnapshot()
	}
	if v := s.p.Load(); v != nil {
		return v
	}
	return EmptySnapshot()
}

// Store 发布新快照。nil 视为空快照（防御式，避免读方拿到 nil）。
//
// 方法名与类型名相同在 Go 里是合法的（类型与方法是两个命名空间），
// 这里保留同名是为了贴合规格里的"Store.Store(snapshot)"表述。
func (s *Store) Store(snap *Snapshot) {
	if s == nil {
		return
	}
	if snap == nil {
		snap = EmptySnapshot()
	}
	s.p.Store(snap)
}
