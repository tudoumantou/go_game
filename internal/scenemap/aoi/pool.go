package aoi

import (
	"context"
	"fmt"
	"sync"

	"game_study/internal/scenemap"
	"game_study/internal/scenemap/grid"
)

// DefaultPoolSize 是房间池的默认容量（同时存在的 AOI 实例上限）。
//
// 10 的依据不是压测，而是"单进程同时开着的可玩地图"这个量级：
// 主城 1 线 + 若干副本 + 活动图，通常个位数。真正的容量应该按
// 内存/goroutine 数实测后再配，这里只给一个不会一上来就爆的值。
const DefaultPoolSize = 10

// Pool 是固定容量的 AOI 实例池。
//
// 为什么池化：房间图开/关非常频繁（一次副本一开一关）。每次
// new goroutine + 重建索引，会让 GC 压力集中在开房那一瞬间
// （高峰期几十个副本同时创建）；池化把"分配"摊到启动/空闲期，
// 复用已有的 goroutine 与 map 容量。
//
// 语义：
//   - 容量 N 是**同时占用**上限，不是预创建数（懒创建：第一次 Acqurie
//     才真正分配，空池启动不占资源）；
//   - 池空返回 ErrRoomPoolExhausted，调用方应回"场景繁忙"而不是排队
//     （排队会把开房延迟转嫁给玩家）；
//   - Release 只接受已经停止的实例；还在跑的实例会被丢弃（宁可少一个
//     槽位，也不能把脏状态放回池里）。
//
// 并发安全：内部一把互斥锁。Acquire/Release 是开/关房路径，低频，
// 锁竞争可以忽略；真正的热路径（Enter/Sync）不碰这把锁。
type Pool struct {
	mu   sync.Mutex
	size int
	// base 是模板配置：AcquireWith 用它补齐调用方没填的字段。
	base Config
	idle []*AOI
	// total 是"已创建且未被丢弃"的实例数（含空闲与在用）。
	total  int
	closed bool
}

// NewPool 创建房间池。
//
// size <= 0 取 DefaultPoolSize。base 允许不完整（例如没有 Key/Sink）：
// 它只是补齐模板，真正的配置在 Acquire/AcquireWith 时合并并校验。
func NewPool(size int, base Config) *Pool {
	if size <= 0 {
		size = DefaultPoolSize
	}
	return &Pool{size: size, base: base}
}

// Acquire 按地图键取一个 AOI（其余配置取池模板）。
func (p *Pool) Acquire(key scenemap.MapKey) (*AOI, error) {
	return p.AcquireWith(Config{Key: key})
}

// AcquireWith 用调用方给的配置取一个 AOI。
//
// 合并规则：cfg 里"零值"的字段用池模板补齐。这样 Room 只需要传
// 每张图不同的部分（Key/Route/Size/Sink），Behavior 之类的共用件
// 放在模板里。
func (p *Pool) AcquireWith(cfg Config) (*AOI, error) {
	merged := mergeConfig(p.baseLocked(), cfg)

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, fmt.Errorf("%w: 房间池已关闭", scenemap.ErrClosed)
	}
	if n := len(p.idle); n > 0 {
		a := p.idle[n-1]
		p.idle = p.idle[:n-1]
		p.mu.Unlock()
		// 复用：整份重建状态（索引、外观、邮箱、goroutine），
		// 而不是逐字段清理——清理漏一个字段就是跨局数据污染。
		if err := a.restart(merged); err != nil {
			p.mu.Lock()
			p.total--
			p.mu.Unlock()
			return nil, err
		}
		return a, nil
	}
	if p.total >= p.size {
		p.mu.Unlock()
		return nil, fmt.Errorf("%w: 容量 %d 已全部占用", scenemap.ErrRoomPoolExhausted, p.size)
	}
	// 先占槽位再创建：创建失败要归还，否则池会"越用越少"。
	p.total++
	p.mu.Unlock()

	a, err := NewAOI(merged)
	if err != nil {
		p.mu.Lock()
		p.total--
		p.mu.Unlock()
		return nil, err
	}
	return a, nil
}

// baseLocked 读取模板配置（模板在构造后不变，无需加锁，这里保留
// 一个方法是为了让"以后允许热更模板"有落点）。
func (p *Pool) baseLocked() Config { return p.base }

// Release 归还 AOI。
//
// 归还前必须已经 Close（loop 退出）。若实例还活着，直接丢弃该槽位：
// 把它放回池里会让下一个房间继承一条正在跑的 goroutine 与旧索引，
// 这比"少一个房间容量"严重得多。
func (p *Pool) Release(a *AOI) {
	if a == nil {
		return
	}
	// 清空内部索引与快照：复用不能带上一局的实体。
	a.resetAfterStop()

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	if len(p.idle) >= p.size {
		// 理论上不会发生（Acquire 与 Release 成对）；防御式丢弃。
		p.total--
		return
	}
	if !a.Stopped() {
		p.total--
		return
	}
	p.idle = append(p.idle, a)
}

// Size 返回容量。
func (p *Pool) Size() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.size
}

// Idle 返回空闲实例数（监控/测试用）。
func (p *Pool) Idle() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.idle)
}

// InUse 返回在用实例数。
func (p *Pool) InUse() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.total - len(p.idle)
}

// Close 关闭池并停止所有空闲实例。
//
// 在用的实例不在这里处理：它们属于各自的 Room，必须由 RoomManager
// 逐个关闭（先 MGR 后 AOI）。池只负责"我手里的空闲资源"。
func (p *Pool) Close(ctx context.Context) error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	idle := p.idle
	p.idle = nil
	p.total -= len(idle)
	p.mu.Unlock()

	for _, a := range idle {
		if err := a.Close(ctx); err != nil {
			return err
		}
	}
	return nil
}

// mergeConfig 用 over 覆盖 base 中的非零字段。
//
// 不用反射、不用指针判空：显式列字段虽然啰嗦，但 Config 增删字段时
// 编译器会在这里提醒你"忘了合并"。
func mergeConfig(base, over Config) Config {
	out := base
	if over.Key != (scenemap.MapKey{}) {
		out.Key = over.Key
	}
	if over.Route != 0 {
		out.Route = over.Route
	}
	if over.Geo != (grid.Geometry{}) {
		out.Geo = over.Geo
	}
	if over.Size != (grid.Size{}) {
		out.Size = over.Size
	}
	if over.MailboxSize > 0 {
		out.MailboxSize = over.MailboxSize
	}
	if over.SameScreenLimit > 0 {
		out.SameScreenLimit = over.SameScreenLimit
	}
	if over.GridSize > 0 {
		out.GridSize = over.GridSize
	}
	if over.Behavior != nil {
		out.Behavior = over.Behavior
	}
	if over.Sink != nil {
		out.Sink = over.Sink
	}
	if over.Logger != nil {
		out.Logger = over.Logger
	}
	return out
}
