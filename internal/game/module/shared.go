// internal/game/module/shared.go —— 跨玩家共享实体的仓储基座
//
// 为什么需要它：玩家私有状态有一套统一契约（Repository，见 repository.go），
// 但公会、队伍、好友、聊天、邮件这类数据不属于单个玩家快照——它们是多行表、
// 要事务、要行锁，走不了 Ext 槽位。五个模块因此各写一份
//
//	type Repo struct { db *db.DB }
//	func NewRepo(database *db.DB) *Repo { return &Repo{db: database} }
//
// 加十二处 `if m.repo == nil` 的手工降级（每个模块自己决定「没有 DB 时
// 降级成什么」），再各自决定要不要接归属索引。样板本身不难写，难的是
// 五份样板各自演化：一处加了指标，另一处没有；一处判空写了 Ready，
// 另一处忘了就 nil deref。
//
// Shared 把这些收成一个基座：
//
//	Repo{ *module.Shared }            // 模块的仓储就是这个基座 + 表相关的 SQL
//	r.Ready()  r.Tx(...)  r.DB()      // 三层能力，语义五处一致
//
// 它不做 ORM、不碰 SQL 生成。**这一层唯一的价值是「同一件事只有一个判断」**：
// 无 DB 怎么办、事务怎么起、索引接没接，全项目一个答案。
package module

import (
	"context"
	"database/sql"
	"errors"

	"game_study/pkg/cache"
	"game_study/pkg/db"
	"game_study/pkg/log"
)

// ErrNoDB 没有配置数据库。
//
// 单测和「无依赖环境跑协议演示」是真实存在的场景：项目里五个直写模块都
// 允许 db 为 nil，模块层判空后降级回包。哨兵错误让「忘了判空」变成一次
// 可识别的失败而不是 nil pointer dereference。
var ErrNoDB = errors.New("module: database not configured")

// Shared 跨玩家共享实体的仓储基座。
//
// 零值不可用：必须经 NewShared 构造（name 参与日志标签与指标）。
type Shared struct {
	db     *db.DB
	name   string
	logger *log.Logger
	index  *cache.OwnerIndex
}

// NewShared 创建仓储基座。
//
// database 可以为 nil：那是「本次进程不接数据库」的显式声明，Ready() 会
// 返回 false，调用方降级。**不接受「忘了传」** —— 组装层永远显式传。
func NewShared(database *db.DB, name string) *Shared {
	return &Shared{db: database, name: name, logger: log.New(name)}
}

// Ready 是否可用（数据库已配置）。
//
// 所有直写模块的降级判断都走这里，不再各自写 `m.repo == nil`：
// 「没有 DB 时降级成功」和「没有 DB 时报错」都是合法策略，但**判断条件
// 必须一致**，否则同一套代码在不同模块里行为不同，排查时无从下手。
func (s *Shared) Ready() bool { return s != nil && s.db != nil }

// Name 模块/仓储名（日志字段与指标标签）。
func (s *Shared) Name() string {
	if s == nil {
		return ""
	}
	return s.name
}

// Log 仓储日志器（名字已作为固定字段带上）。
func (s *Shared) Log() *log.Logger {
	if s == nil {
		return log.New("")
	}
	return s.logger
}

// DB 底层连接池。
//
// 暴露出来是为了让仓储层直接用 QueryRowContext / QueryContext 这些标准
// 方法：包一层 Query/Exec 只会把 *sql.DB 的三十个方法抄一遍，既没有约束
// 也没有收益。约束在别处——模块的 SQL 只允许出现在 store.go（docs §2）。
func (s *Shared) DB() *db.DB {
	if s == nil {
		return nil
	}
	return s.db
}

// SQL 底层 *sql.DB（gendb 生成的函数要的就是它）。
//
// 与 DB() 的分工：DB() 用于起事务（带 readTimeout 等封装），SQL() 用于
// 把连接交给生成函数。两者都没有时返回 nil，标准库方法会 panic——
// 所以调用前必须 Ready()，这也是 Ready() 存在的理由。
func (s *Shared) SQL() *sql.DB {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.DB
}

// Tx 在事务里执行 fn。
//
// 无数据库时返回 ErrNoDB 而不是 nil：调用方**必须**先判 Ready() 决定降级
// 策略（成功还是失败），悄悄当成成功会让「写没写进去」变成谜。
func (s *Shared) Tx(ctx context.Context, fn func(*sql.Tx) error) error {
	if !s.Ready() {
		return ErrNoDB
	}
	return s.db.Tx(ctx, fn)
}

// AttachIndex 注入归属索引（组装层调用；nil 表示不启用）。
//
// 与模块自己存一个 index 字段相比，好处是「有没有索引」这件事在仓储层
// 可查：index.go 里的 Seed/Reconcile 需要知道模块接没接索引，不用再
// 让模块转发一次。
func (s *Shared) AttachIndex(oi *cache.OwnerIndex) {
	if s == nil {
		return
	}
	s.index = oi
}

// Index 归属索引；未启用返回 nil。
//
// 调用方一律写成「nil 就回落 SQL」——索引是加速层，不是正确性前提
// （与 cache.OwnerIndex 的设计约定一致）。
func (s *Shared) Index() *cache.OwnerIndex {
	if s == nil {
		return nil
	}
	return s.index
}

// IndexSpec 一个归属索引的声明。
//
// 存在的理由：索引原来只由配置文件的 idtab.mappings 声明。模块要索引，
// 改 yaml；漏了那一行，`s.ownerIdx["mail"]` 就是 nil，模块静默回落 SQL——
// 服务能跑、性能掉一半、日志里一个字都没有。索引是「代码用得到的东西」，
// 声明就该和用它的代码在一起，配置文件只做部署开关。
type IndexSpec struct {
	// Table 表名。
	Table string
	// By 归属者列（ZSET 的 score，如 player_id）。
	By string
	// Key 实体 id 列（ZSET 的 member，通常是主键）。
	Key string
}

// IndexDeclarer 可选接口：模块声明自己需要哪些归属索引。
//
// Registry 在组装期遍历模块收集声明，缺的补建（yaml 没配也能工作），
// 建好的再经 IndexInjector 交回模块。未实现该接口的模块不会被问第二次。
type IndexDeclarer interface {
	IndexSpecs() []IndexSpec
}

// IndexInjector 可选接口：组装层把建好的归属索引交给模块。
//
// 传 table 而不是「就是我的索引」：一个模块可能声明多个索引
// （主表和明细表各自成索引），按表名分发比按顺序对位安全。
type IndexInjector interface {
	AttachIndex(table string, oi *cache.OwnerIndex)
}
