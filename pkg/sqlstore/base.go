// pkg/sqlstore/base.go —— MySQL 仓储基座
//
// 为什么需要它：玩家私有状态有一套统一契约（internal/game/module.Repository），
// 但公会、队伍、好友、聊天、邮件、抢位置、登录账号、区服列表这些数据不属于
// 单个玩家快照——它们是多行表、要事务、要行锁，走不了 Ext 槽位。每处都写
//
//	type Repo struct { db *db.DB }
//	func NewRepo(database *db.DB) *Repo { return &Repo{db: database} }
//
// 再各自决定「没有 DB 时降级成什么」，样板本身不难写，难的是每份样板各自
// 演化：一处加了指标另一处没有；一处判了空另一处忘了就 nil deref。
//
// Base 把这些收成一个基座：
//
//	type Repo struct{ *sqlstore.Base }              // 仓储 = 基座 + 本表 SQL
//	r.Ready()  r.Tx(...)  r.DB()  r.SQL()  r.Index() // 能力各表一致
//
// 它不做 ORM、不生成 SQL。**这一层唯一的价值是「同一件事只有一个判断」**：
// 无库怎么办、事务怎么起、索引接没接，全项目一个答案。
//
// 为什么在 pkg/ 而不是 internal/game/module/：本文件依赖只有 pkg/cache、
// pkg/db、pkg/log，零 internal 依赖。放在 module 包里会逼着登录服为了
// account 表去 import 一整套 game 的模块系统（actor/codec/pb/writebehind），
// 那是纯粹的方向倒置。game 的 5 个直写模块与 seat、login 现在共用一个基座，
// 区别只在 name 与各自的 SQL。
package sqlstore

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
// 单测和「无依赖环境跑协议演示」是真实存在的场景：直写库的仓储都允许
// db 为 nil，上层判 Ready 后降级。哨兵错误让「忘了判空」变成一次可识别的
// 失败，而不是 nil pointer dereference。
var ErrNoDB = errors.New("sqlstore: database not configured")

// Base 仓储基座。
//
// 零值不可用：必须经 New 构造（name 参与日志标签与指标）。
type Base struct {
	db     *db.DB
	name   string
	logger *log.Logger
	index  *cache.OwnerIndex
}

// New 创建仓储基座。
//
// database 可以为 nil：那是「本次进程不接数据库」的显式声明，Ready() 返回
// false，调用方据此降级。**不接受「忘了传」**——组装层永远显式传。
func New(database *db.DB, name string) *Base {
	return &Base{db: database, name: name, logger: log.New(name)}
}

// Ready 是否可用（数据库已配置）。
//
// 「没有 DB 时降级成功」和「没有 DB 时报错」都是合法策略，但**判断条件
// 必须一致**，否则同一套代码在不同仓储里行为不同，排查时无从下手。
func (b *Base) Ready() bool { return b != nil && b.db != nil }

// Name 仓储名（日志字段与指标标签）。
func (b *Base) Name() string {
	if b == nil {
		return ""
	}
	return b.name
}

// Log 仓储日志器（名字已作为固定字段带上）。
func (b *Base) Log() *log.Logger {
	if b == nil {
		return log.New("")
	}
	return b.logger
}

// DB 底层连接池。
//
// 暴露出来是为了让仓储直接用 QueryRowContext / QueryContext 这些标准方法：
// 包一层 Query/Exec 只会把 *sql.DB 的三十个方法抄一遍，既没有约束也没有
// 收益。约束在别处——SQL 只允许出现在 store/repo 文件里。
func (b *Base) DB() *db.DB {
	if b == nil {
		return nil
	}
	return b.db
}

// SQL 底层 *sql.DB（gendb 生成的函数要的就是它）。
//
// 与 DB() 的分工：DB() 用于起事务（带 readTimeout 等封装），SQL() 用于把
// 连接交给生成函数。两者都没有时返回 nil，标准库方法会 panic——所以调用前
// 必须 Ready()，这也是 Ready() 存在的理由。
func (b *Base) SQL() *sql.DB {
	if b == nil || b.db == nil {
		return nil
	}
	return b.db.DB
}

// Tx 在事务里执行 fn。
//
// 无数据库时返回 ErrNoDB 而不是 nil：调用方**必须**先判 Ready() 决定降级
// 策略（成功还是失败），悄悄当成成功会让「写没写进去」变成谜。
func (b *Base) Tx(ctx context.Context, fn func(*sql.Tx) error) error {
	return b.TxWith(ctx, nil, fn)
}

// TxWith 带事务选项的版本（隔离级别 / 只读）。
//
// 只读一致性读取（如落库版本的冷启动快照）需要 ReadOnly + RepeatableRead，
// 以前这类调用点只能自己 BeginTx，于是「事务怎么起」在仓储里又长出第二种
// 写法。有了本方法，仓储层不再出现裸 BeginTx。
func (b *Base) TxWith(ctx context.Context, opts *sql.TxOptions, fn func(*sql.Tx) error) error {
	if !b.Ready() {
		return ErrNoDB
	}
	return b.db.TxWith(ctx, opts, fn)
}

// AttachIndex 注入归属索引（组装层调用；nil 表示不启用）。
//
// 与仓储自己存一个 index 字段相比，好处是「有没有索引」这件事在仓储层可查：
// index.go 里的 Seed/Reconcile 需要知道仓储接没接索引，不用再转发一次。
func (b *Base) AttachIndex(oi *cache.OwnerIndex) {
	if b == nil {
		return
	}
	b.index = oi
}

// Index 归属索引；未启用返回 nil。
//
// 调用方一律写成「nil 就回落 SQL」——索引是加速层，不是正确性前提
// （与 cache.OwnerIndex 的设计约定一致）。
func (b *Base) Index() *cache.OwnerIndex {
	if b == nil {
		return nil
	}
	return b.index
}
