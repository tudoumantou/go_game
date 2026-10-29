// internal/game/module/mail/mail.go —— 邮件模块（P5 投递 / P6 列表与领取）
//
// 架构职责：邮件的 Kafka 投递、列表查询、附件领取，对应设计文档 4.9.3 / R2.2。
//  1. 投递：Deliver（Kafka 消费）建主邮件 + 附件；DeliverOffline（Tier-1
//     发放，充值到账等）以 biz_id 幂等建带附件的系统邮件；
//  2. 列表：onList 倒序取最近 100 封，附件有无用 EXISTS 子查询一次带出；
//  3. 领取：onTake 走玩家快照的 ApplyOnce 业务键（A 类幂等），
//     与道具在同一次 Redis CAS 提交，SQL 侧 claimed 只做尽力展示标记。
//
// Ext 槽位：无（邮件数据全在 MySQL）。但领取幂等「寄生」在玩家快照上：
// 业务键 mail:<id> 随快照 CAS 原子提交——这是与签到 ApplyOnce 同构的
// 「快照业务键幂等」模式，也是 R2.2 修复的核心：SQL 先标记会在 Redis
// 提交失败时丢奖励，快照业务键不会。
//
// 事件订阅：无。
//
// 配置表关系：邮件正文走模板（template_id 关联配置），demo 期直接存
// Kafka 消息里的标题/正文，不查模板表。
//
// 懒初始化：不适用（邮件行由投递动作显式产生）。

package mail

import (
	"context"

	"game_study/internal/game/module"
	"game_study/pkg/cache"
	"game_study/pkg/log"
	"game_study/pkg/model"
)

type mailRepo interface {
	// Ready 数据库是否可用；无 DB 环境时模块降级回包（见 module.Shared）。
	Ready() bool
	UnreadCount(ctx context.Context, playerID int64) (int64, error)
	Insert(ctx context.Context, m *model.Mail) (int64, error)
	InsertAttachment(ctx context.Context, mailID int64, itemID, count int32) error
	HasAttachments(ctx context.Context, mailIDs []int64) (map[int64]bool, error)
	ListByPlayer(ctx context.Context, playerID int64, limit int) ([]MailRow, error)
	FindAttachments(ctx context.Context, playerID, mailID int64) ([]Attachment, error)
	MarkTaken(ctx context.Context, mailID int64) error
	Delete(ctx context.Context, playerID, mailID int64) error
	DeliverOffline(ctx context.Context, playerID int64, bizID, title, content string, items []Attachment) (int64, bool, error)
}

// mailTable 邮件主表名。
//
// 抽常量是因为它出现在三个地方且必须一致：store.go 的 SQL、归属索引的
// 声明（IndexSpecs）、Redis key 前缀（cache.OwnerIndexKey）。三处写字面量
// 时改一处忘两处，索引会静默指向一张不存在的表——Expansion 永远为空，
// 列表静默回落 SQL，没有任何报错。
const mailTable = "mail"

// Module 邮件模块。
//
// 无内存态、无锁：邮件数据全在 MySQL，OnMessage 在玩家 Actor 内串行执行，
// 跨玩家一致性由 DB 约束（唯一键）与快照业务键分工保证。
//
// 基座是 Define[NoState]：邮件本身没有 Ext 槽位；领取的幂等键寄生在
// 玩家快照上（ApplyOnce），那是 Context 的能力，与 D 的选择无关。
type Module struct {
	*module.Bound[module.NoState]

	// repo 仓储；nil 表示无 DB 环境（列表/领取降级，离线发放报错，见各方法）
	repo   mailRepo
	logger *log.Logger

	// index 归属索引（by_owner:mail）。nil = 不启用，列表回落直查 SQL。
	//
	// 邮件是「一人多行」里最标准的一个：player_id 单一归属、新增/删除只动
	// 自己那一份。因此它是第一个真正消费归属索引的模块——新增时 Add、
	// 列表时 Expand，从而把「索引 → 展开 → 回填 → 自愈」整条链路跑在真实
	// 业务上，而不是只活在单测里。
	index *cache.OwnerIndex
}

// ready 数据库是否可用。
//
// 两种「不可用」都要算：仓储压根没注入（无 DB 环境的模块级降级，repo 是
// nil 接口），以及仓储在但没配连接池（NewRepo(nil)，Ready() 为 false）。
// 只判前者会让后者变成 nil deref，只判后者会对 nil 接口调用方法——
// 所以这里是唯一一处同时判两者的地方，业务只写 if !m.ready()。
func (m *Module) ready() bool { return m.repo != nil && m.repo.Ready() }

// SetOwnerIndex 注入归属索引（组装层在 Register 之前调用）。
//
// nil 表示不启用（单测/无 Redis 环境）：列表自动回落 ListByPlayer，
// 行为与接入索引之前完全一致——这正是「索引是加速层，不是正确性前提」
// 在模块侧的体现。
func (m *Module) SetOwnerIndex(oi *cache.OwnerIndex) {
	m.index = oi
	if r, ok := m.repo.(*Repo); ok {
		r.AttachIndex(oi) // 仓储层也记一份，便于「有没有索引」在仓储侧可查
	}
}

// IndexSpecs 声明本模块需要的归属索引（module.IndexDeclarer）。
//
// 声明在代码里而不是只写在 configs/*.yaml 的 idtab.mappings：漏配 yaml
// 没有任何提示，只会让列表静默回落 SQL——服务照跑，性能掉一半，日志里
// 一个字都没有。配置现在只负责「要不要建」（Redis 不可用时不建）。
func (m *Module) IndexSpecs() []module.IndexSpec {
	return []module.IndexSpec{{Table: mailTable, By: "player_id", Key: "id"}}
}

// AttachIndex 接住组装层建好的索引（module.IndexInjector）。
func (m *Module) AttachIndex(table string, oi *cache.OwnerIndex) {
	if table == mailTable {
		m.SetOwnerIndex(oi)
	}
}

// New 创建邮件模块。
func New(repo mailRepo, logger *log.Logger) *Module {
	m := &Module{repo: repo, logger: logger}
	m.Bound = module.Define[module.NoState]("mail").
		WithLogger(logger).
		WithLogin(m.onLogin).
		WithLogout(m.onLogout)
	module.Handle(m.Bound, m.onList)
	module.Handle(m.Bound, m.onTake)
	module.Handle(m.Bound, m.onDelete)
	return m
}

// onLogin 登录时拉一次未读数（经 WithLogin 挂接）。
//
// 目前只记 Debug 日志（红点以客户端列表请求为准，这里是服务端留痕，
// 供「玩家反馈没收到邮件」时对账）。
// 查询失败直接上抛：OnLogin 失败会中断登录流程——DB 故障时拒绝放行
// 好过放行后玩家发现奖励全丢。
func (m *Module) onLogin(ctx *module.Ctx[module.NoState]) error {
	n, err := m.repo.UnreadCount(context.Background(), ctx.State.PlayerID())
	if err != nil {
		return err
	}
	if n > 0 {
		m.logger.Debug("unread mails", "player_id", ctx.State.PlayerID(), "count", n)
	}
	return nil
}

// onLogout 下线时触发一次常规 flush 周期（经 WithLogout 挂接）。
//
// 邮件无 Ext 快照，MarkDirty 只是触发一次常规 flush 周期（与其他模块
// 生命周期保持一致，无害）；真正的数据在 MySQL，发送时已直写。
func (m *Module) onLogout(ctx *module.Ctx[module.NoState]) error {
	ctx.State.MarkDirty("mail")
	return nil
}
