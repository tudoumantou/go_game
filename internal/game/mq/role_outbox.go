// internal/game/mq/role_outbox.go —— 角色索引 outbox 投递协程
//
// 架构职责：把 role_sync_outbox 里的待投递行可靠地送到 gs.role.created，
// 让登录服的 user_role 索引追上游戏服的创角事实。是「选角列表能看见新角色」
// 这条链路的唯一驱动者。
//
// 协作关系：创角事务（player.RoleStore.Create）写行 → 本协程轮询投递 →
// login 侧 RoleCreatedConsumer 消费并 UPSERT。本协程只依赖 DB + Producer，
// 不感知 Actor/在线状态，因为创角是登录前的事。
//
// 设计取舍：
//   - 为什么是轮询而不是「创角后直接触发」：直接触发需要把 Dispatcher 注入
//     到 RoleStore，而 RoleStore 属于持久化层、不该认识 MQ；轮询把解耦放在
//     这里，代价是最多 interval 的延迟（创角后客户端直接进游戏，选角列表
//     要等下次登录才看，2s 延迟无感）。
//   - 为什么成功后才置 PUBLISHED，而不是「先置再发」：先置会丢消息
//     （at-most-once），后置是 at-least-once —— 重复投递由 user_role 的
//     唯一键吸收，代价远小于丢。
//   - 为什么重试有上限：Broker 长期故障时无限重试会让 pending 表无限膨胀，
//     且这些行本来也发不出去。转 DEAD 留行不删，是为了「哪些角色没同步」
//     可查、可人工补投；删掉就等于放弃。
package mq

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"time"

	"game_study/internal/game/player"
	"game_study/pkg/db"
	"game_study/pkg/log"
	"game_study/pkg/metrics"
	"game_study/pkg/model"
	pkgmq "game_study/pkg/mq"
)

const (
	// roleOutboxBatch 单轮最多投递行数。
	// 正常情况 pending ≈ 0，这个值只在「Kafka 恢复后追积压」时有意义；
	// 50 条/轮 × 2s 一轮 ≈ 25 条/s，追 10 万条要一小时——够用，且不会在
	// 恢复瞬间把 Producer 打满影响充值/邮件等高频 topic。
	roleOutboxBatch = 50
	// roleOutboxInterval 轮询间隔。
	roleOutboxInterval = 2 * time.Second
	// roleOutboxMaxRetry 重试上限（转 DEAD 前的最大失败次数）。
	roleOutboxMaxRetry = 10
	// roleOutboxMaxBackoff 单条最大退避，防止 2^n 退避到天荒地老。
	roleOutboxMaxBackoff = 5 * time.Minute
)

// roleOutboxPendingQuery 取待投递行。
//
// 列顺序与 model.RoleSyncOutbox 的字段顺序严格一致（scanRoleOutboxRow 依赖它）。
// 走 idx_pending(status, next_retry_ms)：status 是等值条件、next_retry_ms 是
// 范围条件，正好命中联合索引前缀，不会退化成全表扫描。
const roleOutboxPendingQuery = `SELECT id, player_id, account_id, server_id, nickname, level, status, retry_count, next_retry_ms, created_at
FROM role_sync_outbox
WHERE status = ? AND next_retry_ms <= ?
ORDER BY id
LIMIT ?`

// EnvelopeProducer 信封发送能力（*pkgmq.Producer 实现；测试注入假实现）。
//
// 定成接口而不是直接用 *pkgmq.Producer：投递器的全部逻辑都在「发失败怎么办」
// 上（退避、转 DEAD、留行），这些分支必须能测；真 Kafka 只能测到成功路径。
type EnvelopeProducer interface {
	ProduceEnvelope(ctx context.Context, env *pkgmq.Envelope) error
}

// RoleOutboxDispatcher 角色索引 outbox 投递器。
type RoleOutboxDispatcher struct {
	db       *db.DB
	producer EnvelopeProducer
	logger   *log.Logger
	interval time.Duration
}

// NewRoleOutboxDispatcher 创建投递器。
// producer 为 nil 时投递器不启动（无 Kafka 的单机/测试环境），
// 代价是创角只落本地库、登录服看不到索引——这在单服部署下可接受。
//
// 注意：调用方必须先判断 *pkgmq.Producer 是否为 nil 再传进来。
// 把一个 nil 的 *pkgmq.Producer 赋给接口会得到非 nil 接口值，
// 这里的 nil 判断会失效（Go 的 typed-nil 陷阱）。
func NewRoleOutboxDispatcher(database *db.DB, producer EnvelopeProducer, logger *log.Logger) *RoleOutboxDispatcher {
	return &RoleOutboxDispatcher{
		db:       database,
		producer: producer,
		logger:   logger,
		interval: roleOutboxInterval,
	}
}

// Run 阻塞轮询直到 ctx 取消。
// 启动即投一轮：进程重启后积压的行不该再等一个 interval（那会让
// 「重启后玩家短暂看不到角色」变成可观测的问题）。
func (d *RoleOutboxDispatcher) Run(ctx context.Context) error {
	if d == nil || d.db == nil || d.producer == nil {
		return nil
	}
	ticker := time.NewTicker(d.interval)
	defer ticker.Stop()
	for {
		d.drain(ctx)
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

// Flush 在关服时把当前可投递的行尽力投完。
//
// 与 Run 的差别是「只跑一轮、不循环」：关服有总时限，不能因为 Kafka 卡住
// 就把停机拖死。没投完的行留在库里，下次启动由 Run 的首轮补上。
func (d *RoleOutboxDispatcher) Flush(ctx context.Context) {
	if d == nil || d.db == nil || d.producer == nil {
		return
	}
	d.drain(ctx)
}

// drain 连续投递直到本轮不足一批或 ctx 取消。
func (d *RoleOutboxDispatcher) drain(ctx context.Context) {
	for {
		n, err := d.dispatchOnce(ctx)
		if err != nil {
			// 查询失败（DB 抖动）只告警：下一轮会重来，不该让协程退出。
			if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				d.logger.Warn("role outbox dispatch failed", "err", err)
			}
			return
		}
		// 不足一批说明已追平；满批则可能还有，继续下一轮。
		if n < roleOutboxBatch {
			return
		}
		select {
		case <-ctx.Done():
			return
		default:
		}
	}
}

// dispatchOnce 投递一批，返回本批处理行数。
func (d *RoleOutboxDispatcher) dispatchOnce(ctx context.Context) (int, error) {
	rows, err := d.loadPending(ctx)
	if err != nil {
		return 0, err
	}
	if len(rows) == 0 {
		metrics.RoleOutboxPendingRows(0)
		return 0, nil
	}
	metrics.RoleOutboxPendingRows(len(rows))

	published := 0
	for i := range rows {
		// 每条都检查 ctx：关服时能立刻让路，而不是把这批发完才响应。
		if err := ctx.Err(); err != nil {
			return i, err
		}
		if err := d.publishOne(ctx, &rows[i]); err != nil {
			// 单条失败继续下一条：一条脏消息不该阻塞后面的角色同步。
			// 失败处理已在 publishOne 内写库（退避/DEAD），这里只计数。
			d.logger.Warn("role outbox publish failed",
				"player_id", rows[i].PlayerId, "retry", rows[i].RetryCount+1, "err", err)
			continue
		}
		published++
	}
	metrics.RoleOutboxPublished(published)
	return len(rows), nil
}

// loadPending 读取到期可投递的行。
func (d *RoleOutboxDispatcher) loadPending(ctx context.Context) ([]model.RoleSyncOutbox, error) {
	rows, err := d.db.QueryContext(ctx, roleOutboxPendingQuery,
		player.RoleOutboxPending, time.Now().UnixMilli(), roleOutboxBatch)
	if err != nil {
		return nil, fmt.Errorf("query role_sync_outbox: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := make([]model.RoleSyncOutbox, 0, roleOutboxBatch)
	for rows.Next() {
		var m model.RoleSyncOutbox
		if err := scanRoleOutboxRow(&m, rows); err != nil {
			return nil, fmt.Errorf("scan role_sync_outbox: %w", err)
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// scanRoleOutboxRow 按 roleOutboxPendingQuery 的列顺序扫描。
func scanRoleOutboxRow(m *model.RoleSyncOutbox, sc interface{ Scan(...any) error }) error {
	return sc.Scan(&m.Id, &m.PlayerId, &m.AccountId, &m.ServerId, &m.Nickname,
		&m.Level, &m.Status, &m.RetryCount, &m.NextRetryMs, &m.CreatedAt)
}

// publishOne 投递单条并在库内记录结果。
func (d *RoleOutboxDispatcher) publishOne(ctx context.Context, row *model.RoleSyncOutbox) error {
	env, err := pkgmq.NewEnvelope(
		// biz_id 用角色 ID：同一角色的索引同步天然是个「状态」而不是「事件」，
		// 重投同一 biz_id 只会让消费端重复写同一行（唯一键吸收），
		// 因此不需要按时间戳区分——那会让幂等键失去意义。
		"role:"+strconv.FormatInt(row.PlayerId, 10),
		pkgmq.TopicRoleCreated,
		// Key 用 account_id：同一账号在多个区服的角色同步落到同一分区，
		// 选角列表不会出现「后建的角色先到、先建的后来」的顺序错乱。
		strconv.FormatInt(row.AccountId, 10),
		pkgmq.RoleCreatedMsg{
			PlayerID:  row.PlayerId,
			AccountID: row.AccountId,
			ServerID:  row.ServerId,
			Nickname:  row.Nickname,
			Level:     row.Level,
			CreatedAt: row.CreatedAt,
		},
	)
	if err != nil {
		// 编码失败是代码/数据问题（不会自愈），但仍按失败退避处理：
		// 让它在 DEAD 里留痕，而不是在这里 panic 掉整个投递协程。
		d.markFailed(ctx, row)
		return fmt.Errorf("build envelope: %w", err)
	}
	if err := d.producer.ProduceEnvelope(ctx, env); err != nil {
		d.markFailed(ctx, row)
		return err
	}
	d.markPublished(ctx, row)
	return nil
}

// markPublished 置 PUBLISHED。
// 写库失败只告警：消息已经发出去了，下轮会重投（消费端幂等），
// 数据正确性不受影响，只是多一次无用投递。
func (d *RoleOutboxDispatcher) markPublished(ctx context.Context, row *model.RoleSyncOutbox) {
	row.Status = player.RoleOutboxPublished
	row.RetryCount = 0
	row.NextRetryMs = 0
	if _, err := model.UpdateRoleSyncOutboxByPK(ctx, d.db, row); err != nil {
		d.logger.Warn("role outbox mark published failed",
			"player_id", row.PlayerId, "err", err)
	}
}

// markFailed 累加重试次数并计算下次可投递时间；超上限转 DEAD。
//
// 退避用 2^retry 秒并封顶：Broker 抖动通常几秒恢复，指数退避能让恢复后
// 的第一批立刻打进去；封顶 5 分钟避免长时间故障后「积压行同时到期」
// 造成尖峰（配合每轮 50 条的上限已经足够平滑）。
func (d *RoleOutboxDispatcher) markFailed(ctx context.Context, row *model.RoleSyncOutbox) {
	metrics.RoleOutboxFailed()
	row.RetryCount++
	if row.RetryCount >= roleOutboxMaxRetry {
		row.Status = player.RoleOutboxDead
		row.NextRetryMs = 0
		if _, err := model.UpdateRoleSyncOutboxByPK(ctx, d.db, row); err != nil {
			d.logger.Warn("role outbox mark dead failed",
				"player_id", row.PlayerId, "err", err)
		}
		metrics.RoleOutboxDead()
		// Error 级别：这一行意味着「某个玩家在选角界面看不到自己刚建的角色」，
		// 且不再自动重试，必须有人看见。
		d.logger.Error("role outbox dead, role index will stay missing",
			"player_id", row.PlayerId, "account_id", row.AccountId,
			"server_id", row.ServerId, "retries", row.RetryCount)
		return
	}
	backoff := time.Duration(1<<uint(row.RetryCount)) * time.Second
	if backoff > roleOutboxMaxBackoff {
		backoff = roleOutboxMaxBackoff
	}
	row.NextRetryMs = time.Now().Add(backoff).UnixMilli()
	if _, err := model.UpdateRoleSyncOutboxByPK(ctx, d.db, row); err != nil {
		d.logger.Warn("role outbox mark failed",
			"player_id", row.PlayerId, "err", err)
	}
}

// 编译期断言：*sql.DB 满足 Querier（model 生成函数的入参约束）。
// 放在这里是为了让「db.DB 内嵌 *sql.DB」这一事实在改动时编译期暴露。
var _ model.Querier = (*sql.DB)(nil)
