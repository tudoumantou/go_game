// internal/game/mq/loginlog_consumer.go —— 登录流水落库（gs.login.log，R3.2）
//
// 架构职责：把 login 服产生的登录流水异步落库，从登录热路径上摘走 DB 写入。
// 协作关系：上游 login 服生产 gs.login.log；本消费者经 pkg/mq.Consumer
// 驱动（重试/死信/commit 由 pkg/mq 统一负责），依赖 db.DB 执行 INSERT。
// 设计取舍：
//   - 共享 group 而非每实例独立 group：Kafka 同 group 内一个分区只被一个
//     实例消费，登录流水只需要写一份；每实例独立 group 等于广播，同一行
//     流水会被 N 个游戏服实例各写一次（group.go 里其它 topic 正相反，
//     它们是刻意的广播语义）。
//   - 不做业务去重：流水是事实记录，重复投递只产生重复行，不影响玩家
//     状态；为它引 Redis 幂等键反而增加依赖与延迟，不值得。
package mq

import (
	"context"
	"fmt"

	"game_study/pkg/db"
	"game_study/pkg/log"
	"game_study/pkg/model"
	pkgmq "game_study/pkg/mq"
	"game_study/pkg/sqlstore"
)

// LoginLogConsumer 把登录流水写入 login_log 表。
//
// 与其它消费者不同，它使用**集群共享 group**（group.go 里不带实例后缀）：
// 每实例独立消费会让同一行流水被所有游戏服实例各写一次。
// 也不做业务去重：流水是事实记录，重投产生重复行不影响玩家状态。
// 字段契约：consumer 由外部装配（含 topic/group 选择）；db 为 nil 时
// handle 返回错误（装配缺失应暴露，不该静默丢消息）。
// 连接池经 sqlstore.Base 提供，nil 判断统一走 Ready()。
type LoginLogConsumer struct {
	consumer *pkgmq.Consumer
	*sqlstore.Base
	logger *log.Logger
}

// NewLoginLogConsumer 创建消费者。
// 依赖全部外部注入（pkg/mq.Consumer + db.DB + logger），内部不新建连接——
// 与 Group 的装配方式保持一致，便于测试替换。
func NewLoginLogConsumer(consumer *pkgmq.Consumer, database *db.DB, logger *log.Logger) *LoginLogConsumer {
	return &LoginLogConsumer{consumer: consumer, Base: sqlstore.New(database, "login_log"), logger: logger}
}

// Run 阻塞消费。
// 直接转发给 pkg/mq.Consumer.Run：重试、退避、死信、offset 提交全部
// 由框架层负责，本消费者只提供 handle 业务逻辑。
func (c *LoginLogConsumer) Run(ctx context.Context) error {
	return c.consumer.Run(ctx, c.handle)
}

// Close 关闭。
// nil 判空：未装配 Kafka（cfg.Kafka.Brokers 为空）时 Group 不会创建本
// 消费者，但测试/装配流程可能拿到零值实例，Close 需幂等安全。
func (c *LoginLogConsumer) Close() error {
	if c.consumer == nil {
		return nil
	}
	return c.consumer.Close()
}

// handle 落库一条登录流水。
// 返回 error 即触发 pkg/mq 的重试/死信路径；返回 nil 即视为处理完成。
func (c *LoginLogConsumer) handle(ctx context.Context, env *pkgmq.Envelope) error {
	var msg pkgmq.LoginLogMsg
	// 载荷解析失败直接返回 error：由 pkg/mq 重试 N 次后进死信，
	// 不在这里单独吞掉——死信里留下原始字节才有人工补偿的线索。
	if err := env.DecodePayload(&msg); err != nil {
		return fmt.Errorf("decode login log: %w", err)
	}
	// account_id 是流水的主干字段，<=0 说明生产者发错了数据；
	// 返回 error 让它进死信暴露问题，而不是写一行 account_id=0 的脏数据
	// 静默污染流水表（重试几次成本远低于事后清洗）。
	if msg.AccountID <= 0 {
		return fmt.Errorf("invalid login log: account_id=%d", msg.AccountID)
	}
	// db 缺失是装配错误（Group 里 deps.DB 为 nil 时本不该被创建），
	// 显式报错而不是 panic——让上层看到消费者停止并记录原因。
	if !c.Ready() {
		return fmt.Errorf("login log consumer without database")
	}
	// 用生成的 InsertLoginLog：列名来自 schema，加列/改列时编译期就能发现。
	// 参数化与 ctx 取消语义不变（生成函数内部仍是 ExecContext）。
	// 重复行由「登录流水不去重」的设计兜底（每次登录尝试都是一条独立事实）。
	_, err := model.InsertLoginLog(ctx, c.SQL(), &model.LoginLog{
		AccountId: msg.AccountID,
		PlayerId:  msg.PlayerID,
		Ip:        msg.IP,
		Device:    msg.Device,
		Result:    msg.Result,
		Reason:    msg.Reason,
	})
	if err != nil {
		return fmt.Errorf("insert login_log: %w", err)
	}
	return nil
}
