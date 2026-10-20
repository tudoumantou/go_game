// internal/login/audit.go —— 登录流水生产（R3.2）
//
// 架构职责：把每次登录尝试（成功/失败）构造成 LoginLogMsg 投递到 Kafka
// topic gs.login.log，由游戏服消费后落 login_log 表，供风控与客服追溯。
// 协作关系：handler.AuthHandler.record 在登录返回前后调用 Log；
// 消息格式由 pkgmq.Envelope 统一封装（biz_id + 分区键 + payload）。
// 设计取舍：审计链路与主链路彻底解耦——生产失败只 warn 不返回错误，
// 登录是主链路，不能因审计抖动把玩家挡在门外；代价是极端情况下
// 流水可能丢失，用「允许重复、容忍丢失」换主链路可用性。
// 分区键用 account_id 字符串：同一账号的登录事件落同一分区，
// 消费端按账号有序处理（重放/乱序攻击的检测依赖顺序）。
package login

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"game_study/internal/login/handler"
	"game_study/pkg/log"
	pkgmq "game_study/pkg/mq"
)

// loginAudit 把登录事件写到 gs.login.log，由游戏服消费落 login_log 表。
//
// 生产失败只记日志：登录是主链路，不能因为审计链路抖动把玩家挡在门外。
// 每条事件用 account+毫秒 做 biz_id，消费端不做去重（流水允许重复行）。
// 知识点：结构体用值接收者（a loginAudit）而非指针——只读两个字段、
// 无内部状态，值拷贝零成本；handler.LoginLogger 接口因此既能被
// *loginAudit 也能被 loginAudit 满足（值方法集包含于指针方法集）。
type loginAudit struct {
	producer *pkgmq.Producer
	logger   *log.Logger
}

// Log 记录一次登录尝试。
// producer 为 nil（未配 Kafka）时静默返回：审计是可选能力，
// 主流程不依赖它存在。ctx 直接透传给 ProduceEnvelope，调用方取消
// （如 HTTP 请求已结束）时生产随之放弃，避免无谓投递。
func (a loginAudit) Log(ctx context.Context, ev handler.LoginEvent) {
	if a.producer == nil {
		return
	}
	msg := pkgmq.LoginLogMsg{
		AccountID: ev.AccountID,
		PlayerID:  ev.PlayerID,
		IP:        ev.IP,
		Device:    ev.Device,
		Result:    ev.Result,
		Reason:    ev.Reason,
		At:        time.Now().UnixMilli(),
	}
	// biz_id 带毫秒时间戳：同一账号连点两次登录也会得到不同 id，
	// 重复行可区分；配合分区键保证同账号事件有序
	bizID := fmt.Sprintf("login-%d-%d", ev.AccountID, msg.At)
	// 知识点：UnixMilli 毫秒时间戳而非 RFC3339 字符串——省带宽、
	// 免时区歧义，展示层再格式化；代价是人类不可直读，用日志兜底
	env, err := pkgmq.NewEnvelope(bizID, pkgmq.TopicLoginLog, strconv.FormatInt(ev.AccountID, 10), msg)
	if err != nil {
		a.warn("login log envelope failed", err)
		return
	}
	if err := a.producer.ProduceEnvelope(ctx, env); err != nil {
		a.warn("login log produce failed", err)
	}
}

// warn 统一的告警出口；logger 为 nil（理论上不该发生）时安全退化为 no-op，
// 防止审计路径自己把主流程带崩。
func (a loginAudit) warn(msg string, err error) {
	if a.logger != nil {
		a.logger.Warn(msg, "err", err)
	}
}
