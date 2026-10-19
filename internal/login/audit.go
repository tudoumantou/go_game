// internal/login/audit.go —— 登录流水生产（R3.2）
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
type loginAudit struct {
	producer *pkgmq.Producer
	logger   *log.Logger
}

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
	bizID := fmt.Sprintf("login-%d-%d", ev.AccountID, msg.At)
	env, err := pkgmq.NewEnvelope(bizID, pkgmq.TopicLoginLog, strconv.FormatInt(ev.AccountID, 10), msg)
	if err != nil {
		a.warn("login log envelope failed", err)
		return
	}
	if err := a.producer.ProduceEnvelope(ctx, env); err != nil {
		a.warn("login log produce failed", err)
	}
}

func (a loginAudit) warn(msg string, err error) {
	if a.logger != nil {
		a.logger.Warn(msg, "err", err)
	}
}
