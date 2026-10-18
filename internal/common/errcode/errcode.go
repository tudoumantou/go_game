// Package errcode 统一错误码（设计文档 4.10）。
//
// 单一事实源在 api/proto/common/v1/errcode.proto（随生成体系进 pkg/pb，客户端同源生成）；
// 本包提供 Go 侧常量别名、中文文案表（可热更新，不进 wire）与带码 error。
package errcode

import (
	"errors"
	"fmt"

	commonv1 "game_study/pkg/pb/common/v1"
)

// Code 错误码类型（proto 枚举别名）
type Code = commonv1.ErrCode

// 错误码常量（与 errcode.proto 一一对应；parity 由 errcode_test.go 守护）
const (
	OK                       = commonv1.ErrCode_ERR_CODE_OK
	ErrParam                 = commonv1.ErrCode_ERR_CODE_PARAM
	ErrTokenExpired          = commonv1.ErrCode_ERR_CODE_TOKEN_EXPIRED
	ErrMaintenance           = commonv1.ErrCode_ERR_CODE_MAINTENANCE
	ErrRateLimited           = commonv1.ErrCode_ERR_CODE_RATE_LIMITED
	ErrServerBusy            = commonv1.ErrCode_ERR_CODE_SERVER_BUSY
	ErrAccountBanned         = commonv1.ErrCode_ERR_CODE_ACCOUNT_BANNED
	ErrKicked                = commonv1.ErrCode_ERR_CODE_KICKED
	ErrNoRole                = commonv1.ErrCode_ERR_CODE_NO_ROLE
	ErrNameUsed              = commonv1.ErrCode_ERR_CODE_NAME_USED
	ErrNameIllegal           = commonv1.ErrCode_ERR_CODE_NAME_ILLEGAL
	ErrItemNotExist          = commonv1.ErrCode_ERR_CODE_ITEM_NOT_EXIST
	ErrItemNotEnough         = commonv1.ErrCode_ERR_CODE_ITEM_NOT_ENOUGH
	ErrBagFull               = commonv1.ErrCode_ERR_CODE_BAG_FULL
	ErrItemNotUsable         = commonv1.ErrCode_ERR_CODE_ITEM_NOT_USABLE
	ErrChannelClosed         = commonv1.ErrCode_ERR_CODE_CHANNEL_CLOSED
	ErrPeerNotFound          = commonv1.ErrCode_ERR_CODE_PEER_NOT_FOUND
	ErrChatTooFast           = commonv1.ErrCode_ERR_CODE_CHAT_TOO_FAST
	ErrChatMuted             = commonv1.ErrCode_ERR_CODE_CHAT_MUTED
	ErrMailNotFound          = commonv1.ErrCode_ERR_CODE_MAIL_NOT_FOUND
	ErrMailTaken             = commonv1.ErrCode_ERR_CODE_MAIL_TAKEN
	ErrNoAttachment          = commonv1.ErrCode_ERR_CODE_NO_ATTACHMENT
	ErrBattleNotFound        = commonv1.ErrCode_ERR_CODE_BATTLE_NOT_FOUND
	ErrBattleTokenInvalid    = commonv1.ErrCode_ERR_CODE_BATTLE_TOKEN_INVALID
	ErrResultReported        = commonv1.ErrCode_ERR_CODE_RESULT_REPORTED
	ErrBattleFull            = commonv1.ErrCode_ERR_CODE_BATTLE_FULL
	ErrResultRewardInvalid   = commonv1.ErrCode_ERR_CODE_RESULT_REWARD_INVALID
)

// messages 中文文案表（可热更新；不进协议 wire）
var messages = map[Code]string{
	OK:                     "成功",
	ErrParam:               "参数错误",
	ErrTokenExpired:        "登录已过期，请重新登录",
	ErrMaintenance:         "服务器维护中",
	ErrRateLimited:         "操作过于频繁，请稍后再试",
	ErrServerBusy:          "服务器繁忙",
	ErrAccountBanned:       "账号已被封禁",
	ErrKicked:              "账号在其他设备登录",
	ErrNoRole:              "尚未创建角色",
	ErrNameUsed:            "角色名已被占用",
	ErrNameIllegal:         "角色名含非法字符",
	ErrItemNotExist:        "道具不存在",
	ErrItemNotEnough:       "道具数量不足",
	ErrBagFull:             "背包已满",
	ErrItemNotUsable:       "道具当前不可使用",
	ErrChannelClosed:       "频道未开放",
	ErrPeerNotFound:        "对方不在线或不存在",
	ErrChatTooFast:         "发言过于频繁",
	ErrChatMuted:           "你已被禁言",
	ErrMailNotFound:        "邮件不存在",
	ErrMailTaken:           "附件已领取",
	ErrNoAttachment:        "邮件没有附件",
	ErrBattleNotFound:      "战斗不存在或已结束",
	ErrBattleTokenInvalid:  "战斗令牌无效",
	ErrResultReported:      "战斗结果已上报",
	ErrBattleFull:          "战斗房间已满",
	ErrResultRewardInvalid: "上报的奖励不存在",
}

// Message 取中文文案（未登记返回通用文案）
func Message(c Code) string {
	if m, ok := messages[c]; ok {
		return m
	}
	return fmt.Sprintf("未知错误(%d)", int32(c))
}

// Error 带错误码的 error（业务层返回，接入层转 Resp.code / ErrorNotify）
type Error struct {
	Code Code
	Msg  string // 空则取文案表
}

func (e *Error) Error() string {
	if e.Msg != "" {
		return e.Msg
	}
	return Message(e.Code)
}

// New 构造错误码 error
func New(c Code) *Error { return &Error{Code: c} }

// Newf 构造带上下文文案的错误码 error
func Newf(c Code, format string, args ...any) *Error {
	return &Error{Code: c, Msg: Message(c) + "：" + fmt.Sprintf(format, args...)}
}

// FromError 提取错误码（非 Error 类型按 ErrParam 兜底）
func FromError(err error) Code {
	if err == nil {
		return OK
	}
	var e *Error
	if errors.As(err, &e) {
		return e.Code
	}
	return ErrParam
}
