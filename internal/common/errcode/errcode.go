// Package errcode 统一错误码（设计文档 4.10）。
//
// 单一事实源在 api/proto/common/v1/errcode.proto（随生成体系进 pkg/pb，客户端同源生成）；
// 本包提供 Go 侧常量别名、中文文案表（可热更新，不进 wire）与带码 error。
//
// 架构职责：错误码的「双源一致性」守护层——proto 定义数值，本包定义
// Go 侧名字、中文文案与 error 封装。
//
// 现状与边界（重要，别被下面的设计意图误导）：
//   - 本包目前主要被 errcheck_test.go 使用：它守护「业务模块自建码不得与
//     proto 已定义值撞车」这条规则（撞了客户端会按 proto 文案提示错误的失败原因）。
//   - 业务模块实际返回的是 int32 码（各自分段的 codeXxx），不是 *Error；
//     即「proto 语义码」与「模块段码」两套并存：proto 那套覆盖跨模块共性
//     语义（道具不足/背包满…），模块段码覆盖各自玩法（41xx 场景、5xxx 邮件…）。
//   - FromError/New 是给「将来接入层统一错误通道」预留的封装，当前无调用点；
//     真要用起来时，应当在网关/RPC 边界收口，而不是逐模块改返回类型。
//
// 设计取舍：
//   - 数值单一事实源放 proto：客户端（多语言）与服务端从同一份定义生成，
//     杜绝「两边枚举值手抄不一致」；errcode_test.go 的 parity 测试守护
//     Go 常量与 proto 一一对应，漏加/漏改 CI 即红。
//   - 文案表放 Go 侧而非 proto/wire：文案不进协议省带宽，且可独立热更
//     （改文案不用改协议版本）；服务端 Msg 字段仅作上下文补充
//     （如「道具数量不足：还差 3 个」），客户端提示以本地文案表为准。
package errcode

import (
	"errors"
	"fmt"

	commonv1 "game_study/pkg/pb/common/v1"
)

// Code 错误码类型（proto 枚举别名）
// 用类型别名（=）而非新定义类型：与 pkg/pb 的枚举零成本互转——
// switch、map 键、proto 赋值处无需任何转换，两套名字指同一类型。
type Code = commonv1.ErrCode

// 错误码常量（与 errcode.proto 一一对应；parity 由 errcode_test.go 守护）
//
// 分段规则（proto 侧编号，按业务域占段、段内从 x001 顺序发号）：
//
//	0        OK —— 成功，唯一非错误码
//	1001+    通用/登录态：参数、令牌过期、维护、限频、过载（协议级故障）
//	2001+    账号/角色：封禁、踢下线、未创角、角色名相关
//	3001+    道具/背包：不存在、不足、背包满、不可用
//	4001+    社交/聊天：频道、对象、发言频率、禁言
//	5001+    邮件：不存在、已领取（幂等保护）、无附件
//	6001+    战斗：不存在、令牌、结果已上报（幂等保护）、房间满、奖励无效
//
// 为什么按域分段而非全局递增：新业务的错误码只需在自己段里发号，
// 不与其他域争抢；客户端可按段做粗粒度分类（如 2xxx 一律弹回登录页）。
// 段内留号（如 1006+ 空置）：同域新增错误向后追加，不打乱已有编号。
const (
	OK                     = commonv1.ErrCode_ERR_CODE_OK
	ErrSystem              = commonv1.ErrCode_ERR_CODE_SYSTEM
	ErrParam               = commonv1.ErrCode_ERR_CODE_PARAM
	ErrTokenExpired        = commonv1.ErrCode_ERR_CODE_TOKEN_EXPIRED
	ErrMaintenance         = commonv1.ErrCode_ERR_CODE_MAINTENANCE
	ErrRateLimited         = commonv1.ErrCode_ERR_CODE_RATE_LIMITED
	ErrServerBusy          = commonv1.ErrCode_ERR_CODE_SERVER_BUSY
	ErrAccountBanned       = commonv1.ErrCode_ERR_CODE_ACCOUNT_BANNED
	ErrKicked              = commonv1.ErrCode_ERR_CODE_KICKED
	ErrNoRole              = commonv1.ErrCode_ERR_CODE_NO_ROLE
	ErrNameUsed            = commonv1.ErrCode_ERR_CODE_NAME_USED
	ErrNameIllegal         = commonv1.ErrCode_ERR_CODE_NAME_ILLEGAL
	ErrItemNotExist        = commonv1.ErrCode_ERR_CODE_ITEM_NOT_EXIST
	ErrItemNotEnough       = commonv1.ErrCode_ERR_CODE_ITEM_NOT_ENOUGH
	ErrBagFull             = commonv1.ErrCode_ERR_CODE_BAG_FULL
	ErrItemNotUsable       = commonv1.ErrCode_ERR_CODE_ITEM_NOT_USABLE
	ErrChannelClosed       = commonv1.ErrCode_ERR_CODE_CHANNEL_CLOSED
	ErrPeerNotFound        = commonv1.ErrCode_ERR_CODE_PEER_NOT_FOUND
	ErrChatTooFast         = commonv1.ErrCode_ERR_CODE_CHAT_TOO_FAST
	ErrChatMuted           = commonv1.ErrCode_ERR_CODE_CHAT_MUTED
	ErrMailNotFound        = commonv1.ErrCode_ERR_CODE_MAIL_NOT_FOUND
	ErrMailTaken           = commonv1.ErrCode_ERR_CODE_MAIL_TAKEN
	ErrNoAttachment        = commonv1.ErrCode_ERR_CODE_NO_ATTACHMENT
	ErrBattleNotFound      = commonv1.ErrCode_ERR_CODE_BATTLE_NOT_FOUND
	ErrBattleTokenInvalid  = commonv1.ErrCode_ERR_CODE_BATTLE_TOKEN_INVALID
	ErrResultReported      = commonv1.ErrCode_ERR_CODE_RESULT_REPORTED
	ErrBattleFull          = commonv1.ErrCode_ERR_CODE_BATTLE_FULL
	ErrResultRewardInvalid = commonv1.ErrCode_ERR_CODE_RESULT_REWARD_INVALID
	ErrSigninConfig        = commonv1.ErrCode_ERR_CODE_SIGNIN_CONFIG
	ErrSigninRewardLimit   = commonv1.ErrCode_ERR_CODE_SIGNIN_REWARD_LIMIT
	ErrSigninBagFull       = commonv1.ErrCode_ERR_CODE_SIGNIN_BAG_FULL
)

// messages 中文文案表（可热更新；不进协议 wire）
// map[Code]string：O(1) 查找，热路径（每个错误响应一次）无压力；
// 文案与错误码的对应关系集中一张表，运营改文案只动这里。
// 与常量块同等重要：新增错误码必须同步登记文案，否则 Message 走
// 「未知错误」兜底——客户端提示退化成数字，是可观测的故障信号。
var messages = map[Code]string{
	OK:                     "成功",
	ErrSystem:              "服务器内部错误",
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
	ErrSigninConfig:        "签到奖励配置错误",
	ErrSigninRewardLimit:   "签到奖励数量超出上限",
	ErrSigninBagFull:       "背包已满，无法领取签到奖励",
}

// Message 取中文文案（未登记返回通用文案）
// 兜底带数值（未知错误(%d)）：新老版本服务端/客户端错配时，运营从提示里
// 就能认出是哪个未登记码，排障不用翻服务端日志。
func Message(c Code) string {
	if m, ok := messages[c]; ok {
		return m
	}
	return fmt.Sprintf("未知错误(%d)", int32(c))
}

// Error 带错误码的 error（业务层返回，接入层转 Resp.code / ErrorNotify）
// 字段契约：Code 是必填的机器可读部分；Msg 可空——空则回退文案表，
// 业务方绝大多数场景 New(c) 即可，需要上下文（缺多少、哪张订单）才用 Newf。
type Error struct {
	// Code 业务错误码；客户端按它切换文案与行为（如 token 过期→回登录页）
	Code Code
	Msg  string // 空则取文案表
}

// Error 实现 error 接口。
// Msg 非空时优先返回（附带上下文，如具体缺少的道具数量）；
// 为空时回退文案表，保证日志与客户端提示始终中文可读。
// 指针 receiver（*Error）的含义：只有 *Error 实现 error 接口，
// 值类型 Error 不实现——配合 New 返回 *Error，避免「值拷贝丢码」，
// 也让 errors.As(&e) 的提取目标与构造端类型严格一致。
func (e *Error) Error() string {
	if e.Msg != "" {
		return e.Msg
	}
	return Message(e.Code)
}

// New 构造错误码 error
// 最常用入口：业务层 return errcode.New(errcode.ErrItemNotEnough)。
func New(c Code) *Error { return &Error{Code: c} }

// Newf 构造带上下文文案的错误码 error
// 文案拼接规则：文案表文案 + "：" + 格式化上下文，前缀保证客户端
// 未命中本地表时（服务端 Msg 直出场景）也有可读主干。
func Newf(c Code, format string, args ...any) *Error {
	return &Error{Code: c, Msg: Message(c) + "：" + fmt.Sprintf(format, args...)}
}

// FromError 提取错误码（非 Error 类型按 ErrParam 兜底）
// errors.As 而非类型断言：业务层常用 fmt.Errorf("...: %w", errcode.New(...))
// 包装错误，As 沿错误链逐层解包（依赖 Unwrap），包装不丢码。
// 兜底 ErrParam 而非 OK：未知错误对客户端表现为「参数错误」比「成功」
// 安全得多——至少不会让用户在失败时看到成功提示；同时也避免把内部
// 错误原文透出客户端（安全面）。
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
