package domain

import (
	"errors"
	"fmt"
)

// 领域错误码（文档 15.4）。这些是**业务码**，与传输层错误码
// （internal/common/errcode 的 2001/4001 等）是不同概念：
// 业务码随 map_command.result_code 持久化，客户端按它决定
// “修正参数”还是“用同一 request_id 重试”。
const (
	CodeOK                  int32 = 0
	CodeInvalidPosition     int32 = 44001
	CodePositionOccupied    int32 = 44002
	CodeObjectNotFound      int32 = 44003
	CodeNotOwner            int32 = 44004
	CodeSlotBusy            int32 = 44005
	CodeTargetOccupied      int32 = 44006
	CodeTroopBusy           int32 = 44007
	CodeStaleVersion        int32 = 44008
	CodeIdempotencyConflict int32 = 44009
	CodePathBlocked         int32 = 44010
	CodeCapacityExhausted   int32 = 44011
	CodeWorldRecovering     int32 = 44012
	CodeOverloaded          int32 = 44013
	CodeOperationPending    int32 = 44014
	CodeQueueFull           int32 = 44015
	CodePayloadMismatch     int32 = 44016
	CodeFeatureDisabled     int32 = 44017
)

// 领域哨兵错误。用 errors.Is 判定，不在调用方做字符串匹配。
// 每个哨兵错误通过 %w 包装携带上下文（对象 ID、数值），
// 既保留可判定性又不丢诊断信息。
var (
	// ErrInvalidTopology 拓扑配置非法（尺寸/整除/溢出）。
	ErrInvalidTopology = errors.New("worldmap: 拓扑非法")
	// ErrInvalidArgument 参数非法（格式类错误，不写回执）。
	ErrInvalidArgument = errors.New("worldmap: 参数非法")
	// ErrOverflow 整数运算溢出。
	ErrOverflow = errors.New("worldmap: 整数溢出")
	// ErrObjectNotFound 对象不存在或已删除。
	ErrObjectNotFound = errors.New("worldmap: 对象不存在")
	// ErrNotOwner 对象不属于该玩家。
	ErrNotOwner = errors.New("worldmap: 对象归属不符")
	// ErrPositionOccupied 目标格被占用（map_occupancy 唯一键）。
	ErrPositionOccupied = errors.New("worldmap: 位置被占用")
	// ErrSlotBusy 玩家活动槽位已被占用（INV-05）。
	ErrSlotBusy = errors.New("worldmap: 出征槽位占用")
	// ErrTargetOccupied 资源点已被其他玩家采集（不是事务错误，
	// 客户端按规则回城或攻击）。
	ErrTargetOccupied = errors.New("worldmap: 目标被占用")
	// ErrTroopBusy 部队有未完成的 Busy 转换。
	ErrTroopBusy = errors.New("worldmap: 部队忙")
	// ErrStaleVersion 对象版本过期（乐观并发失败）。
	ErrStaleVersion = errors.New("worldmap: 版本过期")
	// ErrIdempotencyConflict 同幂等键不同参数。
	ErrIdempotencyConflict = errors.New("worldmap: 幂等键冲突")
	// ErrPathBlocked 路径被静态地形阻挡。不允许偷偷穿墙。
	ErrPathBlocked = errors.New("worldmap: 路径受阻")
	// ErrCapacityExhausted 负载/余量不足。
	ErrCapacityExhausted = errors.New("worldmap: 容量不足")
	// ErrWorldRecovering 世界恢复中，拒绝新写。
	ErrWorldRecovering = errors.New("worldmap: 世界恢复中")
	// ErrOverloaded 过载（mailbox 满/限流）。未接受新命令。
	ErrOverloaded = errors.New("worldmap: 过载")
	// ErrOperationPending 已接受但结果待解析。
	ErrOperationPending = errors.New("worldmap: 操作待解析")
	// ErrQueueFull 目标战斗队列满。
	ErrQueueFull = errors.New("worldmap: 战斗队列满")
	// ErrPayloadMismatch 交接单 payload 与玩家收据不一致（隔离）。
	ErrPayloadMismatch = errors.New("worldmap: 交接单不一致")
	// ErrFeatureDisabled 功能未开放（如第一版关闭改速）。
	ErrFeatureDisabled = errors.New("worldmap: 功能未开放")
	// ErrInvalidState 状态机非法组合。
	ErrInvalidState = errors.New("worldmap: 状态非法")
)

// CodeOf 把领域错误映射为业务码。未识别的错误按 44013（Overloaded）
// 处理而不是 0——把未知错误当成成功是最危险的事故模式。
func CodeOf(err error) int32 {
	if err == nil {
		return CodeOK
	}
	switch {
	case errors.Is(err, ErrInvalidTopology), errors.Is(err, ErrInvalidArgument):
		return CodeInvalidPosition
	case errors.Is(err, ErrObjectNotFound):
		return CodeObjectNotFound
	case errors.Is(err, ErrNotOwner):
		return CodeNotOwner
	case errors.Is(err, ErrPositionOccupied):
		return CodePositionOccupied
	case errors.Is(err, ErrSlotBusy):
		return CodeSlotBusy
	case errors.Is(err, ErrTargetOccupied):
		return CodeTargetOccupied
	case errors.Is(err, ErrTroopBusy):
		return CodeTroopBusy
	case errors.Is(err, ErrStaleVersion):
		return CodeStaleVersion
	case errors.Is(err, ErrIdempotencyConflict):
		return CodeIdempotencyConflict
	case errors.Is(err, ErrPathBlocked):
		return CodePathBlocked
	case errors.Is(err, ErrCapacityExhausted):
		return CodeCapacityExhausted
	case errors.Is(err, ErrWorldRecovering):
		return CodeWorldRecovering
	case errors.Is(err, ErrOverloaded):
		return CodeOverloaded
	case errors.Is(err, ErrOperationPending):
		return CodeOperationPending
	case errors.Is(err, ErrQueueFull):
		return CodeQueueFull
	case errors.Is(err, ErrPayloadMismatch):
		return CodePayloadMismatch
	case errors.Is(err, ErrFeatureDisabled):
		return CodeFeatureDisabled
	case errors.Is(err, ErrInvalidState):
		return CodeTroopBusy
	default:
		return CodeOverloaded
	}
}

// Reject 构造一个“规划阶段拒绝”的错误：携带业务码语义，
// 由调用方写入 MutationPlan.Rejected 并生成稳定回执。
func Reject(code int32, format string, args ...any) error {
	return fmt.Errorf("worldmap: code=%d: %s", code, fmt.Sprintf(format, args...))
}
