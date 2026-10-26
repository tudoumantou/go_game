package scenemap

import "errors"

// 场景框架的哨兵错误。全部用 errors.Is 判定，调用方不做字符串匹配。
//
// 分层：
//   - ErrInvalidArgument / ErrInvalidState  调用方/数据问题（4xx 语义）
//   - ErrNotFound / ErrAlreadyExists        存在性（房间、实体）
//   - ErrOverloaded / ErrClosed             容量与生命周期（可重试/不可重试）
//   - ErrBlocked / ErrOutOfRange            移动校验（客户端要纠偏）
//   - ErrTimeout                            同步 call 超时（结果未知）
var (
	// ErrInvalidArgument 参数非法。
	ErrInvalidArgument = errors.New("scenemap: 参数非法")
	// ErrInvalidState 状态机非法（例如对已关闭的房间投递命令）。
	ErrInvalidState = errors.New("scenemap: 状态非法")

	// ErrNotFound 实体/房间/地图配置不存在。
	ErrNotFound = errors.New("scenemap: 不存在")
	// ErrAlreadyExists 重复创建（房间号已被占用、实体已在该图）。
	ErrAlreadyExists = errors.New("scenemap: 已存在")

	// ErrOverloaded 过载：mailbox 满、房间池耗尽、同屏人数超限。
	// 语义是「未接受」，调用方可重试且不会产生副作用。
	ErrOverloaded = errors.New("scenemap: 过载")
	// ErrClosed Actor/房间已关闭，不再接受新命令。
	ErrClosed = errors.New("scenemap: 已关闭")

	// ErrBlocked 目标格不可走（静态阻挡或动态阻挡）。
	// 客户端收到后应停在原地并等待纠偏包。
	ErrBlocked = errors.New("scenemap: 目标格不可走")
	// ErrOutOfRange 坐标越界或单步位移过大（异常包）。
	ErrOutOfRange = errors.New("scenemap: 坐标越界")

	// ErrTimeout 同步调用超时。**结果未知**：命令可能已经执行，
	// 调用方不能当作失败重试（重试必须带幂等键）。
	ErrTimeout = errors.New("scenemap: 调用超时")

	// ErrNoEffect 命令执行完但没有改变状态。
	//
	// 为什么单独定义：更新一旦漏产出新状态、框架又只记 warning，状态就
	// 静默不更新（规格 11.2）。这里用 revision 检测并返回这个错误，
	// 让开发期就能炸出来，而不是线上靠猜。
	ErrNoEffect = errors.New("scenemap: 命令未改变状态")

	// ErrMapTypeMissing 地图类型查不到配置。
	//
	// 有意不用默认值兜底（规格 11.13）：查不到就给个默认地图，
	// 结果是"看起来能进图、结算全错"。这里显式报错。
	ErrMapTypeMissing = errors.New("scenemap: 地图类型未配置")

	// ErrRoomFull 目标分线人数已满（route_user_limit）。
	ErrRoomFull = errors.New("scenemap: 分线已满")
	// ErrRoomPoolExhausted 房间池耗尽（所有 AOI 实例都在用）。
	ErrRoomPoolExhausted = errors.New("scenemap: 房间池耗尽")
)
