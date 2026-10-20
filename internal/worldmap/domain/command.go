package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

// CommandEnvelope 是命令信封（文档 7.1）。
// PlayerID 来自内部鉴权（Game 已绑定会话），绝不从客户端 body 取信。
// PayloadHash 用标准化领域参数计算，不含 trace/deadline/session 等
// 可变化字段——否则客户端换个 trace 重放就会绕过幂等。
type CommandEnvelope struct {
	WorldID               WorldID
	PlayerID              int64
	SessionGeneration     uint64
	RequestID             string
	OperationID           string
	PayloadHash           [32]byte
	ExpectedObjectVersion uint64
	WorldEpoch            uint64
	DeadlineMS            int64
	TraceID               string
}

// CommandKey 是幂等键：作用域 (world_id, player_id, request_id)。
type CommandKey struct {
	WorldID   WorldID
	PlayerID  int64
	RequestID string
}

// Envelope 从信封提取幂等键。
func (e CommandEnvelope) Envelope() CommandKey {
	return CommandKey{WorldID: e.WorldID, PlayerID: e.PlayerID, RequestID: e.RequestID}
}

// Validate 做信封级校验。格式非法（缺 request_id、缺 world）
// 直接拒绝，不写命令回执——只有“已接受但结果未知”的命令才需要回执
// （文档 7.1）。
func (e CommandEnvelope) Validate() error {
	if e.WorldID == 0 {
		return fmt.Errorf("%w: world_id 不能为 0", ErrInvalidArgument)
	}
	if e.PlayerID <= 0 {
		return fmt.Errorf("%w: player_id 必须为正, got %d", ErrInvalidArgument, e.PlayerID)
	}
	if e.RequestID == "" {
		return fmt.Errorf("%w: request_id 不能为空(幂等键)", ErrInvalidArgument)
	}
	if len(e.RequestID) > 64 {
		return fmt.Errorf("%w: request_id 超长(>64)", ErrInvalidArgument)
	}
	if e.DeadlineMS < 0 {
		return fmt.Errorf("%w: deadline_ms 不能为负", ErrInvalidArgument)
	}
	return nil
}

// PayloadHashOf 计算标准化参数摘要。canonical 必须是字段顺序稳定的
// 序列化（如 protobuf 的确定性序列化或领域结构手工拼接），
// 不能依赖 map 遍历顺序——Go map 的 range 顺序是随机的
// （文档 7.1：不能依赖 protobuf 原始编码顺序同理）。
func PayloadHashOf(canonical []byte) [32]byte {
	return sha256.Sum256(canonical)
}

// HashString 返回摘要的十六进制形式，用于日志与对账（不用于比较，
// 比较用 [32]byte 直接 ==，数组可比较是 Go 的实用特性）。
func HashString(h [32]byte) string { return hex.EncodeToString(h[:]) }

// CommandStatus 命令状态。第一版只有 COMMITTED 一个持久值；
// PENDING 只存在于运行队列，绝不把 SQL 失败写成成功回执（INV-09）。
type CommandStatus int32

const (
	CommandStatusPending   CommandStatus = 0
	CommandStatusCommitted CommandStatus = 1
)

// OperationKind 延迟操作种类（map_operation.kind）。
type OperationKind int32

const (
	OperationKindUnspecified OperationKind = 0
	// OperationKindMarch 出征：部队创建提交时即 DONE，不等回城。
	OperationKindMarch OperationKind = 1
	// OperationKindRecall 召回：RETURNING 提交时 DONE；
	// 战斗中延迟召回先 PENDING。
	OperationKindRecall OperationKind = 2
	// OperationKindSettlement 回城交接：资产确认后 DONE。
	OperationKindSettlement OperationKind = 3
)

// OperationStatus 操作进度。与首次命令结果分离存储：
// 查询把“首次结果”与“最新进度”分开返回，不能用进度覆盖首次幂等响应。
type OperationStatus int32

const (
	OperationStatusPending OperationStatus = 0
	OperationStatusDone    OperationStatus = 1
	OperationStatusFailed  OperationStatus = 2
)

// MutationPlan 是一次命令对持久化层的全部写入意图（文档 7.2/14.4）。
// 铁律：Plan 内的所有切片/map/JSON 必须是深拷贝，TxWorkerPool
// 不允许持有 SceneLoop 的 map 或对象指针（文档 4.2）。
type MutationPlan struct {
	// CommandID 服务端生成的命令 ID（UUID），也是回执主键。
	CommandID string
	// Key 幂等键。
	Key CommandKey
	// PayloadHash 客户端请求摘要，用于“同键不同参”检测。
	PayloadHash [32]byte
	// Epoch 提交时的 world epoch；旧 epoch 的计划必须被栅栏拒绝。
	Epoch uint64

	// AffectedSceneRevisions 受影响场景的期望 revision（升序加锁顺序）。
	AffectedSceneRevisions []SceneRevision

	// ExpectedObjectVersions 参与对象的期望版本。
	ExpectedObjectVersions []ObjectVersion

	// ObjectsUpsert 需要写入/更新的对象快照（深拷贝）。
	ObjectsUpsert []ObjectSnapshot
	// ObjectsTombstone 需要标记删除的对象。
	ObjectsTombstone []ObjectKey

	// OccupancyClaims 占位声明；OccupancyReleases 占位释放。
	OccupancyClaims   []OccupancyClaim
	OccupancyReleases []OccupancyRelease

	// CollectStart / CollectSettle 采集会话的新建与结算。
	CollectStart  []CollectSession
	CollectSettle []CollectSettleChange

	// BattleUpsert 战斗记录写入。
	BattleUpsert []BattleRecord

	// DeliveryCreate 回城交接单（先留单再清货，文档 9.5）。
	DeliveryCreate []Delivery

	// CommandReceipt 首次命令回执。业务失败也写回执，
	// 避免同一请求稍后重试突然变成功（文档 7.2）。
	CommandReceipt *CommandReceipt

	// OutboxEvents 投影事件（outbox，可靠投递；内存 channel 不是日志）。
	OutboxEvents []OutboxEvent

	// Rejected 非 nil 表示规划阶段即拒绝（稳定业务失败）。
	Rejected error
}

// SceneRevision 场景级序号。与对象 version、world epoch 三者用途不同，
// 不用同一个 version 字段混装（文档 7.2）。
type SceneRevision struct {
	SceneID  SceneID
	Revision uint64
}

// ObjectVersion 对象级版本期望。
type ObjectVersion struct {
	Key     ObjectKey
	Version uint64
}

// ObjectSnapshot 是提交给仓储层的对象快照（类型化 JSON 前的中间态）。
// 第一版 map_object.payload 按 kind 分三种 schema，不允许
// map[string]any 混写（文档 14.1）。
type ObjectSnapshot struct {
	Key           ObjectKey
	Kind          ObjectKind
	OwnerScene    SceneID
	Version       uint64
	SchemaVersion uint32
	PlayerID      int64
	City          *City
	Troop         *Troop
	Resource      *ResourceNode
	Deleted       bool
}

// OccupancyClaim 静态占位声明。最终约束是 map_occupancy 唯一键
// (world,cell_x,cell_y,layer)，内存预检只是加速（文档 5.3）。
type OccupancyClaim struct {
	CellX, CellY int64
	Layer        int32
	ObjectKey    ObjectKey
}

// OccupancyRelease 占位释放。
type OccupancyRelease struct {
	CellX, CellY int64
	Layer        int32
	ObjectKey    ObjectKey
}

// CollectSettleChange 采集结算变更。
type CollectSettleChange struct {
	SessionID  string
	SettledQty int64
	SettleAtMS int64
}

// BattleRecord 是持久化战斗（文档 11.1）。
// input_hash 保证 Resolver 的相同输入→相同结果；
// random_seed 保证可回放；applied_operation_id 保证结果只应用一次（INV-06）。
type BattleRecord struct {
	BattleID           string
	WorldID            WorldID
	TargetID           ObjectID
	AttackerID         ObjectID
	DefenderID         ObjectID
	InputHash          [32]byte
	AttackerStats      CombatSnapshot
	DefenderStats      CombatSnapshot
	RuleVersion        string
	RandomSeed         int64
	Status             BattleStatus
	DeadlineMS         int64
	Result             *BattleResult
	AppliedOperationID string
}

// BattleResult 战斗裁决结果。文档 11.3：迟到结果不能反转已生效终态。
type BattleResult struct {
	WinnerID    ObjectID
	LoserID     ObjectID
	AppliedAtMS int64
	// AttackerWon 攻击方是否获胜；败者保留已有 Cargo 回城（10.4）。
	AttackerWon bool
	DetailJSON  []byte
}

// Delivery 是回城交接单（文档 13.2）。delivery_id 全局唯一，
// 业务去重键 slg:delivery:{delivery_id}；payload 创建后不可修改。
type Delivery struct {
	DeliveryID  string
	WorldID     WorldID
	TroopID     ObjectID
	PlayerID    int64
	Cargo       map[int32]int64
	PayloadHash [32]byte
	CreatedAtMS int64
	Status      DeliveryStatus
	Attempts    int32
	NextRetryMS int64
	// SettlementNo 同部队多次回城的序号，配合 UNIQUE(world,troop,no)。
	SettlementNo int64
}

// DeliveryStatus 交接单状态。
type DeliveryStatus int32

const (
	DeliveryStatusPending DeliveryStatus = 1
	DeliveryStatusApplied DeliveryStatus = 2
	// DeliveryStatusQuarantined 隔离（payload 冲突等永久错误）。
	DeliveryStatusQuarantined DeliveryStatus = 3
)

// OutboxEvent 投影事件。发布失败不回滚已提交业务，
// 消费者通过 revision 缺口重新拉快照（文档 7.3）。
type OutboxEvent struct {
	EventID     string
	WorldID     WorldID
	SceneID     SceneID
	Revision    uint64
	Kind        string
	Payload     []byte
	Status      int32
	NextRetryMS int64
}

// CommandReceipt 首次命令回执（map_command）。重试从回执读取，
// 不重新按当前对象状态生成含义不同的“首次结果”（文档 7.1）。
type CommandReceipt struct {
	CommandID     string
	Key           CommandKey
	PayloadHash   [32]byte
	ResultCode    int32
	ResultJSON    []byte
	CommittedAtMS int64
}

// CommitResult 是事务提交回执。文档 7.3：
// 连接断开/超时不得立即认为回滚，调用方必须能区分
// “确定失败”与“结果未知”。
type CommitResult struct {
	// Committed true 表示确定提交成功。
	Committed bool
	// Unknown true 表示结果不可解析（回包丢失/超时）——
	// 此时必须冻结相关场景走解析流程，禁止用旧镜像继续写（INV-10）。
	Unknown bool
	// SceneRevisions 提交后的场景 revision。
	SceneRevisions []SceneRevision
	// ObjectVersions 提交后的对象版本。
	ObjectVersions []ObjectVersion
	// Err 确定失败时的错误（回滚）。
	Err error
}

// CommandResolution 是幂等查询结果（文档 7.3）。
type CommandResolution struct {
	// Found 是否已有回执。
	Found bool
	// HashMismatch 同键不同参数：返回 IdempotencyConflict。
	HashMismatch bool
	Receipt      *CommandReceipt
}
