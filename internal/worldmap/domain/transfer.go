package domain

import "fmt"

// transfer.go 承载跨场景所有权移交的领域契约（文档第 8 章）：
// TransferStatus 状态机、TransferPlan/TransferResult/TransferRecord
// 三种形态（规划意图/提交回执/持久化事实），以及前置约束
// 校验 ValidateTransfer。
//
// 背景（8.1）：普通行军穿越物理边界、攻击方进入防守目标
// 场景、回城脱离目标场景，都走同一个 Transfer 流程。协议
// 骨架是 Freeze → Prepare → SQL 原子提交 → Activate →
// Release（8.2），本文件只覆盖其中“纯领域”的部分：
// 状态枚举、计划与持久化字段、以及不变量前置检查。
// 协调器、两场景 barrier 属于 runtime/application 层。

// TransferStatus 移交状态机（文档 8.2 协议阶段）。
//
// 状态图：
//
//	PREPARING ─提交前失败→ ABORTED（终态）
//	    │
//	    ▼
//	 PREPARED ─提交前失败→ ABORTED（终态）
//	    │
//	    ▼ SQL 原子提交
//	 COMMITTED ──► ACTIVATED ──► RELEASED（终态）
//
// 关键性质：
//   - COMMITTED 是“SQL 已决定归属”的分界点。提交前失败可以
//     Abort 并解除屏障；提交结果未知必须按 7.3 解析，绝不能
//     自动 Abort——内存回滚会造出两个写入者（INV-02）；
//   - 提交后只能按数据库结果完成激活，不允许内存回滚
//     OwnerScene（8.2 约束 6）。
type TransferStatus int32

const (
	// TransferStatusUnspecified 零值非法。
	TransferStatusUnspecified TransferStatus = 0
	// TransferStatusPreparing 请求已接受，源场景冻结中
	// （等已有 pending 命令结束，8.2 约束 1）。
	TransferStatusPreparing TransferStatus = 1
	// TransferStatusPrepared 双方 Prepare 完成，待 SQL 提交。
	// Prepare 仅是运行时预留：目标不能把对象公开为可写，
	// 也不能开始采集/战斗（8.2 约束 3）。
	TransferStatusPrepared TransferStatus = 2
	// TransferStatusCommitted SQL 已提交：owner、两场景
	// revision、transfer 行同一事务落库。
	TransferStatusCommitted TransferStatus = 3
	// TransferStatusActivated 目标场景已用提交后快照激活。
	TransferStatusActivated TransferStatus = 4
	// TransferStatusReleased 源场景已清理旧副本，屏障解除。
	// 终态。
	TransferStatusReleased TransferStatus = 5
	// TransferStatusAborted 提交前取消，屏障解除。终态。
	// 提交后到达不了这个状态。
	TransferStatusAborted TransferStatus = 6
)

// Valid 报告是否为已知状态。
func (s TransferStatus) Valid() bool {
	switch s {
	case TransferStatusPreparing, TransferStatusPrepared, TransferStatusCommitted,
		TransferStatusActivated, TransferStatusReleased, TransferStatusAborted:
		return true
	default:
		return false
	}
}

// Terminal 报告是否终态。未终态即“仍在途”，ValidateTransfer
// 用它判断一个部队是否已有 active 移交。
func (s TransferStatus) Terminal() bool {
	return s == TransferStatusReleased || s == TransferStatusAborted
}

// String 返回日志用名称。domain 的枚举定义文件已锁定、
// 不为旧枚举补 Stringer，这里为新枚举实现一个，
// 保证日志里出现的是 PREPARED 而不是 2。
func (s TransferStatus) String() string {
	switch s {
	case TransferStatusPreparing:
		return "PREPARING"
	case TransferStatusPrepared:
		return "PREPARED"
	case TransferStatusCommitted:
		return "COMMITTED"
	case TransferStatusActivated:
		return "ACTIVATED"
	case TransferStatusReleased:
		return "RELEASED"
	case TransferStatusAborted:
		return "ABORTED"
	default:
		return fmt.Sprintf("TransferStatus(%d)", int32(s))
	}
}

// TransferPlan 是一次移交的规划意图（文档 8.2 协议前半段）。
// 它是不可变输入：一旦构造并冻结，SceneLoop 重试使用同一
// 计划，不按新状态重新生成含义不同的移交。
type TransferPlan struct {
	// TransferID 移交单号（服务端 UUID）。重复 transfer_id
	// 必须返回相同结果（8.3 幂等），它是移交层的幂等键。
	TransferID string
	// Object 被移交对象。第一版只支持部队：结构上一次
	// 计划只携带单个 ObjectKey，从数据形态上保证
	// “一次只移交一支部队”（8.1）——批量移交会让两场景
	// barrier 覆盖多个对象，第一版明确不做。
	Object ObjectKey
	// SourceScene/DestScene 源与目标 OwnerScene。
	SourceScene SceneID
	DestScene   SceneID
	// BeforeVersion 冻结时的对象版本，SQL 提交时做 CAS
	// 校验（8.2 约束 4：验证预期 revision、OwnerScene 和
	// version）。版本过期说明冻结后对象又被改过，必须
	// 拒绝而不是覆盖。
	BeforeVersion uint64
	// ExpectedSourceRevision/ExpectedDestRevision 两场景的
	// 期望 revision。与对象 version、world epoch 三者用途
	// 不同，不混用一个 version 字段（7.2）。
	ExpectedSourceRevision uint64
	ExpectedDestRevision   uint64
}

// TransferResult 是移交提交回执（文档 8.2：committed(new_version)）。
// Committed=false 表示提交前失败（可 Abort）；提交结果未知
// 时调用方必须走 7.3 解析流程，不能用本结构猜测成败。
type TransferResult struct {
	// TransferID 对应的移交单号，供调用方把回执挂回计划。
	TransferID string
	// Committed true 表示 SQL 确定提交成功。
	Committed bool
	// NewVersion 提交后的对象版本。对象按新 OwnerScene
	// 与新版本重新加载，旧版本快照不得继续使用。
	NewVersion uint64
}

// TransferRecord 是移交的持久化形态（map_transfer 行，
// 文档 8.2 约束 4：写一条完整的 COMMITTED transfer）。
// 崩溃恢复按它判断归属：COMMITTED 及以后状态以数据库为
// 准，内存镜像一律作废（8.4）。
type TransferRecord struct {
	TransferID string
	WorldID    WorldID
	Object     ObjectKey
	// Kind 被移交对象种类。第一版恒为 TROOP；城池/资源点
	// 不跨场景迁移（9.5：第一版主堡不可迁移和摧毁）。
	Kind ObjectKind
	// SourceScene/DestScene 移交前后的 OwnerScene。
	SourceScene SceneID
	DestScene   SceneID
	// BeforeVersion 冻结时版本；NewVersion 提交后版本
	// （未提交时为 0）。
	BeforeVersion uint64
	NewVersion    uint64
	Status        TransferStatus
	// MoveID 移交时固化的移动段。8.2 约束 4：边界移动
	// 状态一并固化，避免新场景倒退到旧起点；恢复时按
	// 它继续推进而不是从 From 重来。
	MoveID string
	// CreatedAtMS/CommittedAtMS 建档与提交时刻，用于
	// 告警指标 transfer_pending/age（17.3）。
	CreatedAtMS   int64
	CommittedAtMS int64
}

// ValidateTransfer 校验一次跨场景所有权移交的前置约束
// （文档 8.1）。
//
// 规则与理由：
//
//  1. BUSY/FIGHTING 对象不能另起移动移交。FIGHTING 时战斗
//     结果归属原 OwnerScene（11.1：攻击方先共置到防守目标
//     场景才开战），移交中战斗结束会把结果写到错误的写入者；
//     Busy 非空闲说明有未完成的转换（移交/防守迁入/战斗/
//     回城移交），重复移交违反“一个部队同时最多一个
//     transfer”（8.1）。
//
//  2. 一次只移交一支部队。结构上 TransferPlan 只携带单个
//     ObjectKey；运行时再用 inflight 记录兜底：同一部队
//     存在未终态移交时拒绝，调用方幂等等待或改用既有
//     transfer_id（8.3：重复 transfer_id 返回相同结果）。
//
//  3. 源/目场景必须不同且都在拓扑内。同场景移交是调用方
//     bug；越界场景说明路由目录缓存了脏数据（8.3）。
//
// 错误选择：忙/在途用 ErrTroopBusy（业务码 44007，客户端
// “稍后用相同 request_id 重试或查询”）；参数类用
// ErrInvalidArgument；对象缺失用 ErrObjectNotFound；
// 种类不符用 ErrInvalidState。哨兵错误用 errors.Is 判定，
// 调用方不做字符串匹配。
func ValidateTransfer(t *Troop, plan TransferPlan, topo Topology, inflight ...*TransferRecord) error {
	if plan.TransferID == "" {
		return fmt.Errorf("%w: transfer_id 不能为空(幂等键)", ErrInvalidArgument)
	}
	if plan.Object.WorldID == 0 || plan.Object.ID == 0 {
		return fmt.Errorf("%w: 移交计划缺少对象标识", ErrInvalidArgument)
	}
	if t == nil {
		return fmt.Errorf("%w: 部队 %s 不存在", ErrObjectNotFound, plan.Object)
	}
	// 传入的部队必须就是计划要移交的对象：传错快照会让
	// 版本/场景校验全部落空。
	if t.Header.Key() != plan.Object {
		return fmt.Errorf("%w: 部队 %s 与计划对象 %s 不符", ErrInvalidArgument, t.Header.Key(), plan.Object)
	}
	// 第一版只移交部队。主堡不可迁移（9.5），资源点由刷新
	// 流程管理，都不走 Transfer。
	if t.Header.Kind != ObjectKindTroop {
		return fmt.Errorf("%w: 对象 %s 种类 %s 不是部队，第一版不移交",
			ErrInvalidState, t.Header.Key(), t.Header.Kind)
	}
	// 拓扑非法时无法判断场景范围，直接透出拓扑错误。
	if err := topo.Validate(); err != nil {
		return err
	}
	if plan.SourceScene == plan.DestScene {
		return fmt.Errorf("%w: 移交 %s 源目场景相同(%d)", ErrInvalidArgument, plan.TransferID, plan.SourceScene)
	}
	if plan.SourceScene < 1 || plan.SourceScene > topo.MaxSceneID() ||
		plan.DestScene < 1 || plan.DestScene > topo.MaxSceneID() {
		return fmt.Errorf("%w: 移交 %s 场景 %d→%d 越界(1..%d)",
			ErrInvalidArgument, plan.TransferID, plan.SourceScene, plan.DestScene, topo.MaxSceneID())
	}
	// FIGHTING 与 Busy 检查放在参数校验之后：参数错了先报
	// 参数，避免同一份错误数据在不同忙状态下来回变脸。
	if t.Phase == TroopPhaseFighting {
		return fmt.Errorf("%w: 部队 %s FIGHTING(battle=%s) 不能另起移交",
			ErrTroopBusy, t.Header.Key(), t.BattleID)
	}
	if !t.Busy.Idle() {
		return fmt.Errorf("%w: 部队 %s Busy=%d(op=%s) 不能另起移交",
			ErrTroopBusy, t.Header.Key(), int32(t.Busy.Reason), t.Busy.OpID)
	}
	// inflight 是变参：通常传 0 或 1 条该部队的既有移交记录。
	// 传多条（批量预检）也支持——逐条检查对象与终态。
	for _, rec := range inflight {
		if rec == nil {
			continue
		}
		if rec.Object == plan.Object && !rec.Status.Terminal() {
			return fmt.Errorf("%w: 部队 %s 已有在途移交 %s(状态 %s)",
				ErrTroopBusy, t.Header.Key(), rec.TransferID, rec.Status)
		}
	}
	return nil
}
