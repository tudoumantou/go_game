// Package worldmap 是 SLG 大地图子系统，对应设计文档 docs/slg场景地图设计开发.md。
//
// 它是独立于 internal/game/module/scene（RPG 场景模块）的新子系统：
// RPG 场景管“一个房间里的玩家与怪物”，本包管“一张 5000×5000 格的大世界上
// 的城池、部队与资源点”。两者坐标模型、并发模型、持久化路径都不同，
// 因此不复用 scene 模块的任何结构，只共享底层 pkg/actor、pkg/db 等基础设施。
//
// 目录职责（文档第 18.1 章）：
//
//	domain      纯领域类型与计算：坐标换算、状态机、采集/移动公式。
//	            禁止导入 MySQL、Protobuf、Redis 等任何外部依赖。
//	spatial     空间索引：场景网格、静态占位、直线路径、行军线包围盒。
//	application 命令规划器：只读快照 + 命令 → MutationPlan，不做 IO。
//	runtime     运行时：WorldManager/WorldInstance/SceneLoop/TxWorkerPool。
//	store       SQL 事务仓储（M2 实现，本期仅定义接口）。
//
// 本期（M0+M1）只交付 domain/spatial/application/runtime 的纯计算部分，
// 以及 cmd/map 最小入口；MySQL 持久化、AOI 投影、协议与跨进程 RPC
// 属于文档 M2 及以后阶段，尚未实现。
package worldmap
