// Package flush 写后落库框架。
//
// 能力：
//   - 分片调度与批量 UPSERT（flusher.go：10 分片错峰轮询，攒批窗口内
//     多个玩家的写合并为少量多行 INSERT … ON DUPLICATE KEY UPDATE）；
//   - 单行表工具 FlatOps（flat.go：玩家级单行状态表的读写适配）；
//   - 合批 SQL 构建（batch.go）与事务内执行（exec.go）；
//   - Querier 抽象（querier.go：同时接受 *sql.DB 与 *sql.Tx）；
//   - 墓碑删除（delete.go：业务删除登记为墓碑，落库周期合批物理删除）。
//
// 协作关系：业务 Actor 修改玩家数据后调 Flusher.MarkDirty(playerID)，
// 分片 worker 到点调 PersistFunc（通常由 pkg/writebehind 提供）完成写库；
// 进程退出时 FlushAll 同步排空。设计取舍见各文件头（攒批窗口、分片错峰、
// 墓碑削峰），核心是「用延迟换吞吐」：允许数据在内存里多待一个周期，
// 换取语句数与数据库往返的成倍下降。
package flush
