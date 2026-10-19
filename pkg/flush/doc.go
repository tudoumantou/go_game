// Package flush 写后落库。
//
// 能力：分片调度与批量 UPSERT、单行表工具（FlatOps）、Querier 抽象（同时接受 *sql.DB 与 *sql.Tx）。
// 实现在 flusher.go / batch.go / flat.go / querier.go。
package flush
