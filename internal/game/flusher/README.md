# flusher（占位目录）

落库编排不在本目录：分时轮询/合批/墓碑在 `pkg/flush`，
Redis 快照副本与版本栅栏在 `pkg/writebehind`。

> v3.6 起「Redis 脏队列 → Worker → MySQL」那条落库通道已整条删除
> （`cmd/flusher`、`Store.Commit`、脏 ZSET、`Worker`）。落库只剩
> 「Actor 内存 → flusher → MySQL」一条路。
