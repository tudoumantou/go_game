# flusher（占位目录）

落库编排不在本目录：入口 `cmd/flusher/main.go`，分时轮询/合批/墓碑在 `pkg/flush`，
Write-Behind 快照与版本栅栏在 `pkg/writebehind`。
