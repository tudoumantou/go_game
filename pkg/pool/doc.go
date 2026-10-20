// Package pool 通用池：字节缓冲池（网关收发包复用），实现见 buffer.go。
//
// 架构位置：位于网关 net 层之下，是收发路径直接依赖的分配优化点。
// 只提供「取/还」成对 API（Acquire*/Release*），不提供跨调用持有的
// 长生命周期缓存——池对象的所有权在两次调用之间转移，这是 sync.Pool
// 用法的核心纪律（详见 buffer.go 文件头）。
package pool
