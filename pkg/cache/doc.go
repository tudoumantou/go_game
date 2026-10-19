// Package cache Redis 封装。
//
// 能力：key 规范（keygen）、ID 段分配（idtable）、在线状态原子占位（Lua，见 presence）、
// 幂等键（idem）、分布式锁（lock）。
package cache
