// pkg/cache/doc.go —— 包级文档

// Package cache Redis 封装。
//
// 能力：key 规范（keygen）、ID 段分配（idtable）、在线状态原子占位（Lua，见 presence）、
// 幂等键（idem）、分布式锁（lock）。
//
// 架构位置：五个进程（gateway/game/center/cross/login）都把 pkg/cache 作为
// 访问 Redis 的唯一入口——连接管理在 redis.go，各业务子模块
// （idtab/idtable/idem/lock/presence）共用同一个 redis.UniversalClient。
// 协作关系：keygen.go 统一定义 key 前缀（seq:/idtab:/e:/lock:/online: 等）；
// idem.go 实现 pkg/mq 的 Deduper/Forgetter 接口（依赖倒置，消费方注入）；
// 配置来自 pkg/config 的 redis 段。
//
// 关键取舍：
//   - 原子操作（SETNX/INCR/Lua）全部下沉到 Redis 单命令/Lua 执行，应用层
//     不做「读-判-写」——多进程部署下那是最经典的竞态来源；
//   - 降级优先于报错：Redis 异常时存在性判断回落 MySQL，缓存故障不升级
//     成业务故障。
package cache
