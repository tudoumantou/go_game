// Package db MySQL 连接池与事务辅助。
//
// 池参数按设计文档第 14 章配置；Tx 提供「同一事务内多次执行，出错整体回滚」的封装。
//
// 架构位置：所有进程（gate/game/center/cross/login）共享这一入口访问 MySQL/TiDB；
// pkg/flush 的合批写、pkg/writebehind 的快照落库、gendb 生成的表级函数都以
// *sql.DB / *sql.Tx 为协作边界，本包不引入 ORM，SQL 由生成层与业务层显式书写。
// 设计取舍：database/sql 自带连接池，本包只做「配置 + 探活 + 事务模板」三件事，
// 保持薄封装——过度封装会挡住 SetMaxOpenConns 等池行为的细粒度调优。
package db
