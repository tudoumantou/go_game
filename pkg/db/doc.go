// Package db MySQL 连接池与事务辅助。
//
// 池参数按设计文档第 14 章配置；Tx 提供「同一事务内多次执行，出错整体回滚」的封装。
package db
