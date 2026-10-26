// pkg/cache/keygen.go —— Redis key 命名规范（设计文档 14.1）
package cache

import "fmt"

// key 命名规范：<域>:<实体>:<id>[:<子键>]
const (
	PrefixPlayer  = "player"  // player:<player_id>:<field>
	PrefixOnline  = "online"  // online:<player_id>
	PrefixToken   = "token"   // token:<token>
	PrefixIDTab   = "idtab"   // idtab:<table>（存在性集合，见 idtable.go）
	PrefixSeq     = "seq"     // seq:<name>（ID 分配计数器，见 idtab.go）
	PrefixLock    = "lock"    // lock:<resource>
	PrefixMailBox = "mailbox" // mailbox:<player_id>
	PrefixShard   = "shard"   // shard:<index>:<player_id>

	// PrefixOwnerIndex 归属索引（ZSET：score = 归属者 id，member = 实体 id）。
	//
	// 与 PrefixIDTab 的分工：idtab 只回答「存在吗」（Set）；本 key 还回答
	// 「属于谁」和「某人有哪些」。一对多的表（伙伴/装备/邮件）用本 key，
	// 一对一的表（签到/属性）用 idtab——后者配了索引是纯浪费。
	PrefixOwnerIndex = "by_owner"
)

// PlayerKey 玩家数据 key。
func PlayerKey(playerID int64, field string) string {
	if field == "" {
		return fmt.Sprintf("%s:%d", PrefixPlayer, playerID)
	}
	return fmt.Sprintf("%s:%d:%s", PrefixPlayer, playerID, field)
}

// OnlineKey 在线状态 key。
func OnlineKey(playerID int64) string {
	return fmt.Sprintf("%s:%d", PrefixOnline, playerID)
}

// IDTabKey ID 表 key（存在性集合，Set 类型）。
func IDTabKey(table string) string {
	return fmt.Sprintf("%s:%s", PrefixIDTab, table)
}

// IDTabReadyKey ID 表构建完成标记。
func IDTabReadyKey(table string) string {
	return fmt.Sprintf("%s:%s:ready", PrefixIDTab, table)
}

// SeqKey ID 分配计数器 key（String 类型，INCR）。
func SeqKey(name string) string {
	return fmt.Sprintf("%s:%s", PrefixSeq, name)
}

// EntityKey 实体热数据 key（Hash 类型）。
func EntityKey(table string, id int64) string {
	return fmt.Sprintf("e:%s:%d", table, id)
}

// LockKey 分布式锁 key。
func LockKey(resource string) string {
	return fmt.Sprintf("%s:%s", PrefixLock, resource)
}

// ShardKey 落库分片 key。
func ShardKey(index int, playerID int64) string {
	return fmt.Sprintf("%s:%d:%d", PrefixShard, index, playerID)
}

// OwnerIndexKey 归属索引 key。
//
// ZSET：score = 归属者 id（角色/玩家/公会），member = 实体 id。
// 一个结构承担三件事：ZSCORE 判存在、ZSCORE 反查归属、
// ZRANGEBYSCORE 按归属者取列表。
func OwnerIndexKey(table string) string {
	return fmt.Sprintf("%s:%s", PrefixOwnerIndex, table)
}

// OwnerIndexReadyKey 归属索引构建完成标记。
//
// 与 IDTabReadyKey 故意分开：同一张表可能同时有存在性集合与归属索引
// （如 player_partner 两个都建），两者的构建时机与数据来源不同，
// 共用 ready 会让其中一个把另一个误标记为「已就绪」。
func OwnerIndexReadyKey(table string) string {
	return fmt.Sprintf("%s:%s:ready", PrefixOwnerIndex, table)
}
