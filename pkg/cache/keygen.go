// pkg/cache/keygen.go —— Redis key 命名规范（设计文档 14.1）
package cache

import "fmt"

// key 命名规范：<域>:<实体>:<id>[:<子键>]
const (
	PrefixPlayer   = "player"   // player:<player_id>:<field>
	PrefixOnline   = "online"   // online:<player_id>
	PrefixToken    = "token"    // token:<token>
	PrefixIDTab    = "idtab"    // idtab:<name>
	PrefixLock     = "lock"     // lock:<resource>
	PrefixMailBox  = "mailbox"  // mailbox:<player_id>
	PrefixShard    = "shard"    // shard:<index>:<player_id>
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

// IDTabKey ID 表 key。
func IDTabKey(name string) string {
	return fmt.Sprintf("%s:%s", PrefixIDTab, name)
}

// LockKey 分布式锁 key。
func LockKey(resource string) string {
	return fmt.Sprintf("%s:%s", PrefixLock, resource)
}

// ShardKey 落库分片 key。
func ShardKey(index int, playerID int64) string {
	return fmt.Sprintf("%s:%d:%d", PrefixShard, index, playerID)
}
