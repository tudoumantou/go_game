// pkg/cache/keygen.go —— Redis key 命名规范（设计文档 14.1）
//
// 本文件收的是**跨边界共享**的 key：两个以上的包（或两个进程）读写同一个
// key 时，定义必须唯一，否则「登录服写的 token 网关认不出」这类问题会在
// 部署后才暴露。
//
// 域内单点构造的 key 不必上移（center/rank 的 rank:*、center/stats 的
// stats:*、milestone 的 activity:*）：它们只被自己的包读写，「一个定义」
// 已经成立，搬进来只会让 pkg/cache 认识赛季/功能点这些业务概念。判断标准
// 不是「字符串在哪个文件」，而是「有几个地方各写了一遍」。
package cache

import "fmt"

// key 命名规范：<域>:<实体>:<id>[:<子键>]
const (
	PrefixPlayer = "player" // player:<player_id>:<field>
	PrefixOnline = "online" // online:<player_id>
	PrefixToken  = "token"  // token:<token>
	PrefixIDTab  = "idtab"  // idtab:<table>（存在性集合，见 idtable.go）
	PrefixSeq    = "seq"    // seq:<name>（ID 分配计数器，见 idtab.go）
	PrefixShard  = "shard"  // shard:<index>:<player_id>

	// 登录服与网关共享：serv_info 是全量快照（整表一个 key），
	// acc_summary / user_role 是登录服内部的 cache-aside 单键。
	PrefixServInfo   = "serv_info"   // serv_info:all
	PrefixAccSummary = "acc_summary" // acc_summary:<accuid>
	PrefixUserRole   = "user_role"   // user_role:list:<account_id>

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

// TokenKey 登录 token key（token → player_id）。
//
// 这是全项目唯一一处 token key 的定义：登录服写（login/repo.TokenRepo），
// 网关握手时读（gateway 的 token 校验器）。两边各写一遍 "token:" 的旧写法
// 已删——那是个只能靠注释维持的跨进程契约，注释里写着「改动需两处同步」，
// 而两处真正同步的唯一保证是「只有一处可改」。
//
// 格式是**线上兼容契约**：keygen_test.go 里有一条用例把 "token:" 前缀钉死，
// 改前缀会让所有在线会话立刻失效（网关校验 redis.Nil → 全体掉线）。
func TokenKey(token string) string {
	return PrefixToken + ":" + token
}

// ServInfoKey 区服列表全量快照 key（整表一个 String，JSON 序列化）。
//
// 区服配置是低频变更的小表，整表一个 key 比逐行缓存更简单：失效只有
// 「整表 Del」一种，没有部分过期的中间态。
func ServInfoKey() string {
	return PrefixServInfo + ":all"
}

// AccSummaryKey 账号摘要 key（Hash/String，cache-aside）。
func AccSummaryKey(accuid int64) string {
	return fmt.Sprintf("%s:%d", PrefixAccSummary, accuid)
}

// UserRoleListKey 账号角色列表 key（cache-aside）。
func UserRoleListKey(accountID int64) string {
	return fmt.Sprintf("%s:list:%d", PrefixUserRole, accountID)
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
