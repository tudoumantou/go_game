// internal/game/module/peer.go —— 跨玩家只读访问
//
// 解决什么问题：好友列表要显示对方等级、公会成员要显示名字——这类「读别人
// 一小块数据」的展示需求，过去每个模块各拼一套（friend 自己写 SQL JOIN、
// chat 自己定义 Messenger）。这里把它收敛成一个窄接口。
//
// ⚠️ 当前未接线（v3.23 标注）：全仓零调用。PeerReader 的实现由 player.Repository
// 提供、在 actor.go 注入到 module.Context.Peers，但没有任何业务模块使用它；
// 「读别人一小块数据」实际仍走各自 SQL：
//
//	friend/store.go:34,54,354   JOIN player 取 name/level（好友/申请/黑名单）
//	team/store.go:169           LEFT JOIN player（成员列表）
//
// 不迁移的一个合理理由：列表场景一次 JOIN 就拿到 N 个人的名字，而 ViewMany
// 需要「先查关系表拿 ID、再批量回源」两次往返。也就是说 PeerReader 真正的
// 适用面只有「单点读一个人」，而这个需求目前没有出现。
//
// 判断标准（同 internal/scenemap 的处理）：三个月内没有「让某个模块改用
// View/ViewMany」的排期，就删掉接口与 Context.Peers 字段——零消费者的代码
// 不会报错，只会让「怎么读别人数据」继续有两个答案，而维护者会以为统一过。
//
// 与写路径的分工：
//   - 读：走 Redis 快照（比 MySQL 新，MySQL 是异步落库）→ 未命中回落库；
//   - 写：必须给对方 Actor 发消息，绝不能直接改别人的内存。
//
// 只读视图只暴露展示字段，不给背包/邮件等明细——「能读」不等于「该读」。
package module

import "context"

// PeerView 其他玩家的只读摘要。
//
// 字段刻意保持最小：展示用的名字与等级。要读更多（头像/公会名）时扩展这里，
// 而不是反过来把整个 Snapshot 暴露给模块。
type PeerView struct {
	// PlayerID 玩家 ID
	PlayerID int64
	// Name 角色名
	Name string
	// Level 等级
	Level int32
}

// PeerReader 跨玩家只读访问（由 player.Repository 实现）。
//
// 未找到不是错误：View 返回 (nil, nil)，ViewMany 的结果里没有该 id——
// 「这个人不存在/从未进过游戏」是展示层的正常分支，不值得用 error 表达。
type PeerReader interface {
	// View 读单个玩家摘要。
	View(ctx context.Context, playerID int64) (*PeerView, error)
	// ViewMany 批量读（一次 Redis pipeline + 一条 IN 回源），返回 id → 摘要。
	// 未命中的 id 不出现在结果里。
	ViewMany(ctx context.Context, ids []int64) (map[int64]*PeerView, error)
}
