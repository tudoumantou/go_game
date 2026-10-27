// types.go —— 聊天域的常量、行类型与对外依赖接口。
//
// Messenger 是模块边界：聊天要投递给「对方」，但对方可能在别的节点，
// 甚至不在线（走离线队列）。接口让本模块不关心这件事怎么实现。
package chat

import (
	"context"

	pkgmq "game_study/pkg/mq"
)

// 消息内容上限。
//
// 按「字符」限制而不是字节：中文一个字 3 字节，只数字节会把 200 字的中文
// 拦成 66 字。同时再卡一道字节上限——极端 emoji 组合（一个视觉字符可占
// 十几字节）能把 rune 数压得很低、字节数很大，两条一起卡才拦得住。
//
// 为什么必须有：包大小上限（64KB）只拦单包，一条 64KB 的私聊照样会写进
// MySQL、推给对手（客户端渲染卡死）、作为 Kafka 消息撑大分区。
const (
	maxContentRunes = 200
	maxContentBytes = 900 // 200 字中文 = 600 字节，留余量给 emoji
)

// 私聊频道。
//
// 协议里 channel 传 0 也按私聊兼容（老客户端不传频道字段）；
// 2 是 P6 阶段唯一开放的频道，其余一律 4001。

// 私聊频道。
//
// 协议里 channel 传 0 也按私聊兼容（老客户端不传频道字段）；
// 2 是 P6 阶段唯一开放的频道，其余一律 4001。
const channelPrivate int32 = 2

// 消息方向。
//
// chat_record 按 owner 视角逐人行存储：发送方写一行 out、接收方写一行 in，
// 两边历史互不影响；uk_owner_msg（owner_id, msg_id）唯一键兜底防重。

// 消息方向。
//
// chat_record 按 owner 视角逐人行存储：发送方写一行 out、接收方写一行 in，
// 两边历史互不影响；uk_owner_msg（owner_id, msg_id）唯一键兜底防重。
const (
	DirectionOut int32 = 0 // 发出
	DirectionIn  int32 = 1 // 收到
)

// Messenger 投递能力（由游戏服组装层实现：在线判定 + 本服推送 + 跨服生产 + 离线暂存）。
//
// 四个方法对应私聊投递的完整决策链，实现见 server.go 的 chatMessenger。
// 接口注入使模块可单测（fake 三路投递），也与 Redis/Kafka 的具体依赖解耦。

// Messenger 投递能力（由游戏服组装层实现：在线判定 + 本服推送 + 跨服生产 + 离线暂存）。
//
// 四个方法对应私聊投递的完整决策链，实现见 server.go 的 chatMessenger。
// 接口注入使模块可单测（fake 三路投递），也与 Redis/Kafka 的具体依赖解耦。
type Messenger interface {
	// SelfNode 返回本节点标识（配置里的 Server.Name），
	// 用于「对端是否就在本服」的字符串比对。
	SelfNode() string
	// OnlineNode 查对端所在节点；ok=false 表示不在线（走离线暂存）。
	// 注意：内部走 Redis 查询，调用方应传 background ctx（见 deliver）。
	OnlineNode(ctx context.Context, playerID int64) (nodeID string, ok bool)
	// Push 本服推送：优先经玩家 Actor 保证与玩家自身回包的顺序，
	// Actor 不存在时直写会话。返回 false 表示推送失败（掉线等）。
	Push(ctx context.Context, playerID int64, msgID uint32, body []byte) bool
	// ProduceCross 跨服路径：把消息生产到 Kafka（按 ToID 分区，
	// 接收方所在节点的广播消费者落地后处理）。
	ProduceCross(ctx context.Context, msg *pkgmq.ChatCrossMsg) error
	// PendOffline 离线暂存：写入 Redis List（有上限 + TTL），
	// 接收方下次登录由 offline.go 的 drainOfflineChat 补投。
	PendOffline(ctx context.Context, playerID int64, msg *pkgmq.ChatCrossMsg) error
}

// Repo 聊天记录仓储。
//
// MySQL 直写模块（与好友/公会同类的「不过 Persister」设计）：
// 历史是流水数据、量随聊天天数增长，放玩家快照 JSON 列会把快照撑爆，
// 因此独立成表直写。db 可为 nil（无 DB 环境），调用方需判空降级。

// ChatLine 聊天记录行。
type ChatLine struct {
	// MsgID 全局消息 ID（nextMsgID 生成，时间戳在高位、天然有序）
	MsgID int64
	// Direction 相对 owner 的方向（out=我发的，in=我收到的）
	Direction int32
	// Content 文本内容
	Content string
}

// ListRecent 读取与某好友的最近 N 条记录。
//
// limit 钳制到 [1,100]，越界/非法值回落 50：防客户端传 0 或负数导致
// 语义异常，也防超大页把 DB 和回包一起拖垮。
// ORDER BY id DESC LIMIT ?：利用主键索引倒序取最近 N 条（不回表排序）。
