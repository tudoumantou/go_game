// internal/game/module/chat/chat.go —— 聊天模块（私聊全链路，17.3）
//
// 架构职责：私聊的「发送 → 投递 → 历史」全链路，对应设计文档 17.3。
//  1. 发送：onSend 校验 → 生成全局 msgID → 本端落 out 历史 → 按对端在线状态三路投递；
//  2. 投递：deliver 三路——本服在线直推 / 跨服走 Kafka / 离线 Redis 暂存；
//  3. 历史：onHistory 倒序查库、正序回包；跨服来讯由 DeliverInbound 落 in 历史。
//
// Ext 槽位：无。聊天没有玩家快照内状态——历史是 MySQL 直写（Repo），
// 在线判定/暂存走 Redis（Messenger），模块本身只做编排，不持有状态。
//
// 事件订阅：无（聊天不参与升级/任务等事件流）。
//
// 配置表关系：无（频道开放范围是代码常量 channelPrivate，P6 仅私聊）。
//
// 懒初始化：不适用（无 Ext 行概念；DB 缺失时 repo 可为 nil，模块降级为只校验不回包）。
package chat

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"time"

	"game_study/internal/game/module"
	"game_study/pkg/codec"
	"game_study/pkg/db"
	"game_study/pkg/log"
	pkgmq "game_study/pkg/mq"

	gamev1 "game_study/pkg/pb/game/v1"
	"google.golang.org/protobuf/proto"
)

// 协议号（4.9：6001/6002 发送，6010 推送，6020/6021 历史）。
const (
	msgIDSendReq     = codec.MsgChatSendReq
	msgIDSendResp    = codec.MsgChatSendResp
	msgIDNotify      = codec.MsgChatMessageNotify
	msgIDHistoryReq  = codec.MsgChatHistoryReq
	msgIDHistoryResp = codec.MsgChatHistoryResp
)

// 业务码（4.10：与战斗等模块一致的约定——0 成功，非 0 为可预期失败）。
const (
	codeOK            int32 = 0
	codeChannelClosed int32 = 4001 // 频道未开放（P6 仅私聊）
	codeContentEmpty  int32 = 4005 // 内容为空 / 对端 id 非法
)

// 私聊频道。
//
// 协议里 channel 传 0 也按私聊兼容（老客户端不传频道字段）；
// 2 是 P6 阶段唯一开放的频道，其余一律 4001。
const channelPrivate int32 = 2

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
type Repo struct {
	db *db.DB
}

// NewRepo 创建仓储。
func NewRepo(database *db.DB) *Repo {
	return &Repo{db: database}
}

// Insert 落库一条聊天记录（uk_owner_msg 幂等，重复插入忽略）。
//
// ON DUPLICATE KEY UPDATE id = id 是 MySQL 惯用的「冲突即静默忽略」写法：
// 自更新主键为零操作，既幂等又不报错。写入路径可能重投（Kafka 消费重试 /
// 双端重复发送），依赖它保证同一 (owner_id, msg_id) 永远只有一行。
func (r *Repo) Insert(ctx context.Context, ownerID, peerID, msgID int64, direction int32, channel int32, content string) error {
	const q = `INSERT INTO chat_record (owner_id, peer_id, msg_id, direction, channel, content)
	           VALUES (?, ?, ?, ?, ?, ?)
	           ON DUPLICATE KEY UPDATE id = id`
	if _, err := r.db.ExecContext(ctx, q, ownerID, peerID, msgID, direction, channel, content); err != nil {
		return fmt.Errorf("insert chat_record: %w", err)
	}
	return nil
}

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
func (r *Repo) ListRecent(ctx context.Context, ownerID, peerID int64, limit int) ([]ChatLine, error) {
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	const q = `SELECT msg_id, direction, content
	           FROM chat_record
	           WHERE owner_id = ? AND peer_id = ?
	           ORDER BY id DESC LIMIT ?`
	rows, err := r.db.QueryContext(ctx, q, ownerID, peerID, limit)
	if err != nil {
		return nil, err
	}
	// database/sql 契约：rows 必须 Close，否则连接一直占着直到 GC 兜底；
	// 闭包吞掉 Close 错误是惯用法——查询语义已完成，关闭失败无补救意义
	defer func() { _ = rows.Close() }()

	var out []ChatLine
	for rows.Next() {
		var c ChatLine
		if err := rows.Scan(&c.MsgID, &c.Direction, &c.Content); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	// rows.Err() 不能省：Next() 中途遇错会直接结束循环，
	// 不查 Err 会把「查了一半」当成「查完了」，静默丢数据
	return out, rows.Err()
}

// chatRepo 是模块对历史仓储的全部依赖（接口隔离）。
//
// 抽成接口只为一件事：让「频道校验」「三路投递」「方向还原」这些纯编排逻辑
// 能在内存里测，不必为一条断言拉起 MySQL。*Repo 隐式实现它，装配层不变。
type chatRepo interface {
	Insert(ctx context.Context, ownerID, peerID, msgID int64, direction int32, channel int32, content string) error
	ListRecent(ctx context.Context, ownerID, peerID int64, limit int) ([]ChatLine, error)
}

// Module 聊天模块。
//
// 无玩家态：所有状态在 MySQL（历史）与 Redis（在线/暂存），模块只做编排，
// 因此 OnMessage 在玩家 Actor 内执行但不读写任何共享结构，无需加锁。
type Module struct {
	*module.Thin

	// repo 历史仓储；nil 表示无 DB 环境（降级：只校验 + 投递，不留历史）
	repo      chatRepo
	messenger Messenger
	logger    *log.Logger
}

// New 创建聊天模块（messenger 可为 nil：单机降级为只落库）。
func New(repo chatRepo, messenger Messenger, logger *log.Logger) *Module {
	m := &Module{repo: repo, messenger: messenger, logger: logger}
	m.Thin = module.NewThin("chat",
		module.ThinLogger(logger),
		module.ThinLogout(m.onLogout),
	)
	m.HandleR(msgIDSendReq, msgIDSendResp, &gamev1.ChatSendResp{}, module.Typed(msgIDSendResp, m.onSend))
	m.HandleR(msgIDHistoryReq, msgIDHistoryResp, &gamev1.ChatHistoryResp{}, module.Typed(msgIDHistoryResp, m.onHistory))
	return m
}

// onLogout 下线时触发一次常规 flush 周期。
//
// 聊天无 Ext 快照，这里 MarkDirty 只是让 Persister 走一次常规周期
// （保持与其他模块的生命周期行为一致，无害）；真正要落的历史在 MySQL，
// 发送时已直写，不依赖登出触发。
func (m *Module) onLogout(ctx *module.Context) error {
	ctx.State.MarkDirty("chat")
	return nil
}

// DeliverInbound 投递一条「收到」方向的跨服私聊（Kafka 消费调用）。
//
// 由 chat_cross_consumer 在「本地性判定 + msg_id 幂等」通过后调用，
// 职责只有落 in 方向历史；repo 为 nil（无 DB 环境）时静默跳过——
// 历史是尽力而为的旁路数据，不因缺库阻断 Kafka 消费。
func (m *Module) DeliverInbound(ctx context.Context, msg *pkgmq.ChatCrossMsg) error {
	if m.repo == nil {
		return nil
	}
	return m.repo.Insert(ctx, msg.ToID, msg.FromID, msg.MsgID, DirectionIn, msg.Channel, msg.Content)
}

// onSend 发送私聊（17.3：本端 out → 对方在线本服直投 / 跨服走 Kafka / 离线 pend）。
func (m *Module) onSend(ctx *module.Context, req *gamev1.ChatSendReq, resp *gamev1.ChatSendResp) error {
	*resp = gamev1.ChatSendResp{}
	// channel=0 按私聊兼容（老客户端不传频道字段），其余频道 P6 未开放
	if req.Channel != channelPrivate && req.Channel != 0 {
		resp.Code = codeChannelClosed // P6 仅开放私聊
		return nil
	}
	if req.Content == "" || req.PeerId <= 0 {
		resp.Code = codeContentEmpty
		return nil
	}

	// msgID 先于一切 IO 生成：历史行与投递消息必须共用同一个 ID，
	// 否则 uk_owner_msg 幂等与「撤回/引用」都失去锚点
	msgID := nextMsgID()
	now := time.Now().Unix()
	// 身份一律取 Actor 内会话，不信协议体携带的发送者字段（防伪造他人发言）
	fromID := ctx.State.PlayerID()
	fromName := ctx.State.Profile().Name

	// ① 本端历史（direction=out）
	if m.repo != nil {
		// 用 context.Background() 而非请求 ctx：onSend 返回后 ctx 即取消，
		// 会把这次落库误杀；历史丢失只 Warn——不回错误码、不阻塞发送体验
		if err := m.repo.Insert(context.Background(), fromID, req.PeerId, msgID, DirectionOut, channelPrivate, req.Content); err != nil {
			m.logger.Warn("save outbound chat failed", "err", err)
		}
	}

	// ② 投递对端
	notify := &gamev1.ChatMessageNotify{
		MsgId:      fmt.Sprintf("%d", msgID),
		Channel:    channelPrivate,
		SenderId:   fromID,
		SenderName: fromName,
		PeerId:     req.PeerId,
		Content:    req.Content,
		SentAt:     now,
	}
	// 简单 proto 消息（无 map/oneof）Marshal 不会失败，忽略错误的务实取舍
	raw, _ := proto.Marshal(notify)
	// 注意单位换算只在这一处：协议 SentAt 用秒（客户端契约），
	// MQ 跨消息 SendTime 用毫秒（pkgmq 契约）， downstream 各自解各自的
	crossMsg := &pkgmq.ChatCrossMsg{
		MsgID: msgID, FromID: fromID, FromName: fromName,
		ToID: req.PeerId, Channel: channelPrivate, Content: req.Content, SendTime: now * 1000,
	}
	// 投递失败不影响回包：私聊是尽力而为链路，回包只表示「服务端已受理」
	m.deliver(req.PeerId, raw, crossMsg)

	resp.Code = codeOK
	resp.MsgId = fmt.Sprintf("%d", msgID)
	resp.SentAt = now
	return nil
}

// deliver 按在线状态选择投递路径。
//
// 三路互斥：离线 → Redis 暂存；本服在线 → 落 in 历史 + Actor 推送；
// 跨服在线 → Kafka 生产（对端所在节点的广播消费者落地后处理）。
func (m *Module) deliver(toID int64, raw []byte, crossMsg *pkgmq.ChatCrossMsg) {
	if m.messenger == nil {
		return // 无组装层注入（单机模式）：只落库不投递
	}
	// 全程用 Background：OnlineNode 走 Redis 查询可能慢，且请求 ctx 在回包后
	// 即取消——投递是「受理之后」的异步动作，不能挂在请求生命周期上
	bg := context.Background()
	node, online := m.messenger.OnlineNode(bg, toID)
	switch {
	case !online:
		// 暂存失败只 Warn：接收方下次登录补投，丢一条私聊可接受
		if err := m.messenger.PendOffline(bg, toID, crossMsg); err != nil {
			m.logger.Warn("pend offline chat failed", "to", toID, "err", err)
		}
	case node == m.messenger.SelfNode():
		// 本服在线：落 in + 推送
		// 先落后推的顺序有意为之：客户端收到推送立刻拉历史时，
		// 这条 in 记录已可查，不会出现「消息到了、历史里没有」
		if m.repo != nil {
			if err := m.repo.Insert(bg, toID, crossMsg.FromID, crossMsg.MsgID, DirectionIn, channelPrivate, crossMsg.Content); err != nil {
				m.logger.Warn("save inbound chat failed", "err", err)
			}
		}
		// Push 返回 false 是掉线竞态（判定在线与推送之间掉线），静默丢弃
		m.messenger.Push(bg, toID, msgIDNotify, raw)
	default:
		// 跨服：生产失败只 Warn——Kafka 侧重投由 broker/生产者重试兜底
		if err := m.messenger.ProduceCross(bg, crossMsg); err != nil {
			m.logger.Warn("produce cross chat failed", "to", toID, "err", err)
		}
	}
}

// onHistory 拉取与某好友的聊天历史。
func (m *Module) onHistory(ctx *module.Context, req *gamev1.ChatHistoryReq, resp *gamev1.ChatHistoryResp) error {
	*resp = gamev1.ChatHistoryResp{Code: codeOK}
	if m.repo == nil {
		return nil
	}
	lines, err := m.repo.ListRecent(context.Background(), ctx.State.PlayerID(), req.PeerId, int(req.Limit))
	if err != nil {
		return err
	}
	// 查询是倒序，回包按时间正序
	// 逆序遍历把 DESC 结果反转：比在 SQL 里再套一层子查询省一次排序
	selfID := ctx.State.PlayerID()
	for i := len(lines) - 1; i >= 0; i-- {
		l := lines[i]
		msg := &gamev1.ChatMessageNotify{
			MsgId:   fmt.Sprintf("%d", l.MsgID),
			Channel: channelPrivate,
			Content: l.Content,
		}
		// 用 direction 还原对话视图：一行 out 一行 in 共享同一 msg_id，
		// 比每行都存 sender_id 省一半存储；回包时按 owner 视角反推收发方
		if l.Direction == DirectionOut {
			msg.SenderId = selfID
			msg.PeerId = req.PeerId
		} else {
			msg.SenderId = req.PeerId
			msg.PeerId = selfID
		}
		resp.Messages = append(resp.Messages, msg)
	}
	// 取满一页即提示可能还有更多：精确判断要额外 count，
	// 私聊翻页场景用「取满」近似足够（客户端可再拉一页确认）
	resp.HasMore = len(lines) >= int(req.Limit)
	return nil
}

// nextMsgID 生成消息 ID（时间戳毫秒 << 20 | 20 位随机）。
//
// 位运算组装的三重考量：
//  1. 毫秒时间戳占高位 → ID 整体随时间单调递增，MySQL 主键顺序插入、
//     索引页分裂最少；当前毫秒约 2^40.7，左移 20 位后约 2^60.7 < 2^63，
//     不会溢出成负数；
//  2. 低 20 位（1,048,576 个槽位）放同毫秒随机数：这是**概率唯一**而非
//     绝对唯一，碰撞概率上界 n(n-1)/2^21——每毫秒 10 条约 0.004%，
//     100 条约 0.5%，1000 条约 48%。私聊实际峰值远低于每毫秒 100 条，
//     所以够用；真要支撑每毫秒上千条，必须换成「节点 ID + 序列」发号器
//     （或中心化发号），当前位宽不够；
//  3. 用 crypto/rand 而非 math/rand：msgID 会暴露给客户端且是 Kafka 幂等键，
//     不要求密码学强度，但要避免可预测的序列被客户端猜中后伪造重复消息。
//
// 碰撞的后果：chat_record 的 uk_owner_msg（owner_id, msg_id）会把后一条
// 静默忽略，Kafka 侧同 ID 也会被去重——即「极低概率丢一条消息」，不是
// 串号。这是选择无中心化发号器的已知代价。
func nextMsgID() int64 {
	var b [4]byte
	_, _ = rand.Read(b[:])
	// 只取 4 字节的低 20 位：保证随机段不侵占时间戳的位域
	rnd := int64(binary.BigEndian.Uint32(b[:]) & 0xFFFFF)
	return time.Now().UnixMilli()<<20 | rnd
}
