// types.go —— 战斗的领域类型与对外依赖接口。
//
// 四个接口是模块边界：CrossCaller（跨服战斗服）、CenterMatcher（中心服匹配）、
// RankReporter（排行上报）、Pusher（下行推送）。它们让本模块不依赖具体服务实现，
// 单测里可以直接假造。
package battle

import (
	"context"
	"time"
)

// 战斗时限（超时服务端平局兜底，见 4.8 第 5 条）。
const defaultBattleTimeout = 180 * time.Second

// matchPlayers 一个房间的参战人数（demo 期 1v1）。
const matchPlayers = 2

// CreateRoomReq 建房请求。
//
// RoomID 非 0 时使用中心服分配的房间号（P8 路径，多参战方各自请求同一号，幂等）；
// PresetTokens 携带中心服已签发的准入令牌（令牌权威在中心服）。
type CreateRoomReq struct {
	// RoomID 中心服预分配房间号；0 表示由 CrossServer 自行分配
	RoomID uint64
	// Mode 玩法模式（arena 等），CrossServer 按模式路由房间实现
	Mode string
	// PlayerIDs 全部参战玩家 id
	PlayerIDs []int64
	// PresetTokens 中心服签发的 playerID → 准入令牌
	PresetTokens map[int64]string
	// Timeout 房间时限，超时由 CrossServer 按平局兜底结算
	Timeout time.Duration
	// ExpectPlayers 房间满员人数；0 表示按 len(PlayerIDs) 推断
	ExpectPlayers int32
}

// CrossCaller 跨服调用能力（由游戏服组装层实现）。
//
// 接口注入而非直接依赖 cross 包：一是模块可单测（测试传 fake），
// 二是战斗会话被多个玩家 Actor 并发访问，实现方必须自行保证并发安全。
type CrossCaller interface {
	// Available 是否有可用的跨服节点。
	Available() bool
	// CreateRoom 申请建房，返回 roomID 与各玩家令牌。
	CreateRoom(ctx context.Context, req CreateRoomReq) (uint64, map[int64]string, error)
	// SendSnapshot 送玩家快照入房。
	SendSnapshot(roomID uint64, playerID int64, token string, snapshot []byte) error
	// SendOp 转发投操作。
	// 契约：body 原样透传不解析；失败只记日志不回客户端——操作丢失由客户端重发/超时兜底。
	SendOp(roomID uint64, playerID int64, msgID uint32, body []byte) error
	// SendResult 转发结算。
	// 注意：CrossServer 收齐房间内双方结果才统一结算回推，单方上报不触发结算。
	SendResult(roomID uint64, playerID int64, token string, result int32, rewards []byte) error
}

// CenterMatcher 中心服匹配能力（P8）。为空表示走本地匹配降级路径。
//
// Match 是「上报即返回」的异步语义：结果经 OnMatchResult 回调回来，
// 调用方不阻塞等待凑人——匹配等待时长不可预期，绝不能占住玩家 Actor。
type CenterMatcher interface {
	// Available 中心服 Report 流是否就绪。
	Available() bool
	// Match 上报匹配请求（结果异步经 OnMatchResult 回来）。
	Match(playerID int64, mode string, elo int32, snapshot []byte) error
}

// RankReporter 排行分上报（P14：战斗结算后写全服榜）。
type RankReporter interface {
	// ReportArenaScore 上报竞技场积分（delta 为增减量，中心服负责累加）。
	// bizID 是本次结算的唯一键：中心服按它去重，同一次结算重投不会重复加分。
	ReportArenaScore(playerID int64, score int64, bizID string) error
}

// Pusher 向在线玩家推送（由组装层实现，跨 Actor 安全）。
//
// 返回值语义：false 表示玩家不在线（已掉线），调用方按「丢弃即可」处理——
// 战斗结算不因一方掉线而阻塞另一方。
type Pusher interface {
	Push(playerID int64, msgID uint32, body []byte) bool
}

// Session 单场战斗的会话状态（玩家侧）。
type Session struct {
	// RoomID 所在房间（0 表示无会话）
	RoomID uint64
	// Token 本玩家的准入令牌，结算上报时校验
	Token string
	// PeerID 对手 id（多人玩法取首个；demo 期固定 1v1）
	PeerID int64
	// Started 开战时间，用于统计/超时判断
	Started time.Time
	// Peer 对手快照（proto 序列化后的 PlayerInfoResp）
	Peer []byte
	// Settled 是否已结算；true 之后会话进入「待回收」状态
	Settled bool
}

// Module 战斗模块。
//
// 与玩家内模块（背包/任务等）的本质区别：那些模块的状态挂在玩家快照上，
// 由 Actor 单线程串行访问、天然免锁；而战斗是全模块级共享状态——
// 会被多个玩家的 Actor 并发读写（各自的 OnMessage），
// 还会被 CrossServer/中心服回调线程直接触达（OnBattleSync 等），
// 因此必须用 sync.Mutex 显式保护。
