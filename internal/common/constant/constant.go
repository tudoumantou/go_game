// Package constant 全局常量（设计文档 3.1 目录树：消息号段边界、默认超时等）
package constant

import "time"

// 消息号段（设计文档 4.3 分配规范；内部 gRPC 走 9000+，与客户端协议物理隔离）
const (
	MsgIDSystemMin   = 1     // 系统消息（心跳/握手/踢下线/错误码）
	MsgIDSystemMax   = 99
	MsgIDConnMin     = 100   // 网关/连接管理
	MsgIDConnMax     = 199
	MsgIDLoginMin    = 1000  // 登录/账号
	MsgIDLoginMax    = 1999
	MsgIDPlayerMin   = 2000  // 玩家数据
	MsgIDPlayerMax   = 2999
	MsgIDBagMin      = 3000  // 背包道具
	MsgIDBagMax      = 3999
	MsgIDSceneMin    = 4000  // 场景/地图
	MsgIDSceneMax    = 4999
	MsgIDBattleMin   = 5000  // 战斗（demo 演算在客户端，见 4.9.3）
	MsgIDBattleMax   = 5999
	MsgIDSocialMin   = 6000  // 社交
	MsgIDSocialMax   = 6999
	MsgIDMailMin     = 7000  // 邮件/任务
	MsgIDMailMax     = 7999
	MsgIDInternalMin = 9000  // 内部 gRPC（不暴露给客户端）
	MsgIDInternalMax = 9999
)

// IsClientMsgID 判断是否客户端消息号（内部号段不暴露）
func IsClientMsgID(id uint32) bool {
	return id < MsgIDInternalMin
}

// 默认超时/周期（与 19.1 配置默认值一致；配置缺失时兜底）
const (
	DefaultHeartbeatInterval   = 15 * time.Second // 客户端心跳周期
	DefaultGatewayReadTimeout  = 45 * time.Second // 网关读超时
	DefaultSessionReconnectTTL = 90 * time.Second // 断线会话保留期
	DefaultTCPKeepalive        = 30 * time.Second // OS 层保活
	DefaultNodeHeartbeatIntv   = 5 * time.Second  // 节点→中心服心跳
	DefaultNodeHeartbeatTmeout = 15 * time.Second // 心跳超时判定
)

// 单包上限（4.2 外层包格式）
const MaxPacketSize = 65536 // 64KB
