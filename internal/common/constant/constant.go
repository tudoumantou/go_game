// Package constant 全局常量（设计文档 3.1 目录树：消息号段边界、默认超时等）
//
// 架构职责：客户端协议消息号（msg_id）分段、握手期系统消息号、全进程
// 共享的默认超时与包大小上限——客户端与服务端的「协议坐标系」。
// 协作关系：网关按 IsClientMsgID 过滤出入站消息；客户端与本包从同一份
// 规范生成 msg_id 常量（4.3 分配规范）；配置缺失时各默认值兜底。
// 设计取舍：
//   - 消息号按业务域分段且段间留空档（连接 100-199，登录 1000-1999）：
//     段内新增消息不挤占邻居；段边界即防火墙，客户端只认 <9000 的号，
//     内部 gRPC（9000+）与客户端协议物理隔离，互不阻塞、错误码不交叉。
//   - 默认超时集中定义而非散落各包：配置漏项时全链路有统一的兜底值，
//     且「默认值是多少」只有一个答案，改默认只改一处。
package constant

import "time"

// 消息号段（设计文档 4.3 分配规范；内部 gRPC 走 9000+，与客户端协议物理隔离）
//
// 分段规则：按业务域占一段，段内从 x001 顺序发号，段与段之间留出整数倍
// 空档（连接 100-199 → 登录 1000-1999，中间 200-999 预留）——相邻业务
// 消息量暴涨时不会「段溢出挤占邻居」，扩容只动自己那段。
// Min/Max 成对声明的用途：网关与各模块用区间断言校验消息归属，
// 而不是逐个枚举消息号（新增消息不用回来改校验代码）。
const (
	MsgIDSystemMin   = 1 // 系统消息（心跳/握手/踢下线/错误码）
	MsgIDSystemMax   = 99
	MsgIDConnMin     = 100 // 网关/连接管理
	MsgIDConnMax     = 199
	MsgIDLoginMin    = 1000 // 登录/账号
	MsgIDLoginMax    = 1999
	MsgIDPlayerMin   = 2000 // 玩家数据
	MsgIDPlayerMax   = 2999
	MsgIDBagMin      = 3000 // 背包道具
	MsgIDBagMax      = 3999
	MsgIDSceneMin    = 4000 // 场景/地图
	MsgIDSceneMax    = 4999
	MsgIDBattleMin   = 5000 // 战斗（demo 演算在客户端，见 4.9.3）
	MsgIDBattleMax   = 5999
	MsgIDSocialMin   = 6000 // 社交
	MsgIDSocialMax   = 6999
	MsgIDMailMin     = 7000 // 邮件/任务
	MsgIDMailMax     = 7999
	MsgIDInternalMin = 9000 // 内部 gRPC（不暴露给客户端）
	MsgIDInternalMax = 9999
)

// 系统消息号（4.3；握手期明文，见 15 章）
// 为什么握手消息能明文：密钥交换（RSA）还没完成，握手期间没有可用加密
// 通道，只能明文交换公钥与随机盐——这是协议设计的固有窗口，靠「握手
// 消息不承载敏感信息」来兜底，而不是靠加密。
// 类型统一 uint16：wire 上 msg_id 占 2 字节，与常量类型一致可防超范围赋值。
// 握手请求/应答复用登录段（1001/1002）：语义上属于登录流程，
// 段归属按业务域而非按「谁在处理它」划分。
const (
	MsgIDHeartbeatReq      uint16 = 1
	MsgIDHeartbeatResp     uint16 = 2
	MsgIDKickNotify        uint16 = 10
	MsgIDServerHello       uint16 = 11 // 网关→客户端：RSA 公钥 + 随机盐
	MsgIDClientKeyExchange uint16 = 12 // 客户端→网关：RSA 加密的 pre-master
	MsgIDFinished          uint16 = 13 // 双向：握手完成确认
	MsgIDHandshakeReq      uint16 = 1001
	MsgIDHandshakeResp     uint16 = 1002

	// 连接管理段（100-199）：登出。
	// 100/101 落在连接段而非登录段——登出作用在「已建立的网关连接」上，
	// 但要作废的是登录服签发的 token，网关持有 Redis 客户端直接删。
	MsgIDLogoutReq  uint16 = 100
	MsgIDLogoutResp uint16 = 101
)

// IsClientMsgID 判断是否客户端消息号（内部号段不暴露）
// 入参用 uint32（宽于 wire 的 uint16）：防调用方把更大范围的值窄化前
// 误判；判定只需 <9000 一次比较，O(1)。
// 网关出站/入站都过它：内部 gRPC 消息（9000+）绝不流向客户端 socket，
// 客户端伪造的内部号在入口被丢弃——协议隔离的执行点。
func IsClientMsgID(id uint32) bool {
	return id < MsgIDInternalMin
}

// 默认超时/周期（与 19.1 配置默认值一致；配置缺失时兜底）
//
// 为什么需要这组兜底：Go 里 0 值 time.Duration 在多数 API 语义是
// 「无超时/立即返回」，配置漏一项不会报错、只会线上出灵异故障；
// 在常量层给默认值，装配处「配置为 0 则取默认」，把错误挡在启动期。
const (
	DefaultHeartbeatInterval   = 15 * time.Second // 客户端心跳周期
	DefaultGatewayReadTimeout  = 45 * time.Second // 网关读超时
	DefaultSessionReconnectTTL = 90 * time.Second // 断线会话保留期
	DefaultTCPKeepalive        = 30 * time.Second // OS 层保活
	DefaultNodeHeartbeatIntv   = 5 * time.Second  // 节点→中心服心跳
	DefaultNodeHeartbeatTmeout = 15 * time.Second // 心跳超时判定
	// 注：Tmeout 为历史拼写（Timeout 少一个 i），标识符已被多处引用，
	// 保持原样不修正——改标识符属于代码变更，不在注释职责内。
)

// 单包上限（4.2 外层包格式）
// 64KB 的依据：客户端消息最大业务载荷（聊天/邮件正文/批量道具）远小于
// 此值，留一倍余量；更重要的作用是防御——超限包直接拒读，防止恶意
// 客户端声明一个巨大 length 把网关内存撑爆（经典 DoS 面）。
const MaxPacketSize = 65536 // 64KB
