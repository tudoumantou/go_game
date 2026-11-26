// Package natsx —— NATS Core 的薄封装，给跨服玩法做异步传输层。
//
// 定位：只做「连接管理 + subject 约定 + protobuf 编解码」三件事，不含任何
// 业务语义（报名/分组/积分的规则都在 internal/center/crossplay 与
// internal/game/module/crossplay）。这样换传输层时业务代码不用动。
//
// 为什么是 NATS Core 而不是 JetStream：跨服改分的既有语义是「尽力而为，
// 丢一包只差 1 分，下一轮 60s 上报的自然收敛」。Core 的 at-most-once 正好
// 匹配这个语义，且不需要配置 stream/consumer、不引入重试与重放。若将来
// 要升级为「不丢」，在 natsx 内部换成 JetStream 即可，subject 不变。
//
// subject 约定（{server} 是区服编号 server_id，业务标识不是部署态 node_id）：
//
//	crossplay.score.{server}.from.{node}    game → center  积分上报（CrossScoreBatch）
//	crossplay.group.{server}.from.{center}  center → game  分组快照/增量（CrossGroupDeltaPush）
//	crossplay.delta.{server}.from.{node}    game → game    改分直达目标服（CrossScoreDelta）
//
// 通用 relay（Actor 跨节点投递 / 广播），键用 node_id 而非 server_id：
//
//	relay.node.{node}.from.{node}           game → game    点对点投递某节点的 Actor（RelayPush）
//	relay.group.{group}.from.{node}         game → game    组内广播（RelayPush）
//
// 订阅侧一律把发送者写成通配：relay.node.{self}.from.* —— 谁发的都收，
// 但每条都知道是谁发的。
//
// 命名空间前缀：上面所有 subject 在实际链路上都带 "{env}." 前缀（如
// prod.relay.node.game-01.from.game-02），由 Config.Prefix 统一注入。
// subject 是全局扁平的，多环境共用一套 NATS 集群时不隔离，两个环境的
// 同名节点会互相串消息。因此 subject 的构造全部收在 Bus 方法上，调用方
// 拿不到裸字符串。
//
// # 为什么发送者要写进 subject
//
// NATS Core 不向订阅者提供发布者身份——消息里没有"谁发的"这个元数据。
// 如果 subject 只有目标（relay.node.B），那么 A 发给 B 和 C 发给 B 走的是
// 同一条 subject，服务端无法区分，接收方也就只能相信载荷里自称的
// from_node。任何拿到总线访问权的进程都能冒充任意节点。
//
// 把发送者写进 subject 后，**服务端**的发布权限就能表达「每个节点只能以
// 自己的名义发」：
//
//	# nats.conf，每个节点一个账号
//	{ user: game01, permissions: {
//	    publish:   { allow: ["prod.relay.node.*.from.game01",
//	                         "prod.relay.group.*.from.game01",
//	                         "prod.crossplay.score.1.from.game01",
//	                         "prod.crossplay.delta.*.from.game01"] }
//	    subscribe: { allow: [">"] }   # 读权限放开无害
//	} }
//
// 于是有三层防护，缺一层都不成立：
//
//  1. 认证：没有账号连不上总线；
//  2. 授权：game01 的账号发不出 *.from.game02；
//  3. 交叉校验：接收端比对 subject 里的发送者与载荷 from_node 字段，
//     不一致即丢弃并计数：metrics.NATSRejected("sender_mismatch")，
//     对应指标 nats_rejected_total{kind="sender_mismatch"}。
//
// 第 3 层不是多余：第 2 层依赖运维把 nats.conf 配对，配漏一条规则时
// 第 3 层是最后一道拦截，而且它会留下指标与日志，让配漏这件事可见。
//
// 中心服的定位：只做全服业务中心（报名裁决、分组、积分聚合、排行榜、
// 匹配、统计），**不参与任何业务服之间的消息转发**。crossplay.* 里它
// 是业务参与方（聚合/分组），relay.* 里它完全不订阅。
//
// 用 server_id 而非 node_id 做跨服玩法的路由键：node_id 是部署态
// （扩缩容会变），server_id 是玩家数据里的业务标识（稳定）。改分直达
// 因此不需要中心服反查节点——这是相对 gRPC 中转方案最大的简化。
package natsx
