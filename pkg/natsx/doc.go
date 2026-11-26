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
//	crossplay.score.{server}   game → center  积分上报（CrossScoreBatch）
//	crossplay.group.{server}   center → game  分组快照/增量（CrossGroupDeltaPush）
//	crossplay.delta.{server}   game → game    改分直达目标服（CrossScoreDelta）
//
// 通用 relay（Actor 跨节点投递 / 广播），键用 node_id 而非 server_id：
//
//	relay.node.{node}          game → game    点对点投递某节点的 Actor（RelayPush）
//	relay.group.{group}        game → game    组内广播（RelayPush）
//
// 命名空间前缀：上面所有 subject 在实际链路上都带 "{env}." 前缀（如
// prod.relay.node.game-01），由 Config.Prefix 统一注入。subject 是全局扁平
// 的，多环境共用一套 NATS 集群时不隔离，两个环境的同名节点会互相串消息。
// 因此 subject 的构造全部收在 Bus 方法上，调用方拿不到裸字符串。
//
// 中心服的定位：只做全服业务中心（报名裁决、分组、积分聚合、排行榜、
// 匹配、统计），**不参与任何业务服之间的消息转发**。crossplay.* 里它
// 是业务参与方（聚合/分组），relay.* 里它完全不订阅。
//
// 用 server_id 而非 node_id 做跨服玩法的路由键：node_id 是部署态
// （扩缩容会变），server_id 是玩家数据里的业务标识（稳定）。改分直达
// 因此不需要中心服反查节点——这是相对 gRPC 中转方案最大的简化。
package natsx
