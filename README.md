# game_study

通用游戏服务器框架（Go 1.22+）——面向中小型实时/半实时游戏（MMO、MOBA、卡牌、ARPG）。

- **设计文档**：[`docs/game_study-服务器框架设计文档.md`](docs/game_study-服务器框架设计文档.md)（唯一事实源，架构/协议/数据/部署全部以它为准）
- **当前阶段**：**P1–P6 已完成**。机器人可完成「登录 → 网关 RSA/AES 握手 → 账号鉴权 → 创角 → 进游戏 → 拉取角色信息 → 收邮件 → 领取附件 → 查看背包 → 使用道具」完整玩法闭环；数据经 Write-Behind 分片落库 MySQL，Kafka 异步链路（充值发货、邮件投递、跨服私聊）按 `biz_id` 幂等消费。详见下文「已实现能力」。

## 已实现能力（P1–P6）

| 阶段 | 能力 | 关键代码 |
| --- | --- | --- |
| P1 加密接入 | RSA-OAEP 换钥、AES-256-GCM 会话加密、Seq 64 窗口防重放、报文封包、TCP 接入、会话状态机、Login HTTP + Redis Token | `pkg/crypto`、`pkg/codec`、`pkg/net`、`internal/login`、`internal/gateway/{conn,session,router}` |
| P2 内部通信 | etcd 注册/发现/keepalive、gRPC etcd:// 解析器、GameService.Channel 双向流、网关→游戏服 BIND 路由 | `pkg/registry/etcd`、`pkg/rpc`、`internal/game/rpc`、`internal/gateway/backend` |
| P3 游戏服内核 | 自研 Actor 运行时（分片事件循环、有界信箱背压、定时器）、玩家 Actor 生命周期、attr/bag/item 模块 | `pkg/actor`、`internal/game/player`、`internal/game/module/*` |
| P4 数据持久化 | MySQL 连接池与事务、Redis keygen/分布式锁/ID 表、10 分片 Write-Behind flusher、优雅关闭、登录预加载与落库 | `pkg/db`、`pkg/cache`、`pkg/flush`、`pkg/shutdown`、`internal/game/player/persist.go` |
| P5 异步链路 | 9 个 topic 常量、信封 + `biz_id`、幂等 Producer/Consumer（手动 commit + 重试 + 死信）、Redis SETNX 幂等、在线状态/离线暂存、充值下单→回调→发货→确认回传、邮件投递、跨服私聊双端落库 | `pkg/mq`、`pkg/cache/{idem,presence}.go`、`internal/login/{service,handler,mq}`、`internal/game/mq`、`internal/game/module/{mail,chat}` |
| P6 业务闭环 | 模块消息路由（msgID→Module 索引，不再全量广播）、账号维度绑定（token→account_id→按账号查角色）、创角（重名/职业/名字校验 + 事务建角）、进游戏、角色信息、背包列表/使用道具、邮件列表/附件领取（行锁 + 幂等 5002）、下行通路（game→gateway→客户端加密回写）、每会话一条 Channel stream | `internal/game/module/{profile,bag,mail}`、`internal/game/service/role_service.go`、`internal/game/server.go`、`internal/gateway/{router,backend}` |

**Kafka 本地运行**：`make dev` 起中间件后，`export KAFKA_BROKERS=127.0.0.1:9093`（compose 的 EXTERNAL listener），或直接 `./scripts/run-all.sh`（脚本已设默认值）。未设置时 Kafka 自动降级禁用，其余功能不受影响。

**P5 集成测试**：`KAFKA_BROKERS=127.0.0.1:9093 go test -tags=integration ./test/integration/ -run TestKafkaRechargeFlow -v`（真实 Kafka 验证生产→消费→发奖→重复投递幂等）。

**P6 集成测试**：`go test -tags=integration ./test/integration/ -run TestP6GameplayFlow -v`（真实 MySQL+Redis+etcd，覆盖创角→重名 2004→进游戏→角色信息→邮件列表→领取→重复领取 5002→背包→使用道具）。

验收机器人：`test/robot/robot_client.go`；全链路冒烟：`test/integration/p1_p4_smoke_test.go`（testcontainers 起 MySQL+Redis+etcd，真实跑通三服务）。

## 服务组成

| 服务 | 目录 | 职责 | 端口 |
| --- | --- | --- | --- |
| 登录服 LoginServer | `internal/login` | 账号认证、Token 签发、网关/游戏服列表、充值下单 | 8080(HTTP)/8081(metrics) |
| 网关服 GatewayServer | `internal/gateway` | 客户端加密长连接、会话管理、消息路由 | 9001(TCP)/9101(metrics) |
| 游戏服 GameServer | `internal/game` | Actor 玩家逻辑、业务模块、Write-Behind 落库 | 9100(gRPC)/9102(metrics) |
| 中心服 CenterServer | `internal/center` | 节点注册/中转/排行榜/统计/跨服分组 | 9200(gRPC)/9201(metrics) |
| 跨服节点 CrossServer | `internal/cross` | 跨服房间/战场（不注册 etcd、不连网关） | 9300(gRPC)/9301(metrics) |

## 快速开始

```bash
make gen          # proto/data/db 三大生成（需 buf：https://github.com/bufbuild/buf#installation）
make keygen       # 生成网关握手 RSA 密钥对 → deploy/keys
make dev          # 起中间件 etcd/redis/mysql/kafka
make db-init      # 建表
make db-seed      # 灌初始数据（含测试账号）
make run-all      # 按 19.5 启动顺序起五进程
make test         # 单测（go test ./...；集成测试需 -tags=integration，见 test/integration）
make lint         # golangci-lint + buf lint + 生成一致性
```

> 集成测试（23.3）需本机 Docker：`go test -tags=integration ./test/integration/ -v -count=1`（testcontainers 起 MySQL 8.0 + Redis 7）。

## P0 生成物规模

| 产物 | 数量 | 生成器 | CI 卡点 |
| --- | --- | --- | --- |
| `pkg/pb/**/*.pb.go` | 26 个 proto | buf generate | ① ② |
| `pkg/gamedata/*_gen.go` | 32 张配置表 | gendata | ① |
| `pkg/model/*_gen.go` | 25 张表 CRUD | gendb（sql/schema 19 个 DDL） | ① |
| `bin/{login,gateway,game,center,cross}` | 5 个静态二进制 | scripts/build.sh | ⑤ |

## 目录铁律（设计文档 3.2）

| 目录 | 规则 |
| --- | --- |
| `api/` | 协议单一事实源，只放 `.proto` 与 buf 配置 |
| `pkg/pb` `pkg/model` `pkg/gamedata` | **生成代码，禁止手改**（`make gen` 产出，CI 卡点守护） |
| `pkg/` | 可复用基础库，禁止 import `internal/` |
| `internal/` | 服务私有实现，禁止跨服务 import |
| `cmd/` | 只做组装（配置 → 组件 → 服务 → 注册中心 → 优雅退出），不含业务 |
| `sql/schema` | 手写 DDL（gendb 输入）；`sql/seed` 初始数据 |

## 分层规范

```
cmd/main.go            组装层
internal/<svc>/server.go
  ├── handler/router   接入层（协议解析/校验/中间件）
  ├── service          领域层（流程编排/事务边界）
  └── repo             数据层（SQL/Redis）
游戏服特殊分层：rpc.server → actor → player/scene → module → flusher
```

依赖方向铁律：`handler/router → service → repo`，`module → actor context → pkg/*`，任何一层不得反向依赖。
