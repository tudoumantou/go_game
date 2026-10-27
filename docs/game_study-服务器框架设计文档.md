# game_study 游戏服务器框架设计文档

> **项目代号**：game_study
> **版本**：v3.3（完整版，合并 v1 架构 + v2 优化 + v3 演进，经 v3.1 交互模型修订、v3.2 命名统一、v3.3 Demo 契约与质量保障）
> **适用范围**：总体架构、工程目录、通信协议、五大服务详细设计、数据层、代码生成、集群部署
> **读者**：服务端开发、运维、测试

---

## 文档演进记录

| 版本 | 主题 | 关键增量 |
| --- | --- | --- |
| v1 | 总体架构 | 登录/网关/游戏三服务、Actor 模型、Cache-Aside + Write-Behind、目录结构 |
| v2 | 通信与生成优化 | gRPC 内部通信、etcd Resolver、Kafka、RSA+AES 加密、XML→Go / DDL→CRUD 代码生成、优雅关闭 |
| v3 | 中心服与数据分级 | 中心服（统筹/中转/排行榜/统计）、小组跨服节点、数据一致性分级、10 分片分时落库、Redis ID 表、登录预加载 |
| v3.1 | 交互模型修订 | **CrossServer 不连网关**（战斗消息经主游戏服 CrossService 流中转，取消会话迁移）、跨服聊天改 Kafka 扇出 + Redis 双端历史、go:embed 路径修复、MySQL 连接容量规划、进程启动顺序 |
| v3.2 | 命名统一与工程修复 | XServer 统一更名 **CrossServer**（CrossService/cross.proto）、分时落库改为 10 worker 共享连接池（不独占）、K8s 编排规划、端口规划表、Redis key 登记补全、CI 卡点落地、compose 配置挂载 |
| v3.3 | Demo 契约与质量保障 | 新增 [4.9](#49-demo-客户端协议契约首批) Demo 客户端协议契约（首批 32 条消息清单 + protobuf 定义 + **战斗职责划分：演算在客户端，服务端只通知发起/透传/收结果**）、[4.10](#410-错误码规范首批) 错误码规范（首批 27 个）、[第 23 章](#23-测试策略) 测试策略（金字塔/关键单测点/集成/压测/故障演练/对账）；战斗防作弊验证后置 |
| v3.4 | 跨节点 Actor 投递（P0） | 新增 [8.7](#87-跨节点-actor-投递p0)：`PID.Node` + `RemoteSender` + `RemoteMessage` 显式契约 + JSON 信封 + presence 位置解析，远程投递经中心服中转（零新 proto）；登录入口 `TrySetOnline` 与投递入口跨节点守卫构成纵深，根治双 Actor 双写分叉。详见 [R2.4](R2.4-跨节点Actor投递设计.md) |
| v3.4.1 | 薄模块基座 | 新增 [17.2.1](#1721-薄模块基座thin)：预装配 Module 接口 + 失败回包 + 缓存守卫，业务只写处理器（签到等五模块合计省约 500 行样板） |
| v3.4.13 | 装配层拆分 + 脚手架 | `server.go` 1397 → 465 行，拆为 `server_start.go`（14 个 initXxx）/ `server_session.go`（会话绑定）/ `server_adapters.go`（层间适配器）；新增 `make new-module` 生成模块骨架（业务文件 + persist 骨架 + 注册提示），生成物可直接编译 |
| v3.14 | 补玩法缺口 | 装备强化：`equip_enhance.col3` 提供每级攻击加成，上限用「配置缺失」判定（加一行配置即放开一级，不发版）；抽 `equipBonus(equipID, enhanceLevel)` 四处共用（登录/穿戴/卸下/强化增量）——任一处算法不同，反复穿脱就能刷攻击力。好友黑名单：新表 `friend_block` + 6809–6812，拉黑即解双向好友（同事务，只删一边会留单向好友），`Apply` 双向拦截。背包容量确认早已实现（`capacityFor` + `CanHold`），真正缺的堆叠/过期属架构级（要改背包模型），单独立项 |
| v3.13.8 | 大模块形态统一 | friend/team/guild/chat 四个 400~500 行单文件各拆成 types/module/handler/store（chat 另有 delivery 放 Kafka 入站与本地中继）。52 个函数逐一比对零变更。至此模块只有两种形态：有内存态（`xxx.go + persist_gen.go`，通常 < 400 行）、有独立表（`types/module/handler/store[/delivery/view/cache]`），不再有「大模块要不要拆」的判断题 |
| v3.13.7 | 错误码收敛 + battle 拆分 | 补转 `c.Reply(&Resp{Code:X})` 字面量 37 处与「已填其他字段」3 处（用 `FailCode(resp,X)` 而非清空重建）；剩余 34 处 `c.Fail` 保留——它要记 Warn 日志，与 FailCode 的「业务拒绝不记日志」是两回事。`battle.go` 596 行拆成 types/module/handler/match/callback：callback 单独成文的理由是它跑在 gRPC goroutine 里、每个函数自己加锁，与 handler（玩家 Actor 内串行）上下文不同，混写最容易出「以为在 Actor 里所以没加锁」的 bug |
| v3.13.5 | 模块文件职责拆分 | `mail.go` 831 行拆成 7 个文件：types（领域类型）/ store（唯一 SQL 出口）/ module（装配）/ cache（归属索引）/ delivery（Kafka 与离线发放）/ view（缓存行还原）/ handler（业务）。拆分本身零逻辑变更，价值在下一步：文件不分开就写不出按文件的 import 约束。`.golangci.yml` 加 depguard，`**/module/*/handler.go` 禁 `database/sql`/`pkg/db`/`pkg/model`/`pkg/writebehind`——「业务不许碰存储」从此是 CI 卡点而非 review 约定。通配匹配，后续拆 battle/friend/team/guild/chat 自动生效 |
| v3.13.4 | 集成测试容器共享 | 每个重型用例各自 `mysql.Run` 起容器，10 个用例 30 次启动，CI 上把 15 分钟超时打满、docker daemon 拖到 exec inspect 超时（报错看起来像「etcd 起不来」，实际是容器太多）。改为 `TestMain` 起一套 MySQL/Redis/etcd 共用，用例用 `freshEnv` 领独立库（库名由测试名派生，先 DROP 再 CREATE，并补 GRANT——testcontainers 的默认用户只被授权了 MYSQL_DATABASE 那一个库）＋清空共享 Redis/etcd。顺带把 testDSN 的 loc=Local 去掉，与容器 UTC 对齐 |
| v3.13.3 | 模块脚手架重写 | `tools/newmodule` 之前生成的是 `NewStateful` + `HandleR` + 手写 `StateCodec`，而仓库 8 个模块早已全部迁到 `Define` + `HandleRoute`——生成的骨架与现役代码不同形态，使用者第一件事得先改写法。现在按两种底座分别生成：有内存态出 module/types/handler（`Define` + `Route` + `@codec`），有独立表出 module/handler/store（`Thin` + `Handle`）；生成期检查 schema 里是否有该表。配套 `docs/模块开发约定.md` 固化「handler 只做取状态→判规则→调 store→回包」与「SQL 三个来源」 |
| v3.13.1 | 快照完整性与跨玩家只读 | `Actor.dropCache` → `resyncCache`（Drop + Seed 整份重建）：只删不建会让下一次提交按脏字段写出「有 profile、没 items」的残缺快照，跨玩家读会看到空背包。新增 `module.Context.Peers`（`View`/`ViewMany`）：Redis 快照优先、miss 回落 MySQL、不回填、只暴露展示字段，批量走 pipeline + 一条 IN（gendb 新增 `@listin`）。写别人状态仍只能发消息 |
| v3.13 | SQL 全面生成化 | 业务层不再出现 SQL 字符串、不再持有 DB 驱动。创角改为 `service.RoleStore` 窄接口（业务只算规则）+ `player.RoleStore` 实现（生成函数组事务）；`player` 落库层（`persist`/`grant`/`cache_load`/`preload`）与 mail/chat/friend/team/guild/loginlog 的规则 SQL 全部换 `pkg/model` 生成函数。`tools/gendb` 新增八类表前注解（`@findby` `@listby` `@lockby` `@locklistby` `@deleteby` `@countby` `@update` `@updateby`，列名生成期校验）与「可空 VARCHAR → `sql.NullString`」（`mail.biz_id` 1062 的根因）。`pkg/db.IsDuplicate` 收敛 1062 判定，替掉三处 `strings.Contains(err, "1062")`。剩余手写 SQL 只有 JOIN/聚合只读查询，登记在《功能实现状态》例外清单 |
| v3.12 | 归档验收测试 | 6 条集成测试（归档四条路径 + 统计覆盖/并发）；修 `rank_archive_task.rows` 撞 MySQL 保留字（该表自建表起从未读写成功）；`gendb` 加保留字检查（只收 reserved，避免 non-reserved 误报） |
| v3.11 | 清死配置与死代码 | 删 `config.CacheMapping.LoadOnLogin`（v3.9 删登录展开后变成「写了不生效」的配置项，四个 yaml 的键一并移除）；删 `writebehind.PersistModules`（全量版，v3.7 删 Redis 权威路径后零调用方）。保留 `Index.Owner/Exists/CountByOwner`（ZSET 的访问面，业务未用但删掉会让结构不完整且测试退化）并在注释里写明。`preload_all` 标注「未启用」——实现与调用方都在，但没有表需要整表预热 |
| v3.10 | 无状态模块写法统一 | 新增 `module.Handle` / `module.HandlePlain`：协议号由消息类型反查（`codec.ProtoID`），响应由 `new(TResp)` 现造，注册从 4 个参数降到 1 个函数值。10 个模块（bag/battle/chat/friend/guild/mail/profile/rank/team + scene 的一条路由）迁移，删掉 60 余行 `msgID*` 常量与 32 个手写 `codeXxx`；这 32 个错误码进 `errcode` 枚举（proto 新增 4xxx/41xx/6xxx/61xx/62xx/67xx/68xx 共 25 条）。scene 的 enter/move/leave 保留原始 handler 签名——它们「成功不回包、失败才回」，自动回包会改变协议行为。实现见 `internal/game/module/proto.go`（`Handle`/`HandlePlain`）与 12.6 节 |
| v3.9 | 登录展开删除 + 邮件删除协议 | 删除零消费者的「登录预取」链路（`module.PrefetchSource`、`Context.Prefetch`、`Actor.prefetch`、`Preloader.ExpandOwners`/`SetOwnerIndexes`、`ExpandOwners` 的两处调用）：现有有内存态的玩法都是单行 JSON，一对多的模块都无内存态，登录展开挂 Actor 上只对登录那一刻有效——形态不匹配，留着的唯一后果是让人以为它在生效。新增 `MailDeleteReq/Resp`（7022/7023）与 `OwnerIndex.DropEntity`，`RemoveEntity` 从此有真实调用方；mail 错误码迁到 `errcode`（5001-5003 + 新增 5005），满包由 `ErrBagFull`（3003）替代原手写 5004。详见《缓存架构设计》§4.2/§6 |
| v3.8 | 归属索引接入业务（mail） | `OwnerIndex.Expand`（Members → LoadBatch → 残留自愈）成为登录展开与按需展开的同一实现；`AddEntity`/`RemoveEntity` 补齐运行时写入。邮件模块成为首个真实消费者：`Deliver` 先落库再 ZADD、`onList` 走 Expand 并回落 SQL、附件标记从 N 次 EXISTS 改成一条 IN。顺带修 `biz_id` 空串撞唯一键（同玩家第二封普通邮件必失败）与 `EntityRow` 时间列失真（`fmt.Sprint` 产出不可解析的 `+0800 CST`）。详见《缓存架构设计》§4.2/§5/§6 |
| v3.7 | 落库通道收敛 | 删除「Redis 脏队列 → Worker → MySQL」整条通道：`Store.Commit`/`Pending`/`Ack`/`DropQueue`、脏 ZSET、`writebehind.Worker`、`cmd/flusher`、`DisableInlineWritebehind`。内存权威落地后该队列零写入方（`Store.Sync` 不入队、`Commit` 无调用方），进程内 Worker 与独立进程都在扫空队列。落库只剩「Actor 内存 → flusher → MySQL」，并把「只写变过的模块表」一并落地（12 张语句 → 2~4 条）。详见《缓存架构设计》§10 |
| v3.6 | 归属索引与登录展开 | 新增 Redis 第三张表 `by_owner:{table}`（ZSET：score=归属者、member=实体 id），一个结构承担判存在/按归属取列表/反查归属；`idtab.mappings` 声明式配置（启动校验查错查重）；`OwnerIndex` 构建（keyset 分页 + 临时 key + RENAME + ready）+ 未就绪回落 MySQL；`Preloader.ExpandOwners` 登录批量展开（pipeline + 一条 IN + 回填），缓存/非缓存两种模式都接；读时自愈 + 10 分钟对账；`IDTable.Preheat` 支持 preload_all 整表预热。详见《缓存架构设计》 |
| v3.5 | 协议与 DDL 收敛单文件 | `api/proto/game/v1/` 21 个 .proto → **game.proto**（按消息号段分区），`sql/schema/` 45 个 .sql → **game.sql**（51 张表，保留 `@flat` 标注）；生成物（`msgids_gen.go` + 52 个 model）逐字节不变，`genmsgid`/`gendb` 无需改动 |
| v3.4.12 | Typed 全覆盖 | `module.Typed` 铺到 24 个模块 27 个协议（模块层 −97 行）；`bag` 同时迁到 `Thin` 并保留 `Discardable` 覆盖（背包写操作不可丢弃）。仅剩 3 处不回包路径保留手写解包 |
| v3.4.11 | 类型化处理器 | 新增 [17.2.5](#1725-类型化处理器-moduletyped) `module.Typed`：自动解包请求 + 回包响应，业务只写「拿到 req、填 resp」；friend/guild/team 三个关系型模块同时迁到 `Thin`（合计 −116 行），24 个模块全部统一形态 |
| v3.4.10 | 响应声明全覆盖 | `HandleR` 推广到 14 个模块的 36 个协议（仅剩 3 处无回包路径用 `Handle`）；`Plain` 补齐 `HandleR`/`PlainErrCode`/`CacheFailureReply`。所有一问一答的协议不再需要各自的错误回包工厂 |
| v3.4.9 | 删除旧格式兼容 | `Load`/`Seed` 统一为分字段格式：删除 `Record.Legacy`、`LegacySignin`、整份 `data` 读取与 `HDEL data`；`Seed` 改为逐字段写入（此前它仍写整份 `data`，与 `Load` 的新格式不匹配）。项目为独立 demo，无历史数据需要兼容 |
| v3.4.7 | 字段按需序列化 | `Commit` 改接 `FieldSource`：提交时由 `State` 按字段现取，不再先构造整份 `Snapshot`（4,393 ns/14 allocs → **18 ns/0 allocs**）；校验从「整份快照」收窄为「本次要写的 Ext 字段」 |
| v3.4.6 | 扣费路径统一 | [17.2.4](#1724-发奖与扣费-modulegrant--modulespend) 新增 `module.Spend`：四处「先查后扣」收成一次调用，并修掉「丢弃 `RemoveItem` 返回值」导致的静默扣失败；`count<=0` 视为免费，省掉调用方的 `if cost > 0` |
| v3.4.5 | 发奖路径统一 | [17.2.4](#1724-发奖与扣费-modulegrant--modulespend) 新增 `module.Grant`/`PrecheckGrant`/`ToItemChanges`：七个模块的「容量预检 + 溢出防护 + 入包 + 变更推送」收成一次调用（业务文件净减 32 行），并清掉 11 处冗余 `MarkDirty("bag")`；补上原先只有 signin 有的 int32 溢出防护 |
| v3.4.4 | 响应声明内建 | [17.2.1](#1721-薄模块基座thin) 新增 `HandleR`：注册协议时顺带声明响应号与零值原型，错误包由基座克隆原型置 `code` 生成；signin 的 14 行 `respOf` switch 消失（业务文件 316→297 行） |
| v3.4.3 | 状态结构托管 | 新增 [17.2.3](#1723-状态结构托管-modulestatefult)：`module.Stateful[T]` + `writebehind.RegisterStateful`，业务只声明结构体与处理器，Ext 编解码与表行转换收进基座（signin 业务文件 −49 行、持久化代码归零） |
| v3.4.2 | 三档基座与无状态快路径 | 新增 [17.2.2](#1722-三档基座先选档再写代码) `module.Plain` + `StatelessAware` + `Actor.canBypassCache`：零状态模块（榜单/聊天转发）跳过 clone/CAS 全流程；同时落地 errgroup/lo/automaxprocs/testify 四库，修复预加载 `wg.Add` 计数错配导致的降级路径死锁 |

---

## 目录

1. [项目概述](#1-项目概述)
2. [总体架构](#2-总体架构)
3. [工程目录结构](#3-工程目录结构)
4. [通信协议设计](#4-通信协议设计)
5. [服务注册发现与集群](#5-服务注册发现与集群)
6. [登录服（LoginServer）设计](#6-登录服loginserver设计)
7. [网关服（GatewayServer）设计](#7-网关服gatewayserver设计)
8. [游戏服（GameServer）设计](#8-游戏服gameserver设计)
9. [中心服（CenterServer）设计](#9-中心服centerserver设计)
10. [小组跨服（CrossServer）设计](#10-小组跨服crossserver设计)
11. [数据一致性分级](#11-数据一致性分级)
12. [数据层设计](#12-数据层设计)
13. [Kafka 消息队列设计](#13-kafka-消息队列设计)
14. [连接池设计](#14-连接池设计)
15. [安全设计（RSA + AES）](#15-安全设计rsa--aes)
16. [代码生成体系](#16-代码生成体系)
17. [前期功能模块设计](#17-前期功能模块设计)
18. [优雅关闭与数据落库保证](#18-优雅关闭与数据落库保证)
19. [部署架构](#19-部署架构)
20. [非功能性设计](#20-非功能性设计)
21. [开发路线图](#21-开发路线图)
22. [风险与技术权衡](#22-风险与技术权衡)
23. [测试策略](#23-测试策略)

- [附录 A：生成命令速查](#附录-a生成命令速查)
- [附录 B：参考开源实现](#附录-b参考开源实现)
- [附录 C：关键设计决策记录](#附录-c关键设计决策记录)

---

## 1. 项目概述

### 1.1 项目背景

`game_study` 是一套使用 Go 语言编写的通用游戏服务器框架，面向中小型实时/半实时游戏（MMO、MOBA、卡牌、ARPG 等）。框架核心解决五类工程问题：

1. **海量长连接接入**：客户端先通过 HTTP 从登录服获取网关与游戏服信息，再与网关建立加密长连接，最终经网关与游戏服通信；
2. **有状态服务的集群化**：登录服、网关服、游戏服、中心服、跨服节点均支持集群，可分开部署到不同机器，通过 etcd + 中心服协同；
3. **跨服玩法**：游戏服之间不直连，全部经中心服中转；小组跨服节点承载多个游戏服玩家一起玩的跨服玩法；
4. **高并发数据存取**：业务逻辑采用 **Actor 模型**组织，配合 **Redis（内存态权威）+ MySQL（持久化）** 的分层存储，落库走 **10 分片分时合批（worker 共享连接池）**；
5. **工程效率**：协议（proto）、游戏数据配置（XML）、数据库 CRUD（DDL）三类代码全部由脚本生成，杜绝重复劳动。

### 1.2 设计目标

| 维度 | 目标 |
| --- | --- |
| 连接容量 | 单网关进程支撑 10W+ 客户端长连接（需调高 `ulimit -n`） |
| 逻辑容量 | 单游戏服进程支撑 5K~1W 在线玩家（视业务复杂度） |
| 高可用 | 五类服务均无单点，支持水平扩容、故障摘除与自动重连 |
| 可扩展 | 业务以「玩家 Actor + 场景 Actor + 功能模块」方式插件化扩展 |
| 可观测 | 全链路 TraceID、Prometheus 指标、结构化日志 |
| 工程化 | Proto/XML/DDL 三大生成体系、多环境配置、CI 检查 |

### 1.3 技术选型

| 领域 | 选型 | 说明 |
| --- | --- | --- |
| 语言 | **Go 1.22+** | goroutine 天然适合海量连接与 Actor 并发模型 |
| 客户端协议 | **Protobuf 3 + buf** | 二进制、跨语言、向后兼容；buf 做 lint 与兼容性检查 |
| 客户端传输 | TCP 长连接 + **RSA/AES 加密** | RSA 交换密钥，业务期 AES-256-GCM |
| 内部通信 | **gRPC（双向流 + Unary）** | 网关↔游戏服（GameService）、游戏服↔跨服节点（CrossService）用 bidi stream；服务间用 Unary |
| 并发模型 | **Actor 模型（分片事件循环）** | 单线程处理单实体，天然无锁 |
| 缓存 | **Redis 7**（Cluster） | 内存态权威数据、Token、在线状态、分布式锁、排行榜、ID 表 |
| 数据库 | **MySQL 8.0+** | 持久化，主从架构，分表预留，10 分片分时落库 |
| 消息队列 | **Kafka（segmentio/kafka-go）** | 充值、邮件、事件流、中心服中转、落库重试 |
| 注册中心 | **etcd 3.5+** | 服务注册、租约心跳、选主、gRPC Resolver |
| 游戏数据配置 | **XML + gendata 生成** | 策划维护 XML，脚本生成强类型 Go 配置代码 |
| 服务器配置 | YAML + viper | 按服务 × 环境拆分，支持热加载 |
| 日志/指标 | zap + Prometheus + Grafana | 结构化日志、全链路 TraceID |
| 部署 | Docker + K8s / docker-compose | 开发态单机一键起全栈，生产态多机部署 |

### 1.4 核心术语

| 术语 | 说明 |
| --- | --- |
| **Session（会话）** | 一条客户端连接在网关侧的上下文，含鉴权状态、绑定的玩家与后端流 |
| **Actor** | 游戏服内承载游戏状态与行为的最小单元（如一个在线玩家、一个场景） |
| **Shard（分片）** | ActorSystem 的调度单元，一个分片 = 一个事件循环 goroutine + 一批 Actor |
| **gRPC Channel** | 网关到某台游戏服/跨服节点的 HTTP/2 连接，池化管理，多路复用 |
| **SessID** | 后端 stream 上标识客户端会话的逻辑通道号，用于响应回路由 |
| **CenterServer（中心服）** | 游戏服集群的控制面枢纽：节点管理、消息中转、全服排行榜、分组统计；不连网关 |
| **CrossServer（小组跨服节点）** | 承载跨服玩法的专用进程；**不连网关、不注册 etcd**，只与游戏服（CrossService 流）和中心服（Report 流）交互 |
| **Battlegroup（跨服分组）** | 中心服把若干游戏服划为一组，组内共享跨服节点玩跨服玩法 |
| **idtab（ID 表）** | Redis 中维护的内存优先表主键集合，「查不到 ID 即库中没有」 |
| **分时落库** | 10 个落库 worker 按 `crc32(key)%10` 分片，每分钟错峰 6s 轮询合批 UPSERT；连接从业务池共享借还，不独占 |
| **Write-Behind** | 先写 Redis 内存态，脏数据异步批量刷回 MySQL 的持久化策略 |

---

## 2. 总体架构

### 2.1 架构总览

```
  客户端面                                    控制面 / 数据面

┌──────────┐   HTTP(TLS)   ┌────────────┐
│  Client  │──────────────▶│   Login    │
└────┬─────┘               │  (无状态)   │
     │ TCP 加密长连接        └────────────┘
     ▼
┌─────────────┐  gRPC      ┌──────────────┐  CrossService   ┌──────────────────┐
│   Gateway   │─Channel池─▶│  GameServer  │─流(游戏中转)▶│  CrossServer(小组    │
│    集群     │◀───────────│    集群       │◀────────────│  跨服节点)        │
└─────────────┘            └───┬──────┬───┘             └────────┬─────────┘
                               │      │ 上报+中转+指令            │
                               │      └──────────────────────────┘
                               ▼
                      ┌─────────────────┐
                      │  CenterServer   │  ❌ 不连网关
                      │ 统筹/中转/排行榜 │
                      │ /分组统计       │
                      └─────────────────┘
```

### 2.2 服务划分与职责

| 服务 | 有状态性 | 核心职责 | 集群方式 |
| --- | --- | --- | --- |
| **LoginServer** | 无状态 | 账号认证、Token 签发、网关/游戏服列表下发、充值下单 | 水平扩展 + Nginx |
| **GatewayServer** | 有状态（连接） | 客户端长连接管理、RSA/AES 握手鉴权、消息路由转发、限流 | etcd 暴露，登录服按负载分配 |
| **GameServer** | 有状态（玩家 Actor） | 游戏逻辑、Actor 调度、业务模块、数据读写、分时落库 | 按玩家维度绑定，可整进程扩缩容 |
| **CenterServer** | 弱状态（聚合数据） | 节点管理、GS↔GS 中转、全服排行榜、分组统计、跨服分组 | 集群 + etcd 选主（全局单例职责） |
| **CrossServer** | 有状态（战斗房间） | 跨服玩法战场（竞技场/公会战/跨服副本），只与游戏服/中心服交互 | 上报中心服，由中心服划分分组并下发地址（不注册 etcd、不连网关） |

**职责边界（重要）**：

- 网关**不持有任何游戏数据**，只维护「连接 → 会话 → 后端流」的映射，且**只连接游戏服**（不直连跨服节点）；
- 游戏服**不直接面向客户端**，只与网关通信，屏蔽网络细节；
- **游戏服之间不建直连**：控制面（组队/公会战/匹配）经中心服中转；广播类消息（跨服聊天/邮件/事件流）经 Kafka 扇出 + 共享 Redis 定位玩家所在服；
- **CrossServer 不连网关**：跨服战斗消息由主游戏服经 CrossService 流中转——数据面多一跳内网延迟，换取零会话迁移、网关零改动；
- **中心服不连网关**，只做内网控制面，避免成为客户端链路单点；
- 登录服**不参与游戏过程**，仅在接入阶段签发凭证，Token 校验下沉到网关 + Redis。

### 2.3 连接矩阵（谁可以连谁）

| 从 \ 到 | Gateway | GameServer | CrossServer | CenterServer | Redis/MySQL/Kafka |
| --- | --- | --- | --- | --- | --- |
| **Gateway** | — | ✅ gRPC Channel 池 | ❌ **不直连** | ❌ | ✅（Token/在线状态） |
| **GameServer** | ❌（被网关连） | ❌ **不建直连** | ✅ CrossService 流（游戏服为客户端，中转战斗消息） | ✅ 上报 + 中转 + 指令 | ✅ |
| **CrossServer** | ❌ **明确不连网关** | ✅（被游戏服连） | — | ✅ 上报 + 分组/匹配指令 | ✅（房间/令牌） |
| **CenterServer** | ❌ **明确不连网关** | ✅（被连） | ✅（被连） | ✅（集群间经 Kafka） | ✅ |
| **LoginServer** | ❌ | ❌ | ❌ | ❌ | ✅ |

> CrossServer 不注册 etcd（无需被网关发现）；游戏服与组内 CrossServer 的连接地址由中心服 `GroupUpdate` 下发，见 [10.2](#102-分组管理)。

### 2.4 客户端接入流程（时序图）

```
Client                 LoginServer              etcd              Gateway            GameServer
  │                         │                     │                  │                   │
  │─① POST /v1/server/assign▶│                    │                  │                   │
  │   {account, channel}    │─ 查询网关负载/健康 ──▶│                  │                   │
  │                         │◀─ 网关列表+负载 ─────│                  │                   │
  │                         │─ 生成 Token 写入 Redis（TTL）           │                   │
  │◀─ {gateway:[{ip,port}],  │                    │                  │                   │
  │    token, expireAt,      │                    │                  │                   │
  │    recommendServerId}    │                    │                  │                   │
  │                         │                    │                  │                   │
  │─② TCP Connect(gateway) ──────────────────────▶│                  │                   │
  │◀── ServerHello(RSA公钥) ──────────────────────│                  │                   │
  │─ ClientKeyExchange(RSA加密的AES密钥) ─────────▶│                  │                   │
  │◀── Finished(AES加密确认) ─────────────────────│                  │                   │
  │─③ HandshakeReq{token}(AES加密) ──────────────▶│                  │                   │
  │                         │                    │  校验 Token(Redis)│                   │
  │                         │                    │                  │─ 读在线状态        │
  │                         │                    │                  │  Redis: online:{pid}│
  │                         │                    │                  │  命中→旧服(重连)   │
  │                         │                    │                  │  未命中→选一台      │
  │                         │                    │                  │─ gRPC Channel/Stream│──▶│
  │◀── HandshakeResp{pid}(AES) ──────────────────│                  │◀─ Bind 成功 ───────│
  │◀── LoginOk（进入游戏） ───────────────────────│                  │                   │
  │                         │                    │                  │                   │
  │─④ 游戏消息（AES加密长连接）──────────────────▶│─ gRPC stream 转发▶│─ Actor 信箱 ──────▶│
  │◀──────── 响应 ◀────────────────────────────────│◀─────────────────│◀─ Actor 处理结果 ──│
```

> 游戏服/跨服节点启动时另需向中心服上报（`NodeRegister` + 心跳），见 [第 9 章](#9-中心服centerserver设计)。

### 2.5 一条游戏消息的完整流转

以「玩家使用道具」为例：

1. **客户端 → 网关**：发送 AES-GCM 加密包 `Length | MsgID | Seq | 密文+Tag`；
2. **网关**：解密校验（Seq 防重放）→ 鉴权中间件校验 Session 处于 `BOUND` → 按 `playerId` 找到目标 GameServer → 从该服的 **gRPC Channel 池**取一条 Channel → 在会话固定的 stream 上发送 `ClientPacket{session_id, msg_id, body}`；
3. **游戏服**：`GameService.Channel` 收到包 → 按 session_id 找客户端上下文 → 按 playerId 哈希定位 Shard → 投递 Envelope 到玩家 Actor 信箱；
4. **玩家 Actor 单线程处理**：`module.Bag.Use()` 校验数量 → 扣减 → 更新内存态 → `markDirty`（写 Redis + 入分片脏队列）→ 构造 `UseItemResp`；
5. **响应回程**：GameServer → 同一条 stream 回写 → 网关按 session_id 找回 Session → 加密写客户端；
6. **异步落库**：10 个落库 worker 按分钟错峰合批 UPSERT MySQL（见 [12.4](#124-数据落库架构10-分片落库-worker)）。

### 2.6 设计原则

- **网关无业务、游戏服无网络**：两层彻底解耦，各自独立扩缩容；
- **一切皆消息**：Actor 之间、Actor 与外部资源之间全部通过消息/异步回调交互；
- **控制面集中、数据面分级**：GS↔GS 控制面一律经中心服；广播类消息经 Kafka 扇出；跨服战斗数据面经 CrossService 流由主游戏服中转，不过中心服；
- **数据分级**：充值/角色等严格一致（MySQL 权威），其余以 Actor 内存态 + Redis 为准；
- **生成代替手写**：协议、配置、CRUD 全部生成，人只写业务逻辑；
- **故障域隔离**：五类服务分进程、分机器部署，一类故障不影响其余；
- **配置与代码分离**：所有环境差异走 YAML 配置，不走硬编码；
- **启动有依赖顺序**：etcd → 中间件 → 中心服 → 游戏服/跨服节点 → 网关 → 登录服，逐层就绪验证（清单见 [19.5](#195-进程启动顺序与首次部署清单)）。

---

## 3. 工程目录结构

### 3.1 总体目录树

```
game_study/
├── api/                                  # 【协议层】所有 .proto 定义与 buf 工程配置（单一事实源）
│   └── proto/                            # buf 工程根（buf.yaml/buf.gen.yaml 必须与 proto 同级：
│       │                                 #   buf generate 的 out 路径相对 buf.gen.yaml 解析，
│       │                                 #   见 Makefile gen-proto：cd api/proto && buf generate）
│       ├── buf.yaml                      # buf 工程配置（lint 规则、依赖管理）
│       ├── buf.gen.yaml                  # 代码生成模板（go/pb 插件配置）
│       ├── common/v1/                    # 公共消息：错误码、基础结构、通用请求响应
│       │   ├── error.proto
│       │   └── common.proto
│       ├── login/v1/                     # 登录服对外 HTTP 协议契约
│       │   └── login.proto
│       ├── gate/v1/                      # 客户端 <-> 网关 长连接协议
│       │   ├── handshake.proto           # 握手/鉴权/心跳/踢下线
│       │   └── system.proto              # 系统级通知（错误码推送、维护公告）
│       ├── game/v1/                      # 游戏业务协议（单一文件，按消息号段分区，见第 17 章）
│       │   └── game.proto                # 全部客户端消息：1000 登录 / 2000 玩家 / 3000 背包
│       │                                 #   4000 场景 / 5000 战斗 / 6000 社交 / 7000 邮件任务
│       └── internalv2/                   # 服务间内部 gRPC 协议
│                                          # （目录不带 internal 段：Go 的 internal 可见性规则
│                                          #   会阻止 internal/game 等包导入 pkg/pb/internal/v2）
│           ├── game.proto                # GameService.Channel（网关↔游戏服，双向流）
│           ├── cross.proto                   # CrossService.Channel（游戏服↔跨服节点，双向流）
│           ├── center.proto              # CenterService.Report（节点↔中心服，双向流）
│           └── registry.proto            # 注册中心数据模型
│
├── cmd/                                  # 【入口层】各服务可执行 main，只做组装，不含业务
│   ├── login/main.go                     # 登录服启动
│   ├── gateway/main.go                   # 网关服启动
│   ├── game/main.go                      # 游戏服启动
│   ├── center/main.go                    # 中心服启动
│   ├── cross/main.go                   # 小组跨服节点启动
│
├── tools/                                # 开发/运维小工具（Makefile 直接调用，见 19.3）
│   ├── gendata/main.go                   # XML → Go 配置生成器（自研）
│   ├── gendb/main.go                     # DDL → Go CRUD 生成器（参考 goctl）
│   ├── keygen/main.go                    # RSA 密钥对生成（网关握手私钥）
│   └── bench/main.go                     # 压测客户端（模拟加密长连接）
│
├── configs/                              # 【配置层】按服务 × 环境拆分的 YAML
│   ├── login/{login-dev,login-test,login-prod}.yaml
│   ├── gateway/{gateway-dev,gateway-test,gateway-prod}.yaml
│   ├── game/{game-dev,game-test,game-prod}.yaml
│   ├── center/{center-dev,center-test,center-prod}.yaml
│   ├── cross/{cross-dev,cross-test,cross-prod}.yaml
│   └── gamedata/
│       └── xml/                          # 游戏业务数据（策划维护，gendata 输入）
│           ├── item.xml  level.xml  skill.xml  scene.xml
│           ├── shop.xml  activity.xml  quest.xml  copy.xml
│           ├── guild.xml  partner.xml  equip.xml  battle.xml
│           ├── career.xml  create_name.xml  login_reward.xml
│           ├── recharge_product.xml  mail_template.xml  chat_channel.xml
│           └── ...
│
├── internal/                             # 【服务私有实现】Go internal 机制，禁止跨服务引用
│   │
│   ├── common/                           # 跨服务共享的内部代码
│   │   ├── errcode/                      # 统一错误码定义
│   │   └── constant/                     # 全局常量（消息号段边界、默认超时等）
│   │
│   ├── login/                            # ===== 登录服 =====
│   │   ├── server.go                     # HTTP Server + 中间件 + 优雅关闭钩子
│   │   ├── handler/                      # HTTP 接入层
│   │   │   ├── handler.go                # handler 基类与统一响应结构
│   │   │   ├── auth.go                   # 账号认证相关接口
│   │   │   ├── server_list.go            # 服务器列表/分配接口
│   │   │   └── pay_callback.go           # 支付回调
│   │   ├── service/                      # 业务逻辑层（可单测）
│   │   │   ├── auth_service.go           # 认证、Token 签发与复核
│   │   │   ├── dispatch_service.go       # 网关/游戏服选择与负载排序
│   │   │   └── recharge_service.go       # 充值下单（Tier-1 严格一致）
│   │   ├── repo/                         # 数据访问层（基于 pkg/model）
│   │   │   ├── account_repo.go
│   │   │   └── token_repo.go
│   │   └── mq/recharge_producer.go       # Kafka：充值订单生产
│   │
│   ├── gateway/                          # ===== 网关服 =====
│   │   ├── server.go                     # 服务组装
│   │   ├── conn/                         # 客户端连接层（网络 IO）
│   │   │   ├── conn.go                   # 单条客户端连接：readLoop/writeLoop/拆包
│   │   │   ├── crypto.go                 # RSA+AES 握手与会话加密封装
│   │   │   ├── manager.go                # 连接管理器
│   │   │   └── options.go                # 读写缓冲区、超时等参数
│   │   ├── session/                      # 会话层（有状态核心）
│   │   │   ├── session.go                # 会话定义与状态机（无跨服迁移态，跨服玩法不迁移会话，见第 10 章）
│   │   │   └── manager.go                # 会话管理（本地 map + Redis 在线状态）
│   │   ├── router/                       # 消息路由层
│   │   │   ├── router.go                 # MsgID -> Handler 注册表
│   │   │   ├── middleware.go             # 鉴权/限流/日志/恢复 中间件链
│   │   │   └── handler.go                # 网关自身消息处理器（握手、心跳、踢下线）
│   │   └── backend/                      # 网关 -> 游戏服/跨服节点（gRPC 层）
│   │       ├── channel_pool.go           # gRPC Channel 池（每目标 N 条）
│   │       ├── stream_mgr.go             # 会话 <-> stream 映射、粘性、重绑、迁移
│   │       ├── game_client.go            # GameServiceClient 封装（etcd 发现）
│   │       └── broadcast.go              # 全服广播下发
│   │
│   ├── game/                             # ===== 游戏服 =====
│   │   ├── server.go                     # 服务组装
│   │   ├── rpc/{server,codec}.go         # gRPC GameService.Channel 实现 + CrossService.Channel 客户端
│   │   ├── player/                       # 玩家 Actor
│   │   │   ├── actor.go                  # 玩家 Actor：生命周期 + 消息分发
│   │   │   ├── state.go                  # 玩家内存状态结构
│   │   │   ├── manager.go                # 在线玩家管理器
│   │   │   └── persist.go                # 加载/预加载/落库（见第 12 章）
│   │   ├── scene/                        # 场景 Actor
│   │   │   ├── scene.go / aoi.go / manager.go
│   │   ├── module/                       # 业务功能模块（见第 17 章映射表）
│   │   │   ├── module.go                 # Module 接口
│   │   │   ├── create/  # 创角        ├── login/    # 登录/重连/踢掉
│   │   │   ├── recharge/ # 充值        ├── level/    # 升级
│   │   │   ├── attr/     # 属性        ├── equip/    # 装备
│   │   │   ├── skill/    # 技能        ├── bag/      # 背包
│   │   │   ├── guild/    # 公会        ├── chat/     # 聊天（含跨服）
│   │   │   ├── mail/     # 邮件        ├── team/     # 组队
│   │   │   ├── copy/     # 副本        ├── shop/     # 商店
│   │   │   ├── activity/ # 活动        ├── quest/    # 任务
│   │   │   ├── friend/   # 好友        └── partner/  # 伙伴
│   │   ├── service/                      # 领域服务（跨 Actor）
│   │   │   ├── guild_service.go          # 公会聚合根
│   │   │   ├── team_service.go           # 队伍匹配
│   │   │   └── world_service.go          # 跨服聊天/广播（Kafka 扇出 + 本地性判定，见 17.3）
│   │   ├── flusher/{flusher,strategy}.go# Write-Behind 落库编排（驱动 pkg/flush）
│   │   ├── mq/                           # Kafka 消费者统一入口
│   │   │   ├── consumer.go               # Consumer Group 管理
│   │   │   ├── recharge_consumer.go      # 幂等发货
│   │   │   ├── mail_consumer.go          # 幂等投递
│   │   │   ├── event_consumer.go         # 事件流 → 任务/成就/活动
│   │   │   ├── chat_cross_consumer.go    # 跨服聊天（广播消费+本地性判定+双端历史，见 17.3）
│   │   │   └── recharge_delivered_consumer.go # 充值发货确认回传登录服（Tier-1，见 13.3）
│   │   ├── scheduler/                    # 全局定时（etcd leader + Kafka 触发）
│   │   └── report/                       # 中心服上报客户端（Register/Heartbeat/Rank/Feature/Relay）
│   │
│   ├── center/                           # ===== 中心服 =====
│   │   ├── server.go                     # CenterService gRPC Server + 优雅关闭
│   │   ├── session/                      # Report 流管理（node_id → stream 路由表）
│   │   ├── registry/                     # 节点注册表（心跳、负载、版本）
│   │   ├── relay/                        # 中转引擎（点对点/组播/跨实例经 Kafka）
│   │   ├── rank/                         # 全服排行榜（Redis ZSet + 赛季归档）
│   │   ├── stats/                        # 功能上报聚合（分组×时间窗）
│   │   ├── group/                        # 跨服分组管理（Battlegroup）
│   │   ├── leader/                       # etcd 选主（全局调度/赛季结算）
│   │   └── api/                          # GM/运营查询（排行榜/统计）
│   │
│   └── cross/                          # ===== 小组跨服节点 =====
│       ├── server.go                     # 复用 pkg 层 actor/flush/rpc 组装 + 上报中心服（不注册 etcd、不连网关）
│       ├── scene/                        # 跨服房间/战场 Actor
│       ├── battle/                       # 战斗逻辑（快照驱动）
│       ├── match/                        # 匹配配合（接收 MatchResult、建房）
│       └── result/                       # 战斗结果推送（BattleResult → 各参战游戏服，经 CrossService 流）
│
├── pkg/                                  # 【公共库】可被任何服务复用的基础能力
│   ├── codec/                            # 编解码
│   │   ├── packet.go                     # 定长+变长包协议（Length|MsgID|Seq|Body）
│   │   ├── protobuf.go                   # protobuf 消息注册表与反射分发
│   │   └── registry.go                   # MsgID <-> MessageType 注册（代码生成产出）
│   ├── crypto/                           # 安全
│   │   ├── rsa.go                        # 密钥对生成/加载/加解密（OAEP）
│   │   ├── aes.go                        # AES-256-GCM 会话加解密
│   │   ├── handshake.go                  # 握手协议（ServerHello/KeyExchange/Finished）
│   │   └── session_cipher.go             # 单连接会话密码器（Seq 防重放）
│   ├── net/                              # 网络层封装（TCP 服务端/客户端）
│   │   └── tcp_server.go / tcp_client.go / options.go
│   ├── pool/                             # 通用池
│   │   ├── tcp_pool.go                   # TCP 连接池（备选/工具场景）
│   │   └── buffer_pool.go                # 字节缓冲区对象池（sync.Pool）
│   ├── rpc/                              # gRPC 基础设施
│   │   ├── server.go                     # gRPC Server 组装 + 拦截器链
│   │   ├── client.go                     # Channel 池 + etcd Resolver 封装
│   │   ├── interceptor.go                # Recovery/Logging/Metrics/Auth 拦截器
│   │   └── resolver/etcd/etcd_resolver.go# gRPC etcd Resolver（Builder 实现）
│   ├── mq/{kafka,producer,consumer,idempotent}.go # Kafka 封装 + 幂等工具
│   ├── flush/                            # 分时分片落库（10 worker 共享连接池）
│   │   ├── flusher.go                    # ShardFlusher：分散轮询编排
│   │   ├── batcher.go                    # 合批多行 UPSERT 构造
│   │   ├── tombstone.go                  # 物理删除队列处理
│   │   └── metrics.go                    # 落库指标（分片延迟/批量大小/失败率）
│   ├── actor/                          # Actor 运行时（框架核心，game/cross 共用）
│   │   ├── actor.go / envelope.go      # Actor 抽象 + 消息信封（Tell/Call/Broadcast）
│   │   ├── system.go / shard.go        # ActorSystem + 分片单 goroutine 事件循环
│   │   ├── mailbox.go                  # 信箱（有界队列 + 背压策略）
│   │   ├── scheduler.go                # 定时器（时间轮）
│   │   └── context.go / registry.go    # Actor 上下文 + ActorID 编址（类型+实例ID）
│   ├── registry/                         # 服务注册发现
│   │   ├── registry.go                   # Registry 抽象接口
│   │   ├── etcd/                         # etcd 实现（租约 + KeepAlive + Watch）
│   │   └── redis/                        # Redis 实现（轻量备选）
│   ├── cache/                            # Redis 封装
│   │   ├── redis.go                      # 客户端构建 + 连接池参数
│   │   ├── keygen.go                     # Key 规范生成器（全项目唯一入口）
│   │   ├── idtab.go                      # ID 表构建/维护/对账
│   │   ├── exist.go                      # 存在性判断 + 回源加载
│   │   ├── shard.go                      # Redis key → DB 连接分片哈希
│   │   ├── lock.go                       # 分布式锁（SET NX + 看门狗续期）
│   │   └── pubsub.go                     # 发布订阅封装
│   ├── db/{mysql,tx,builder}.go          # MySQL 封装
│   ├── model/                            # 【gendb 生成】CRUD（见 16.2）
│   ├── gamedata/                         # 【gendata 生成】配置数据（见 16.1）
│   │   ├── data.go / *_gen.go            # 生成代码（全量覆盖）
│   │   └── embed/xml/*.xml               # gendata 拷贝的 XML 源（go:embed 输入，见 16.1.3）
│   ├── shutdown/                         # 优雅关闭编排（见第 18 章）
│   ├── log/  config/  util/
│   └── pb/                               # 【buf 生成】protobuf + gRPC 代码
│
├── sql/                                  # 【数据库】
│   ├── schema/game.sql                   # 手写全量 DDL（单一文件，按表名排序；gendb 输入，见 12.3）
│   └── seed/                             # 初始数据（GM 账号、测试号；仅 dev/test）
│
├── scripts/{build.sh,gen.sh,lint.sh,db-init.sh,db-seed.sh,run-all.sh}
├── deploy/                               # 部署编排
│   ├── docker/{login,gateway,game,center,cross}.Dockerfile
│   ├── docker-compose.yaml               # 本地一键起 etcd+redis+mysql+kafka+五服务
│   ├── keys/                             # RSA 密钥对（make keygen 产出，私钥不入库不进镜像）
│   └── k8s/                              # 生产 K8s 编排（StatefulSet/Deployment/HPA/探针，见 19.6）
├── docs/                                 # 设计文档（本文档）
├── test/{integration/,benchmark/}
├── .github/workflows/ci.yaml             # CI：lint + gen 一致性 + test + build
├── go.mod
├── Makefile                              # make gen / build / test / lint / run
└── README.md
```

### 3.2 目录职责速查表

| 目录 | 层级定位 | 关键约束 |
| --- | --- | --- |
| `api/` | 协议单一事实源 | 只放 `.proto` 与 buf 配置；**禁止**手写生成目录内容 |
| `pkg/pb/`、`pkg/model/`、`pkg/gamedata/` | 生成代码 | 由 `make gen` 产出，禁止手改 |
| `pkg/` | 可复用基础库 | 只依赖 `pkg/` 内部与第三方；**禁止** import `internal/`；Actor 运行时等框架代码放 `pkg/`（game/cross 共用） |
| `internal/` | 业务私有实现 | 禁止跨服务 import（如 gateway 不得 import game） |
| `cmd/` | 组装层 | 只做依赖注入与启动，不含业务判断 |
| `configs/gamedata/xml/` | 策划数据 | 只放 XML，由 gendata 生成 Go 代码 |
| `sql/schema/` | 数据库 DDL | 手写带注释，gendb 输入 |
| `configs/` | 服务器配置 | 敏感信息（密码）用环境变量注入，不入库 |

### 3.3 分层规范

```
cmd/main.go               组装：配置 → 组件 → 服务 → 注册中心 → 优雅退出
        │
        ▼
internal/<service>/server.go
        │
        ├── handler/router  接入层：协议解析、参数校验、中间件
        │
        ├── service         领域层：业务流程编排、事务边界
        │
        └── repo            数据层：SQL/Redis 操作，只被 service 调用

游戏服特殊分层：
internal/game/server.go
        │
        ├── rpc.server      接入层：gRPC GameService.Channel
        │
        ├── actor           调度层：分片事件循环、信箱、定时器（框架内核）
        │
        ├── player/scene    实体层：状态 + 生命周期
        │
        ├── module          业务层：功能模块，只操作所属 Actor 的状态
        │
        └── flusher         持久化层：驱动 pkg/flush 分时落库
```

**依赖方向铁律**：`handler/router → service → repo`，`module → actor context → pkg/*`；任何一层不得反向依赖。

### 3.4 命名规范

| 对象 | 规范 | 示例 |
| --- | --- | --- |
| Proto 包 | `game_study.<域>.v<版本>` | `game_study.game.v1` |
| Go 包名 | 小写单词，不用下划线 | `session`、`backend` |
| 接口实现 | 接口 `Foo`，实现 `fooImpl` 或直接同名 | `Registry` / `etcdRegistry` |
| 消息号常量 | `MsgID_<模块>_<动作>` | `MsgID_ITEM_USE_REQ = 3001` |

---

## 4. 通信协议设计

### 4.1 Protobuf 工程管理（buf）

所有协议以 `api/proto/` 为**单一事实源**，使用 `buf` 统一管理：

```yaml
# api/buf.yaml
version: v1
name: buf.build/game_study/api
lint:
  use:
    - DEFAULT
  except:
    - PACKAGE_DIRECTORY_MATCH
breaking:
  use:
    - FILE
```

- `buf lint`：CI 强制执行命名/注释规范；
- `buf breaking`：防止对已发布协议做不兼容变更；
- `make gen-proto`：一键生成 Go 代码到 `pkg/pb/`，生成文件**禁止手改**。

### 4.2 外层包格式（客户端 <-> 网关）

长连接采用「**长度前缀 + 消息号 + 序号 + Protobuf Body**」二进制协议，业务期整体 AES-256-GCM 加密：

```
 0                4                6          10                    10+N
+----------------+----------------+-----------+---------------------+
|    Length      |     MsgID      |    Seq    |   AES-GCM 密文      |
|   uint32 大端   |   uint16 大端   | uint32 大端|  (Body + 16B Tag)  |
+----------------+----------------+-----------+---------------------+

Length = 4 + N（Seq + 密文长度，不含自身 4 字节）
AAD   = {Length, MsgID, Seq, session_id}   （防篡改 + 防重放）
```

设计要点：

1. **定长头拆包简单**：先读 4 字节拿 Length，再读 Length 字节，一次 `io.ReadFull` 完成，便于零拷贝缓冲区复用；
2. **消息号驱动分发**：网关与游戏服均以 MsgID 为 key 查 Handler 注册表（`pkg/codec/registry.go`，代码生成产出），新增协议只需注册；
3. **Seq 单调递增**：每方向从 1 开始，滑窗（1024）接受乱序、拒绝回放；
4. **大小限制**：单包默认上限 64KB（`gateway.maxPacketSize`），超限直接断开；
5. **握手期明文**：握手消息（ServerHello/ClientKeyExchange/Finished）走独立系统消息号，不加密，见 [第 15 章](#15-安全设计rsa--aes)。

### 4.3 消息号分配规范

| 号段 | 用途 | 说明 |
| --- | --- | --- |
| 1 ~ 99 | 系统消息 | 心跳、握手（RSA/AES 换钥）、踢下线、错误码通知 |
| 100 ~ 199 | 网关/连接管理 | 重连通知、维护倒计时、顶号通知 |
| 1000 ~ 1999 | 登录/账号 | 登录、创角、服务器列表 |
| 2000 ~ 2999 | 玩家数据 | 进入游戏、玩家信息、属性变更 |
| 3000 ~ 3999 | 背包道具 | 使用、出售、扩容 |
| 4000 ~ 4999 | 场景/地图 | 进入场景、实体同步、移动 |
| 5000 ~ 5999 | 战斗 | 技能、伤害结算（demo 期演算在客户端，服务端只通知发起/透传/收结果，见 [4.9.3](#493-战斗职责划分demo-约定)） |
| 6000 ~ 6999 | 社交 | 聊天、好友、公会 |
| 7000 ~ 7999 | 邮件/任务 | — |
| 9000 ~ 9999 | 内部 gRPC 消息 | 不暴露给客户端 |

- **Req/Resp 成对分配**：奇数给请求，偶数给响应（如 `UseItemReq=3001`、`UseItemResp=3002`）；
- **推送消息**从段内单独起号，注释标明 `[Push]`。

### 4.4 核心系统消息定义（示例）

```protobuf
// api/proto/gate/v1/handshake.proto
syntax = "proto3";
package game_study.gate.v1;
option go_package = "game_study/pkg/pb/gate/v1;gatev1";

// 握手请求：加密握手完成后客户端发送的第一条业务消息，携带 Token
message HandshakeReq {
  string token = 1;            // 登录服签发的 Token
  string client_version = 2;   // 客户端版本号（网关可做版本校验）
  string platform = 3;         // 平台：ios/android/pc
}

message HandshakeResp {
  int32 code = 1;              // 0 成功，非0见错误码表
  int64 account_id = 2;        // 账号 ID（一个账号可有多角色）
  int64 server_time = 3;       // 服务器时间（客户端校准时钟）
  int32 heartbeat_interval = 4;// 心跳间隔（秒）
  int64 player_id = 5;         // 已选角色 ID，0 = 尚未创角/选角
}
```

### 4.5 内部 gRPC 协议（网关 <-> 游戏服、游戏服 <-> 跨服节点）

内部通信统一走 gRPC。网关与游戏服之间、游戏服与跨服节点之间均为**双向流**，每个客户端会话 = 一条 stream：

```protobuf
// api/proto/internalv2/game.proto
syntax = "proto3";
package game_study.internal.v2;
option go_package = "game_study/pkg/pb/internalv2;internalv2";

// 客户端上行/下行数据单元（网关透传，不解析业务）
message ClientPacket {
  uint64 session_id = 1;   // 网关会话 ID（内部路由用，即 SessID）
  uint32 msg_id     = 2;
  bytes  body       = 3;   // 业务 protobuf 序列化结果
  uint32 seq        = 4;   // 客户端序号（限流/去重）
}

// 连接级控制消息
message CtrlMsg {
  enum Type {
    BIND      = 0;  // 网关通知游戏服：会话绑定到玩家
    UNBIND    = 1;  // 会话断开（进入重连保留期）
    KICK      = 2;  // 踢下线（顶号/封禁/GM）
    BROADCAST = 3;  // 广播
    HEALTH    = 4;  // 健康检查
  }
  Type   type       = 1;
  uint64 session_id = 2;
  int64  player_id  = 3;
  bytes  payload    = 4;
}

message StreamFrame {
  oneof data {
    ClientPacket client_packet = 1;
    CtrlMsg      ctrl          = 2;
  }
}

// 游戏服对外服务（网关接入）
service GameService {
  rpc Channel(stream StreamFrame) returns (stream StreamFrame);
}
```

游戏服 ↔ 跨服节点（CrossService）：一条流 = 一个游戏服节点，**由游戏服主动建立**（地址来自中心服 `GroupUpdate`），CrossServer 不连网关：

```protobuf
// api/proto/internalv2/cross.proto
syntax = "proto3";
package game_study.internal.v2;
option go_package = "game_study/pkg/pb/internalv2;internalv2";

service CrossService {
  rpc Channel(stream CrossFrame) returns (stream CrossFrame);
}

message CrossFrame {
  oneof data {
    PlayerSnapshot snapshot = 1; // GS→X：玩家进入跨服（token + 战斗快照）
    BattleMsg      battle   = 2; // 双向：战斗消息（GS 代收客户端消息 / X 同步）
    BattleResult   result   = 3; // X→GS：战斗结算（奖励在主游戏服应用）
    CrossCtrl          ctrl     = 4; // 控制消息（房间就绪/关闭、心跳）
  }
}

message PlayerSnapshot {
  uint64 room_id      = 1; // 中心服分配的房间
  string battle_token = 2; // 中心服签发的准入令牌
  int64  player_id    = 3;
  bytes  snapshot     = 4; // 战斗所需数据（战力/阵容/段位），不含完整状态
}

message BattleMsg {
  uint64 room_id   = 1;
  int64  player_id = 2;
  uint32 msg_id    = 3;
  bytes  body      = 4; // 业务 protobuf（与客户端协议一致，GS 原样透传）
}

message BattleResult {
  uint64 room_id     = 1;
  int64  player_id   = 2;
  int32  result      = 3; // 0 胜 1 负 2 平
  bytes  rewards     = 4; // 奖励明细
  int64  finished_at = 5;
}

message CrossCtrl {
  enum Type {
    ROOM_READY  = 0; // X→GS：房间就绪，可推送快照
    ROOM_CLOSED = 1; // 双向：房间关闭
    HEALTH      = 2; // 心跳
  }
  Type   type    = 1;
  uint64 room_id = 2;
}
```

要点：

- **SessID = session_id**：响应沿同一条 stream 回写，天然保证单玩家消息有序；
- **SessID = 0 的控制消息**：Bind/Unbind/Kick/Broadcast/Health；
- HTTP/2 多路复用：一个 Channel（默认每目标 16 条）承载全部会话流，无需为每个会话建连接；
- **无会话迁移**：跨服玩法时会话始终绑定主游戏服，战斗消息由游戏服经 CrossService 流中转（见第 10 章）；
- 游戏服与组内每个 CrossServer 维持一条 CrossService 流，断流指数退避重连（1s→30s 封顶）；
- 内部消息号走 9000+ 号段，与客户端协议物理隔离。

### 4.6 中心服上报协议（节点 <-> 中心服）

```protobuf
// api/proto/internalv2/center.proto
syntax = "proto3";
package game_study.internal.v2;
option go_package = "game_study/pkg/pb/internalv2;internalv2";

service CenterService {
  // 长连接：节点上报 + 中心服下发指令（bidi stream，一个节点一条流）
  rpc Report(stream NodeReport) returns (stream CenterCommand);

  // GM/运营查询（Unary）
  rpc QueryGlobalRank(QueryRankReq) returns (stream RankEntry);
  rpc QueryFeatureStats(QueryStatsReq) returns (QueryStatsResp);
}

message NodeReport {
  oneof body {
    NodeRegister   register      = 1; // 启动上报（连接后第一条，必发）
    NodeHeartbeat  heartbeat     = 2; // 心跳（在线数、负载、版本）
    RankReport     rank          = 3; // 排行榜分数上报（增量）
    FeatureReport  feature       = 4; // 功能上报（分组统计）
    RelayRequest   relay         = 5; // 中转请求（发给指定节点/广播）
    MatchRequest   match         = 6; // 跨服匹配请求（游戏服发）
  }
}

message CenterCommand {
  oneof body {
    RelayPush    relay_push   = 1; // 中转投递（来自其它节点）
    RankNotify   rank_notify  = 2; // 排行榜变更/结算通知
    GroupUpdate  group        = 3; // 跨服分组变更
    MatchResult  match_result = 4; // 匹配结果（分配跨服节点 + battle_token）
    ConfigPush   config       = 5; // 配置热更新推送
    KickNode     kick         = 6; // 摘除指令（配合优雅关闭）
    MatchAssign  match_assign = 7; // 中心服→CrossServer：分配房间（建房+令牌）
  }
}

message NodeRegister {
  string node_id   = 1; // 如 "game-01" / "cross-01"
  string node_type = 2; // "game" | "cross"
  string addr      = 3; // gRPC 地址
  string group_id  = 4; // 所属跨服分组（中心服分配/确认）
  int64  start_at  = 5;
}

message NodeHeartbeat {
  string node_id      = 1;
  int32  player_count = 2;
  int32  cpu_load     = 3; // 0-100
  int32  mem_load     = 4;
  int64  msg_qps      = 5;
  string version      = 6;
}

message MatchRequest {
  string node_id   = 1;  // 发起方游戏服
  int64  player_id = 2;
  string mode      = 3;  // 玩法模式（arena/guild_war/copy）
  bytes  snapshot  = 4;  // 玩家战斗快照
  int32  elo       = 5;  // 匹配分值
}

message MatchAssign {
  string node_id      = 1; // 目标 CrossServer
  uint64 room_id      = 2;
  string battle_token = 3; // 准入令牌（CrossServer 校验快照时比对）
  string mode         = 4;
  int32  expect_players = 5;
}

message MatchResult {
  uint64 room_id      = 1;
  string battle_token = 2;
  string mode         = 3;
  // cross 地址由接收方本地分组信息补全（GroupUpdate 已下发组内地址）
}

message GroupUpdate {
  string group_id            = 1;
  repeated string game_nodes = 2; // 组内游戏服
  repeated string cross_addrs = 3; // 组内跨服节点 gRPC 地址（游戏服据此建 CrossService 流）
  string action              = 4; // create / dissolve / expand
}
```

### 4.7 心跳、保活与超时

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| 客户端心跳间隔 | 15s | 客户端定时发送 `HeartbeatReq` |
| 网关读超时 | 45s | 超过未收到任何数据 → 主动断开 |
| TCP Keepalive | 30s | 操作系统层保活，清理死连接 |
| gRPC Channel 保活 | 10s | HTTP/2 PING + 应用层 HEALTH 消息 |
| 节点→中心服心跳 | 5s | 超时 15s 判定节点不可用 |

网关采用「**超时被动断开 + 客户端主动心跳**」双保险，避免半开连接占用文件描述符。

### 4.8 断线重连与状态恢复

1. 客户端断线后，网关保留 Session **90 秒**（可配置），期间标记为 `RECONNECTING`；
2. 客户端重连 → 重新 RSA/AES 握手 → 携带 Token 握手 → 网关命中旧 Session → 通知游戏服「会话恢复」，玩家 Actor 状态原样保留，游戏内进度不丢；
3. 超过保留时间，网关通知游戏服执行玩家下线流程（落库 → 摘除在线状态 → 停止 Actor）；
4. Token 过期（Redis TTL）前，客户端需重新走登录服刷新；
5. 玩家处于跨服战斗中被踢下线/掉线：demo 期战斗由客户端演算，服务端无「当前进度」可依——客户端已上报结果则按结果结算，否则房间超时（`BattleStartNotify.timeout_sec`）后按平局兜底结算 → 经 CrossService 流推送主游戏服应用（不经中心服）。

### 4.9 Demo 客户端协议契约（首批）

Demo 期「客户端 ↔ 网关」业务协议的第一批契约。目标：**P6 业务功能开发启动前，客户端与服务端对消息号、字段、错误码达成一致**，避免联调期返工。消息号分配遵循 [4.3](#43-消息号分配规范)，错误码遵循 [4.10](#410-错误码规范首批)。

#### 4.9.1 契约原则

1. **Req/Resp 成对**：奇数请求、偶数响应；无响应需求的上行用 `*Report`（如 `BattleResultReport`），服务端主动下发用 `*Notify`；
2. **推送从段内单独起号**：注释标明 `[Push]`，客户端不得回复；
3. **所有 Resp 首字段都是 `code`**（int32，0 成功，非 0 见错误码表）；异步失败与全局事件（踢下线、维护）额外走 `ErrorNotify(20)` 推送；
4. **msg_id 幂等**：涉及奖励/消耗的消息（聊天发送、战斗结果上报、附件领取）必须有全局唯一 ID，重复投递必须安全；
5. **兼容演进**：字段只增不改不回收；号段内新消息从段尾续号，废弃号不复用。

#### 4.9.2 消息清单（首批 32 条）

| 消息号 | 方向 | 消息 | 载荷要点 | 说明 |
| --- | --- | --- | --- | --- |
| 1 / 2 | C↔S | HeartbeatReq / HeartbeatResp | 空 / server_time | 15s 周期（见 [4.7](#47-心跳保活与超时)） |
| 10 | S→C | KickNotify [Push] | reason、ban_until | 顶号/封禁/GM 踢下线 |
| 20 | S→C | ErrorNotify [Push] | code、msg、context | 异步失败与全局事件（如维护） |
| 30 | S→C | MaintenanceNotify [Push] | start_at、end_at、countdown_sec | 全服维护倒计时 |
| 1001 / 1002 | C↔S | HandshakeReq / HandshakeResp | token、client_version / code、account_id、player_id、server_time | 已定义于 [4.4](#44-核心系统消息定义示例) |
| 1010 / 1011 | C↔S | EnterGameReq / EnterGameResp | role_id / code、role_id、server_time | role_id=0 引导创角（2003） |
| 1020 / 1021 | C↔S | CreateRoleReq / CreateRoleResp | name、career / code、role_id | 职业取 career.xml |
| 2001 / 2002 | C↔S | PlayerInfoReq / PlayerInfoResp | — / 玩家全量信息 | 进游戏后主动拉一次 |
| 2010 | S→C | AttrChangeNotify [Push] | diff 列表（attr_id、old、new） | 属性增量推送 |
| 3001 / 3002 | C↔S | UseItemReq / UseItemResp | item_uid、count / code、剩余数量 | 道具使用 |
| 3003 / 3004 | C↔S | BagListReq / BagListResp | - / code、items[] | 背包列表（P6 新增，堆叠模式） |
| 3010 | S→C | BagChangeNotify [Push] | 变更列表（增/删/改） | 背包任何变动后推送 |
| 5001 | S→C | BattleStartNotify [Push] | room_id、battle_token、对手快照、timeout_sec | 服务端「通知发起」 |
| 5002 / 5003 | C↔S | MatchReq / MatchResp | mode / code、queued | 发起跨服匹配（P13 补齐：原契约遗漏了入队入口） |
| 5010 | C→S | BattleResultReport | room_id、battle_token、result、rewards、finished_at | 客户端「上报结果」 |
| 5011 | S→C | BattleSettleNotify [Push] | room_id、code、rewards | 服务端结算回执（权威奖励） |
| 5020 | S→C | BattleSyncNotify [Push] | room_id、player_id、msg_id、body | 服务端「透传」对手/房间事件 |
| 5021 | C→S | BattleOpReport | room_id、msg_id、body | 客户端操作上行，原样转对手 |
| 6001 / 6002 | C↔S | ChatSendReq / ChatSendResp | channel、peer_id、content、local_id / code、msg_id | 私聊/世界聊（见 [17.3](#173-跨服聊天全链路双端历史--kafka-扇出)） |
| 6010 | S→C | ChatMessageNotify [Push] | msg_id、channel、sender、content | 收消息（含跨服扇出） |
| 6020 / 6021 | C↔S | ChatHistoryReq / ChatHistoryResp | peer_id、before_msg_id、limit / 消息列表 | 拉双端历史 |
| 7001 / 7002 | C↔S | MailListReq / MailListResp | — / 邮件列表（摘要） | 邮件模块 |
| 7010 | S→C | MailNewNotify [Push] | mail_id、标题、有附件 | 新邮件到达 |
| 7020 / 7021 | C↔S | MailTakeAttachmentReq / MailTakeAttachmentResp | mail_id / code、奖励列表 | 领取附件（幂等 5002） |
| 2011 | S→C | LevelInfoNotify [Push] | level、exp、exp_max | 等级/经验变化（P6-B1 新增） |
| 7101–7106 | C↔S | QuestList/Accept/Claim (Req+Resp) | quest_id / code、status、progress、exp_gain、rewards | 任务列表/接取/领奖（P6-B1 新增） |
| 3101–3106 | C↔S | EquipList/Wear/TakeOff (Req+Resp) | uid / code、attrs[] | 装备列表/穿戴/脱下（P6-B2 新增） |
| 3201–3206 | C↔S | SkillList/Learn/Upgrade (Req+Resp) | skill_id / code、level | 技能列表/学习/升级（P6-B2 新增） |
| 6101–6108 | C↔S | GuildInfo/Create/Join/Quit (Req+Resp) | name、guild_id / code、member_count、is_leader | 公会（P6-B4 新增） |
| 6201–6208 | C↔S | TeamInfo/Create/Join/Leave (Req+Resp) | copy_id、team_id / code、members[] | 组队（P6-B4 新增） |
| 6301–6304 | C↔S | CopyList / CopySettle (Req+Resp) | copy_id、star / code、first_pass、exp_gain、rewards | 副本列表/结算（P6-B4 新增） |
| 6401–6404 | C↔S | ShopList / ShopBuy (Req+Resp) | goods_id、count / code、bought | 商店列表/购买（P6-B4 新增） |
| 6501–6504 | C↔S | ActivityList / ActivityClaim (Req+Resp) | activity_id / code、rewards | 活动列表/领奖（P6-B4 新增） |

#### 4.9.3 战斗职责划分（demo 约定）

**约定：战斗演算由客户端负责，服务端不演算、不校验战斗过程**，只做「通知发起、消息透传、接收结果」三件事；防作弊验证（伤害合法性、操作时序、上报频率）后置到战斗 2.0 迭代。

```
客户端（演算责任方）                       服务端（编排责任方，不演算）
─────────────────────                    ─────────────────────────────
· 技能/伤害/胜负全部本地计算
· 关键操作 BattleOpReport(5021) ───────▶  消息透传：BattleMsg 原样转发
                                          给对手（不解析 body）
· 收对手事件 ◀───── BattleSyncNotify(5020)
· 战斗结论 BattleResultReport(5010) ───▶  接收结果：令牌校验 + 幂等
                                          → 奖励存在性复核 → 应用奖励
        ◀───── BattleSettleNotify(5011)（权威奖励明细）
```

服务端三件事的具体语义：

| 服务端职责 | 消息 | 语义 |
| --- | --- | --- |
| 通知发起 | `BattleStartNotify(5001)` | 匹配完成后下发房间号 + 准入令牌 + 对手快照 + 超时；客户端凭令牌开局 |
| 消息透传 | `BattleOpReport(5021)` / `BattleSyncNotify(5020)` ↔ 内部 `BattleMsg` | 客户端操作原样进 CrossService 流 → 房间 Actor → 对手所在 GS → 对手客户端；**服务端不解析 body** |
| 接收结果 | `BattleResultReport(5010)` → `BattleSettleNotify(5011)` | 校验 `battle_token` 与房间匹配 → 幂等（同房间只结算一次）→ 奖励配置存在性复核 → 应用 → 回执 |

demo 期服务端的战斗相关校验只有三条：**令牌匹配、幂等去重、奖励配置存在性**。伤害数值、操作时序、上报频率等可信度验证不在本期范围（列入 [23.7](#237-demo-期测试范围) 后置项）。超时未上报的战斗，服务端按平局兜底结算（见 [4.8](#48-断线重连与状态恢复) 第 5 条）。

#### 4.9.4 核心消息 protobuf 定义

```protobuf
// api/proto/gate/v1/demo.proto（首批 demo 消息，号段见 4.3）
syntax = "proto3";
package game_study.gate.v1;
option go_package = "game_study/pkg/pb/gate/v1;gatev1";

// ===== 1000 ~ 1999 登录/账号 =====
message EnterGameReq {
  int64 role_id = 1;      // 0 = 尚无角色，服务端返回 2003 引导创角
}
message EnterGameResp {
  int32  code        = 1; // 0 成功；2003 无角色
  int64  role_id     = 2;
  string server_time = 3; // RFC3339，客户端校准时钟
}

message CreateRoleReq {
  string name   = 1;      // 角色名（唯一性 + 敏感词校验）
  int32  career = 2;      // 职业 ID（career.xml）
}
message CreateRoleResp {
  int32 code    = 1;
  int64 role_id = 2;
}

// ===== 5000 ~ 5999 战斗（demo 约定：演算在客户端，见 4.9.3）=====
message BattleStartNotify {
  uint64 room_id      = 1; // 房间号
  string battle_token = 2; // 准入令牌（结果上报须回带）
  bytes  opponent     = 3; // 对手快照（战力/阵容/段位，非完整状态）
  int64  timeout_sec  = 4; // 战斗时限，超时服务端按平局兜底结算
}

message BattleOpReport {              // C→S：操作上行，服务端原样透传
  uint64 room_id = 1;
  uint32 msg_id  = 2;                 // 客户端自定义战斗消息号
  bytes  body    = 3;
}

message BattleSyncNotify {            // [Push] S→C：透传对手/房间事件
  uint64 room_id   = 1;
  int64  player_id = 2;               // 事件来源玩家
  uint32 msg_id    = 3;
  bytes  body      = 4;
}

message BattleResultReport {          // C→S：客户端上报战斗结论
  uint64 room_id      = 1;
  string battle_token = 2;
  int32  result       = 3;            // 0 胜 1 负 2 平
  bytes  rewards      = 4;            // 客户端计算的奖励明细（服务端复核后应用）
  int64  finished_at  = 5;
}

message BattleSettleNotify {          // [Push] S→C：结算回执（权威奖励）
  uint64 room_id = 1;
  int32  code    = 2;                 // 0 成功；6002 令牌无效；6003 已上报
  bytes  rewards = 3;
}

// ===== 6000 ~ 6999 社交 =====
message ChatSendReq {
  int32  channel  = 1;   // 频道（chat_channel.xml：1 世界 2 私聊 …）
  int64  peer_id  = 2;   // 私聊对端 player_id（其他频道忽略）
  string content  = 3;
  string local_id = 4;   // 客户端本地 ID（本地回显 + 发送侧幂等）
}
message ChatSendResp {
  int32  code    = 1;
  string msg_id  = 2;    // 服务端分配的消息 ID（全局唯一，幂等键）
  int64  sent_at = 3;
}
message ChatMessageNotify {           // [Push]
  string msg_id      = 1;
  int32  channel     = 2;
  int64  sender_id   = 3;
  string sender_name = 4;
  int64  peer_id     = 5;             // 私聊对端
  string content     = 6;
  int64  sent_at     = 7;
}

// ===== 7000 ~ 7999 邮件 =====
message MailTakeAttachmentReq {
  int64 mail_id = 1;
}
message MailTakeAttachmentResp {
  int32  code    = 1;                 // 0 成功；5002 已领取（幂等）
  bytes  rewards = 2;
}
```

### 4.10 错误码规范（首批）

#### 4.10.1 定义位置与使用方式

- **协议定义**：`api/proto/common/v1/errcode.proto`（enum，随生成体系进 `pkg/pb`，客户端可同源生成）；
- **Go 侧**：`internal/common/errcode/errcode.go`（enum 值 → 中文文案 map；文案可热更新，不进协议 wire）；
- **下发双通道**：Req/Resp 的 `code` 字段承载请求的直接结果；`ErrorNotify(20)` 推送承载异步失败与全局事件（维护、封禁、顶号原因）；
- **新增规则**：只增不改；废弃码标记 `deprecated` 并保留（历史包兼容），号永不复用。

#### 4.10.2 错误码表（首批 27 个）

| 错误码 | 常量名 | 文案 | 典型场景 |
| --- | --- | --- | --- |
| 0 | OK | 成功 | — |
| 1001 | ErrParam | 参数错误 | 请求字段缺失/非法 |
| 1002 | ErrTokenExpired | 登录已过期，请重新登录 | Token TTL 失效 |
| 1003 | ErrMaintenance | 服务器维护中 | 全服停服更新 |
| 1004 | ErrRateLimited | 操作过于频繁，请稍后再试 | 协议层限流（见 [20.4](#204-容量与限流)） |
| 1005 | ErrServerBusy | 服务器繁忙 | Actor 信箱满/过载保护 |
| 2001 | ErrAccountBanned | 账号已被封禁 | 登录服鉴权 |
| 2002 | ErrKicked | 账号在其他设备登录 | 顶号（配合 KickNotify） |
| 2003 | ErrNoRole | 尚未创建角色 | EnterGame 引导创角 |
| 2004 | ErrNameUsed | 角色名已被占用 | CreateRole 唯一性 |
| 2005 | ErrNameIllegal | 角色名含非法字符 | CreateRole 敏感词/长度 |
| 3001 | ErrItemNotExist | 道具不存在 | UseItem |
| 3002 | ErrItemNotEnough | 道具数量不足 | 消耗类操作 |
| 3003 | ErrBagFull | 背包已满 | 发奖/拾取 |
| 3004 | ErrItemNotUsable | 道具当前不可使用 | UseItem |
| 4001 | ErrChannelClosed | 频道未开放 | 聊天频道配置 |
| 4002 | ErrPeerNotFound | 对方不在线或不存在 | 私聊对端定位失败 |
| 4003 | ErrChatTooFast | 发言过于频繁 | 聊天限流（5 条/10s） |
| 4004 | ErrChatMuted | 你已被禁言 | GM 禁言 |
| 5001 | ErrMailNotFound | 邮件不存在 | 领取附件 |
| 5002 | ErrMailTaken | 附件已领取 | 幂等保护 |
| 5003 | ErrNoAttachment | 邮件没有附件 | 领取附件 |
| 6001 | ErrBattleNotFound | 战斗不存在或已结束 | 房间已销毁 |
| 6002 | ErrBattleTokenInvalid | 战斗令牌无效 | 令牌过期/不匹配 |
| 6003 | ErrResultReported | 战斗结果已上报 | 幂等保护 |
| 6004 | ErrBattleFull | 战斗房间已满 | 进房 |
| 6005 | ErrResultRewardInvalid | 上报的奖励不存在 | 奖励配置复核失败 |

#### 4.10.3 号段约定

错误码按模块分段，与消息号段解耦（自身独立分段）：`1xxx` 通用/系统、`2xxx` 登录账号、`3xxx` 背包道具、`4xxx` 社交聊天、`5xxx` 邮件、`6xxx` 战斗。客户端可按首位分发到不同 UI 处理：`2xxx` 弹回登录页、`4xxx` toast、`6xxx` 战斗内提示——分段即路由，新增模块续段即可。

---

## 5. 服务注册发现与集群

### 5.1 etcd 注册模型

```
/gs/
├── login/
│   └── login-01            → {"ip":"192.0.2.10","port":8080,...}
├── gateway/
│   ├── gateway-01          → {"ip":"192.0.2.21","port":9001,"conns":5231,"maxConns":100000}
│   └── gateway-02          → {...}
├── game/
│   ├── game-01             → {"ip":"192.0.2.31","grpcPort":9100,"players":3120}
│   └── game-02             → {...}
├── center/
│   └── center-01           → {"ip":"192.0.2.41","grpcPort":9200,...}
└── leader/
    └── center              → 选主（全局调度/赛季结算）
```

- **租约机制**：每个实例注册时创建 Lease（TTL=10s），KeepAlive 每 3s 续约；进程崩溃租约自动过期，实例自动摘除；
- **Watch**：网关 Watch `/gs/game/`，后端上下线秒级感知，动态调整 Channel 池目标；
- **CrossServer 不注册 etcd**：无需被网关发现，其地址由中心服分组（`GroupUpdate`）下发给组内游戏服，见 [10.2](#102-分组管理)；
- **优雅退出**：SIGTERM 第一动作 `Delete` 注册键，杜绝「已下线仍被分配」。

### 5.2 gRPC 服务发现（etcd Resolver + round_robin）

客户端侧（网关连游戏服、游戏服连中心服）使用自定义 etcd Resolver；游戏服连跨服节点不走 etcd（地址由中心服下发，见 [10.2](#102-分组管理)）：

```go
// pkg/rpc/client.go：基于 etcd 的 gRPC 客户端
// 参考实现：github.com/geniuscirno/grpcresolver、github.com/wothing/wonaming
func DialGameService(etcdEndpoints []string) (internalv2.GameServiceClient, error) {
    // 目标格式：etcd://127.0.0.1:2379/gs/game/
    conn, err := grpc.NewClient(
        "etcd://"+strings.Join(etcdEndpoints, ",")+"/gs/game/",
        grpc.WithDefaultServiceConfig(`{"loadBalancingPolicy":"round_robin"}`),
        grpc.WithTransportCredentials(insecure.NewCredentials()),
        grpc.WithChainUnaryInterceptor(rpcinterceptor.Recovery, rpcinterceptor.Logging, rpcinterceptor.Metrics),
    )
    return internalv2.NewGameServiceClient(conn), nil
}
```

- `pkg/registry/etcd` 封装 Register/Watch（租约 + KeepAlive）；
- `pkg/rpc/resolver/etcd` 实现 `resolver.Builder`，从 etcd Watch 实例列表，配合 `round_robin` 做客户端负载均衡；
- 支持带元数据（负载权重）的端点更新， unhealthy 端点自动摘除。

### 5.3 服务集群方案对比

| 维度 | LoginServer | GatewayServer | GameServer | CenterServer | CrossServer |
| --- | --- | --- | --- | --- | --- |
| 状态 | 无状态 | 连接状态 | 玩家状态 | 聚合数据 | 战斗房间 |
| 发现方式 | Nginx 直连 | etcd + 登录服分配 | etcd + 网关分配 | etcd + 节点上报 | 不注册 etcd；中心服分组下发地址 |
| 扩容窗口 | 任意时刻 | 任意时刻（新连接） | 新玩家登录时 | 任意时刻 | 任意时刻（新匹配） |
| 扩容影响 | 无 | 无（存量不动） | 存量玩家留旧服 | 无 | 无 |
| 宕机恢复 | LB 摘除 | 客户端重连其他网关 | 玩家重登分配新服 | 节点重连其他实例 | 战斗强制结算 |
| 数据位置 | MySQL + Redis | Redis（在线状态） | Redis + MySQL | Redis + MySQL（聚合） | 内存 + Redis（房间） |

### 5.4 中心服集群与选主

- **无主部分**：Report 接入、中转、排行榜写入——每实例独立承载自己连接的节点，天然水平扩展；
- **有主部分**（etcd 选主，leader 执行）：全局活动调度（Kafka `gs.activity.schedule` 生产者）、赛季结算归档、全服进度活动里程碑判定；
- **跨实例中转**：节点 A 连中心服#1、节点 B 连中心服#2 时，中心服#1 写 Kafka `center.relay`（Key=目标 node_id），中心服#2 消费后本地下发，节点无感知；
- leader 故障 → etcd 租约过期 → 抢主 → 从 Redis/MySQL 恢复上下文。

### 5.5 故障场景与应对

| 故障 | 现象 | 应对 |
| --- | --- | --- |
| 单网关宕机 | 该网关全部连接断开 | 客户端按登录服下发的列表重连其他网关；在线状态随 Session 保留期失效后清理 |
| 单游戏服宕机 | 该服玩家集体掉线 | 重连时在线状态已失效 → 分配新服 → 从 Redis/MySQL 恢复（丢失 ≤1 个落库周期） |
| 中心服单实例宕机 | 其连接的节点掉线 | 节点指数退避重连其他中心服实例；存量跨服房间由 CrossServer 兜底结算 |
| 中心服全宕 | 中转/排行榜暂停 | 游戏服本地缓存路由表继续运行（降级）；恢复后补报 |
| Redis 主节点宕机 | 缓存不可用 | 集群自动切换；游戏服降级直连 MySQL（性能下降但可用） |
| MySQL 主库宕机 | 写失败 | 主从切换；flush 失败重试队列保护，恢复后补偿 |
| etcd 集群故障 | 无法注册/发现 | 服务使用本地缓存的实例列表继续运行，恢复后重新同步 |
| 网络分区 | 脑裂 | 以 etcd 选主为准；顶号互斥（online key 抢占）保证同一玩家单会话 |

---

## 6. 登录服（LoginServer）设计

### 6.1 定位与职责

登录服是**无状态 HTTP 服务**，承担整个接入流程的「入口调度」：

1. 账号认证（渠道账号、游客、设备号）；
2. 签发 Token（写 Redis，带 TTL）；
3. 从注册中心拉取网关/游戏服列表，按负载排序后下发给客户端；
4. 充值下单（Tier-1 严格一致，见 [11.2](#112-tier-1-严格一致充值--角色)）；
5. 提供 Token 复核接口（网关可选调用）。

### 6.2 HTTP API 定义

统一响应结构：

```json
{ "code": 0, "message": "ok", "data": { } }
```

| 方法 | 路径 | 说明 | 鉴权 |
| --- | --- | --- | --- |
| POST | `/v1/auth/guest` | 游客登录（设备号注册/登录） | 无 |
| POST | `/v1/auth/channel` | 渠道账号登录 | 无 |
| GET | `/v1/server/gateway/list` | 获取网关列表（含负载，按空闲度排序） | 无 |
| GET | `/v1/server/game/list` | 获取游戏服列表（选服界面） | 无 |
| POST | `/v1/server/assign` | **一键分配**：返回最优网关 + Token + 推荐游戏服 | 无（输入账号凭证） |
| POST | `/v1/auth/token/verify` | 校验 Token 有效性 | 内部 |
| POST | `/v1/pay/callback` | 支付渠道回调（创建/推进订单） | 渠道验签 |
| GET | `/healthz` | 健康检查（LB 探活） | 无 |

`POST /v1/server/assign` 响应示例：

```json
{
  "code": 0,
  "data": {
    "gateways": [
      { "id": "gw-01", "ip": "192.0.2.11", "port": 9001, "load": 0.62 },
      { "id": "gw-02", "ip": "192.0.2.12", "port": 9001, "load": 0.88 }
    ],
    "recommend_gateway": { "id": "gw-01", "ip": "192.0.2.11", "port": 9001 },
    "recommend_game_server": 3,
    "token": "a1b2c3...",
    "expire_at": 1735689600
  }
}
```

> 客户端**拿到多个网关**而非单个：首选网关连不上时可快速故障转移，无需再请求登录服。

### 6.3 Token 机制

采用「**随机 Token + Redis 存储**」而非纯 JWT，理由：可主动失效、可绑定设备、可携带会话上下文。

```
Key:   token:{token值}
Value: { "player_id": 10001, "account_id": 888, "device": "...", "login_at": ... }
TTL:   24h（滑动续期，玩家在线期间每次握手续期）
```

- 生成：`crypto/rand` 32 字节，Base62 编码；
- 校验：网关本地直连 Redis 校验（**不依赖登录服在线**，这是登录服可无状态集群的关键）；
- 失效：顶号登录、封禁、修改密码时 `DEL` 即可；
- 防重放：`token_once:{token}`（SET NX）握手成功后即焚，重连走 Session 保留机制而非复用 Token。

### 6.4 网关/游戏服排序策略

登录服从 etcd 拉取实例列表后按以下权重排序：

1. **过滤**：剔除不健康（心跳超时）、满载（连接数 ≥ 阈值）的实例；
2. **加权**：`score = w1*(1-连接负载) + w2*(1-CPU负载) + w3*延迟惩罚`；
3. **打散**：同一账号短时间内重复登录，尽量分配不同网关（防单点聚集）；
4. **粘性**：玩家已在线（Redis `online:{playerId}` 命中）时，返回其所在网关，支持秒级重连。

### 6.5 集群设计

- **无状态水平扩展**：登录服不存任何会话，N 个实例完全对等，前置 Nginx（ip_hash / least_conn）；
- **共享状态全部外置**：Token、账号数据分别在 Redis / MySQL，任何实例可处理任何请求；
- **优雅退出**：SIGTERM 时先在 etcd 摘除注册，LB 停止分发，处理完在途请求后退出；
- **容量估算**：单实例 5K QPS 量级，按登录峰值（同时在线玩家的 10%~20%）规划实例数。

---

## 7. 网关服（GatewayServer）设计

### 7.1 定位与职责

网关是客户端长连接的**唯一入口**，核心职责：

1. 管理海量客户端 TCP 连接（收发、拆包、心跳）；
2. RSA/AES 握手与会话加解密（`conn/crypto.go`）；
3. 握手鉴权（校验 Token，建立会话）；
4. 消息路由：客户端消息 → 游戏服（**网关只连游戏服**，跨服节点不可见）；后端响应 → 对应客户端；
5. 安全与流控：包大小限制、频率限流、未鉴权拦截；
6. **无会话迁移**：跨服玩法时会话始终绑定主游戏服，战斗消息由游戏服经 CrossService 流中转（见第 10 章）。

网关**不解析业务协议内容**（除系统消息外一律透传），新增游戏玩法无需改网关代码。

### 7.2 客户端连接层（`conn/`）

单连接采用「**2 goroutine 模型**」：一个读循环 + 一个写循环，读写解耦：

```go
type ClientConn struct {
    conn      net.Conn
    session   *Session
    cipher    *SessionCipher   // AES-256-GCM 会话密码器（Seq 防重放）
    readBuf   []byte           // 复用缓冲区，减少 GC
    writeQ    chan []byte      // 待发送队列（有界，超出即背压/断开）
    closeOnce sync.Once
}

func (c *ClientConn) readLoop() {
    // 1. 定长读头（4B Length）→ 2. 读 Body → 3. 解密校验 → 4. 交路由分发
}

func (c *ClientConn) writeLoop() {
    // 从 writeQ 取包，加密后合并批量 write，降低系统调用次数
}
```

要点：

- **写队列合并**：高频小包场景下合并写，可降低 50%+ `write` 系统调用；
- **优雅关闭**：`closeOnce` 保证通道只关一次，避免 panic；
- **缓冲区池化**：`pkg/pool/buffer_pool.go` 用 `sync.Pool` 复用读写 buffer；
- **epoll 模型**：Go 运行时原生 netpoll，10W 连接下单机内存约 1~2GB（每连接 ~10~20KB）。

### 7.3 会话管理（`session/`）

会话状态机：

```
CONNECTED ──握手+鉴权成功──▶ AUTHED ──绑定游戏服──▶ BOUND ──断开──▶ CLOSED
    │                          │                      │  │
    │                          └──── 90s 内重连 ──────┘  └─（跨服玩法不迁移会话，见第 10 章）
```

```go
type Session struct {
    ID        uint64          // 网关内自增，同时作为后端 stream 的 SessID
    conn      *ClientConn
    PlayerID  int64
    State     SessionState
    Backend   string          // 当前后端节点 ID（仅 game-01 等游戏服）
    LoginAt   time.Time
    LastActive time.Time
}
```

- 本地用 `sync.Map`/分段 map 维护全量会话（按 `ID` 与 `PlayerID` 双索引）；
- **在线状态外置 Redis**：`online:{playerId} -> {gateway_id, session_id, game_server_id}`，供中心服/其他服务定位玩家所在网关；
- 会话保留期（90s）内重连：恢复同一 Session，玩家无感。

### 7.4 后端连接：gRPC Channel 池与多路复用（`backend/`）

#### 7.4.1 Channel 池架构

```
Gateway                                    GameServer
  │  gRPC Channel Pool                       │
  │  ┌────────┐┌────────┐┌────────┐         │   ┌──────────────────────┐
  ├─▶│Chan #1 ││Chan #2 ││Chan #3 │◀──HTTP/2──▶│  gRPC Server          │
  │  └───┬────┘└───┬────┘└───┬────┘         │   │  GameService.Channel │
  │      │         │         │              │   │   ├─ Stream#101 ──▶ 玩家Actor#A │
  │   stream101  stream205  stream377       │   │   ├─ Stream#202 ──▶ 玩家Actor#B │
  │   (会话1)    (会话2)    (会话3)          │   │   └─ Stream#303 ──▶ 玩家Actor#C │
```

设计要点：

1. **Channel 池**：网关为每台后端维护 `2×NumCPU` 个 gRPC Channel（默认 16），发送时 Round-Robin；Channel 内置连接复用、自动重连、HTTP/2 PING 保活；
2. **会话 = Stream**：每个客户端会话在某 Channel 上开一条 bidi stream，游戏服响应沿**同一条 stream** 返回 → 天然保证单玩家消息有序；
3. **粘性**：会话首次绑定后固定使用某 Channel 的某 stream；Channel 故障时该 Channel 上所有 stream 断开，触发会话重绑（重连保留期内可恢复到新 Channel）；
4. **流控**：HTTP/2 层流控兜底；应用层每 Channel 限制最大并发 stream（默认 2000），防把游戏服打挂；
5. **超时**：Unary 调用默认 3s deadline；stream 无业务超时，靠 HEALTH 心跳（10s）检测死链；
6. **etcd 发现**：Channel 目标由 etcd Resolver 动态解析（仅 `/gs/game/`），后端上下线秒级感知。

```go
// pkg/rpc/interceptor.go 拦截器链
grpc.WithChainUnaryInterceptor(
    rpcinterceptor.Recovery,   // panic 恢复
    rpcinterceptor.Logging,    // TraceID=playerID+sessionID
    rpcinterceptor.Metrics,    // Prometheus
    rpcinterceptor.Auth,       // metadata x-internal-token
)
```

### 7.5 消息路由与中间件（`router/`）

```
收到客户端包
   │
   ▼
┌─────────────┐   ┌───────────┐   ┌───────────┐   ┌──────────┐
│ 解密+拆包    │──▶│ 中间件链  │──▶│ 路由查找  │──▶│ Handler  │
└─────────────┘   │ 恢复panic │   │ MsgID→fn  │   │ 透传/自研│
                  │ 限流      │   └───────────┘   └──────────┘
                  │ 鉴权检查  │
                  │ 日志/Trace│
                  └───────────┘
```

- **系统消息**（握手、心跳、踢下线）由网关自身 Handler 处理；
- **业务消息**（1000+）封装 `ClientPacket` 转发后端，网关零业务逻辑；
- 中间件链式注册，支持按消息号段启用/关闭（未鉴权前只放行握手消息）。

### 7.6 集群设计

| 问题 | 方案 |
| --- | --- |
| 如何被发现 | 启动时向 etcd 注册 `/gs/gateway/{id}`（ip、port、当前连接数），租约心跳 5s |
| 客户端如何找到网关 | 登录服查询 etcd，按负载排序返回列表 |
| 扩容 | 新网关启动即自动加入，登录服下一轮分配即生效，存量连接不受影响 |
| 缩容/故障 | 心跳超时 → etcd 租约过期 → 登录服不再分配；网关 SIGTERM 时先摘注册，再向全部客户端发「请重连」后关闭 |
| 跨网关通信 | 全服广播：游戏服 → 中心服中转 → 各网关 → 各自连接 |

---

## 8. 游戏服（GameServer）设计

### 8.1 为什么选择 Actor 模型

游戏逻辑天然是「**大量独立实体 + 少量交互**」的结构：每个玩家有自己的背包、属性、任务，彼此通过战斗/聊天/交易交互。传统「共享内存 + 锁」方案在实体量大时会出现锁竞争毛刺、状态竞争 bug 难复现、局部修改引发全局性能回归。

Actor 模型将每个玩家/场景封装为独立单元，**单线程顺序处理自己的消息**：

- 无锁：实体状态只被自己的处理线程访问；
- 有序：同一玩家的消息严格 FIFO；
- 隔离：单个 Actor panic 不影响其他 Actor（由 shard 恢复）。

### 8.2 分片事件循环架构

为每个 Actor 建一个 goroutine 在万级在线时开销过大，采用「**分片（Shard）事件循环**」：

```
                    ┌────────────────────────────────────────┐
                    │            ActorSystem                 │
                    │  shardCount=1024                       │
                    │  ┌─────────┐ ┌─────────┐    ┌─────────┐│
                    │  │Shard #0 │ │Shard #1 │... │Shard#1023││
                    │  │ goroutine│ │goroutine│    │goroutine││
                    │  │ mailbox  │ │ mailbox │    │ mailbox ││
                    │  │ A1,A5,A9 │ │A2,A6... │    │A3,A7... ││
                    │  └─────────┘ └─────────┘    └─────────┘│
                    └────────────────────────────────────────┘
       投递: shardIndex = hash(actorID) & shardMask
       保证: 同一 Actor 的消息永远落入同一 Shard → 单线程有序
```

核心类型：

```go
// pkg/actor/actor.go —— 框架级纯数据结构（ActorID/信封，game/cross 共用）
type ActorID uint64 // 高 32 位：类型（玩家/场景/全局），低 32 位：实例 ID

type MessageType uint8

const (
    Tell      MessageType = iota // 投递即忘（大多数业务消息）
    Call                        // 请求-响应（携带 Reply channel）
    Broadcast                   // 广播（按群组）
)

type Envelope struct {
    From    ActorID
    To      ActorID
    Type    MessageType
    MsgID   uint32
    Payload proto.Message
    Reply   chan *Envelope // Call 模式专用
}
```

```go
// pkg/actor/actor.go —— Actor 抽象（框架运行时，game/cross 共用）
type Actor interface {
    ID() ActorID
    OnMessage(env *Envelope)      // 由所属 Shard 单线程调用，禁止阻塞
    OnTimer(timerID uint64)
    OnStop()                      // 落库、清理、通知周边
}

type ActorSystem struct {
    shards []*Shard
    mask   uint64
    ids    *IDGenerator
}

// Tell/Call 并发安全：任何 goroutine 都可投递
func (s *ActorSystem) Tell(to ActorID, msg proto.Message)
func (s *ActorSystem) Call(to ActorID, msg proto.Message) (*Envelope, error)
func (s *ActorSystem) Spawn(a Actor) error
func (s *ActorSystem) Stop(id ActorID)
```

Shard 事件循环：

```go
func (sh *Shard) loop() {
    defer sh.recover()            // panic 不影响整个进程
    for {
        select {
        case env := <-sh.mailbox:
            if a, ok := sh.actors[env.To]; ok {
                a.OnMessage(env)  // 单线程顺序处理
            }
        case t := <-sh.timerCh:   // 时间轮到期的定时器转为消息
            if a, ok := sh.actors[t.actorID]; ok {
                a.OnTimer(t.id)
            }
        case <-sh.stopCh:
            sh.drainAndStop()     // 排空信箱，逐个停止 Actor（保证落库）
            return
        }
    }
}
```

设计要点：

1. **信箱**：基于 channel 的有界队列（默认 4096），写满时按策略处理（阻塞发送方 / 丢弃低频消息 / 告警）；
2. **禁止在 OnMessage 中阻塞**：IO 一律异步化——发起请求时携带「完成回调」，回调中以 `Tell` 形式把结果投回 Actor；
3. **定时器**：全局时间轮统一管理，到期后生成消息投递到对应 Shard，玩家下线时批量取消；
4. **Call 模式**：`Reply chan` 由调用方持有，注意设置超时，避免协程泄漏；
5. **背压与监控**：每个 Shard 暴露队列深度指标，深度持续上涨告警。

### 8.3 玩家 Actor 生命周期

```
创建（登录/重连）
   │
   ▼
① InitActor ── 分配 ActorID，注册进 Shard
   │
   ▼
② Preload ──── 登录预加载：并行拉取角色全部相关数据（Redis → miss 回源 MySQL），见 12.7
   │
   ▼
③ Bind ────── 与网关注册的 Session 绑定，写入 Redis online:{playerId}
   │
   ▼
④ Running ─── 处理业务消息（模块系统），markDirty → 分时落库
   │
   ▼
⑤ Unbind ──── 断连超时/顶号：与 Session 解绑
   │
   ▼
⑥ Flush ───── 强制落库（分片 UPSERT），清理 Redis 热数据（可延迟保留）
   │
   ▼
⑦ Stop ────── 从 Shard 注销，Actor 生命周期结束
```

```go
type PlayerActor struct {
    id       ActorID
    playerID int64
    state    *PlayerState          // 纯内存状态，只有本 Shard goroutine 能访问
    modules  map[string]Module     // create/login/recharge/level/bag/equip...
    dirty    *DirtySet             // 脏字段集合
    cache    *cache.Redis
    db       *db.MySQL
}

func (p *PlayerActor) OnMessage(env *Envelope) {
    switch m := env.Payload.(type) {
    case *pbv1.UseItemReq:
        p.handleUseItem(m, env)
    case *loadDoneMsg:             // 异步 IO 完成后的回投消息
        p.onLoadDone(m)
    }
}

// 异步 IO 的标准写法：发起时带回调，回调里 Tell 回自己
func (p *PlayerActor) loadDataAsync() {
    p.cache.HGetAllAsync(p.ctx, keygen.EntityKey("player", p.playerID),
        func(fields map[string]string, err error) {
            p.ctx.Tell(p.id, &loadDoneMsg{fields: fields, err: err})
        })
}
```

### 8.4 场景 Actor 与 AOI

- 每个场景（副本/大地图区块）一个 Scene Actor，管理实体进出、位置同步、广播；
- **九宫格 AOI**：实体只关注周围 9 格，进入/离开时维护关注集合并推送可见性变化；
- 跨场景交互（组队、世界 BOSS）通过 ActorSystem 的跨 Shard `Tell` 完成；
- 大规模同屏（100+ 人）时，可将单一场景拆为多个区域 Actor，按区域路由。

### 8.5 业务模块系统

```go
type Module interface {
    Name() string
    Init(a *PlayerActor)                       // 挂载时注入 Actor 引用
    OnMessage(env *Envelope) bool              // 返回 true 表示已消费
    OnSave(dirty *DirtySet)                    // 产出需要持久化的字段
    OnLogin()                                  // 登录事件（推送数据等）
    OnLogout()                                 // 下线事件
}
```

- 模块只通过 `PlayerActor` 暴露的受控接口访问状态与外部资源，**不允许直接持有 Redis/MySQL 连接**（统一走 Actor 的异步 IO 门面）；
- 新功能 = 新模块 + 新协议，玩家 Actor 与框架代码零修改。

### 8.6 集群设计

| 问题 | 方案 |
| --- | --- |
| 玩家如何分配到游戏服 | 登录时网关从注册中心选取（默认负载最低），玩家 ID 写入 Redis `online:{playerId}`；**一个玩家同一时刻只在一个游戏服进程** |
| 重连如何找到旧服 | 网关握手时读 Redis 在线状态，命中则直连旧游戏服，未命中则选新服 |
| 扩容 | 新游戏服注册 etcd 后，仅**新登录/未绑定玩家**分配过去；存量玩家不受影响 |
| 宕机 | 该服玩家集体掉线 → 重连时在线状态已失效 → 分配新服 → 从 Redis/MySQL 恢复 |
| 跨服交互 | 控制面（组队/公会战/匹配）经中心服中转；广播类（跨服聊天）经 Kafka 扇出；跨服战斗经 CrossService 流由主游戏服中转——游戏服之间不建直连 |

> **有状态扩容的本质**：以「玩家未登录」为扩容窗口。框架刻意不做运行时热迁移（复杂度高、收益低），而是通过「掉线重连 + 数据外置」达到准无缝效果。

### 8.7 跨节点 Actor 投递（P0）

多节点部署后，「玩家 Actor 在哪、消息怎么送到」由一套显式契约回答，完整设计见 [R2.4 跨节点 Actor 投递设计](R2.4-跨节点Actor投递设计.md)：

| 契约层 | 机制 | 要点 |
|---|---|---|
| 地址 | `PID.Node` | 空 = 本地（单机零感知）；非空 = 节点 ID（= `cfg.Server.Name`） |
| 出口 | `RemoteSender` 接口 + `System.SendWithSender` 路由决策 | 目标节点 ≠ 本节点才走远程；未注 Relay 属装配错误，显式失败 |
| 消息 | `RemoteMessage` 显式接口 + type_url 注册表 | 能否跨节点是编译期决定；重复注册 panic |
| 信封 | `RemoteEnvelope`（JSON：ActorID + TypeURL + Payload） | 中心服 relay payload 原样透传，解码侧对称还原 |
| 位置 | `player.Manager.Resolve` / 跨节点守卫的 `GetOrSpawn` | 本地表 → presence 归属表；**归属别节点时绝不本地 Spawn**（双 Actor = 双写分叉） |
| 发送 | `internal/game/remote.Sender` → 中心服 `SendRelay` | 复用 9.3 中转能力，零新 proto、零 etcd 发现 |
| 接收 | `centerHandler.OnRelayPush` → `DecodeRemoteEnvelope` → `DeliverRemote` | 尽力而为：解码失败 Warn、Actor 已下线 Debug，丢推送由客户端重连拉全量兜底 |

语义边界：**只有下行推送类消息（PushMsg/PacketMsg）可远程**；带 `Done channel` 的请求-响应消息（如 `GrantItemsMsg`）显式不可远程，走原 Kafka 重投/对账路径。与 `bindPlayer` 的 `TrySetOnline` 顶号守卫构成纵深——登录入口挡跨节点顶号，投递入口挡跨节点误 Spawn。

---

## 9. 中心服（CenterServer）设计

### 9.1 定位与职责

中心服是**游戏服集群的控制面枢纽**，四大职责：

| 职责 | 说明 |
| --- | --- |
| **节点管理** | 游戏服/跨服节点启动上报、心跳保活、负载画像、优雅退出摘除 |
| **消息中转** | 控制面跨服消息（组队邀请、公会战指令、匹配）经中心服中转；跨节点 Actor 投递（玩家不在本节点时的下行推送，见 8.7）经中心服 RelayRequest/RelayPush 中转；广播类消息（跨服聊天/邮件）经 Kafka 扇出不经中心服；跨服战斗数据面不经中心服（见第 10 章） |
| **全服玩法** | 全服排行榜、全服进度活动 |
| **分组统计** | 各节点功能上报，按分组（跨服分组）/时间窗聚合统计，供 GM/运营查询 |

中心服**不持有玩家业务数据**（玩家数据在游戏服），只持有聚合数据（排行榜、统计），因此可集群化 + 快速重建。**中心服不连接网关**（详见 2.3 连接矩阵）。

### 9.2 上报协议（Report 长连接）

协议定义见 4.6 节 `CenterService.Report`（bidi stream，一个节点一条流）。要点：

- **一条节点一条 Report 流**：既是上报通道也是指令通道（中转投递、排行榜通知、分组变更都走它下发）；
- **心跳 5s**，超时 15s 未心跳 → 标记节点不可用，通知各网关不再分配新会话，并通知其组内节点；
- **节点重连**：Report 流断开后节点指数退避重连（1s→2s→4s…→30s 封顶），重连后补发 Register + 全量状态（中心服侧按 node_id 恢复上下文）。

### 9.3 中转能力（GS ↔ GS 不直连的实现）

```
GameServer#3                         CenterServer                    GameServer#7
     │                                      │                              │
     │── RelayRequest{to:"game-07", ...} ───▶│                              │
     │   （组队邀请/公会战指令…）              │ 查路由表 node_id → stream      │
     │                                      │── RelayPush{from:"game-03"} ─▶│
     │                                      │                              │
```

- 路由表：`node_id → Report stream`，内存维护，节点上下线即时更新；
- **广播中转**：`RelayRequest{broadcast:group_id}` → 中心服向组内所有节点 RelayPush（全服广播、跨服活动开启通知）；
- **中心服集群下的跨实例中转**：节点 A 连中心服#1、节点 B 连中心服#2 时，中心服#1 把消息写入 Kafka Topic `center.relay`（Key=目标 node_id），中心服#2 消费后经本地 stream 下发——**节点无感知**；
- **可靠性**：中转消息带 `relay_id`，目标节点回 ACK；未 ACK 的重投（指数退避，最多 5 次）；中心服不持久化中转消息（丢消息由业务层幂等/重查兜底，中转本身尽力而为）；
- **不走中转的两类消息**：① 广播类（跨服聊天/邮件/事件流）经 Kafka 扇出 + 共享 Redis 定位，见 [13.3](#133-跨服消息投递模式重要)；② 跨服战斗数据面经 CrossService 流直连参战游戏服，见第 10 章。

### 9.4 全服排行榜

```
各 GameServer                          CenterServer                         Redis
   │  玩家分数变化（批量，30s 或阈值触发）   │                                    │
   │──── RankReport{type, player, score} ─▶│                                    │
   │                                      │ 聚合并写入 ZSet:                    │
   │                                      │  rank:global:{season}:{type}       │
   │                                      │  rank:group:{groupId}:{type}       │
   │                                      │──── ZADD / ZREVRANGE ─────────────▶│
   │                                      │                                    │
   │◀─── RankNotify（上榜/掉榜/赛季结算）────│                                    │
```

| 项 | 设计 |
| --- | --- |
| 数据源 | 游戏服批量上报（增量，`player_id + score + version`），中心服按 version 丢弃旧值 |
| 存储 | Redis ZSet（score=分数，member=`{playerId}:{serverId}`）；赛季快照落 MySQL `rank_history`（低频，走独立写入） |
| 查询 | Top-N / 我的排名 / 指定玩家排名，中心服直接 `ZREVRANGE`/`ZREVRANK`，O(logN) |
| 通知 | 上榜/掉榜/赛季结算 → `RankNotify` 经 Report 流下发玩家所在游戏服 → 推送客户端 |
| 分组 | 每个跨服分组独立一份排行榜；`global` 为全服总榜 |
| 赛季 | `season` 维度隔离，赛季切换时中心服归档旧 ZSet → MySQL，建新 ZSet |

### 9.5 功能上报与分组统计

```protobuf
message FeatureReport {
  string node_id   = 1;
  string group_id  = 2;
  string feature   = 3;  // 如 "arena_match" / "guild_war" / "economy_gold_out"
  map<string, int64> metrics = 4; // 计数/求和指标，如 {"matches":120,"players":86}
  int64  ts        = 5;
}
```

| 项 | 设计 |
| --- | --- |
| 上报频率 | 游戏服本地 1 分钟聚合一次，随心跳或独立消息上报（批量，单条最多 200 个 feature） |
| 聚合维度 | `group_id × feature × 时间窗`（1m / 5m / 1h / 1d 四档环形缓冲，内存计算） |
| 落地 | 每 5 分钟快照写 Redis Hash（`stats:{group}:{feature}:{window}`）+ 每小时写 MySQL `feature_stats` 表 |
| 用途 | GM/运营查询（`QueryFeatureStats`）；**全服进度活动**（如「全服打造」：中心服汇总各服贡献值，达阈值推送里程碑奖励，经 RelayPush 广播） |
| 集群 | 聚合在本实例内存，快照走 Redis（多实例共享），MySQL 按 `group_id % 10` 分片写 |

### 9.6 集群与选主

- **无主部分**：Report 接入、中转、排行榜写入——每个实例独立承载自己连接的节点，天然水平扩展；
- **有主部分**（etcd 选主，leader 执行）：全局活动调度（`gs.activity.schedule` 的生产者）、赛季结算归档、全服进度活动的里程碑判定；
- leader 故障 → etcd 租约过期 → 其余实例抢主 → 从 Redis/MySQL 恢复上下文（调度状态带持久化）。

### 9.7 优雅关闭

```
SIGTERM
 → ① etcd 摘除注册 + 通知组内节点「本节点即将下线」（经 RelayPush/KickNode）
 → ② 停止接受新 Report 连接；存量流发送「准备关闭」指令
 → ③ 排行榜/统计内存态快照落 Redis + MySQL（快照式，秒级完成）
 → ④ 关闭全部 Report 流（GoAway），节点重连到其它中心服实例
 → ⑤ 退出（总超时 30s）
```

节点侧配合：收到「准备关闭」后主动重连到其它中心服实例，避免被动等待。

### 9.8 中心服落地（P8 已实现）

**组件分工**：

| 组件 | 职责 | 代码位置 |
| --- | --- | --- |
| `registry.Registry` | 节点注册表：node_id → 节点信息 + Report 会话；心跳超时摘除 | `internal/center/registry/` |
| `rpc.Server` | `CenterService.Report` 实现：首帧 Register、帧分发、下行泵 | `internal/center/rpc/` |
| `match.Matcher` | 匹配池：elo 分桶、超时放宽、room_id/token 签发 | `internal/center/match/` |
| `relay.Router` | 中转路由：点对点 / 组播、指数退避重试、失败计数 | `internal/center/relay/` |
| `center.Server` | 组装 + 统一节拍循环（1s：心跳巡检 / 匹配 sweep / 中转重试） | `internal/center/server.go` |
| `game/center.Client` | 游戏服侧 Report 流：注册/心跳/匹配/中转上报 + 下行分发 | `internal/game/center/` |

**匹配路径（P8 主路径）**：

```
A/B 客户端 MatchReq(5002)
  → GS 校验（已在队列/战斗中则拒绝）
  → GS 上报 MatchRequest{node_id, player_id, mode, snapshot, elo}
  → CenterServer 按 (mode, elo/200) 分桶入队
  → 凑够 2 人：分配 room_id + 每人独立 battle_token
  → MatchResult 分发回各自 GS（含 player_id / peer_id / peer_snapshot / cross_addr）
  → GS 用中心服分配的 room_id 请求建房（CrossServer 幂等：首个真建，其余直接成功）
  → GS 送自己的快照（带中心服签发的 token）
  → GS 推 BattleStartNotify(5001) 给客户端
  → 后续战斗透传 / 结算走 P13 的 CrossService 路径
```

> **降级**：未配置 `center.addr` 时，battle 模块自动退回 P13 的「本服凑人直连跨服」路径，
> 保证单机可玩、集成测试不依赖中心服。

**room_id 分配**：`实例号(8b) << 40 | 秒级时间戳(32b) << 8 | 自增(8b)`。
跨实例天然不重号；同一 CrossServer 收到重复建房请求按 room_id 幂等，多个参战 GS 各自请求不会冲突。

**匹配超时放宽**：同桶等待超过 `timeout`（默认 10s）后，与任意等待同样久的其他桶玩家配对，
避免高段位永远配不上。由中心服 1s 节拍循环的 `Sweep()` 驱动。

**中转**：

| 场景 | 行为 |
| --- | --- |
| 点对点（`to_node`） | 查路由表 → RelayPush；失败进 pending，指数退避重试至多 `relay_retry` 次 |
| 组播（`broadcast + group_id`） | 投递给组内全部节点；**不重试**（避免故障时放大流量） |
| 全部送达 | 清理 pending（曾漏这一步导致已成功的消息被反复重投） |
| 集群跨实例 | 本地无目标 → 写 Kafka `center.relay`；对端实例消费后投本地节点（见 9.10） |

**GM 查询**：`QueryGlobalRank` / `QueryFeatureStats` 已暴露 gRPC 接口，排行榜与统计的
Redis/MySQL 落地属 P14，本阶段接口就绪、实现可插拔（`SetRankQuerier` / `SetStatsQuerier`）。

**优雅关闭**（9.7）：

```
KickNode 通知所有节点重连其它实例
  → rpcServer.StopWithTimeout(3s)
  → metrics 停止
```

> 同样必须用带超时的 `StopWithTimeout`：Report 是双向流，`GracefulStop()` 会一直等流结束
> （与 P13 踩过的坑同源，见 10.6）。

### 9.9 排行榜 / 统计 / 选主落地（P14）

**排行榜**（`internal/center/rank`）：

| 项 | 实现 |
| --- | --- |
| 存储 | Redis ZSet，`rank:global:{season}:{type}` / `rank:group:{gid}:{season}:{type}` |
| member | `{playerId}:{serverId}` —— serverId 用于定位玩家所在服，展示名由客户端补 |
| 写入 | `ZINCRBY`（**增量语义**）：`RankReport.score` 是 delta，不是绝对值 |
| 查询 | `ZREVRANGE`（Top-N）/ `ZREVRANK`（我的名次）/ `ZRANGE`（周边） |
| 赛季 | `season` 作为 key 分量天然隔离；切换时 leader 归档到 `rank_history` 再删 key |
| 定位 my member | `ZSCAN` 前缀匹配（大榜单不能用 `ZRANGE` 全量扫） |

> **为什么用 ZINCRBY 而非 ZADD**：竞技场是「胜 +30 / 负 -10」，上报的是增减量。
> 用 ZADD 覆盖会把「负 -10」当成绝对值 -10，语义完全错。绝对值场景（如公会战力）
> 需要额外的 `absolute` 标志位，本阶段未引入。

**结算 → 上榜链路**：

```
CrossServer 结算回推 BattleResult
  → GS battle.OnBattleResult
  → 按 result 折算 delta（胜 +30 / 平 0 / 负 -10）
  → 经 Report 流上报 RankReport
  → 中心服 ZINCRBY
  → GM 查询 QueryGlobalRank 立即可见
```

**统计聚合**（`internal/center/stats`）：

| 项 | 实现 |
| --- | --- |
| 聚合 | 内存四档环形桶（1m/5m/1h/1d），写入 O(1) |
| 快照 | 每 5 分钟写 Redis Hash `stats:{group}:{feature}`，field 为窗口名 |
| 查询 | `QueryFeatureStats` 按 group/feature 过滤，返回 1h 窗口累计 |
| 为什么不用 Redis 计数 | 每条上报走 Redis 会增加往返；内存聚合后批量快照，成本从 O(N) 降到 O(1) |

**选主**（`internal/center/leader`）：

- 基于 etcd `concurrency.Election`（租约 + 前缀比较），TTL 默认 10s；
- leader 失联 → 租约过期 → 其余实例自动抢主，回调 `OnChange(bool)` 通知上层；
- 有主任务（赛季归档、活动调度）读 `IsLeader()` 决定是否执行；
- 优雅关闭时 `Resign()` 主动让位，其他实例立即接管而非等租约过期；
- 指标：`gs_center_is_leader`（1/0）。

**本阶段未做**：
- `RankNotify` 下发客户端（协议未定义，待客户端契约补齐）；
- `rank_history` / `feature_stats` 的 MySQL 归档（P15 定时任务）。

### 9.10 跨实例中转落地（P9）

**问题**：中心服多实例时，节点 A 连 #1、节点 B 连 #2。#1 的 Report 流上看不到 B，
`resolveTargets` 返回空，消息就丢了。

**链路**：

```
节点 A ──RelayRequest{to:game-b}──▶ 中心服#1
                                     │ 本地无 game-b
                                     ▼
                             Kafka center.relay  (Key=node:game-b)
                                     │ 每个实例独立消费组（广播语义）
                    ┌────────────────┴────────────────┐
                    ▼                                 ▼
              中心服#1（跳过）                  中心服#2
              origin == self                    deliverLocal(game-b) → RelayPush → B
```

**关键设计**：

| 点 | 做法 | 原因 |
| --- | --- | --- |
| 消费组 | `center.relay.{instanceID}`，每实例独立 | 广播语义：所有实例都要看到，各自投自己承载的节点 |
| 自发自收 | 消息带 `origin_instance`，消费时比对跳过 | 同一实例已经在本地投递过，不跳过就重复 |
| 实例标识 | `服务名@hostname` | StatefulSet Pod 名稳定，重启后消费组不变，可续上 offset |
| 起始位点 | `last`（生产）/ `first`（测试） | 中转尽力而为，重启期间的消息不补；从 first 会重放历史 |
| 分区键 | `node:{to}` / `group:{gid}` | 同一目标的消息落同一分区，保证有序 |
| 重试 | 跨实例路径**不重试** | Kafka 已 ack；重复投递比丢失更糟（9.3 尽力而为） |
| 广播 | 本地投 + 写 Kafka | 本实例投自己承载的节点，其余实例各投各的，每个节点只收一次 |

**指标**：`gs_kafka_produced_total{topic="center.relay"}` / `gs_kafka_consumed_total{topic="center.relay",result=ok|skip}`。
`skip` 包含「自发自收」与「目标不归本实例」，属正常路径，不应告警。

**未做**：中转消息的持久化与 ACK 重投（9.3 明确为尽力而为，业务侧靠幂等/重查兜底）。

---

## 10. 小组跨服（CrossServer）设计

### 10.1 概念

- **跨服分组（Battlegroup）**：中心服把若干游戏服（默认 8 个）划为一组，组内玩家可一起玩跨服玩法；
- **小组跨服节点（CrossServer）**：承载跨服玩法的专用进程（跨服竞技场、跨服公会战、跨服副本），一个分组可挂多个 CrossServer；
- **CrossServer 不与网关交互**：客户端会话始终挂在主游戏服上，跨服战斗消息由主游戏服经 CrossService 流中转；
- CrossServer 复用框架基础设施（`pkg/actor` 运行时、gRPC Server、`pkg/flush`），**不注册 etcd、不连网关**，只上报中心服。

### 10.2 分组管理

```
中心服 GroupManager：
  - group-01: [game-01 ~ game-08] + cross_addrs:[cross-01:9300, cross-02:9300]
  - group-02: [game-09 ~ game-16] + cross_addrs:[cross-03:9300]
  - 分组规则：按开服时间/容量均衡划分；新游戏服上线 → 上报中心服 → 分配组
            → GroupUpdate 广播组内（携带组内 CrossServer 地址，见 4.6 节协议）
  - 游戏服收到 GroupUpdate → 与组内每个 CrossServer 建立 CrossService 流（断流指数退避重连）
  - 游戏服掉线 → 中心服标记，组内广播，匹配时剔除
  - CrossServer 上线上报 → 中心服纳入可用资源池（按负载分配战斗）
```

### 10.3 跨服玩法全流程（以跨服竞技场为例）

```
玩家A          GS#3(主游戏服)      CenterServer       CrossServer#1
 │               │                    │                │
 │─①匹配请求────▶│                    │                │
 │               │─② MatchRequest ───▶│                │
 │               │  {player快照,elo}   │ 选组内 CrossServer │
 │               │◀─③ MatchResult ────│                │
 │               │  {room_id,token}   │                │
 │               │                    │─④ MatchAssign ─▶│ 建房间+BattleActor
 │               │─⑤ PlayerSnapshot(token,快照)────────▶│ 校验token，玩家入房
 │◀────────────── ⑥「进入跨服战斗」 ──│                │
 │               │                    │                │
 │─⑦战斗消息────▶│─⑧ BattleMsg ──────────────────────▶│ 房间Actor透传
 │◀────────────── 战斗同步/结算 ◀────│◀─ BattleMsg ───│
 │               │◀─⑨ BattleResult ───────────────────│
 │               │─ 应用奖励到玩家Actor │                │
 │◀────────────── ⑩「战斗结束」 ──────│                │
```

流程说明：

1. **匹配在中心服**：GS 只上报请求与玩家战斗快照（战力、阵容、段位），中心服按组内资源匹配对手/房间；
2. **建房在 CrossServer**：中心服向选中的 CrossServer 下发 `MatchAssign`（房间 + 准入令牌），CrossServer 预创建房间与 BattleActor；
3. **快照由游戏服直送 CrossServer**：GS 收到 MatchResult 后，经已建立的 CrossService 流把玩家快照送入房间；CrossServer 校验令牌后玩家入房；
4. **会话不迁移**：玩家客户端连接始终在网关↔GS#3，战斗消息由 GS#3 中转——多一跳同机房内网延迟（<1ms），换取零会话迁移复杂度与网关零改动；
5. **战斗中主游戏服冻结玩家 Actor**：状态置 `IN_CROSS_SERVER`，期间到达的消息（邮件、好友、公会）入待投递队列，战斗结束后补发；
6. **结算回主游戏服**：demo 期客户端把战斗结论经 `BattleResultReport` 上报所在 GS，GS 校验令牌 + 幂等后应用奖励并回 `BattleSettleNotify`；跨服兜底（超时平局）仍由 CrossServer 经 CrossService 流把 `BattleResult` 推给各参战玩家所在 GS；中心服经 FeatureReport 拿统计数据，**不参与结算转发**；
7. **战斗中掉线**：demo 期无服务端战斗进度可依——客户端已上报结果则按结果结算，否则房间超时后按平局兜底（见 4.8），经 CrossService 流推送 GS 应用；
8. **Demo 期战斗职责划分**：战斗演算（技能/伤害/胜负）由**客户端**负责，服务端只做三件事——通知发起（`BattleStartNotify`）、消息透传（`BattleMsg` 原样转发不解析）、接收结果（`BattleResultReport` → 令牌校验 + 幂等 + 应用奖励）；防作弊验证后置（见 [4.9.3](#493-战斗职责划分demo-约定)）。

### 10.4 备选方案与权衡

| 方案 | 路径 | 评价 |
| --- | --- | --- |
| **A（采用）** | 客户端→网关→GS→CrossServer（GS 中转战斗消息） | 网关零改动、无会话迁移、跨服节点不暴露给客户端链路；代价是每战斗消息多一跳内网延迟 |
| B（废弃） | CrossServer 直连网关 + 会话迁移（MigrateSession） | 少一跳，但网关需支持会话迁移、跨服节点成为客户端链路单点，复杂度与故障面大增 |

### 10.5 CrossServer 生命周期

- 启动：上报中心服（NodeRegister，声明类型 `cross`、容量）→ 等待中心服分配房间；**不注册 etcd、不连网关**；
- 运行：组内游戏服主动与其建立 CrossService 流；战斗房间 Actor 管理（建房→战斗→结算→销毁），房间数据纯内存 + Redis（不落 MySQL，仅结算结果回参战游戏服）；
- 优雅关闭：不接受新匹配 → 等待存量战斗结束（最多 5 分钟，超时强制结算）→ 上报中心服下线 → 退出。

### 10.6 跨服战斗落地（P13 已实现）

**组件分工**：

| 组件 | 职责 | 代码位置 |
| --- | --- | --- |
| `cross/battle.Room` | 房间 Actor：入房校验、满员开战、消息扇出、结算 | `internal/cross/battle/room.go` |
| `cross/battle.Manager` | 建房/摘除/优雅 drain，roomID → PID 索引 | `internal/cross/battle/manager.go` |
| `cross/rpc.Server` | CrossService.Channel：BIND 握手、帧分发、精确回推 | `internal/cross/rpc/cross_server.go` |
| `game/cross.Pool` | 游戏服侧连接池：每节点一条流、断流退避重连 | `internal/game/cross/pool.go` |
| `game/module/battle` | 匹配队列、开战通知、操作透传、结算上报 | `internal/game/module/battle/battle.go` |

**流协议首帧约定**：`CrossCtrl{BIND, node_id}` 必须是首帧。CrossServer 据此把连接绑定到具体游戏服，
结算时**精确回推到该节点**——不做全网广播（否则玩家 A 的结算会发给 B 所在的所有游戏服）。

**准入令牌**：CrossServer 用进程内随机密钥对 `HMAC(nodeID|roomID|playerID)` 签名，入房与结算各校验一次。
密钥不落盘：房间纯内存，重启后房间与令牌同时失效，天然一致。

**结算双路径**（对应 4.9.3）：

| 路径 | 触发 | 权威方 |
| --- | --- | --- |
| 客户端上报 | `BattleResultReport(5010)` → GS 校验令牌+幂等 → 转发跨服 | 双方都上报后 CrossServer 结算回推 |
| 超时兜底 | 房间 `timeout_sec` 到期 | CrossServer 按平局（result=2）强制结算 |

**匹配路径**：P13 阶段由游戏服本地凑人后直接向 CrossServer 申请建房（`RoomCreateReq`），
便于不依赖中心服独立验证；P8 中心服上线后改为 `MatchRequest → MatchResult → MatchAssign`，
游戏服只负责送快照，`RoomCreateReq` 保留为降级路径。

**已验证的关闭顺序**（踩过的坑）：

```
Drain（拒新房 + 存量兜底结算）
  → CloseAllPeers（主动断所有游戏服流）
  → rpcServer.StopWithTimeout(2s)
  → actorSys.Stop()
  → metricsSrv.Stop()
```

> **必须主动断流**：gRPC 的 `GracefulStop()` 会等所有活跃 RPC 自然结束，而 `CrossService.Channel`
> 这种双向流在客户端不主动断开时永远不会结束。曾导致跨服节点优雅关闭卡 5 分 28 秒
> （只能等 K8s `terminationGracePeriodSeconds` 到期被 SIGKILL）。
> 同类问题也存在于 `GameService.Channel`，因此 `pkg/rpc.Server.Stop()` 已统一改为
> 「有限优雅期 + 超时强制关闭」。

---

## 11. 数据一致性分级

> **除充值、角色等比较重要的数据严格保证一致性外，其他数据一律以 Actor 内存态和 Redis 为准。**

### 11.1 分级定义

| 级别 | 数据范围 | 权威源 | 一致性模型 |
| --- | --- | --- | --- |
| **Tier-1 严格一致** | 账号、角色创建、充值订单与发货、货币类充值获取 | **MySQL** | 同步事务 + 幂等 + 对账，强一致 |
| **Tier-2 内存+Redis 为准** | 背包、装备、技能、属性、任务、好友、邮件、组队、副本记录、商店记录、活动进度、伙伴、公会等一切玩法数据 | **Actor 内存 + Redis** | 最终一致，秒级落库延迟可接受 |

### 11.2 Tier-1 严格一致（充值 / 角色）

**角色创建（同步事务 + 三写一致）**：

```go
func (s *CreateService) CreateCharacter(ctx context.Context, accountID int64, req *CreateReq) (*Player, error) {
    // ① MySQL 事务：player + player_attr + 初始背包，原子提交
    tx := db.Begin()
    pid, err := tx.Insert("player", row)
    tx.Insert("player_attr", attrRow)
    tx.Insert("player_item", initItems(pid))
    if err := tx.Commit(); err != nil { return nil, err }

    // ② Redis ID 表同步登记（先库后缓存，保证「idtab 有 ⇒ 库一定有」）
    rdb.SAdd(ctx, "idtab:player", pid)
    rdb.SAdd(ctx, "idtab:player_attr", pid)

    // ③ Redis 热数据写入（此后该角色按 Tier-2 走内存+Redis 路径）
    rdb.HSet(ctx, entityKey("player", pid), row.Fields())
    rdb.HSet(ctx, entityKey("player_attr", pid), attrRow.Fields())
    return player, nil
}
```

**充值（订单状态机 + Kafka 幂等发货）**：

```
① 登录服创建订单：MySQL INSERT（uk_order_id 唯一索引兜底重复回调）+ 状态=待支付
② 支付回调：MySQL 事务推进状态（待支付→已支付，CAS 更新防并发）
③ 发 Kafka gs.recharge.order（Key=order_id，分区有序）
④ 游戏服 recharge 模块消费（广播消费 + 本地性判定，见 13.3）：
     a. 查 Redis online:{playerId}：玩家在本服在线 → 继续；否则丢弃（由对账补偿）
     b. Redis SETNX idem:gs.recharge.order:{order_id} 去重（24h）
     c. 应用奖励 → 发 Kafka gs.recharge.delivered（Key=order_id）→ 登录服消费更新订单状态=已发货
        （订单表在登录服库，游戏服不跨库写，见 13.3 模式③）
⑤ 对账与兜底：每小时对账任务扫「已支付未发货」订单重投 Kafka；**玩家登录时登录服也主动检查一次**（`/v1/server/assign` 链路顺带查询该账号未发货订单，有则立即重投，不等对账）——付费到账对延迟敏感，登录即时兜底 + 小时级对账双保险
```

规则：**Tier-1 的写永远先落 MySQL 成功，再更新 Redis**；读取时 Tier-1 以 MySQL 为准（Redis 仅作加速，且带版本号校验）。

### 11.3 Tier-2 内存 + Redis 为准

- **写**：Actor 处理消息 → 改内存态 → `markDirty`（写 Redis hash + 入分片脏队列）→ 异步落库（第 12.4 节）；
- **读**：Actor 内存态优先；未加载实体 → Redis hash → miss 且 idtab 命中 → 回源 MySQL（第 12.5 节）；
- **进程重启**：玩家数据从 Redis 恢复（hash 完整）；Redis 故障 → MySQL 恢复（丢失 ≤1 个落库周期的变更，敏感玩法用 Tier-1 或同步双写兜底）。

### 11.4 读写路径总表

| 操作 | Tier-1（角色/充值） | Tier-2（玩法数据） |
| --- | --- | --- |
| 创建 | MySQL 事务 → idtab SADD → Redis 写 | 内存+Redis 写 → 异步落库 |
| 读取 | idtab 判定 → MySQL（Redis 加速） | 内存 → Redis →（idtab 命中）回源 MySQL |
| 更新 | MySQL 同步更新 → 失效/更新 Redis | 内存+Redis 更新 → 分时落库 |
| 删除 | MySQL 删除 → idtab SREM → Redis DEL | 内存删除 + tombstone → 分时落库执行 DELETE |
| 崩溃恢复 | MySQL 权威，零丢失 | Redis 优先恢复，MySQL 兜底（≤1 周期延迟） |

---

---

## 12. 数据层设计

### 12.1 分层存储策略

```
                    ┌─────────────────────────────────────────────┐
                    │      玩家 Actor（内存权威 / 读写热点）        │
                    │  单线程无锁；业务读写全在内存完成，0 次网络    │
                    └──────┬──────────────────────▲───────────────┘
                           │ 尽力同步副本          │ 登录加载
                           ▼                      │
        ┌──────────────────────────────────────────┴──────────┐
        │      Redis（副本：跨进程读 + 崩溃恢复，不是权威）      │
        │  热数据 hash ｜ ID 表 ｜ 在线状态 ｜ 排行榜            │
        └──────────────────────────────────────────────────────┘

                    Actor 内存 ──标脏──▶ 本进程 flusher
                                       （分片 + 错峰 + 合批 UPSERT）
                                            │
                                            ▼
        ┌──────────────────────────────────────────────┐
        │      MySQL（持久化权威 / Tier-1 数据源）       │
        └──────────────────────────────────────────────┘
```

| 策略 | 说明 |
| --- | --- |
| **Cache-Aside（读）** | 登录时：Redis 副本命中且版本不落后于库 → 用副本；否则回源 MySQL 重建并回填。副本落后判定用 `player_writebehind.version`（同步落库会推进它） |
| **Write-Behind（写，B 类）** | Actor 改内存（**权威**）→ 尽力同步 Redis 副本（`Store.Sync`，**不登记脏队列**）+ 标脏 → 本进程 flusher 分片合批刷 MySQL。副本同步失败只告警，不影响业务 |
| **写穿透（Tier-1，A 类）** | 先 MySQL 事务成功 → 再改内存与 Redis。代码见 12.4「同步落库」，入口是 `GrantItemsMsg.Sync` |
| **下线/定时 flush** | 玩家下线全量 flush；Actor 侧 30s 兜底周期；跨天重置等定时任务走 scheduler |

**权威归属**：B 类数据以 **Actor 内存**为准，A 类（充值/交易）以 **MySQL** 为准。
Redis 在任何一类里都不是权威——它只负责「让别的进程看得见」和「进程崩了能恢复」，
因此 `Store.Sync` 刻意不把玩家放进落库队列：那会引来 writebehind worker 用可能
落后的副本，和以内存为源的 flusher 争写同一批表。

> 两条写入路径的完整时序、Redis 三类表（数据表 / ID 表 / 映射表）的 key 设计、
> 启动加载与懒加载规则、增删改要动几张表，见独立文档《缓存架构设计》
> （`docs/缓存架构设计.md`）。

### 12.2 Redis 设计

Key 规范统一为 `业务:子业务:id`：

| Key | 类型 | 内容 | 维护方 |
| --- | --- | --- | --- |
| `token:{token值}` | String | 玩家 ID + 过期时间 | 登录服签发/校验 |
| `online:{playerId}` | Hash | `{gateway_id, session_id, game_server_id}` | 游戏服 Bind/Unbind |
| `gateway:load:{gatewayId}` | String | 连接数 | 网关定时上报 |
| `rank:{type}` / `rank:global:{season}:{type}` / `rank:group:{groupId}:{type}` | ZSet | 排行榜 | 中心服 |
| `idtab:{table}` | Set | 该表全部现存主键 ID | 启动构建 + 创建/删除同步 |
| `idtab:{table}:ready` | String | `"1"` = 构建完成 | 启动构建完成后置位 |
| `e:{table}:{id}` | Hash | 实体热数据（全字段） | Actor 写 / 回源回填 |
| `idx:{table}:{ownerId}` | Set | 一对多属主索引（如玩家的道具列表） | 创建/删除时同步 |
| `dirty:{table}:{id}` | Set | 脏字段明细 | markDirty |
| `dirtyq:{shard}` / `dirtyq:{shard}:proc` | Set | 分片脏队列 / 处理中 | flusher |
| `delq:{shard}` | Set | 待物理删除队列 | flusher |
| `idem:{topic}:{biz_id}` | String | 幂等标记（24h） | Kafka 消费端 |
| `lock:{resource}` | String | 分布式锁（SET NX + 看门狗续期） | 业务按需 |
| `stats:{group}:{feature}:{window}` | Hash | 分组统计快照 | 中心服 |
| `chat:hist:{playerId}:{peerId}` | List | 双端一致的私聊历史（发送时双写， capped 200 条/对） | chat 模块 |
| `pend:{biz}:{playerId}` | List | 离线暂存队列（邮件/好友申请/跨服消息补投），登录预加载时 drain | 各生产/消费模块 |
| `idem:chat:{msg_id}` | String | 聊天消息幂等标记（24h） | chat 模块 |
| `mail:unread:{playerId}` | List | 未读邮件 ID 列表（→ `e:mail:{mailId}`） | mail 模块 |
| `guild:player:{playerId}` | String | 玩家所属公会 ID（→ `e:guild:{guildId}`） | guild 模块 |

> 上表是**框架级核心 key**；业务模块级 key（`e:team:{tid}`、`shop:refresh:{pid}`、`activity:state:{id}`、`e:copy:instance:{id}` 等）按功能登记在 [17.1](#171-功能总览21-项) 的 Redis 列，新增模块时同步登记，保持「一个 key 只有一个登记处」的原则。

连接池（go-redis）：`PoolSize = 10 × NumCPU`，`MinIdleConns ≤ PoolSize`（原 `MinIdleConns = 100` 会超过 8C 机器的 PoolSize 80，属无效配置，已修正），读写超时 3s，Cluster 模式按 slot 自动路由。

### 12.3 MySQL 设计

**表清单**（`sql/schema/`，DDL 手写 → gendb 生成 CRUD，见第 16 章）：

| 表 | 分表键 | 归属服务 |
| --- | --- | --- |
| `account` | — | 登录服 |
| `player` | `id % 512` | 游戏服 |
| `player_attr` | `player_id % 512` | 游戏服 |
| `player_item` | `player_id % 512` | 游戏服 |
| `player_equip` | `player_id % 512` | 游戏服 |
| `player_skill` | `player_id % 512` | 游戏服 |
| `player_partner` | `player_id % 512` | 游戏服 |
| `player_quest` | `player_id % 512` | 游戏服 |
| `guild`、`guild_member` | `guild_id % 64` | 游戏服 |
| `mail`、`mail_attachment` | `player_id % 512` | 游戏服 |
| `team`、`team_member` | —（短期数据，可只存 Redis） | 游戏服 |
| `copy_record` | `player_id % 512` | 游戏服 |
| `shop_record` | `player_id % 512` | 游戏服 |
| `activity_record` | `player_id % 512` | 游戏服 |
| `friend`、`friend_apply` | `player_id % 512` | 游戏服 |
| `chat_record` | `player_id % 512` | 游戏服 |
| `recharge_order`、`recharge_record` | `player_id % 512` | 登录服 |
| `login_log` | 按月 | 登录服 |
| `rank_history`、`feature_stats` | `group_id % 10` | 中心服 |

**示例：玩家表 DDL**：

```sql
-- sql/schema/game.sql（player 表）
CREATE TABLE `player` (
  `id`         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '玩家ID',
  `account_id` BIGINT UNSIGNED NOT NULL COMMENT '账号ID',
  `name`       VARCHAR(32)  NOT NULL DEFAULT '' COMMENT '昵称',
  `level`      INT          NOT NULL DEFAULT 1  COMMENT '等级',
  `exp`        BIGINT       NOT NULL DEFAULT 0  COMMENT '经验',
  `coin`       BIGINT       NOT NULL DEFAULT 0  COMMENT '金币',
  `scene_id`   INT          NOT NULL DEFAULT 0  COMMENT '所在场景',
  `created_at` TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_account_id` (`account_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='玩家表';
```

**示例：充值订单表（幂等设计，Tier-1）**：

```sql
-- sql/schema/game.sql（recharge_order 表）
CREATE TABLE `recharge_order` (
  `id`          BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增ID',
  `order_id`    VARCHAR(64)  NOT NULL COMMENT '业务订单号(全局唯一,幂等键)',
  `player_id`   BIGINT UNSIGNED NOT NULL COMMENT '玩家ID',
  `product_id`  INT          NOT NULL COMMENT '商品档位(见 recharge_product.xml)',
  `amount_fen`  INT          NOT NULL COMMENT '金额(分)',
  `channel`     TINYINT      NOT NULL COMMENT '支付渠道',
  `status`      TINYINT      NOT NULL DEFAULT 0 COMMENT '0待支付 1已支付 2已发货 3已关闭',
  `paid_at`     TIMESTAMP    NULL DEFAULT NULL,
  `delivered_at` TIMESTAMP   NULL DEFAULT NULL,
  `created_at` TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_order_id` (`order_id`),          -- 幂等：重复回调直接冲突忽略
  KEY `idx_player_status` (`player_id`, `status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='充值订单表';
```

**示例：聊天记录表（双端各存一份，见 17.3）**：

```sql
-- sql/schema/game.sql（chat_record 表）
CREATE TABLE `chat_record` (
  `id`         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增ID',
  `owner_id`   BIGINT UNSIGNED NOT NULL COMMENT '归属玩家(历史查询方)',
  `peer_id`    BIGINT UNSIGNED NOT NULL COMMENT '对方玩家',
  `msg_id`     BIGINT UNSIGNED NOT NULL COMMENT '全局消息ID(幂等键)',
  `direction`  TINYINT      NOT NULL COMMENT '0 发出 1 收到',
  `channel`    TINYINT      NOT NULL DEFAULT 1 COMMENT '1 私聊',
  `content`    VARBINARY(1024) NOT NULL COMMENT '消息内容(业务序列化)',
  `created_at` TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_owner_msg` (`owner_id`, `msg_id`),   -- 幂等：双端落库不重复
  KEY `idx_owner_peer` (`owner_id`, `peer_id`, `id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='聊天记录表(每服从本服玩家视角各存一份)';
```

**落库通道选择**：

| 数据 | 通道 | 原因 |
| --- | --- | --- |
| 玩家常规数据（背包/属性/装备…） | Actor Write-Behind → flusher 批量 UPSERT | 高频写，批量合并 |
| 充值发货 | Kafka → 幂等消费 → 单行 UPSERT | 强可靠，不允许丢 |
| 登录流水 | Kafka `gs.login.log` → 消费落库 | 削峰 |
| 公会/队伍等聚合数据 | 直接 Model CRUD（低频） | 无批量必要 |

---

### 12.4 数据落库架构（10 分片落库 worker）

> **现状注记**：本节描述 Redis 脏队列 → writebehind worker 的落库通道。
> 默认部署已改为**内存权威**（见 12.1）：玩家状态由本进程 flusher 从 Actor
> 内存落库，`Store.Sync` 刻意不入队，因此这条队列通常是空的。本节内容仍然
> 有效且保留——它是 `pkg/writebehind` 的兼容路径（`Store.Commit` 入队时
> 启用），分片/错峰/合批/版本栅栏的设计在两条通道里是共用的。

#### 12.4.1 总体流水线

```
① Actor 更新实体（Tier-2）
   │  HSET  e:{table}:{id}  {fields...}        （Redis 热数据，与内存态一致）
   │  SADD  dirty:{table}:{id}  {fields...}    （脏字段明细，TTL 10min）
   │  SADD  dirtyq:{shard}  "{table}:{id}"     （入分片脏队列）
   ▼
② Redis（热数据 + 脏队列）
   │  shard = crc32("e:{table}:{id}") % 10     （Redis key 再哈希 → 落库分片）
   ▼
③ 10 个分片落库 worker（goroutine，共享业务连接池，不独占连接）
   │  第 i 片在「每分钟的第 i×6 秒」唤醒（分散轮询，10 片错开）
   │  a. RENAME dirtyq:{i} → dirtyq:{i}:proc   （原子取出，处理期新标记不丢）
   │  b. 按表分组 → Pipeline 读脏字段 → 组装
   │  c. 合批多行 UPSERT（ON DUPLICATE KEY UPDATE，≤500 行/条）
   │  d. 全部成功 → DEL proc 集合；失败 → 重试/重试队列
   ▼
④ MySQL（10 worker 错峰写入，负载在整分钟内均匀分散）
```

#### 12.4.2 分片哈希（Redis → 落库分片）

```go
// pkg/cache/shard.go
const DBShardCount = 10

// ShardOf 由 Redis 实体 key 哈希到落库分片（决定由哪个 worker 处理）
func ShardOf(entityKey string) int {
    return int(crc32.ChecksumIEEE([]byte(entityKey))) % DBShardCount
}
// entityKey 形如 "e:player_item:10001"，内嵌主键 ID，
// 同一实体的脏数据必然落入同一分片 → 单实体写入有序、不跨 worker 竞争
```

#### 12.4.3 分散轮询（每分钟错峰）

```go
// pkg/flush/flusher.go
type ShardFlusher struct {
    shard  int
    db     *sql.DB        // 共享业务池（*sql.DB 并发安全），不独占连接
    offset time.Duration  // shard * 6s
}

func (f *ShardFlusher) Run(ctx context.Context) {
    for {
        // 对齐到下一分钟 + 本分片偏移（第 0 片 :00，第 1 片 :06 … 第 9 片 :54）
        next := nextMinuteBoundary().Add(f.offset)
        select {
        case <-time.After(time.Until(next)):
        case <-ctx.Done():
            return
        }
        f.FlushOnce(ctx) // 单次失败不影响下一周期
    }
}
```

| 分片 | 唤醒时刻（每分钟） |
| --- | --- |
| shard 0 | :00 |
| shard 1 | :06 |
| … | 每片间隔 6s，10 片均匀铺满 60s |
| shard 9 | :54 |

#### 12.4.4 合批 UPSERT

```go
func (f *ShardFlusher) FlushOnce(ctx context.Context) error {
    // a. 原子取出本分片脏实体（处理期间新 SADD 落入新建的 dirtyq:{i}，下周期处理，不丢标记）
    procKey := fmt.Sprintf("dirtyq:%d:proc", f.shard)
    f.rdb.Rename(ctx, fmt.Sprintf("dirtyq:%d", f.shard), procKey)
    members, _ := f.rdb.SMembers(ctx, procKey).Result() // "table:id" 列表
    if len(members) == 0 { return nil }

    // b. 按表分组，批量读脏字段明细 + 实体数据（Pipeline）
    byTable := groupByTable(members)
    for table, ids := range byTable {
        rows := f.loadDirtyRows(ctx, table, ids) // 读 dirty:{table}:{id} 字段集 → HMGET
        // c. 合批：每 500 行一条多行 UPSERT
        for _, batch := range chunk(rows, 500) {
            sql := buildBatchUpsert(table, batch) // INSERT ... VALUES (..),(..) ON DUPLICATE KEY UPDATE c=VALUES(c)...
            if _, err := f.db.ExecContext(ctx, sql); err != nil {
                return f.onFlushError(ctx, procKey, members, err)
            }
        }
    }
    // d. 全部成功才清理
    f.rdb.Del(ctx, procKey)
    return nil
}

func (f *ShardFlusher) onFlushError(ctx context.Context, procKey string, members []string, err error) error {
    // 保留 proc 集合（下周期重试）；连续失败 3 次 → 拆出重试队列 + 告警
    // 极端情况（MySQL 长时间不可用）：proc 集合设 TTL 24h 防内存泄漏，数据仍可从实体 hash 重建
    log.Error("flush shard failed", "shard", f.shard, "err", err)
    metrics.Inc("flush_error", "shard", f.shard)
    return err
}
```

**合批效果估算**：

| 场景 | 单分片脏实体数/周期 | 合批后语句数 | 说明 |
| --- | --- | --- | --- |
| 5K 在线玩家，60s 落库周期 | ~3000 | ~6 条（每表 500 行/条） | 10 片分散，MySQL 端每秒 ~1 条批量 UPSERT |
| 高峰 5 倍 | ~15000 | ~30 条 | 仍远低于逐行写的 15000 条 |

#### 12.4.5 删除与 tombstone

- 硬删除：Actor 内存删除 → `SREM idtab:{table} {id}` + `SADD delq:{shard} "{table}:{id}"` + `DEL e:{table}:{id}`；
- flusher 每周期优先处理 `delq`，执行 `DELETE FROM {table} WHERE id=?`（合批 `DELETE ... WHERE id IN (...)`）；
- 软删除表（带 `deleted_at`）按普通更新走 UPSERT，不做物理删除。

#### 12.4.6 落库参数（game.yaml）

```yaml
flush:
  shard_count: 10            # 分片数 = 落库 worker 数（共享业务连接池，非独占连接）
  interval: 60s              # 每分片轮询周期
  stagger: 6s                # 分片间错峰 = interval / shard_count
  batch_rows: 500            # 单条多行 UPSERT 行数上限
  batch_bytes: 4194304       # 单条 SQL 大小上限 4MB
  dirty_ttl: 10m             # 脏字段集合 TTL（异常兜底）
  retry_queue: true          # 连续失败进重试队列 + 告警
```

#### 12.4.7 落地实现（P11）

**错峰对齐**（`pkg/flush/flusher.go`）：

唤醒时刻改为「下一分钟边界 + 分片偏移」，而不是「启动后每 interval 一次」。
后者在滚动发布时各 Pod 相位被打散，10 个分片可能挤在同一秒；对齐后无论何时重启，
始终是 `:00 / :06 / :18 … :54` 均匀铺开。

**合批 SQL 构建器**（`pkg/flush/batch.go`）：

| 函数 | 产物 |
| --- | --- |
| `BuildBatchUpsert` | `INSERT INTO t (..) VALUES (..),(..) ON DUPLICATE KEY UPDATE ..` |
| `BuildBatchDelete` | `DELETE FROM t WHERE pk IN (?,?,?)` |
| `BuildBatchDeleteWhere` | `DELETE FROM t WHERE <固定条件> AND pk IN (..)` |
| `Chunk` | 同时按**行数**与**字节预算**切批 |

> 只按行数切批不够：行短时语句数够但字节小，行长（如邮件正文）时 500 行会超
> `max_allowed_packet`。两个限制都要判。
>
> 单列 UPSERT 要生成 `ON DUPLICATE KEY UPDATE id=id` 的自赋值 no-op；
> 直接留空会得到语法错误的 `UPDATE `。

**道具落库改造**（`internal/game/player/persist.go`）：

| 维度 | 改造前 | 改造后 |
| --- | --- | --- |
| 写入范围 | 全量：`DELETE ... WHERE player_id` + 逐条 INSERT | 只写 `AddItem/RemoveItem` 碰过的道具 |
| 语句数 | 1 + N（N = 背包格数） | 1 + 批次（500 行/条） |
| 归零处理 | 靠先删后插隐式完成 | 合成 `DELETE ... AND item_id IN (...)` |

**前提修复**：`player_item` 原来只有 `PRIMARY KEY (id)`，没有 `(player_id, item_id)` 唯一键。
`ON DUPLICATE KEY UPDATE` 只对主键/唯一键生效，缺这个键会让 UPSERT 退化成插入重复行 ——
已补 `uk_player_item`。

**墓碑删除**（`pkg/flush/delete.go`）：

```
业务：Flusher.MarkDelete(table, pk)   ← pk 是**表主键**，不是 player_id
落库：flushDeletes → 按表分组合批 DELETE → 失败回填队列下轮重试
```

墓碑在 upsert **之前**执行：顺序反过来会出现「先写回旧值、再删掉」的中间态。
未注入 `DeleteFn` 时丢弃并计入 `gs_flush_tombstone_total{result="dropped"}`，不无限堆积。

**指标**：`gs_flush_tombstone_total{shard,result=ok|fail|dropped}`，
与已有的 `dirty_entities` / `lag_seconds` / `rows_total` 一起构成落库观测面。

**同步落库（Tier-1 写穿透的代码落地）**：

12.1 节的「写穿透」一档由 `Repository.SaveForce` 承担，服务充值、交易这类
「回执必须在 MySQL 提交之后发出」的数据——异步通道等的是一个落库窗口，
Redis 掉电会连窗口一起丢，对钱不可接受。

```
GrantItemsMsg{Sync: true}
  ├─ 同一事务：player / player_attr / player_item
  │             + player_writebehind.applied_grants（幂等键）
  ├─ 事务内 version + 1        ← 异步落库的栅栏据此放弃写这份快照
  ├─ 删 Redis 快照（Drop）     ← 重登走冷启动，读到库里的新值
  └─ 回执 Done
```

三个细节都不能省：

| 细节 | 为什么 |
| --- | --- |
| 幂等键与资产同一事务 | 拆开就会出现「发了货但没记键」的窗口，Kafka 重投会二次发奖 |
| 事务里推进 version | 旧快照即使没删成功，也不满足 `快照版本 > 库版本`，写不回库 |
| 删缓存失败要回执 err | 静默吞掉会让「重登读旧值」持续到人工介入；上抛给 Kafka 重投，重投走幂等命中分支补删一次，形成自愈 |

异步侧两处配套：`Store.Drop` 删快照时一并清掉脏队列成员；`Worker.FlushBatch`
遇到 `ErrMiss`（快照已被 Drop）跳过该玩家并清残留，不让整批因一条已同步
落库的记录回滚。

判据一句话：**这笔奖励丢了要不要赔钱？要就置 `Sync`，不要就走异步。**

### 12.5 Redis ID 表与启动加载

> **所有以内存态为准的数据库表，启动时都要在 Redis 建立一份 ID 表；业务查询不到 ID 即等于库没有；ID 查到了但 Redis 没有数据，再去数据库加载。**

Key 设计见 12.2 节（`idtab:{table}` / `idtab:{table}:ready` / `e:{table}:{id}`；
归属索引再加 `by_owner:{table}` / `by_owner:{table}:ready`）。

> **已补齐（v3.6）**：本节只有「数据表 + ID 表（Set）」两层，而 Set 只能判存在，
> 表达不了「实体属于谁」——一对多的功能（伙伴、装备、邮件）因此只能各写各的
> 查询，登录变成 N 条 SQL。现已按《缓存架构设计》第 2 节补齐第三层：归属索引
> **ZSET**（`by_owner:{table}`，score = 归属者 id，member = 实体 id），一个结构
> 同时承担「判存在 / 按归属者取列表 / 反查归属」；映射声明在游戏服配置的
> `idtab.mappings` 段（`pkg/config.CacheMapping`），读取走 `OwnerIndex.Expand`
> （v3.9 起不再有「登录预取」入口，见《缓存架构设计》§4.2）。注意它是**新增的
> 第三张表**，不是把 `idtab:{table}` 改成 ZSET——Set 与 ZSET 在 Redis 里不能同名
> 共存，且两者的 ready 生命周期不同。

#### 12.5.1 启动构建流程

```go
// pkg/cache/idtab.go
func BuildIDTables(ctx context.Context, db *sql.DB, rdb *redis.Client, tables []TableMeta) error {
    for _, t := range tables {
        var lastID int64
        for {
            // keyset 分页，避免大表深翻页
            ids := db.QueryIDs(t.Name, t.PK, lastID, 5000)
            if len(ids) == 0 { break }
            rdb.PipelinedSAdd(ctx, "idtab:"+t.Name, ids) // 每批 5000
            lastID = ids[len(ids)-1]
        }
        rdb.Set(ctx, "idtab:"+t.Name+":ready", "1", 0)
        log.Info("idtab built", "table", t.Name, "count", count)
    }
    return nil
}
```

| 表 | 是否建 idtab |
| --- | --- |
| `player`、`player_attr`、`player_item`、`player_equip`、`player_skill`、`player_partner`、`player_quest` | ✅ |
| `mail`、`friend`、`friend_apply`、`guild`、`guild_member`、`team`、`team_member` | ✅ |
| `account`、`recharge_order`（Tier-1） | ❌ MySQL 权威，直接查询 |
| `chat_record`、`copy_record`、`shop_record`、`activity_record`、`login_log`、`rank_history`、`feature_stats` | ❌ 归档/流水表，不建 idtab、无热数据，直接走 MySQL |

- **构建未完成前的降级**：`ready` 未置位时，存在性判断回落 MySQL（保证正确性，牺牲短暂性能）；
- **大表优化**：百万级表的构建约数秒（Pipeline 批量 SADD）；构建期间服务已可处理请求（降级路径），构建完成自动切换。

#### 12.5.2 查询规则（核心）

```go
// pkg/cache/exist.go

// Exists 存在性判断：查不到 ID 即等于库没有（不打 DB）
func Exists(ctx context.Context, table string, id int64) (bool, error) {
    if ready, _ := rdb.Get(ctx, "idtab:"+table+":ready").Result(); ready != "1" {
        return dbExists(ctx, table, id) // 构建未完成，回落 DB
    }
    return rdb.SIsMember(ctx, "idtab:"+table, id).Result() // O(1)
}

// LoadEntity 加载实体：ID 查到了但 Redis 没数据 → 回源 DB → 回填 Redis
func LoadEntity(ctx context.Context, table string, id int64) (map[string]string, error) {
    key := EntityKey(table, id)
    if data, _ := rdb.HGetAll(ctx, key).Result(); len(data) > 0 {
        return data, nil // Redis 命中
    }
    row := db.GetByPK(ctx, table, id) // 回源（idtab 已保证大概率存在）
    rdb.HSet(ctx, key, row.Fields())  // 回填，后续命中 Redis
    return row.Fields(), nil
}
```

三类查询的判定矩阵：

| idtab | Redis hash | 判定 | 动作 |
| --- | --- | --- | --- |
| 无 | — | **库中不存在** | 直接返回不存在，不打 DB |
| 有 | 有 | 正常 | 用 Redis 数据 |
| 有 | 无 | 数据未加载/已清理 | **回源 MySQL 加载 + 回填 Redis** |

#### 12.5.3 对账补偿

| 任务 | 频率 | 动作 |
| --- | --- | --- |
| 数量核对 | 每 10 分钟 | `SCARD idtab:{table}` vs `SELECT COUNT(*)`；不一致 → 后台重建该表 idtab（临时 key 构建 + RENAME 原子切换，不阻塞查询） |
| 孤儿数据清理 | 每小时 | `e:{table}:{id}` 存在但 idtab 无此 id（回填竞态残留）→ DEL 清理 |
| idtab 缺失修复 | 每小时 | 抽样比对 MySQL 与 idtab；发现缺失 SADD 补偿 |

#### 12.5.4 落地实现（P10）

代码：`pkg/cache/idtable.go`（`IDTable`）。

**与 ID 分配器区分**：`idtab.go` 的 `IDTab` 是 **ID 分配**（Redis String `INCR` key=`seq:{name}`），
本节的 `IDTable` 是 **存在性集合**（Redis Set key=`idtab:{table}`）。两者 key 前缀不同，
否则同一 key 被当 String 又当 Set 会直接报 WRONGTYPE。

**构建**：

```go
// 每表：keyset 分页 → SADD 到 idtab:{table}:building → RENAME → SET ready=1
SELECT id FROM {table} WHERE id > ? ORDER BY id LIMIT 5000
```

- **为什么 keyset 不用 OFFSET**：`LIMIT 5000 OFFSET 500000` 在 InnoDB 要扫过前 50 万行再丢弃，
  大表深翻页会退化；keyset 每次都是索引区间扫描。
- **为什么先写 building 再 RENAME**：直接往正式集合里写，构建到一半时 `Exists` 会把
  「还没扫到的 id」判定为不存在 —— 玩家进游戏可能被判成「角色不存在」。
  `RENAME` 是原子操作，切换瞬间完成。
- **热表优先**：`player` / `player_equip` / `mail` 先建，其余后台继续；
  构建期间 `ready` 未置位，`Exists` 自动回落 MySQL。

**接入点**：

| 位置 | 用法 | 收益 |
| --- | --- | --- |
| 创角 `RoleService.Create` | 成功后 `Add("player", roleID)` | 免去等 10 分钟对账才可见 |
| 穿戴 `equip.Repo.Find(uid)` | 先 `Exists("player_equip", uid)` | 客户端伪造/过期 uid 不打库 |
| 后台维护 | 10 分钟 `Reconcile`，1 小时 `CleanOrphans` | 数量漂移自动重建、孤儿 hash 清理 |

**降级**：Redis 不可用或 `ready` 未置位 → 所有判断回落 MySQL，功能不受影响，只是失去 O(1) 收益。

**指标**：`gs_idtab_size{table}` / `gs_idtab_ready{table}` / `gs_idtab_build_duration_seconds` /
`gs_idtab_reconcile_mismatch_total{table}`（后者 >0 即触发 20.3 的「idtab 对账不一致」告警）。

### 12.6 角色登录预加载

#### 12.6.1 流程

```
玩家登录（网关鉴权通过 → Bind 到游戏服）
   │
   ▼
游戏服创建 PlayerActor，进入预加载：
   │
   ├─▶ Pipeline 并行读 Redis：
   │     e:player:{pid}                玩家基础
   │     e:player_attr:{pid}           属性
   │     idx:player_item:{pid}    → 逐个 e:player_item:{uid}
   │     idx:player_equip:{pid}   → 逐个 e:player_equip:{uid}
   │     idx:player_skill:{pid}   → 逐个 e:player_skill:{uid}
   │     idx:player_quest:{pid}   → 逐个 e:player_quest:{uid}
   │     idx:player_partner:{pid} → 逐个 e:player_partner:{uid}
   │     idx:friend:{pid}              好友列表
   │     mail:unread:{pid}             未读邮件 ID 列表 → 逐个 e:mail:{mid}
   │     guild:player:{pid}            公会 ID → e:guild:{gid}
   │
   ├─▶ 收集 miss（idtab 命中但 hash 缺失的实体）
   │     → 按表合批回源 MySQL（每表一条 IN 查询）→ 回填 Redis
   │
   ├─▶ drain 离线暂存队列（13.3 模式②）：LRANGE pend:{biz}:{pid}
   │     → 邮件/好友申请/跨服私聊补投 → 本地落库 + 推送客户端
   │
   ▼
Actor 状态组装完成 → Ready
   │
   ▼
推送 LoginOk + 全量数据给客户端（或按模块懒推送，视客户端协议）
```

#### 12.6.2 要点

1. **全部并行**：Redis 侧一次 Pipeline 拿完；MySQL 回源按表合批（N 个实体一条 `IN` 查询），登录预加载通常 1~3 个 RTT；
2. **预加载范围 = 角色相关数据**：玩家、属性、背包、装备、技能、任务、伙伴、好友、邮件、公会——即 12.5.1 节 idtab 表中归属 `player_id` 的全部表；
3. **未读邮件/限时数据**在预加载时顺带计算过期；
4. **预加载超时**（默认 5s）→ 登录失败返回「服务器繁忙」，客户端重试，避免半成品 Actor 上线；
5. **重连（90s 内）**：Actor 未销毁，跳过预加载，直接恢复会话；
6. **离线暂存 drain**：`pend:{biz}:{pid}` 在预加载阶段统一消费（见 13.3 模式②），补投消息走正常模块链路落库 + 推送。

#### 12.6.3 落地实现（P12）

**并行拉取**（`internal/game/player/preload.go`）：

| 表 | 用途 | 失败影响 |
| --- | --- | --- |
| `player` | 角色基础（名字/等级/经验/金币/场景） | **必需**，缺失即登录失败 |
| `player_attr` | 基础属性（职业/HP/MP/攻防速） | **必需** |
| `player_item` | 背包堆叠 | **必需** |
| `player_equip` | 装备（含穿戴态，用于登录重算属性加成） | 可降级：模块自行查库 |
| `activity_record` | 活动进度（登录类活动需 +1） | 可降级：模块自行查库 |

3 个查询并发执行，耗时 ≈ 单表最慢查询，而非 3 次往返之和。

**超时保护**：默认 5s（`DefaultPreloadTimeout`）。超时视为登录失败，客户端收到错误后重试——
不允许半成品 Actor 上线（否则玩家会看到残缺数据且后续写入可能覆盖正确值）。

**指标**：`gs_login_preload_duration_seconds{result}`，对应 20.1 的「登录耗时 P99 < 2s」。

**模块协同（v3.9 起）**：预加载结果只用于组装 `State`，**不对外暴露**。
v3.6 曾通过 `module.Context.Prefetch`（`PrefetchSource` 接口）把装备/活动/归属
展开的数据递给模块 `OnLogin`，先删了装备/活动（状态改随快照 Ext 加载），再删了
归属展开（零消费者，且与模块形态不匹配）。现在模块要数据就自己查——`Repo` 或
`OwnerIndex.Expand`。

**离线暂存补投**：

```
EnterGame(1010) → bindPlayer → reply(1011) → go drainOffline
                                                 ├─ pend:chat:{pid}  (LRANGE + DEL，原子)
                                                 └─ 逐条转 ChatMessageNotify(6010) 推给客户端
```

> **时序要点**：补投必须在 `1011` **之后**。客户端收到进游戏回执才开始处理推送，
> 过早补投会被当成未知消息丢弃（实测踩过：客户端只认 1011，其余包直接跳过）。
> drain 有 2s 超时且失败不阻断登录——补投是尽力而为，丢了的消息仍有 DB 归档可查。

**未覆盖项**：好友/公会/邮件等表的预取未纳入本轮（这些模块的 `OnLogin` 目前不做 DB 查询或只查一行），
待对应功能接入 Redis 缓存层时统一迁移到 12.6.1 描述的完整 Pipeline 方案。

---

## 13. Kafka 消息队列设计

选型 **segmentio/kafka-go**（纯 Go、维护活跃、支持幂等 Producer 与 Consumer Group）。

### 13.1 Topic 规划

| Topic | 生产者 | 消费者 | 用途 | 可靠性要求 |
| --- | --- | --- | --- | --- |
| `gs.recharge.order` | 登录服（支付回调） | 游戏服（recharge 模块） | 充值下单→发货 | **至少一次 + 幂等**（order_id 去重） |
| `gs.mail` | 登录服/GM 后台 | 游戏服（mail 模块） | 系统邮件、补偿发放 | 至少一次 + 幂等（mail_id） |
| `gs.chat.cross` | 游戏服 A | 全部游戏服 | 跨服私聊投递（发送方生产，Key=接收方 playerID；各服广播消费 + 本地性判定；双端历史见 17.3） | 至少一次 + 幂等（msg_id） |
| `gs.player.event` | 游戏服 | 游戏服（event 模块） | 升级/获得道具/穿装备等事件流 → 任务/成就/活动进度 | 至少一次 + 幂等（event_id） |
| `gs.login.log` | 登录服/游戏服 | 日志消费者 | 登录登出流水 → MySQL/数仓 | 至少一次 |
| `gs.flush.retry` | 游戏服 flusher | 游戏服 flusher | MySQL 落库失败重试（死信兜底） | 至少一次 |
| `gs.activity.schedule` | etcd leader 服务 | 游戏服 | 全局活动定时触发 | 至少一次 + 幂等（schedule_id） |
| `gs.recharge.delivered` | 游戏服 | 登录服 | 充值发货确认 → 登录服更新订单状态（Tier-1，跨服不跨库写） | 至少一次 + 幂等（order_id） |
| `center.relay` | 中心服实例 A | 中心服实例 B | 中心服集群间中转（节点跨实例投递） | 至少一次 + 幂等（relay_id） |

### 13.2 关键配置

```go
// pkg/mq/kafka.go
// Producer：幂等 + acks=all，保证不丢不重（单会话内）
writer := &kafka.Writer{
    Addr:         kafka.TCP(brokers...),
    Balancer:     &kafka.Hash{},     // 按 Key（playerID/orderID）分区 → 同玩家有序
    RequiredAcks: kafka.RequireAll,
    Compression:  kafka.Lz4,
    Async:        false,             // 同步写：WriteMessages 返回即已 ack
    // 注：segmentio/kafka-go v0.4 未提供幂等 Producer（无 Idempotent 字段），
    //     「不丢不重」由 acks=all + 同步写 + 消费端 biz_id 幂等（13.4）三道防线共同保证。
}

// Consumer：Group 消费，按 playerID 分区保证同玩家事件有序
reader := kafka.NewReader(kafka.ReaderConfig{
    Brokers: brokers,
    GroupID: "game-server-" + topic + "-" + instanceID, // 每实例每 topic 独立组 → 广播语义
    Topic:   topic,
    StartOffset:    kafka.FirstOffset,
    CommitInterval: 0, // 关闭自动提交，由消费循环手动 CommitMessages（失败重试后提交）
})
```

### 13.3 跨服消息投递模式（重要）

游戏服每实例独立 consumer group ⇒ **每条消息所有游戏服都会收到**（广播语义）。由此定义三种投递模式：

| 模式 | 适用 | 流程 |
| --- | --- | --- |
| **① 广播消费 + 本地性判定** | 充值、邮件、跨服私聊 | 收到消息 → 查 Redis `online:{playerId}` → 玩家在本服在线：应用 + 本地落库；否则丢弃（由持有者应用，或离线走模式②） |
| **② 离线暂存 + 登录 drain** | 邮件、好友申请、跨服私聊（对方离线） | 生产者写共享 Redis `pend:{biz}:{playerId}`（List，上限 200，TTL 7d）→ 玩家下次登录任意游戏服，预加载阶段 drain → 本地落库 + 推送 |
| **③ Tier-1 对账回传** | 充值发货确认 | 游戏服发货成功 → Kafka `gs.recharge.delivered` → 登录服更新订单状态（订单表在登录服库，游戏服不跨库写）；每小时对账「已支付未发货」重投 `gs.recharge.order` |

> 关键约束：**每个游戏服有自己的库**，因此一切玩家数据（邮件/好友/聊天记录）都落「玩家所在服」的库；跨服只投递消息内容，不跨库读数据。

### 13.4 通用可靠性机制

1. **分区键 = playerID/orderID**：同一玩家的订单、事件、邮件固定落同一分区 → 单分区内有序 → 消费端按序处理不交叉；
2. **幂等消费**：每条消息带全局唯一 `biz_id`（order_id/mail_id/event_id/msg_id），消费端先查 Redis `idem:{topic}:{biz_id}`（SET NX 24h），已处理直接 ack；
3. **失败重试**：处理失败不 ack，kafka-go 自动重投；超过重试上限写入 `gs.flush.retry` 死信 Topic + 告警；
4. **优雅关闭时 producer 必须 Flush 完**：见第 18 章关闭屏障。

---

## 14. 连接池设计

框架涉及四类连接/池化管理，全部内置健康检查、自动重连、指标暴露：

| 池 | 位置 | 参数 | 说明 |
| --- | --- | --- | --- |
| **gRPC Channel 池** | 网关 → 游戏服 | 每目标 `2×NumCPU`（默认 16）条 Channel，单 Channel 最大 2000 并发 stream | HTTP/2 多路复用；Round-Robin 选 Channel；详见 7.4 节 |
| **gRPC Report 流** | 游戏服/跨服节点 → 中心服 | 每节点 1 条 bidi stream | 断流指数退避重连（1s→30s 封顶），重连补发 Register |
| **CrossService 流** | 游戏服 → 跨服节点 | 每（游戏服 × 组内 CrossServer）1 条 bidi stream | 地址由中心服 GroupUpdate 下发；断流指数退避重连 |
| **Redis 连接池** | 全部服务 | `PoolSize = 10×NumCPU`，`MinIdleConns ≤ PoolSize` | go-redis 内置池；Cluster 按 slot 路由 |
| **MySQL 连接** | 游戏服 | 业务 CRUD 通用池 30（flush 10 worker 错峰共享借还，不另占连接） | 分时落库见 12.4 节；容量规划见下 |

**MySQL 连接数容量规划**（修复项：`max_connections` 是 **MySQL 实例级**参数，默认 151，限制整个实例——所有库共享——的并发客户端连接数，**不是每个库 151**）：

| 组件 | 单服连接数 | 说明 |
| --- | --- | --- |
| 游戏服-业务 CRUD 池 | 30 | 以 PK/索引点查为主，单次 <1ms，30 条足以支撑 5K 在线（原示例值 100 缺乏依据，已修正） |
| 游戏服-flush 分片 | 0（共享池） | 12.4 节 10 个 worker 错峰执行，连接从业务池借还，峰值增量 ≤ 10 |
| 登录服 / 中心服 | 各 20 | 低频写 + 对账查询 |

按 10 台游戏服估算：10×30 + 20 + 20 = **340 条** → 生产 MySQL `max_connections` 建议 **≥ 500**（预留 30% 给 DBA/运维连接与 flush 峰值）。若采用「每个游戏服独立数据库实例」，单实例 30 条，默认 151 足够，但需配套多实例运维体系。连接紧张时优先压缩业务池，或引入 ProxySQL 做连接复用。

设计要点：

1. **池化对象带生命周期管理**：借出/归还/健康检查/淘汰四态机，空闲连接超时回收，坏连接自动重建；
2. **指标可观测**：每类池暴露 `in_use / idle / wait / errors` 四组 Prometheus 指标，等待超时打点告警；
3. **优雅关闭统一排空**：所有池注册到 shutdown Manager，退出前归还/关闭，禁止连接泄漏（见第 18 章）。

---

## 15. 安全设计（RSA + AES）

### 15.1 握手流程

```
Client                              Gateway
  │                                   │
  │──── TCP Connect ─────────────────▶│
  │                                   │
  │◀─── ServerHello ─────────────────│  ① 下发 RSA-2048 公钥(PEM) + keyId +
  │      {rsa_pub, key_id,            │     server_random(32B) + proto_ver
  │       server_random, proto_ver}   │
  │                                   │
  │  ② 客户端生成：                    │
  │     aes_key = random(32B)         │
  │     iv      = random(12B)         │
  │     kdf_salt= random(16B)         │
  │                                   │
  │──── ClientKeyExchange ───────────▶│  ③ RSA-OAEP(SHA256) 加密
  │      {enc_keymat = RSA(           │     {aes_key, iv, kdf_salt, client_random}
  │         aes_key+iv+kdf_salt       │
  │         +client_random),          │
  │       client_random, proto_ver}   │
  │                                   │  ④ 网关 RSA 私钥解密 →
  │                                   │     HKDF-SHA256 派生双方向密钥：
  │                                   │     key_c2s = HKDF(secret,"c2s")
  │                                   │     key_s2c = HKDF(secret,"s2c")
  │◀─── Finished ────────────────────│  ⑤ AES-256-GCM 加密下发
  │      AES{session_id,              │     {session_id, heartbeat_interval}
  │          heartbeat_interval}      │
  │                                   │
  │════ 业务期：全部 AES-256-GCM ════│  ⑥ 每包 Length|MsgID|Seq|密文+Tag
  │      Seq 单调递增，防重放          │     AAD = {Length, MsgID, Seq, session_id}
```

### 15.2 参数与实现要点

| 项 | 选型 | 说明 |
| --- | --- | --- |
| 非对称算法 | RSA-2048 + OAEP(SHA-256) | 只交换密钥，不做业务加解密（慢） |
| 对称算法 | AES-256-GCM | 认证加密，防篡改；Tag 16B |
| 密钥派生 | HKDF-SHA256 | 双方向独立密钥，单方向泄露不扩散 |
| 防重放 | 每方向 Seq 从 1 单调递增，写入 AAD；滑窗（窗口 1024）接受乱序，拒绝回放 | 断线重连 Seq 重新从 1 开始（新会话） |
| 私钥存储 | PEM 文件（权限 600）启动加载；`keyId` 轮换期支持「当前+上一把」双公钥 | 轮换不中断服务 |
| 失败处理 | 解密/校验失败累计 3 次 → 断开连接 + 告警（防爆破） | |

### 15.3 各层安全边界

| 链路 | 保护 |
| --- | --- |
| 客户端 → 登录服（HTTP） | TLS 1.3（RSA/ECDSA 证书），Token 仅经 Body 传输 |
| 客户端 → 网关（TCP） | RSA 换钥 + AES-256-GCM + Seq 防重放 |
| 网关 → 游戏服（gRPC） | 内网受信 + metadata `x-internal-token`（服务间共享密钥，防内网伪造） |
| 服务 → Redis/MySQL/Kafka | 内网 + 密码鉴权；Kafka SASL/SCRAM 可选 |

---

## 16. 代码生成体系

> 设计哲学（借鉴 go-zero）：**「凡是能由规则推导的代码，一律不手写」**。

三条生成流水线，统一由 `make gen` 驱动，每次全量覆盖 `*_gen.go`：

```
make gen
  ├── buf generate              # api/proto/*.proto      → pkg/pb/...
  ├── go run ./tools/gendata    # configs/gamedata/xml/*.xml → pkg/gamedata/*_gen.go
  └── go run ./tools/gendb      # sql/schema/*.sql       → pkg/model/*_gen.go + *_model.go
```

### 16.1 gendata：XML 游戏数据 → Go 配置代码

#### 16.1.1 输入规范

游戏业务数据（道具、等级、技能、副本、商店、活动……）全部放 `configs/gamedata/xml/`，一个文件一张配置表：

```xml
<!-- configs/gamedata/xml/item.xml -->
<config name="item" desc="道具配置表">
  <!--
    字段类型由生成器从首行数据推断：int / float / bool / string
    多语言列用后缀约定：desc_cn / desc_en
    ref 列（如 quality）在生成时做取值校验
  -->
  <row id="1001" name="金币" name_en="Gold" type="currency" quality="1"
       stack="999999999" sell_price="1" desc_cn="基础货币" desc_en="Basic currency" />
  <row id="1002" name="经验药水" name_en="Exp Potion" type="consumable" quality="2"
       stack="999" sell_price="10" use_effect="add_exp:100" desc_cn="使用获得100经验" desc_en="Gain 100 exp" />
  <row id="2001" name="铁剑" name_en="Iron Sword" type="equip" quality="3"
       sell_price="50" atk="10" def="0" crit="0" desc_cn="新手铁剑" desc_en="Beginner sword" />
</config>
```

```xml
<!-- configs/gamedata/xml/level.xml -->
<config name="level" desc="等级经验配置表">
  <row level="1" exp="0"     hp="100"  atk="10"  def="5"  />
  <row level="2" exp="100"   hp="120"  atk="12"  def="6"  />
  <row level="3" exp="300"   hp="145"  atk="14"  def="7"  />
</config>
```

#### 16.1.2 生成产物（每次全量覆盖）

```
pkg/gamedata/
├── data.go            # 包级入口：Version、Reload()、Verify()、AllTables 注册表（生成，可覆盖）
├── item_gen.go        # 每张表一个文件（生成，全量覆盖）
├── level_gen.go
├── skill_gen.go
└── ...                # 共 20+ 张表
└── embed/xml/         # gendata 拷贝的 XML 源文件（生成物，全量覆盖；go:embed 输入）
```

`item_gen.go` 生成内容示意：

```go
// Code generated by gendata from configs/gamedata/xml/item.xml. DO NOT EDIT.
// 生成时间由 CI 写入；本文件每次 make gen 全量覆盖。
package gamedata

// ItemCfg 道具配置（name 字段 → Name，name_en → NameEn）
type ItemCfg struct {
    Id        int64  `json:"id"`
    Name      string `json:"name"`
    NameEn    string `json:"name_en"`
    Type      string `json:"type"`     // currency/consumable/equip
    Quality   int32  `json:"quality"`
    Stack     int64  `json:"stack"`
    SellPrice int64  `json:"sell_price"`
    UseEffect string `json:"use_effect"`
    Atk       int32  `json:"atk"`
    Def       int32  `json:"def"`
    Crit      int32  `json:"crit"`
    DescCn    string `json:"desc_cn"`
    DescEn    string `json:"desc_en"`
}

var (
    itemCfgs      = map[int64]*ItemCfg{}    // 主键索引
    itemCfgSlice  []*ItemCfg                // 保序切片（遍历/随机）
    itemCfgByName = map[string]*ItemCfg{}   // 二级索引（按需生成）
)

func init() { registerTable("item", loadItem) }

// loadItem 解析嵌入的 XML 并填充索引；重复 id / 非法取值直接 panic（启动期暴露配置错误）
func loadItem(data []byte) error { /* ... */ }

// GetItemCfg 业务侧唯一访问入口（强类型、O(1)）
func GetItemCfg(id int64) (*ItemCfg, bool) {
    c, ok := itemCfgs[id]
    return c, ok
}

// RangeItemCfg 安全遍历
func RangeItemCfg(f func(cfg *ItemCfg) bool)
```

#### 16.1.3 嵌入、版本与热更新

- XML 源文件由 gendata 在生成时**拷贝到 `pkg/gamedata/embed/xml/`**（生成物，随 `make gen` 全量覆盖），包内以 `//go:embed embed/xml/*.xml` 嵌入二进制——**embed 路径必须位于 Go 包目录内**（Go 语言限制，无法嵌入上层的 `configs/`，此为修复项）；运行时不依赖外部文件，容器部署零挂载；
- 生成器计算全部 XML 的内容哈希写入 `data.go` 的 `Version` 常量；服务器启动时打印版本，GM 后台可核对线上配置版本；
- **热更新（可选）**：GM 指令触发 `Reload()`，重新解析嵌入 XML + 对比版本，变更时原子替换索引 map（copy-on-write），不重启进程。

#### 16.1.4 生成器能力清单

| 能力 | 说明 |
| --- | --- |
| 类型推断 | 从首行数据推断 int/int64/float/bool/string，支持 `i64:`/`f:` 等显式前缀 |
| 主键索引 | 每个表必须有主键列（默认 `id`，可用 `pk="level"` 指定） |
| 二级索引 | `index="name"` 声明后生成 `GetXCfgByName` |
| 多语言 | `_cn`/`_en` 后缀列自动生成对应字段 |
| 校验 | 重复主键 panic、ref 列枚举校验、数值范围校验（`min`/`max` 属性） |
| 注释继承 | XML 注释与 `desc` 属性生成到 Go doc |
| CI 卡点 | `make gen && git diff --exit-code`，防止「改了 XML 没重新生成」 |

### 16.2 gendb：手写 DDL → Go CRUD 代码

**参考设计**：go-zero `goctl model mysql ddl`（`*_model.go` 接口 + `*_gen.go` 实现 + `vars.go`），本项目按游戏服需求裁剪增强。

#### 16.2.1 输入规范

DDL 手写在 `sql/schema/game.sql` 单文件（按表名排序），**带完整注释**（注释生成到 Go doc），示例见 12.3 节。

#### 16.2.2 生成产物与覆盖策略

```
pkg/model/
├── playermodel.go     # 【只创建，不覆盖】接口 + 手工扩展方法写这里
├── player_gen.go      # 【每次全量覆盖】CRUD 实现
├── vars.go            # 【每次全量覆盖】ErrNotFound 等哨兵错误
├── itemmodel.go / item_gen.go
├── guildmodel.go / guild_gen.go
└── ...
```

`playermodel.go`（首次生成，之后**永不覆盖**，业务可扩展）：

```go
package model

type (
    // PlayerModel 数据访问接口：嵌入生成接口，可追加自定义方法
    PlayerModel interface {
        playerModel
        // 自定义方法示例（手写，不走生成）
        CountByLevelGT(ctx context.Context, level int32) (int64, error)
        PageByAccountID(ctx context.Context, accountID int64, page, size int) ([]*Player, error)
    }
    defaultPlayerModel struct{ *sqlx.DB }
)

func NewPlayerModel(conn *sqlx.DB) PlayerModel {
    return &defaultPlayerModel{conn}
}
```

`player_gen.go`（每次 `make gen` **全量覆盖**）生成：

```go
// Code generated by gendb from sql/schema/game.sql. DO NOT EDIT.
package model

// Player 玩家表实体
type Player struct {
    Id        int64     `db:"id"`
    AccountId int64     `db:"account_id"`
    Name      string    `db:"name"`
    Level     int32     `db:"level"`
    Exp       int64     `db:"exp"`
    Coin      int64     `db:"coin"`
    SceneId   int32     `db:"scene_id"`
    CreatedAt time.Time `db:"created_at"`
    UpdatedAt time.Time `db:"updated_at"`
}

// Insert 插入一行（返回 LastInsertId）
// BatchInsert 批量插入（游戏服启动预热/后台补数据用）
// Upsert 主键冲突时更新（flusher 落库核心方法，保证幂等）
// UpsertCols 只更新指定列（Write-Behind 脏列落库，避免覆盖并发写）
// FindByPK / FindOneByAccountID / ExistsByPK
// ListByLevel(ctx, level, page, size) 普通索引分页
// CountByAccountID
// DeleteByPK / SoftDelete（如配置了 deleted_at 列）
```

#### 16.2.3 生成规则

| 规则 | 说明 |
| --- | --- |
| 类型映射 | `bigint→int64`、`int→int32`、`tinyint(1)→bool`、`varchar→string`、`decimal→decimal.Decimal`、`timestamp→time.Time`；`NULL` 列 → `sql.NullXxx` |
| 主键 | 单列主键生成 `FindByPK/DeleteByPK/Upsert`；联合主键生成对应多参版本 |
| 唯一索引 | `uk_channel_openid` → `FindOneByChannelOpenID` |
| 普通索引 | → `ListByXxx`（分页）+ `CountByXxx` |
| 自增列 | Insert 时自动排除，返回 `LastInsertId` |
| 时间列 | `created_at/updated_at` 默认不参与 Insert/Update 字段集（可配 `-i` 忽略列） |
| 软删除 | 表含 `deleted_at` 时生成 `SoftDelete`，查询默认带 `deleted_at IS NULL` |
| 缓存钩子 | 生成代码预留 `cacheHook` 接口（默认空实现），游戏服的 Actor 缓存层可实现它做 Cache-Aside |
| 分表预留 | DDL 注释中标注 `-- @shard: player_id%512` 时，生成 `TableName(playerID)` 路由方法 |

#### 16.2.4 「数据库导出脚本」工作流

用户要求的「自己手搓 SQL 语句，然后通过脚本生成 Go 文件」落地为：

```
① 策划/程序确定表结构 → 手写 DDL（带注释）→ sql/schema/game.sql 提交仓库
② make gen-db  → tools/gendb 解析 DDL → 生成 pkg/model/*_gen.go（覆盖）+ *_model.go（仅新建）
③ make db-init → 执行 sql/schema/*.sql 到本地 MySQL（docker-compose 开发环境）
④ make db-seed → 执行 sql/seed/*.sql（初始 GM 账号、测试数据）
⑤ CI 卡点：make gen && git diff --exit-code，防止「改了 DDL 忘了再生成」
```

复杂查询（排行榜 TopN、跨表统计）不生成，手写在 `*_model.go` 的扩展接口里，或放 `pkg/model/queries/*.sql`（包内目录，满足 embed 路径约束）由 `//go:embed` 载入。

### 16.3 buf：proto → gRPC/消息代码

```bash
# make gen-proto
cd api && buf generate
# 产出：pkg/pb/{common,gate,game,internal}/v*/*.pb.go
#   - gate/v1     客户端 <-> 网关（消息注册表 + 反射分发）
#   - game/v1     游戏业务消息
#   - internalv2 gRPC service（GameService.Channel / CenterService.Report 等）
```

- `pkg/codec/registry.go` 由 proto 自动生成 `MsgID ↔ MessageType` 注册表，网关/游戏服启动时统一注册，新增协议零分发代码；
- `buf breaking` 在 CI 拦截不兼容变更。

### 16.4 生成物管理约定

| 目录 | 覆盖策略 | 能否手改 |
| --- | --- | --- |
| `pkg/pb/**` | 全量覆盖 | 禁止 |
| `pkg/gamedata/*_gen.go`、`data.go` | 全量覆盖 | 禁止 |
| `pkg/model/*_gen.go`、`vars.go` | 全量覆盖 | 禁止 |
| `pkg/model/*model.go` | 仅首次创建 | **可以**（自定义方法） |
| `internal/**` | 不生成 | 业务代码 |

**CI 卡点**（`.github/workflows/ci.yaml`，目录树已登记）：push/PR 触发 → ① `make gen` 后 `git diff --exit-code`（生成物必须随代码提交，杜绝手改）；② `buf lint` + proto 兼容性检查；③ `golangci-lint run`；④ `go test ./...`（含 `test/integration`，testcontainers 起真实中间件）；⑤ `./scripts/build.sh` 交叉编译五服务。任一步失败即阻断合并——生成体系的可信度靠 CI 强制保证。

---

## 17. 前期功能模块设计

### 17.1 功能总览（21 项）

| # | 功能 | 模块目录 | 游戏数据 XML | MySQL 表 | Redis | Kafka |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | 创角 | `module/create` | `career.xml`（职业）、`create_name.xml`（随机名库） | `player`、`player_attr` | `idtab:player`、`e:player:{pid}` | — |
| 2 | 登录 | `module/login` | `login_reward.xml`（登录礼包） | `login_log` | `token:*`、`online:{pid}` | `gs.login.log` |
| 3 | 重连 | `module/login` | — | — | `online:{pid}` | — |
| 4 | 踢掉 | `module/login` | — | — | `online:{pid}` | — |
| 5 | 充值 | `module/recharge` | `recharge_product.xml`（商品档位） | `recharge_order`、`recharge_record` | `idem:gs.recharge.order:{order_id}` | `gs.recharge.order` |
| 6 | 升级 | `module/level` | `level.xml` | `player` | `e:player:{pid}` | `gs.player.event` |
| 7 | 装备 | `module/equip` | `equip.xml`、`equip_enhance.xml`（强化） | `player_equip` | `e:player_equip:{id}` | `gs.player.event` |
| 8 | 技能 | `module/skill` | `skill.xml`、`skill_upgrade.xml` | `player_skill` | `e:player_skill:{id}` | `gs.player.event` |
| 9 | 属性 | `module/attr` | `level.xml`（基础属性）、`attr_formula.xml`（公式） | `player_attr` | `e:player_attr:{pid}` | — |
| 10 | 公会 | `module/guild` + `service/guild_service` | `guild_level.xml` | `guild`、`guild_member` | `e:guild:{gid}`、`idx:guild_member:{gid}` | — |
| 11 | 聊天 | `module/chat` | `chat_channel.xml`（频道配置） | `chat_record`（私聊，含跨服） | `chat:hist:{pid}:{peerId}`（共享双端历史） | — |
| 12 | 邮件 | `module/mail` | `mail_template.xml` | `mail`、`mail_attachment` | `idem:gs.mail:{mail_id}` | `gs.mail` |
| 13 | 组队 | `module/team` + `service/team_service` | `team_limit.xml` | `team`、`team_member` | `e:team:{tid}` | — |
| 14 | 副本 | `module/copy` | `copy.xml`、`copy_drop.xml` | `copy_record` | `e:copy:instance:{id}` | — |
| 15 | 商店 | `module/shop` | `shop.xml`、`shop_goods.xml`、`shop_refresh.xml` | `shop_record` | `shop:refresh:{pid}` | — |
| 16 | 活动 | `module/activity` + `scheduler/` | `activity.xml` | `activity_record` | `activity:state:{id}` | `gs.activity.schedule` |
| 17 | 任务 | `module/quest` | `quest.xml`、`quest_chain.xml` | `player_quest` | `e:player_quest:{id}` | `gs.player.event` |
| 18 | 好友 | `module/friend` | `friend_limit.xml` | `friend`、`friend_apply` | `idx:friend:{pid}` | — |
| 19 | 伙伴 | `module/partner` | `partner.xml`、`partner_star.xml` | `player_partner` | `e:player_partner:{id}` | — |
| 20 | 背包 | `module/bag` | `bag_capacity.xml`（容量公式） | `player_item` | `idx:player_item:{pid}` | — |
| 21 | 跨服聊天 | `module/chat` + `service/world_service` | `chat_channel.xml` | — | `online:{playerId}`（定位）、`chat:hist:*`（双端历史）、`pend:chat:*`（离线暂存） | `gs.chat.cross` |

### 17.2 模块标准结构（以 recharge 为例）

```
internal/game/module/recharge/
├── module.go            # Module 实现：Init/OnMessage/OnSave/OnLogin/OnLogout
├── handler.go           # 协议处理：RechargeProductListReq、RechargeCreateOrderReq
├── order.go             # 订单状态机（创建→待支付→发货→完成）
├── consumer.go          # Kafka 消费发货（幂等：order_id + Redis SET NX）
└── reward.go            # 发货动作（调 bag/attr/mail 模块）
```

协议处理统一走「Actor 信箱 → 模块 OnMessage」链路，**模块间不直接调用**，一律通过玩家 Actor 的 `ctx.Tell` 或同步辅助方法（同一 Actor 内直接调用安全）：

```go
// module/bag/bag.go 被 recharge/reward.go 调用（同 Actor 内，安全）
func (m *BagModule) AddItem(itemID int64, count int32, reason string) error {
    cfg, ok := gamedata.GetItemCfg(itemID)
    if !ok { return errcode.ErrItemNotExist }
    stack := cfg.Stack
    // …叠堆逻辑，全部读写 m.actor.state，无锁
    m.actor.markDirty("bag")
    return nil
}
```

#### 17.2.1 薄模块基座 `module.Thin`（简单玩法零样板）

上面是「重模块」的标准结构（recharge 有订单状态机，值得拆五个文件）。但大量玩法是「协议少、规则强」的增删改查（签到/玩家信息/属性占位/登录礼包），为满足 `Module` 接口要手写 7 个方法，其中大半是样板：`Discardable` 恒 true、三个生命周期空实现、`CacheFailureReply` 按 msgID switch 构造错误回包。改造前 620 行的签到里业务只占三分之一。

`internal/game/module/thin.go` 提供一个**薄模块基座**（框架预装配接口样板，业务只写自己的处理器）。业务模块嵌入 `*module.Thin` 后只剩三件事：声明协议、写处理器、按需挂钩子：

```go
m := &Module{}
m.Thin = module.NewThin(stateKey,
    module.ThinLogger(logger),
    module.ThinErrCode(codeSystem),        // 系统错误码
    module.ThinGuard(module.RequireCache), // 前置校验：缓存权威未就绪一律拒绝（不可双写）
    module.ThinInit(m.initConfig),         // 启动期配置校验
)
m.HandleR(MsgInfoReq, MsgInfoResp, &gamev1.SigninInfoResp{}, m.onInfo)
m.HandleR(MsgClaimReq, MsgClaimResp, &gamev1.SigninClaimResp{}, m.onClaim)
```

基座预装配的内容：`Name/MsgIDs/Init/OnMessage 分发/OnSave/OnLogin/OnLogout`、`DiscardableAware`（已注册协议默认可丢弃）、`CacheFailureAware`（按声明的响应构造错误包）、`Reply/Fail` 辅助方法。

**`HandleR`：把「请求 ↔ 响应」的配对交给基座**。错误回包要回答两个问题——响应号是多少、错误码放哪个字段。原先每个模块为此写一个 12-15 行的 switch 工厂（signin 的 `respOf`），现在只需在注册时多给一个零值原型：

| 写法 | 代码 | 错误回包 |
|---|---|---|
| `Handle(reqID, fn)` | 1 行 | 不实现 `CacheFailureAware`，走网关通用 `ErrorNotify` |
| `HandleR(reqID, respID, &XxxResp{}, fn)` | 1 行 | 基座 `proto.Clone` 零值原型 + 按字段名置 `code` |
| `ThinRespOf(fn)` | 12-15 行 | 逃生口：一个请求回多种响应的场景，优先于 `HandleR` |

原型必须是零值（`&XxxResp{}`）：基座克隆它来构造错误包，带默认值的话错误包会带上那些默认值。响应没有 `code` 字段时仍会回包（空包），比不回包好——客户端至少知道请求失败了，不必等到超时。`nil` 原型直接 panic，属启动期编码错误。

**覆盖情况**：14 个模块的 36 个协议已用 `HandleR`，只剩 3 处 `Handle`——`battle.onOpReport`（战报单向上报）、`scene.onEnter`/`onMove`（转给场景 Actor 异步回包）。这三处没有「请求 → 响应」配对，是刻意保留的。

`Plain` 也有同名能力（`PlainErrCode` + `CacheFailureReply`）：未设错误码时不实现 `CacheFailureAware`，零状态模块不该在缓存不可用时凭空造业务语义。

**适用边界**（注册即承诺）：处理器的重放没有第二次副作用——只读，或业务状态自带幂等（签到按「今天已领」事实去重）。不满足的协议不要注册进 Thin。需要 MySQL 直写事务的关系型模块（好友/公会/队伍）不建议用——复杂度在事务不在样板。外部类型可覆盖嵌入方法收紧策略。

改造效果（纯业务代码留存，样板归零）：signin 620→367 行、loginreward 453→276 行、profile 119→89 行、item 72→49 行、attr 81→54 行；新增简单玩法从「先写 60 行接口样板」变成「只写处理器」。

**已推广到全部可适用模块**（24 个模块中 17 个用基座）：

| 基座 | 模块 | 迁移效果 |
|---|---|---|
| Thin（简单） | signin / profile / loginreward / item / attr | 累计 −500 行 |
| Thin + `ThinOnEvent`（事件型） | quest / activity | 各 −18~20 行，`OnEvent` 只写业务 |
| Thin + `ThinLogin`/`ThinLogout` | scene / skill / equip / partner / mail / chat / battle / copy | 各 −14~33 行 |
| Plain（零状态） | rank | −18 行 + 纯读零快照 |
| 无基座（关系型事务） | friend / guild / team | 保持（复杂度在事务不在样板） |

`ThinOnEvent` 的设计取舍：Thin 无条件实现 `OnEvent`（未挂钩子时为空），因此所有 Thin 模块都会进入 `Registry.DispatchEvent` 的遍历——代价是每个事件多十几次空函数调用（几十纳秒），换来的是「模块无需为不订阅事件写任何代码」。

#### 17.2.2 三档基座：先选档，再写代码

`Thin` 解决的是「有状态的简单模块」，但零状态的模块（榜单查询只是代理一次中心服调用）用不上它——它们连 `Context.State` 都不碰。于是补了第三档：

| 档位 | 适用 | 基座 | 判据 |
|---|---|---|---|
| **Plain** | 零状态：榜单查询、聊天转发、静态表查询 | `module.NewPlain` | 处理器不调用 `State` 的任何写方法 |
| **Thin** | 单玩家状态：签到、任务、背包 | `module.NewThin` | 改 `Ext` 槽位，随快照 CAS |
| 无基座 | 关系型事务：好友、公会、队伍 | 手写 | 双人/多人状态，靠 MySQL 约束 |

```go
// Plain：1 行装配 + 处理器
m.Plain = module.NewPlain("rank", module.PlainLogger(logger))
m.Handle(codec.MsgRankListReq, m.onList)
```

**Plain 不只是少写代码，它省掉的是运行期开销**。Plain 实现 `module.StatelessAware`，玩家 Actor 据此短路：

```go
case *PacketMsg:
    if a.canBypassCache(msg.MsgID) {   // loaded && 无未决提交 && 无待重载 && 模块无状态
        a.receive(ctx)                  // 直接分发
        return
    }
    a.receiveCached(ctx)                // 原流程：clone State → 业务 → JSON 比对 → CAS
```

原本每条消息都要 clone 一份含背包/属性/各模块 Ext 的 State 并做两次 `json.Marshal`——即使 diff 为空、不产生 Redis 写，这份开销也是白付的。榜单这类纯读请求占相当比例，跳过它是有实际意义的优化。

**误判方向的后果不对称**（这是敢做声明式跳过的前提）：漏判只多付一次快照开销；误放行会被三个前提（已加载、无未决提交、无待重载）挡住，不存在静默丢数据的路径。声明错了的最坏结果是「改动要等下一次有状态消息才落库」，而不是数据错乱。

已落地：`module/plain.go`、`Registry.IsStateless`、`Actor.canBypassCache`，`rank` 模块迁移为 Plain（198→180 行 + 纯读零快照）。单测覆盖分发、重复注册 panic、生命周期钩子、`MsgIDs` 副本，以及快路径的 6 种前提组合。

#### 17.2.3 状态结构托管 `module.Stateful[T]`

`Thin` 解决了「接口七件套」，但模块仍要自己维护「内存结构 ↔ Ext JSON ↔ MySQL 行」的三层映射。signin 里这部分有 67 行（占 18%），而且要求业务开发者理解 Ext 与表结构。

`Stateful[T]` 把编解码收进基座，业务只声明结构体 + 写处理器：

```go
type Data struct {
	Days    []int32
	Rewards map[int32]int64
}

type Module struct {
	*module.Stateful[Data]   // 复用 Thin 的 Name/MsgIDs/分发/日志/回包
	now func() time.Time
}

func (m *Module) onClaim(ctx *module.Context, _ uint32, _ []byte) error {
	return m.Mutate(ctx, func(d *Data) bool {
		d.Days = append(d.Days, today)
		return true          // 有改动 → 基座写回 Ext 并标脏
	})
}
```

| 方法 | 语义 |
|---|---|
| `Read(ctx, fn)` | 解码后交给 fn，不回写（纯查询） |
| `Mutate(ctx, fn) bool` | fn 返回 true 才写回并标脏——「只读 + 早退」的请求不产生提交 |
| `State(ctx)` | 取解码副本（循环里要自己缓存） |

**落库侧配套**：`Ext` 存业务结构、表里存行结构，两者由 `writebehind.RegisterStateful[Row, Data]` 的 `StateCodec` 转换。转换代码集中在模块的 `persist.go`，业务文件里不再出现 `json.Marshal` / `SetExt` / `model.XxxRow`。

改造效果（signin）：业务文件 365 → 316 行，持久化代码 0 行（`Reset` 除外）；`persist_gen.go` 由 `tools/gencodec` 按 `db` tag 生成两段编解码（手写持久化 0 行）。

**升级注意**：Ext 的 JSON 从「行结构」变成「业务结构」，旧数据需要一次性迁移（当前 demo 环境未涉及）。`RegisterFlat` 仍然可用——它适用于「业务结构恰好等于表结构」的简单模块。

#### 17.2.4 发奖与扣费 `module.Grant` / `module.Spend`

七个模块（签到/任务/活动/副本/商店/邮件/登录礼包）各自重复同一套发奖三步，而三步各有各的错法：

| 步骤 | 漏做的后果 |
|---|---|
| 容量预检 | 背包满了还发，道具静默丢失 |
| int64 → int32 溢出预检 | 存量越界后协议里变成负数（原先只有 signin 查了） |
| `AddItem` 后标脏 | 改了背包不提交（其实 `AddItem` 内部已标脏，这行本就是冗余） |

统一成一次调用：

```go
changes, err := module.Grant(ctx.State, grants)   // 校验 + 入包，失败时一个都不改
if err != nil {
    resp.Code = codeBagFull                        // errors.Is 区分 ErrBagFull / ErrItemLimit
    return reply(ctx, MsgClaimResp, resp)
}
// 背包推送：模块只负责把 BagChange 转协议
notify := &gamev1.BagChangeNotify{Changes: module.ToItemChanges(changes)}
```

| API | 语义 |
|---|---|
| `Grant(state, grants)` | 溢出预检 → 容量预检 → 入包，返回每个道具的发放后存量 |
| `PrecheckGrant(state, grants)` | 只校验不改状态（`商店`先验后扣需要） |
| `ToItemChanges(changes)` | 转协议条目（ItemUid 占位、Change 恒 1） |

**原子性**：先全量校验再逐项入包，失败时状态零改动——所以「满包拒绝」不会留下半套奖励。

**商店的顺序**：`PrecheckGrant` → 扣费 → `Grant`。反过来做就会出现「钱扣了货没到」或「货到了钱没扣」。

顺带清掉 11 处 `MarkDirty("bag")` 冗余：`AddItem`/`RemoveItem` 内部已经 `touch("bag")`，手写这行是「靠人记得」的历史写法，漏写就是丢提交。

**扣费是对称的一半** `module.Spend`：

```go
if err := module.Spend(ctx.State, itemDiamond, price); err != nil {
    resp.Code = codeDiamondNotEnough
    return reply(ctx, msgIDBuyResp, resp)
}
```

原先四处「先查后扣」都写成两段，且**丢弃了 `RemoveItem` 的返回值**——它内部还会再校验一次，若两次判断之间状态变化（未来引入并发路径、或两个方法语义不一致），扣费会静默失败，表现为「升级成功但钱没扣」。`Spend` 把「够不够」和「扣不扣得掉」合成一个答案，失败时状态零改动；`count <= 0` 直接放行，调用方不必再写 `if cost > 0`。

**为什么 `Grant` 不返回 pb 消息**：转协议放在 `ToItemChanges`，基座主体不依赖协议；这份胶水只有一份，不必七个模块各写一遍。

#### 17.2.5 类型化处理器 `module.Typed`

`HandleR` 解决了「响应是哪个协议」，剩下的重复是每个 handler 都有的四行：

```go
var req gamev1.FriendApplyReq
if err := proto.Unmarshal(body, &req); err != nil { return fmt.Errorf(...) }
resp := &gamev1.FriendApplyResp{}
...
return reply(ctx, msgIDApplyResp, resp)
```

`Typed` 把这段收进基座，业务只写「拿到 req、填 resp」：

```go
m.HandleR(msgIDApplyReq, msgIDApplyResp, &gamev1.FriendApplyResp{},
    module.Typed(msgIDApplyResp, m.onApply))

func (m *Module) onApply(ctx *module.Context, req *gamev1.FriendApplyReq, resp *gamev1.FriendApplyResp) error {
    if req.PlayerId <= 0 {
        resp.Code = codeTargetNotFound
        return nil          // 返回 nil 即自动回包
    }
    ...
}
```

| 语义 | 说明 |
|---|---|
| `body` 为空 | 传入零值 req（列表类请求不需要手动判 `len(body)==0`） |
| 解码失败 | 返回 error、**不回包**（数据已损坏，给空响应没有意义） |
| 业务返回 error | 上抛给错误通道、不回包（与手写 `return err` 一致） |
| 返回 nil | 自动序列化 resp 并回包 |

**类型推断**：`Typed` 的四类型参数（`TReq/TResp` + 指针约束 `PReq/PResp`）都能从
`m.onApply` 的方法值推断出来，调用处不必写显式类型实参。

**为什么需要「指针约束」**：`new(T)` 只能得到结构体类型，而 proto 生成的是指针实现
`proto.Message`。用 `interface{ *T; proto.Message }` 把 T 固定成结构体，
`new(T)` 才合法、也才能断言为 proto.Message——这是 Go 泛型处理 proto 消息的标准写法。

**已迁移**：全部 24 个模块共 27 个协议。首次落地是 friend / guild / team
（各 −36~41 行，它们原先还手写 `Module` 七件套）；第二轮铺开到 bag/chat/copy/
equip/mail/partner/quest/rank/shop/skill/battle/scene（bag 同时迁到 Thin，−27 行）。
模块层现在只剩 3 处 `proto.Unmarshal(body, ...)`——它们都是**不回包**的路径，
刻意保留：

| 位置 | 为什么不用 Typed |
|---|---|
| `battle.onOpReport` | 战报单向上报，没有响应协议 |
| `scene.onEnter` / `onMove` | 请求转给场景 Actor，结果由广播异步送达；`Typed` 会自动回一个空包，是行为变更 |

**一个必须记住的边界**：`Typed` 总是回包。迁移前要确认原 handler 真的回包——
`rank` 原先有三处「内联构造响应直接 m.reply」，批量替换时被吃成 `return nil`，
靠测试（`TestListUnavailable` 断言 code=6702）才发现。机械替换必须有测试兜底。

### 17.3 跨服聊天全链路（双端历史 + Kafka 扇出）

**需求定义**：游戏服 A 的玩家与游戏服 B 的玩家私聊（跨服好友）——能实时聊天、有聊天历史。设计约束：游戏服之间不建直连、每个游戏服有自己的库。

**核心思路**（三个关键设计）：

1. **历史双端各存一份**：A 发消息 → A 的服存 `chat_record{owner=A, direction=out}`；B 的服投递时存 `chat_record{owner=B, direction=in}`。各自读各自库，**零跨库依赖**；
2. **共享 Redis 兜底双端一致性**：发送时双写 `chat:hist:{A}:{B}` 与 `chat:hist:{B}:{A}` 两个 List（capped 200 条/对）——解决「B 离线时投递丢失导致 B 侧历史缺口」，历史读取以共享 List 为近期权威；
3. **投递走 Kafka 扇出**：不建游戏服直连（守住铁律），也不压中心服（聊天是高频广播，控制面不承载）。

```
玩家A(游戏服#1) 发私聊给好友B
  → 玩家A Actor(GS#1)：
      ① 生成全局 msg_id（雪花）
      ② 写本服 MySQL chat_record{owner:A, peer:B, direction:out}（幂等键 owner_id+msg_id）
      ③ Pipeline 双写 Redis：RPUSH chat:hist:{A}:{B} 与 chat:hist:{B}:{A}（LTRIM capped 200）
      ④ 查 online:{B}：
           ├─ B 在线(GS#2) → 生产 Kafka gs.chat.cross{msg_id, to:B, from:A, content}
           └─ B 离线       → RPUSH pend:chat:{B}（B 下次登录任意服时 drain 补推）
  → GS#2 消费（广播消费 + 本地性判定，见 13.3 模式①）：
      ⑤ online:{B} 命中本服 → Tell 玩家B Actor 推送 → 写本服 chat_record{owner:B, direction:in}
         （幂等：idem:chat:{msg_id} SET NX + uk_owner_msg 唯一索引双保险）
      ⑥ 未命中本服 → 丢弃（B 已离线，由 pend 兜底）
  → B 回复 A：对称流程（GS#2 出站 + GS#1 入站）
```

**历史读取**（任意一方、随时）：

1. 玩家请求与某好友的聊天记录 → 分页读本服 MySQL `chat_record`（长期归档，按 `owner_id` 分表）；
2. 最近 200 条热数据读共享 Redis `chat:hist:{A}:{B}`（双端一致，跨服/补投场景无缺口）；
3. 登录预加载：drain `pend:chat:{pid}` → 未读消息推送客户端。

**关键点**：

- **不带业务状态，只投递**：跨服消息只传内容，各服本地状态由本地 Actor 维护，天然分治；
- **有序**：Kafka Key = 接收方 playerID，单玩家消息有序；
- **幂等**：msg_id 全局唯一，双端落库唯一索引兜底，重投不重复；
- **容量**：`chat:hist` 每对好友 200 条封顶（LTRIM），MySQL 全量归档；
- **边界**：组队/公会战等带状态的跨服交互不走此链路，一律经中心服中转（见 9.3 节）；跨服战斗见第 10 章（demo 期战斗演算在客户端，服务端只通知发起/透传/收结果，见 [4.9.3](#493-战斗职责划分demo-约定)）。

---

## 18. 优雅关闭与数据落库保证

### 18.1 关闭编排状态机

```
SIGTERM / GM 指令
   │
   ▼
┌─────────┐  etcd 摘除注册（停止被分配新流量）
│ DRAIN   │  网关：停止 accept，向客户端推送「服务器维护中，请重连」后关闭连接
└────┬────┘  游戏服：停止接受新 Bind；中心服：停止接受新 Report 连接
     ▼
┌─────────┐  等待存量会话自然结束（默认最多 30s，超时强制进入下一阶段）
│ DRAINING│
└────┬────┘
     ▼
┌─────────┐  游戏服：逐个玩家 Actor 强制 Flush：
│ FLUSH   │    ① 内存 dirty → Redis（快）② Redis dirty → MySQL UPSERT（同步等待确认）
│         │  ③ flusher 队列排空  ④ Kafka Producer Flush 完缓冲区
└────┬────┘
     ▼
┌─────────┐  关闭 gRPC Server（GameService.Channel / CenterService.Report 发 GoAway，
│ STOP    │  对端收到后重连其它实例）→ 关闭 Kafka Consumer（提交 offset）→ 退出进程
└─────────┘
```

### 18.2 落库保证的四道防线

| 防线 | 机制 | 防什么 |
| --- | --- | --- |
| ① Redis 先行 | 所有玩家写先落 Redis 热数据 + dirty 标记，进程崩溃 Redis 仍在 | 进程崩溃（kill -9） |
| ② 幂等 UPSERT | flusher 落库一律 `UpsertCols`（主键冲突更新指定列），重复执行无副作用 | 重试导致的重复写 |
| ③ 落库重试队列 | MySQL 写失败 → 本地重试 3 次（指数退避）→ 仍失败写 Kafka `gs.flush.retry` → 重启后消费补偿 | MySQL 瞬断/宕机 |
| ④ 关闭屏障 | 进程退出前必须满足：所有 Actor Flushed ✓ + flusher 空闲 ✓ + Kafka Producer Flushed ✓；不满足则等待（默认 60s 硬超时 + 错误日志 + 告警） | 优雅关闭不完整 |

### 18.3 关键实现

```go
// pkg/shutdown/shutdown.go
type Manager struct {
    steps []Step // 按注册顺序执行，每步可设超时
}

// 游戏服注册的关闭步骤（internal/game/server.go）
func (s *Server) registerShutdown() {
    sm := shutdown.NewManager(60 * time.Second)
    sm.Add("deregister-etcd", s.deregister, 5*time.Second)
    sm.Add("stop-new-bind",  s.rpcServer.StopAccept, 5*time.Second)
    sm.Add("flush-players",  s.playerMgr.FlushAll, 30*time.Second) // 屏障核心
    sm.Add("drain-flusher",  s.flusher.Drain, 10*time.Second)
    sm.Add("flush-kafka",    s.mq.FlushAll, 5*time.Second)
    sm.Add("stop-grpc",      s.rpcServer.GracefulStop, 5*time.Second)
    sm.Wait() // 阻塞直到全部完成或总超时，返回每步结果供日志/告警
}
```

```go
// internal/game/player/persist.go —— FlushAll 逐玩家同步确认
func (m *PlayerManager) FlushAll(ctx context.Context) error {
    // 向每个玩家 Actor 发 FlushReq 并等待 FlushAck（Call 模式，带超时）
    // 任一玩家超时：重试一次；仍失败 → 记录未落库玩家清单，写 Kafka gs.flush.retry
}
```

中心服/跨服节点的关闭步骤分别见 9.7 节与 10.5 节。

### 18.4 发布期零感知重启流程

```
滚动更新一台游戏服：
① GM 指令 / K8s preStop：摘 etcd 注册
② 网关 Watch 到实例消失 → 新 Bind 不再分配给它；存量会话保留
③ 该服执行关闭编排：Flush 全部玩家 → 但**不踢下线**（会话仍挂网关）
④ 网关发现其 gRPC Channel 断开 → 存量会话标记 RECONNECTING
⑤ 客户端 90s 内重连 → 分配到新游戏服 → 从 Redis/MySQL 恢复 → 玩家无感
```

---

## 19. 部署架构

### 19.1 服务器配置（YAML）示例

**端口规划（全服务统一，替代散落各处的硬编码）**：

| 端口 | 归属 | 用途 |
| --- | --- | --- |
| 8080 / 8081 | 登录服 | HTTPS API / metrics |
| 9001 / 9101 | 网关 | 客户端 TCP 长连接 / metrics |
| 9100 / 9102 | 游戏服 | GameService gRPC / metrics |
| 9200 / 9201 | 中心服 | CenterService gRPC / metrics |
| 9300 / 9301 | 跨服节点 | CrossService gRPC / metrics |
| 2379 / 2380 | etcd | 客户端 / Peer 通信 |
| 6379 | Redis | Cluster 各节点同端口 |
| 3306 | MySQL | 主从同端口 |
| 9092 / 9093 | Kafka | 客户端 / Controller |

```yaml
# configs/gateway/gateway-prod.yaml
server:
  name: gateway-01
  env: prod
  tcp_addr: "0.0.0.0:9001"
  max_conns: 100000            # 单网关最大连接（超限拒绝新连接）
  max_packet_size: 65536       # 单包上限 64KB
  read_timeout: 45s
  write_queue_size: 1024       # 单连接写队列深度
  session_reconnect_ttl: 90s    # 断线会话保留期

crypto:                        # RSA+AES 配置
  enabled: true
  rsa_private_key: "/etc/game_study/gateway_rsa.pem"
  rsa_key_id: "k1"
  prev_rsa_public_key: ""       # 轮换期上一把公钥

rpc:                           # gRPC 客户端（连游戏服）
  etcd_endpoints: ["etcd:2379"]
  service_prefix: "/game_study/game"
  channel_per_target: 16        # 每目标 Channel 数
  max_streams_per_channel: 2000 # 单 Channel 最大并发流
  dial_timeout: 3s
  keepalive: 10s

redis:
  addrs: ["redis:6379"]
  pool_size: 100              # Token/在线状态查询为主，单连接耗时 <1ms
  min_idle_conns: 20          # 必须 ≤ pool_size（原 100 > 80 为无效配置）

kafka:
  brokers: ["kafka:9092"]
  producer_required_acks: all
  consumer_group: "gateway-broadcast"

etcd:
  endpoints: ["etcd:2379"]
  lease_ttl: 10
  keepalive_interval: 3

log:
  level: info
  format: json
  sample_first: 100            # 每秒同日志打点前 100 条全量，其余采样

metrics:
  enable: true
  addr: "0.0.0.0:9101"

shutdown:
  total_timeout: 60s
  drain_timeout: 30s
  flush_timeout: 30s
```

```yaml
# configs/game/game-prod.yaml（差异部分）
server: { name: game-01, env: prod, grpc_addr: "0.0.0.0:9100" }
actor:
  shard_count: 1024            # 2 的幂
  mailbox_size: 4096
mysql:
  dsn: "game:******@tcp(mysql:3306)/game_study?parseTime=true"
  max_open_conns: 30           # 业务池 30（PK 点查 <1ms，推导见第 14 章）；flush 10 worker 错峰共享借还（见 12.4）
  max_idle_conns: 10
kafka:
  consumer_group: "game-server-01"   # 每实例独立组，分区按 playerID 有序
```

```yaml
# configs/center/center-prod.yaml
server: { name: center-01, env: prod, grpc_addr: "0.0.0.0:9200" }
etcd:
  endpoints: ["etcd:2379"]
  lease_ttl: 10
  keepalive_interval: 3
  leader_key: "/game_study/leader/center"   # 选主
report:
  heartbeat_interval: 5s        # 期望节点心跳间隔
  heartbeat_timeout: 15s        # 超时判定不可用
  relay_retry: 5                # 中转重投次数
rank:
  snapshot_interval: 5m         # 排行榜快照间隔
  archive_cron: "0 0 * * *"     # 赛季归档（leader 执行）
stats:
  aggregate_windows: [1m, 5m, 1h, 1d]
  snapshot_interval: 5m
  mysql_shard: 10               # 统计落库分片
relay_kafka:                    # 中心服集群间中转
  brokers: ["kafka:9092"]
  topic: "center.relay"
shutdown:
  total_timeout: 30s
```

```yaml
# configs/game/game-prod.yaml（中心服上报与落库新增部分）
center:
  addr: "center:9200"                    # 多地址逗号分隔，故障自动切换
  reconnect_max_wait: 30s
  report_buffer: 4096                    # 上报本地队列（积压时降频丢弃统计类消息）

flush:                                    # 10 分片分时落库（worker 共享连接池）
  shard_count: 10
  interval: 60s
  stagger: 6s
  batch_rows: 500
  dirty_ttl: 10m

idtab:
  build_on_start: true          # 启动构建 Redis ID 表
  build_page_size: 5000
  reconcile_interval: 10m       # 数量对账
```

```yaml
# configs/cross/cross-prod.yaml（复用 game 的 actor/flush 配置，差异部分）
server: { name: cross-01, env: prod, grpc_addr: "0.0.0.0:9300" }
# 注意：无 etcd 段（不注册，无需被网关发现）、无 tcp_addr（不连网关）
center: { addr: "center:9200", report_buffer: 2048 }
battle:
  room_max_idle: 300s           # 房间最大空闲（超时销毁）
  result_retry: 5               # BattleResult 推送重试（经 CrossService 流直推参战游戏服）
xservice:                       # CrossServer 侧被游戏服连接，无需主动拨号
  max_streams_per_game: 1       # 每个游戏服维持一条 CrossService 流
```

```yaml
# configs/login/login-prod.yaml
server:
  name: login-01
  env: prod
  http_addr: "0.0.0.0:8080"
  tls_cert: "/etc/game_study/login.pem"
  tls_key: "/etc/game_study/login.key"

token:
  ttl: 24h
  rsa_private_key: "/etc/game_study/login_rsa.pem"   # 签发 Token 用，与网关握手密钥分离

redis:
  addrs: ["redis:6379"]
  pool_size: 50

mysql:                          # 账号/角色元信息（Tier-1 直连）
  dsn: "game:******@tcp(mysql:3306)/game_study?parseTime=true"
  max_open_conns: 20
```

### 19.2 机器规划（生产环境参考）

| 机器组 | 规格 | 部署内容 | 数量 |
| --- | --- | --- | --- |
| 接入层 | 4C8G × N + SLB | Nginx（TLS 终止）→ 登录服 | 2+ |
| 接入层 | 8C16G | 网关服（10W 连接/台） | 按连接数水平扩展 |
| 逻辑层 | 8C16G | 游戏服（5K~1W 在线/台） | 按在线人数水平扩展 |
| 控制层 | 4C8G | 中心服（集群 ≥2，etcd 选主） | 2~3 |
| 跨服层 | 8C16G | 跨服节点 | 按跨服玩法负载 |
| 中间件 | 按容量规划 | etcd ×3、Redis Cluster ×6、MySQL 主从、Kafka ×3 | 独立机器组 |

> **MySQL 连接容量**：`max_connections` 是**实例级**参数（默认 151，所有库共享，不是每库 151）。按上表 10 台游戏服 × 30 + 登录服 20 + 中心服 20 ≈ 340 连接，生产必须设置 `max_connections ≥ 500` 并预留监控余量（推导见第 14 章「MySQL 连接数容量规划」）。

### 19.3 Makefile（生成与构建入口）

```makefile
.PHONY: gen gen-proto gen-data gen-db keygen db-init db-seed build test lint run-all dev

gen: gen-proto gen-data gen-db          # 一键生成全部代码

gen-proto:            # proto → pkg/pb（buf）
	cd api && buf generate

gen-data:             # XML 游戏数据 → pkg/gamedata（全量覆盖 *_gen.go）
	go run ./tools/gendata -in configs/gamedata/xml -out pkg/gamedata

gen-db:               # DDL → pkg/model（覆盖 *_gen.go，仅新建 *_model.go）
	go run ./tools/gendb -in sql/schema -out pkg/model

keygen:               # 生成网关握手 RSA 密钥对（见 19.5 部署清单）
	go run ./tools/keygen -out deploy/keys

db-init:              # DDL → MySQL 建表（首次部署）
	./scripts/db-init.sh

db-seed:              # sql/seed → 初始数据（道具/等级等安全表）
	./scripts/db-seed.sh

build:                # 交叉编译五服务
	./scripts/build.sh

test:  go test ./...  # 单测 + 集成（test/integration）
lint:  ./scripts/lint.sh   # golangci-lint + buf lint + gen 一致性检查

dev:    docker compose -f deploy/docker-compose.yaml up -d   # etcd/redis/mysql/kafka
run-all: # 本地跑 login+gateway+game+center+cross 五进程
```

### 19.4 本地全栈 docker-compose

```yaml
services:
  etcd:   { image: bitnami/etcd:3.5, ports: ["2379:2379"],
            healthcheck: { test: ["CMD", "etcdctl", "endpoint", "health"], interval: 5s, retries: 12 } }
  redis:  { image: redis:7-alpine, ports: ["6379:6379"],
            healthcheck: { test: ["CMD", "redis-cli", "ping"], interval: 5s, retries: 12 } }
  mysql:  { image: mysql:8.0, ports: ["3306:3306"],
            environment: { MYSQL_ROOT_PASSWORD: game_study, MYSQL_DATABASE: game_study },
            volumes: ["./volumes/mysql:/var/lib/mysql"],
            healthcheck: { test: ["CMD", "mysqladmin", "ping", "-h", "127.0.0.1"], interval: 5s, timeout: 3s, retries: 20 } }
  kafka:  { image: bitnami/kafka:3.7, ports: ["9092:9092"],
            environment: { KAFKA_CFG_NODE_ID: "1", KAFKA_CFG_PROCESS_ROLES: "controller,broker", KAFKA_CFG_CONTROLLER_QUORUM_VOTERS: "1@kafka:9093" },
            healthcheck: { test: ["CMD", "kafka-topics.sh", "--list", "--bootstrap-server", "localhost:9092"], interval: 10s, retries: 12 } }
  login:  { build: ./docker/login.Dockerfile,  ports: ["8080:8080"],
            volumes: ["./configs:/app/configs:ro", "./deploy/keys:/app/keys:ro"],
            depends_on: { mysql: { condition: service_healthy }, redis: { condition: service_healthy } } }
  gateway:{ build: ./docker/gateway.Dockerfile, ports: ["9001:9001"],
            volumes: ["./configs:/app/configs:ro", "./deploy/keys:/app/keys:ro"], depends_on: [login] }
  game:   { build: ./docker/game.Dockerfile,   ports: ["9100:9100"],
            volumes: ["./configs:/app/configs:ro", "./deploy/keys:/app/keys:ro"],
            depends_on: { mysql: { condition: service_healthy }, redis: { condition: service_healthy }, kafka: { condition: service_healthy } } }
  center: { build: ./docker/center.Dockerfile, ports: ["9200:9200"],
            volumes: ["./configs:/app/configs:ro"], depends_on: [etcd] }
  cross:{ build: ./docker/cross.Dockerfile,ports: ["9300:9300"],
            volumes: ["./configs:/app/configs:ro"], depends_on: [center] }
```

> **首次运行**：`make dev` 只起中间件（etcd/redis/mysql/kafka），需先执行 `make db-init && make db-seed` 建表灌数，再 `make run-all` 按 19.5 顺序起五进程。`volumes/` 持久化 MySQL 数据，删除后数据丢失；配置与密钥经 `./configs`、`./deploy/keys` 只读挂载进容器（密钥由 `make keygen` 生成，见 19.5 清单第 2 步）。

### 19.5 进程启动顺序与首次部署清单

**依赖链**（任一环节失败，后续服务重试而非退出）：

```
etcd → redis/mysql/kafka → center → game / cross → gateway → login
```

| 顺序 | 进程 | 前置条件 | 启动要点 |
| --- | --- | --- | --- |
| 1 | etcd | 无 | 集群 ≥3，后续所有服务发现依赖它 |
| 2 | redis / mysql / kafka | — | 先 `make db-init`（建表）+ `make db-seed`（灌初始数据） |
| 3 | center | etcd + mysql | 选主 key `/game_study/leader/center`；就绪后开始接受 Report 流 |
| 4 | game / cross | etcd + mysql + kafka | 游戏服注册 etcd、构建 ID 表、连中心服上报；**CrossServer 只连中心服**（不注册 etcd、不连网关），等中心服 `GroupUpdate` 下发组内 CrossServer 地址后建 CrossService 流 |
| 5 | gateway | etcd（Watch `/gs/game/`） | 解析到 ≥1 游戏服才接受客户端连接 |
| 6 | login | mysql + redis | 签发 Token，客户端登录链路最后才经过它，可最后启动 |

**首次部署清单**：

1. `make gen` 生成三大代码产物，`git status` 确认无 diff（与 CI 卡点一致）；
2. `make keygen` 生成网关握手 RSA 密钥对（私钥进网关配置，公钥由登录服随 Token 下发）；
3. 拉起中间件（compose 或生产机器组），确认全部健康检查通过；
4. `make db-init && make db-seed` 建表 + 灌初始数据（含道具/等级等安全表）；
5. 按上表顺序启动六类进程，逐层验证：etcd 出现注册 → 中心服选主完成 → 游戏服心跳上报 → 网关 Channel 池就绪 → 登录服可签发 Token；
6. 冒烟测试：客户端登录 → 进游戏 → 发一条聊天（验证 Kafka 扇出 + 双端历史）→ 触发一次落库（观察 flusher 指标）→ 触发一次跨服匹配（验证 CrossService 流建立、`BattleStartNotify` 下发与 `BattleResultReport` 上报；战斗演算在客户端，见 [4.9.3](#493-战斗职责划分demo-约定)）；
7. 压测基线入库：`test/benchmark` 输出网关连接数 / 消息 QPS / 落库延迟基线，作为后续扩容依据（配合 [19.2](#192-机器规划生产环境参考) 容量表）。

### 19.6 生产 K8s 编排规划

**K8s（Kubernetes）是什么、解决什么问题**：它是容器编排平台——把 Docker 镜像编排成一组自愈的服务：进程挂了自动重启、机器故障自动漂移、按负载自动扩缩、滚动发布不中断。对本框架的价值：五类服务 + 中间件全部声明式管理，发布从「登机器跑脚本」变成 `kubectl apply`，扩容/缩容/故障恢复有统一控制面。本地开发仍用 19.4 的 docker-compose（更轻），K8s 只用于生产与预发。

**部署形态**（有状态/无状态分流，这是游戏服务上 K8s 的核心决策）：

| 服务 | 工作负载 | 副本 | 扩缩方式 | 关键配置 |
| --- | --- | --- | --- | --- |
| 网关 | Deployment（无状态） | 3+ | **HPA**（CPU 60%） | preStop 排水 30s（见 18.4） |
| 登录服 | Deployment（无状态） | 2+ | HPA（CPU/QPS） | — |
| 游戏服 | StatefulSet（**有状态**：Actor 绑定玩家，不做热迁移） | 固定 10~20 | **手动/脚本扩缩**（不用 HPA，缩容先公告） | `terminationGracePeriodSeconds ≥ 120s`（覆盖优雅关闭全流程，见第 18 章） |
| 中心服 | StatefulSet（选主需稳定网络标识） | 2~3 | 固定 | PDB `minAvailable: 2` |
| 跨服节点 | Deployment（房间纯内存，可弃） | 按跨服负载 | 手动 | 房间空闲 5 分钟自动销毁，缩容对玩家无感知 |

**中间件**：etcd/Redis/MySQL/Kafka 不手写 Deployment——用官方 Operator（etcd Operator、Redis Operator、MySQL Operator、Strimzi Kafka）或云托管 RDS，自带故障自愈、备份与版本升级。

**配置与密钥**：`configs/*.yaml` 进 ConfigMap（按环境 kustomize overlay）；RSA 私钥、MySQL 密码进 Secret（挂载为文件，配置里的 `rsa_private_key` 路径指向 Secret 卷，禁止打进镜像）。

**探针配置**（对应审查项 B12）：

| 探针 | 登录/网关 | 游戏服/跨服节点 | 中心服 |
| --- | --- | --- | --- |
| startupProbe | HTTP `/healthz` | TCP gRPC 端口 | TCP gRPC 端口 |
| readinessProbe | HTTP `/healthz` | etcd 注册完成 + idtab ready（启动构建完成才就绪，见 12.5） | etcd 注册 + 选主完成 |
| livenessProbe | HTTP `/healthz` | actor 分片心跳 | 主循环心跳 |

**目录规划**（`deploy/k8s/`，补全原空目录）：

```
deploy/k8s/
├── base/                        # 公共清单
│   ├── namespace.yaml
│   ├── configmaps/{gateway,game,center,cross,login}.yaml
│   ├── secrets/{tls,rsa,mysql}.yaml        # 生产建议 external-secrets / sealed-secrets
│   └── middlewares/             # Operator CR（etcd/redis/mysql/kafka）
├── overlays/
│   ├── dev/                     # kustomize overlay（副本数、资源规格按环境差异）
│   └── prod/
└── workloads/
    ├── gateway/{deployment,hpa,service,pdb}.yaml
    ├── login/{deployment,hpa,service}.yaml
    ├── game/{statefulset,service}.yaml
    ├── center/{statefulset,service,pdb}.yaml
    └── cross/{deployment,service}.yaml
```

> **落地顺序**：P7 阶段先把网关/登录服迁上 K8s（无状态、收益快）；游戏服/中心服等优雅关闭（第 18 章）验收通过后再迁——有状态服务上 K8s 的前提是关闭编排可靠，否则滚动更新就是掉线潮。

---

## 20. 非功能性设计

### 20.1 性能指标

| 指标 | 目标值 | 手段 |
| --- | --- | --- |
| 网关单机连接 | 10W+ 长连接 | goroutine 轻量、写队列背压、`ulimit -n` 调优 |
| 游戏服单机在线 | 5K~1W 玩家 | Actor 分片（1024 shard）、单实体单线程无锁 |
| 消息端到端延迟 | P99 < 50ms（同机房） | 长连接 + 直连游戏服 + 内存态处理 |
| 落库批量 | 单分片周期 3000 脏实体 → ~6 条 UPSERT | 10 worker 分时合批（12.4 节） |
| 登录耗时 | P99 < 2s | 预加载全并行 + 合批回源（12.6 节） |

### 20.2 可靠性指标

| 指标 | 目标值 | 手段 |
| --- | --- | --- |
| 服务可用性 | 99.9%（全年停机 < 8.7h） | 五类服务集群化 + etcd 摘除 + 滚动重启 |
| 数据丢失窗口 | Tier-1 零丢失；Tier-2 ≤1 个落库周期 | 四道防线（18.2 节） |
| 故障切换时间 | 玩家无感（90s 重连保留期） | 在线状态外置 Redis + 会话重绑 |
| 中心服故障 | 中转/排行榜写入不受影响（集群） | 无主部分水平扩展 + 跨实例 Kafka 中转 |

### 20.3 可观测性

| 维度 | 方案 |
| --- | --- |
| 日志 | zap 结构化日志，全链路 TraceID（playerID + sessionID 贯穿五服务）；关键操作（登录/充值/创角/落库失败）强制打点 |
| 指标 | Prometheus：连接数、QPS、Actor 队列深度/延迟、Channel 池水位、flusher 分片延迟/批量大小/失败率、Kafka 消费 lag、idtab 对账差异 |
| 追踪 | 关键链路（登录→Bind→预加载→进入场景、充值全链路）埋 span，接入 Jaeger/Tempo |
| 告警 | 落库连续失败、Actor 队列积压、Kafka lag 超阈值、idtab 对账不一致、节点心跳超时 |

#### 20.3.1 指标落地（P7 已实现）

指标实现在 `pkg/metrics`，五服务在 `Start()` 中按 `metrics.enable` 挂载 `/metrics`、`/healthz`、`/readyz` 三个端点。

**探针语义**（对应 19.6 探针表）：

| 端点 | 语义 | 未通过时 K8s 动作 |
| --- | --- | --- |
| `/healthz` | 进程存活，**不查依赖** | liveness 失败 → 重启 Pod |
| `/readyz` | MySQL/Redis 可用 | readiness 失败 → 从 Endpoints 摘除，不重启 |
| `/metrics` | Prometheus 抓取 | — |

> 存活不查依赖是刻意设计：中间件短时抖动不应引发全部 Pod 重启风暴，只摘流量等依赖恢复。

**指标清单**（namespace `gs`，全部带 `service` 标签）：

| 指标 | 类型 | 说明 | 告警阈值 |
| --- | --- | --- | --- |
| `gs_gateway_connections` | Gauge | 当前活跃连接数 | > 8.5W（容量 10W） |
| `gs_gateway_connections_total{result}` | Counter | 建连/拒绝累计 | 拒绝速率突增 |
| `gs_message_total{direction,result}` | Counter | 消息数与结果 | 错误率 > 1% |
| `gs_message_duration_seconds` | Histogram | 端到端耗时 | P99 > 50ms |
| `gs_message_size_bytes` | Histogram | 消息体大小 | — |
| `gs_actor_mailbox_depth{shard}` | Gauge | 分片信箱积压 | > 1000 |
| `gs_actor_handle_duration_seconds` | Histogram | Actor 处理耗时 | P99 观察 |
| `gs_actor_processed_total{result}` | Counter | 处理量 | — |
| `gs_actor_panic_total` | Counter | panic 次数（已 recover） | > 0 |
| `gs_actor_rejected_total` | Counter | 信箱满拒绝的新消息数（背压；调用方收到 ErrMailboxFull，旧消息保留） | > 0 |
| `gs_actor_count` | Gauge | 存活 Actor 数（≈在线玩家） | — |
| `gs_flush_dirty_entities{shard}` | Gauge | 待落库实体数 | > 5000 |
| `gs_flush_lag_seconds{shard}` | Gauge | 落库延迟 | > 120s（2 个周期） |
| `gs_flush_failures_total{shard}` | Counter | 落库失败次数 | > 0 |
| `gs_flush_duration_seconds` | Histogram | 单轮落库耗时 | — |
| `gs_flush_rows_total{result}` | Counter | 落库行数 | — |
| `gs_channel_streams` | Gauge | 活跃 stream 数 | 观察 |
| `gs_channel_pool_size` | Gauge | Channel 池大小 | — |
| `gs_channel_errors_total{op}` | Counter | 建流/发送失败 | 突增 |
| `gs_kafka_consumer_lag{topic}` | Gauge | 消费 lag | > 10000 |
| `gs_kafka_consumed_total{topic,result}` | Counter | 消费数（ok/retry/dead） | dead > 0 |
| `gs_kafka_produced_total{topic,result}` | Counter | 生产数 | — |

**配套文件**：

| 路径 | 用途 |
| --- | --- |
| `deploy/monitoring/prometheus.yml` | compose 场景抓取配置（五服务各自独立 job） |
| `deploy/monitoring/rules/gs-alerts.yml` | 告警规则（9 条，对应 20.3 告警项） |
| `deploy/monitoring/grafana/` | 看板（`gs-overview`，5 个分区 15 个面板） |
| `deploy/k8s/monitoring/servicemonitor.yaml` | K8s 场景 ServiceMonitor + PrometheusRule |

启动监控栈：`make monitoring` → Grafana `http://localhost:3000`（admin/admin）。

### 20.4 容量与限流

| 层级 | 限流手段 |
| --- | --- |
| 接入层 | Nginx 连接数/速率限制；网关 `max_conns` 超限拒绝新连接 |
| 协议层 | 单包 64KB 上限；单玩家消息速率限制（如聊天 5 条/10s） |
| 后端层 | gRPC Channel 最大并发 stream 2000；游戏服 Actor 信箱有界（4096）+ 背压 |
| 存储层 | MySQL 10 分片连接上限；Redis Pipeline 批量；Kafka 批量生产 |

---

## 21. 开发路线图

| 阶段 | 主题 | 交付物 |
| --- | --- | --- |
| **P0** | 工程骨架 + 生成体系 | 目录结构（第 3 章）、Makefile、buf/gendata/gendb 三大生成器、CI 生成一致性卡点、单元测试骨架（见 [23.2](#232-关键单元测试点)） |
| **P1** | 加密接入链路 | RSA+AES 握手（第 15 章）、登录服 HTTP、网关连接/会话、HTTP→加密长连接全流程 |
| **P2** | gRPC 内部通信 | GameService.Channel 双向流、etcd Resolver、Channel 池、stream 管理 |
| **P3** | 游戏服内核 | Actor 分片框架、玩家生命周期、基础模块（attr/bag/item） |
| **P4** | 数据层 | gendb CRUD 接入、Cache-Aside + Write-Behind、flusher、优雅关闭编排 |
| **P5** | Kafka 异步链路 | 充值、邮件、跨服聊天、事件流、幂等消费 |
| **P6** | 业务功能 | 按第 17 章 21 项功能逐个落地（每项 = XML 配置 + DDL + proto + module）。**已完成**：创角/进游戏/角色信息/背包/邮件、等级经验、任务、装备、技能、聊天（私聊全链路）、公会、组队、副本、商店、活动 |
| **P7** | 可观测与部署 | Prometheus/告警、K8s、压测（[23.4](#234-压测与验收指标)）、集成/E2E 与故障演练（[23.3](#233-集成测试testcontainers-主链路)/[23.5](#235-故障演练清单)）、滚动重启演练。**已完成**：`pkg/metrics`（22 项指标 + /metrics、/healthz、/readyz）、告警规则 9 条、Grafana 看板、K8s 探针改造、压测工具 `tools/loadtest`、冒烟与 kill -9 演练脚本 |
| **P8** | 中心服骨架 | `internal/center/` + `center.proto` + Report 流 + 节点注册表 + 心跳 + etcd 选主。**已完成**（选主除外）：节点注册表/心跳超时摘除、Report 流首帧 Register、跨服匹配（elo 分桶 + 超时放宽 + room_id/token 签发）、中转路由（点对点/组播 + 退避重试）、GroupUpdate 下发、GM 查询接口、KickNode 优雅关闭。etcd 选主随 P14 全服玩法一并落地 |
| **P9** | 中转能力 | RelayRequest/RelayPush、路由表、广播、跨实例 Kafka 中转。**已完成**：`relay.Bridge`（每实例独立消费组 + origin 去重 + 只投本地节点）、分区键保证同目标有序、start offset 可选、转发/消费指标 |
| **P10** | ID 表与回源 | `pkg/cache/{idtab,exist,shard}`、启动构建、对账补偿、查询矩阵落地。**已完成**：`cache.IDTable`（keyset 构建 + building RENAME 原子切换 + Exists 降级 + Reconcile + CleanOrphans + LoadEntity 回源）、创角同步登记、装备查询前置判存在、四类指标 |
| **P11** | 分时落库 | `pkg/flush/` 10 worker 分散轮询（共享连接池）、合批 UPSERT、tombstone、指标。**已完成**：唤醒对齐分钟边界、`BuildBatchUpsert/Delete/DeleteWhere` + 行数/字节双约束切批、道具改「增量 + 合批」、墓碑队列与合批删除、tombstone 指标；顺带补 `player_item` 唯一键 |
| **P12** | 登录预加载 | 并行 Pipeline 预加载 + miss 合批回源 + 超时保护。**已完成**：5 表并行拉取（player/attr/item/equip/activity）、5s 超时保护、预加载耗时指标、模块经 `Context.Prefetch` 复用结果、离线暂存队列登录补投（1011 之后） |
| **P13** | 小组跨服 | `internal/cross/`、CrossService 流中转（无会话迁移）、分组管理、匹配、战斗结果直推参战游戏服。**已完成**：房间 Actor（入房/开战/扇出/结算/超时兜底）、房间管理器（建房/drain）、CrossService.Channel（BIND 握手/精确回推）、游戏服连接池（退避重连）、battle 模块（匹配/透传/上报）、客户端 5001-5021 全链路 |
| **P14** | 全服玩法 | 排行榜上报/查询/通知、分组统计、全服进度活动。**已完成**：Redis ZSet 排行榜（ZINCRBY 增量/Top-N/周边/赛季隔离/归档）、战斗结算自动上榜、四档窗口内存统计聚合 + Redis 快照、etcd 选主（含优雅让位）、GM 查询接口接上实现。**未做**：RankNotify 下发客户端（协议待定）、MySQL 归档（P15）、全服进度活动里程碑 |
| **P15** | 全链路演练 | 中心服宕机演练、跨节点迁移演练、落库延迟压测、优雅关闭验收。**已完成**：压测工具四模式（conn/login/biz/smoke）+ 分位统计、`make bench` 一键压测出报告、`make drill-suite` 四项故障注入（MySQL/Redis/Kafka/etcd 共 15 项检查）、kill -9 数据完整性演练、演练报告 `docs/P15-全链路演练报告.md`。**演练暴露并修复 4 个问题**：网关读错 etcd 配置段导致会话绑不到游戏服、握手 salt 被换行吃掉导致 1.3% 登录失败、idtab 空表 RENAME 失败、Actor 100ms 轮询把 P50 钉在 100ms |

> 建议节奏：P0-P7 先把「三服务主链路」跑通并上线；P8-P15 每阶段独立可交付，按玩法需求插排。

---

## 22. 风险与技术权衡

| # | 风险/权衡 | 影响 | 应对 |
| --- | --- | --- | --- |
| 1 | **Actor 有状态扩容非热迁移**：扩容窗口是「玩家未登录」，存量玩家不迁移 | 缩容/机器下线时玩家需掉线重连 | 90s 重连保留期 + 数据外置 Redis，做到「准无缝」；提前公告缩容 |
| 2 | **Tier-2 最终一致有秒级延迟**：极端场景（进程崩溃 + Redis 同时故障）丢失 ≤1 周期数据 | 玩法数据小概率回退 | 敏感玩法（货币消耗）单独走 Tier-1 同步双写；对账补偿兜底 |
| 3 | **中心服是跨服单点逻辑枢纽**（集群化后跨实例走 Kafka 中转） | 中心服全集群故障时跨服功能不可用（本服玩法不受影响） | 集群 ≥2 实例 + Kafka 削峰；控制面与数据面分离，故障时本服体验降级但不中断 |
| 4 | **分时落库 60s 周期**：实时性要求高的数据（如全服排行榜分数）有延迟 | 榜单秒级刷新做不到 | 排行榜由中心服独立链路维护（ZSet 近实时），不走 flusher |
| 5 | **跨服战斗多一跳中转**：战斗消息经主游戏服 CrossService 流转发，比网关直连多一次内网转发 | 高频战斗消息（如 10 条/秒/房间）有额外内网延迟与带宽 | 已权衡：取消会话迁移换取网关零改动与实现简洁；若实测延迟不可接受，退化评估「网关直连 CrossServer」方案（见 [10.4](#104-备选方案与权衡)） |
| 6 | **配置热更新风险**：GM 热更新配置可能引发运行时不一致 | 线上故障难复现 | 配置版本号贯穿日志；热更新仅限安全表（如活动），核心表（道具/等级）仍需重启 |
| 7 | **Kafka 跨服聊天允许丢消息** | 极端场景世界聊天丢几条 | 带序号，客户端去重 + 拉取最近 N 条补偿 |
| 8 | **前期过度设计风险**：五服务 + 中心服 + 跨服节点一次性全上，工期失控 | 交付延期 | 按 P0-P15 路线图分阶段交付，每阶段可独立上线验证 |

**核心权衡总结**：

1. **一致性 vs 性能**：只有充值/角色走强一致（贵但必须），玩法数据全部内存+Redis（快但最终一致）——按数据价值分级，不一刀切；
2. **实时性 vs 简洁性**：跨服战斗数据面直连网关（多一跳都不能接受），控制面走中心服（规则集中）——控制面与数据面分离；
3. **批量 vs 实时落库**：10 worker 分时合批把 MySQL 写负载摊平到整分钟（避免周期性尖刺），代价是 ≤60s 落库延迟；
4. **简洁扩容 vs 无缝体验**：不做运行时热迁移，用「掉线重连 + 数据外置」换实现简洁与系统稳定。

---

## 23. 测试策略

测试目标：**demo 期用最小成本守住「主链路不坏」**——登录 → 进游戏 → 聊天 → 落库 → 跨服匹配五步冒烟随时可跑；核心不变量（幂等、背压、编解码、合批）有单测看护；集成测试用真实中间件（testcontainers）而非 mock。本章与 [16.4](#164-生成物管理约定) 的 CI 卡点、[19.5](#195-进程启动顺序与首次部署清单) 的冒烟清单衔接。

### 23.1 测试金字塔（分层与配比）

| 层级 | 占比 | 工具 | 运行时机 | 覆盖对象 |
| --- | --- | --- | --- | --- |
| 单元测试 | ~70% | `go test` + testify | 每次提交（CI） | 纯函数、Actor 逻辑、编解码、幂等、合批 |
| 集成测试 | ~20% | testcontainers（真实 MySQL/Redis/Kafka/etcd） | 每次提交 + 每晚全量 | 跨模块链路、DB/缓存/MQ 交互 |
| E2E 冒烟 | ~5% | 机器人客户端（`test/e2e`，`-tags=e2e`） | 每日 + 发版前 | 全链路五步冒烟（19.5 节清单） |
| 压测 | ~5% | `test/benchmark` 自研客户端 | 里程碑（P7） | 容量验证、瓶颈定位 |

CI 中的测试部分即 [16.4](#164-生成物管理约定) 五步卡点的第 ③④ 步（`golangci-lint`、`go test ./...` 含 `test/integration`）。Actor 相关包强制 `-race`——无锁共享状态是框架最大风险点，竞态必须在 CI 层拦掉，不能留到压测期偶遇。

### 23.2 关键单元测试点

| 模块 | 测试点 | 通过标准 |
| --- | --- | --- |
| `pkg/actor` | 信箱有界 + 背压 | 慢消费者打满信箱 → 拒绝策略生效，进程不 OOM、有指标可观测 |
| `pkg/actor` | 分片路由确定性 | 同 key 十万次路由落同一分片；分片数变更时迁移行为可预期 |
| `pkg/codec` | 编解码 roundtrip | 反射遍历全部 proto 消息 encode→decode 相等 |
| `pkg/crypto` | AES 防重放 | 重放旧包（同 AAD 旧 Seq）→ 拒绝；AAD={Length,MsgID,Seq,session_id} 任一变 → 拒绝 |
| `pkg/flush` | 合批 UPSERT | N 次 markDirty → 1 次批量写；空批次不产生 SQL |
| `pkg/flush` | 分片错峰 | 10 worker 唤醒时刻间隔 6s（注入时钟验证，见 [12.4](#124-数据落库架构10-分片落库-worker)） |
| `pkg/cache/idtab` | 9 种组合 | 存在/不存在 × 内存/Redis/DB 三源全部命中预期 |
| `module/recharge` | 订单幂等 | 同 order_id 重复投递 → 第二次不生效 |
| `module/mail` | 附件领取幂等 | 同 mail_id 重复领取 → 第二次返回 5002 |
| `module/chat` | msg_id 幂等 | Kafka 重复投递同 msg_id → 不产生双份消息 |
| `service/world_service` | 双端历史 | A→B 一条消息 → `chat:hist:{A}:{B}` 与 `chat:hist:{B}:{A}` 各一条 |
| `internal/common/errcode` | 文案完整 | proto enum 每个值都有中文文案（防漏翻译，配合单测断言） |

### 23.3 集成测试（testcontainers 主链路）

`test/integration` 一套容器环境（MySQL + Redis + Kafka + etcd），主链路 6 步：

1. **起环境**：testcontainers 拉起中间件 → 执行 `sql/schema` 建表 → `sql/seed` 灌初始数据；
2. **登录**：注册账号 → 登录服签发 Token（断言 Redis `token:*` 存在）；
3. **接入**：机器人客户端 RSA/AES 握手 → `HandshakeReq` → `EnterGameReq`（断言玩家 Actor 建立、预加载完成）；
4. **业务闭环**：`UseItemReq` → `BagChangeNotify` → 触发 flush → 查 MySQL `player_item` 终态；
5. **跨服聊天**：A 服玩家 → B 服玩家私聊 → 断言双端历史 + 两端归档表落库（见 [17.3](#173-跨服聊天全链路双端历史--kafka-扇出)）；
6. **跨服战斗（demo）**：匹配 → `BattleStartNotify` → `BattleOpReport` 透传 → `BattleResultReport` → 断言奖励落库 + 重复上报返回 6003（见 [4.9.3](#493-战斗职责划分demo-约定)）。

约定：每步断言**终态**（Redis/MySQL），不等中间态，避免时序脆弱；用例间数据隔离（独立 key 前缀）；Teardown 销毁容器。

### 23.4 压测与验收指标

| 指标 | 目标 | 方法 |
| --- | --- | --- |
| 单网关连接 | ≥10W 长连接，RSS < 2GB | `test/benchmark` 全连接 + 保活 30 分钟 |
| 单游戏服在线 | 5K 机器人，P99 消息延迟 < 50ms | 混合业务消息（聊天/道具/移动） |
| 登录吞吐 | ≥2000 TPS | 集中登录（RSA 握手 + Token 签发） |
| 落库吞吐 | 10 服 × 30 并发 = 300，60s 周期内完成 | 观察 flusher 指标 + MySQL 负载 |
| Kafka 扇出 | 世界聊天 1000 条/秒，端到端 < 1s | 生产/消费两端打点 |
| Actor 背压 | 信箱满时拒绝而非 OOM | 构造慢消费者场景 |

压测基线入库（19.5 节第 7 条），作为扩容依据；每个里程碑复跑一次，对比回归。

### 23.5 故障演练清单

| 演练项 | 注入方式 | 预期表现 |
| --- | --- | --- |
| 游戏服 kill -9 | 强杀进程 | 玩家掉线 → 90s 重连保留 → 重登分配新服；Tier-2 数据丢失 ≤1 个落库周期 |
| MySQL 断网 | iptables 丢包 60s | 连接池自动重连恢复；Tier-1（充值/角色）同步双写不丢 |
| Redis 主节点宕 | 手动故障切换 | 切换后 Actor 重建从 DB 加载，玩家可重登 |
| 中心服全集群宕 | 停掉全部实例 | 本服玩法不受影响；跨服功能降级（22 章风险 3） |
| etcd 重启 | 重启 etcd | 各服务 watch 重连，服务列表秒级恢复 |
| Kafka 不可用 | 停 broker | 聊天/邮件转 pend 暂存，恢复后 drain；同步链路告警 |
| 单个落库 worker panic | 注入 panic | 仅影响 1/10 分片；recover + 下周期重试，不扩散 |

**P15 已补齐前 4 项的自动化演练**（`make drill-suite` + `make drill-crash`），实测结果见
[docs/P15-全链路演练报告.md](P15-全链路演练报告.md)：

| 演练项 | 自动化 | 实测结论 |
| --- | --- | --- |
| MySQL / Redis 断连 | `make drill-suite` | 进程存活、就绪摘除、恢复自动回 200，各 4 项检查通过 |
| Kafka 停 broker | `make drill-suite` | 服务**仍就绪**（异步链路降级，不影响登录），冒烟通过 |
| etcd 重启 | `make drill-suite` | 各服务重新注册，冒烟通过 |
| 游戏服 kill -9 | `make drill-crash` | player/item/attr 快照零差异，重连可恢复 |
| Redis 主从切换 / 中心服全宕 / 单分片 panic | 未自动化 | 单机环境无法构造（需多实例），单分片 panic 由 flush 单测覆盖 |

### 23.6 一致性校验（对账）

`tools/check` 每日对账脚本（输出 Prometheus 指标 + 日报）：

| 校验项 | 方法 | 不一致处置 |
| --- | --- | --- |
| 道具行数 | MySQL `player_item` 行数 vs Redis `idx:player_item:*` 计数 | 差异超阈值 → 告警 + 触发回源修复 |
| 充值订单 | `recharge_order` 已完成 vs 发货记录（Tier-1 对账回传 `gs.recharge.delivered`） | 漏发货 → 自动重投 + 告警 |
| 邮件附件 | `mail_attachment` 已领取 vs `idem:gs.mail:*` | 孤儿幂等键 → 清理 |
| 落库延迟 | `max(last_flush_at)` vs now | 超 2 个周期 → 告警（18 章四道防线兜底） |
| ID 表 | Redis idtab vs 全库 count | 启动时构建 + 运行期补偿（见 [12.5](#125-redis-id-表与启动加载)） |

### 23.7 Demo 期测试范围

**本期做（demo 验收必需）**：

- 单测骨架：23.2 清单前 6 项（actor/codec/crypto/flush/idtab）随 P0-P4 落地；
- 集成主链路：23.3 六步（登录 → 进游戏 → 道具 → 落库 → 跨服聊天 → 跨服战斗 demo）；
- E2E 冒烟：19.5 节第 6 条五步冒烟脚本化，每日跑；
- 基础压测：单网关 10W 连接保持（23.4 第 1 行）；
- 故障演练：kill -9 游戏服一项（23.5 第 1 行）。

**后置（迭代再做）**：

- 战斗防作弊验证（伤害合法性、操作时序、上报频率）——demo 期信任客户端上报，只校验令牌/幂等/奖励存在性（见 [4.9.3](#493-战斗职责划分demo-约定)）；
- 全量故障演练（MySQL/Redis/etcd/Kafka/中心服各场景）；
- 容量压测全量项与混沌工程常态化；
- 对账脚本全量上线（demo 期先跑道具行数 + 落库延迟两项）。

---

## 附录

### 附录 A：生成命令速查

```bash
make gen-proto   # api/proto → pkg/pb
make gen-data    # configs/gamedata/xml → pkg/gamedata（覆盖）
make gen-db      # sql/schema → pkg/model（覆盖 *_gen.go）
make db-init     # sql/schema → MySQL 建表
make db-seed     # sql/seed → 初始数据
make gen         # 以上 proto/data/db 一键生成
```

### 附录 B：参考开源实现

| 参考 | 用途 | 仓库 |
| --- | --- | --- |
| go-zero goctl model | DDL→CRUD 生成器设计（接口/生成文件分离覆盖策略） | github.com/zeromicro/go-zero/tools/goctl |
| xgen | XML/XSD→Go 代码生成思路 | github.com/xuri/xgen |
| grpcresolver | gRPC + etcd Resolver | github.com/geniuscirno/grpcresolver |
| wonaming | gRPC balancer + etcd/consul | github.com/wothing/wonaming |
| GoCryptoTCP / udpsocket | RSA 换钥 + AES 会话加密握手模式 | github.com/BacklitCat/GoCryptoTCP、github.com/theredrad/udpsocket |

### 附录 C：关键设计决策记录

| 决策 | 理由 |
| --- | --- |
| 中心服不连网关 | 中心服是控制面，连网关会把客户端链路单点引入控制面；且网关只需 etcd 发现后端 |
| 游戏服不直连、经中心服中转 | 游戏服数量增长后 N² 直连不可维护；中心服单点中转 + 集群间 Kafka 可水平扩展 |
| 跨服节点连网关（数据面直连） | 实时战斗每消息多一跳不可接受；控制面（匹配/结算）仍走中心服，兼顾规则与性能 |
| 10 分片分时落库而非单连接大批量 | 单连接大批量会造成周期性 MySQL 尖刺；10 worker 错峰 6s 把负载摊平到整分钟 |
| Redis key 哈希到连接（而非轮询） | 同实体固定分片 → 单实体写入有序、避免同连接并发写冲突、脏数据局部性好 |
| ID 表用 Redis Set 而非布隆过滤器 | Set 精确无误判；「查不到即没有」要求零假阳性，布隆过滤器不适用 |
| Tier-1 先 MySQL 后 Redis | 严格一致数据不能让缓存超前于库，反序会出现「库无缓存有」的幽灵数据 |
| 登录全量预加载 | 玩家在线期间所有查询走内存，避免游戏中回源 DB 造成延迟毛刺 |
| gRPC bidi stream 取代自研 TCP 池 | HTTP/2 多路复用省去连接池维护；stream 天然映射会话，响应同路返回保证有序 |
| Actor 信箱有界 + 背压 | 无界队列在慢消费场景下会 OOM；有界 + 拒绝策略保护进程 |
| CrossServer 不连网关（v3.1） | 跨服战斗消息由主游戏服经 CrossService 流中转：数据面多一跳内网延迟，换取取消会话迁移、网关零改动、跨服节点自由扩缩；控制面（分组/匹配）仍由中心服集中 |
| 跨服聊天 Kafka 扇出 + 双端历史（v3.1） | 跨服好友私聊只需「能聊 + 有历史」：Kafka 按 playerID 扇出，不建游戏服直连、不压中心服；每服各存一份归档适配每服独立库（零跨库读），Redis 双端 List 兜底离线缺口，msg_id 幂等去重 |
| MySQL 连接池按实例容量规划（v3.1） | `max_connections` 是实例级参数（默认 151 全库共享，非每库 151）；业务池 30（PK 点查 <1ms），flush 10 worker 错峰共享借还不另占连接，10 服 ≈ 340 → 生产 ≥ 500，预留监控余量 |

---

*本文档为 game_study 框架唯一设计文档（v3.3 完整版），由 v1 架构文档、v2 优化文档、v3 演进文档整合，经 v3.1 交互模型修订、v3.2 命名统一与工程修复、v3.3 Demo 契约与质量保障而成。下一步建议进入 P0（工程骨架 + 三大生成器）开发，测试体系随 P0 同步落地（见第 23 章）。*
