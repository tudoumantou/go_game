# 04-模块系统module

> 一句话定位：`internal/game/module` 是玩法模块的公共契约层——Module 接口 + Registry 路由 + 三档基座（Plain/Thin/Stateful）+ 统一的发放/扣费/幂等工具，让每个玩法只写业务三分之二行甚至更少；本篇还记录 P0 响应串号事故如何被泛型回包 + AST 静态检查彻底固化。

## 1. 概述

模块系统的设计目标只有一个：**把「协议 plumbing」从业务里抽走**。一个签到模块为了满足 `module.Module` 接口要写 7 个方法，620 行的签到里业务只占三分之一（`internal/game/module/thin.go:3-7`）——这就是基座存在的理由。

核心契约（`module.go:191-211`）：

```go
// Module 业务功能模块统一接口（每个功能 = Init/OnMessage/OnSave/OnLogin/OnLogout）。
//
// 生命周期由 Registry 驱动：InitAll → LoginAll →（OnMessage 循环）→ LogoutAll → OnSave。
// 实现方通常是包级单例（New 返回 *Module），因为模块只持有配置与仓储，
// 玩家状态全部来自 Context.State——同一模块实例服务所有玩家。
type Module interface {
	Name() string
	MsgIDs() []uint32
	Init(ctx *Context) error
	OnMessage(ctx *Context, msgID uint32, body []byte) error
	OnSave(ctx context.Context) error
	OnLogin(ctx *Context) error
	OnLogout(ctx *Context) error
}
```

**模块是进程级共享的单例**，玩家数据只放 `Context.State` 或按 player_id 索引的容器——严禁在共享 Module 上加 `currentPlayer` 一类字段（`internal/game/server_start.go:224-226` 再次强调了这条红线）。

## 2. 涉及文件

| 文件 | 职责 |
| --- | --- |
| `internal/game/module/module.go` | 契约层：PlayerState 接口、Context、Module、四个可选接口、Registry |
| `internal/game/module/proto.go` | `Typed` 泛型：自动解包请求 + 自动回包响应 |
| `internal/game/module/plain.go` | Plain 基座：无状态模块的预装配 |
| `internal/game/module/thin.go` | Thin 基座：薄模块的预装配（含 Discardable/CacheFailure） |
| `internal/game/module/stateful.go` | Stateful[T] 泛型基座（详见 [05-持久化Stateful](./05-持久化Stateful.md)） |
| `internal/game/module/grant.go` | 发放：溢出预检 → 容量预检 → 入包 |
| `internal/game/module/spend.go` | 扣费：够才扣，失败零改动 |
| `internal/game/module/conv.go` | `ToItemChanges`：发放结果 → 协议变更列表 |
| `internal/game/module/register/register.go` | 空白导入聚合 9 个模块的持久化 init |
| `internal/game/module/convention_test.go` | AST 静态检查：「Typed handler 不得自己回包」 |

## 3. 核心设计

### 3.1 分发链路与可选接口

```mermaid
flowchart LR
    A["PacketMsg 进 Actor"] --> B["Registry.Dispatch<br/>route map O(1)"]
    B --> C{"命中模块?"}
    C -->|否| D["ErrNoRoute<br/>（%w 包装可 errors.Is）"]
    C -->|是| E["Module.OnMessage"]
    E --> F["Typed: 解包 → fn → 自动回包"]
    subgraph 可选接口（类型断言探测）
        G["EventAware OnEvent"]
        H["DiscardableAware Discardable"]
        I["CacheFailureAware CacheFailureReply"]
        J["StatelessAware IsStateless"]
    end
```

Registry 用**类型断言**探测可选接口（`module.go:213-221`）：未实现的模块不会被调用，比在 Module 里塞空的 OnEvent 更省调用。四个可选接口各有职责：

| 接口 | 回答的问题 | 保守默认 |
| --- | --- | --- |
| `DiscardableAware` | 写缓存失败后能否丢弃内存改动？ | 未实现 = 不可丢弃（保留待提交快照） |
| `CacheFailureAware` | 缓存不可用时回什么错误响应？ | 未实现 = 网关通用错误通知 |
| `StatelessAware` | 这条协议改不改玩家状态？ | 未实现 = 有状态（走完整 CAS 流程） |
| `EventAware` | 关心玩家行为事件吗？ | 未实现 = 不订阅 |

`Register` 在协议号冲突时直接 panic（`module.go:279-287`）——属于编码错误，启动期暴露。`Dispatch` 用 `%w` 包装 `ErrNoRoute`（`module.go:296-301`），调用方可 `errors.Is` 区分「没有路由」（协议号配错）与「模块内部错误」（业务 bug）。

### 3.2 Typed：自动回包与 P0 串号事故

```go
// Typed 把「类型化业务函数」包装成 ThinHandler：自动解包请求、回包响应。
//
// 返回未命名函数类型：ThinHandler 与 PlainHandler 的底层签名一致，
// 未命名类型可以隐式赋给两者，因此同一个 Typed 既能喂给 Thin 也能喂给 Plain。
func Typed[TReq, TResp any, PReq ProtoPtr[TReq], PResp ProtoPtr[TResp]](
	respID uint32,
	fn func(ctx *Context, req *TReq, resp *TResp) error,
) func(ctx *Context, msgID uint32, body []byte) error {   // proto.go:52
```

（注意：`Typed` 实际定义在 `proto.go`，`conv.go` 是另一个文件——前者管自动回包，后者管发放结果转协议，两者职责不同。）

三个实现细节：

1. **每次 new 一个实例**（`proto.go:57-59`）：模块是单例，被多个玩家 Actor 并发调用，复用实例会让两个玩家的请求内容互相串味；
2. **fn 返回 error 不自动回包**（`proto.go:50-51`）：error 表示「不能给出业务响应」，走错误通道记日志——与手写 `return err` 语义一致；
3. **`ProtoPtr` 约束的 Go 知识点**（`proto.go:21-30`）：不能用 `proto.Message` 直接约束 T——proto 生成的结构体是指针实现该接口，而 `new(T)` 只能得到结构体；用「约束里带 `*T`」的写法把 T 固定成结构体类型。

**P0 响应串号事故**（提交 `dcd8354`）是本篇必须记录的历史：6 个模块（quest/mail/bag/shop/battle/copy）的 handler 内手动 `reply` 与 `Typed` 自动回包**重复**，客户端收到两条响应——第二条把 seq 对错，表现为响应晚一拍、幂等失效。修复是删掉所有 handler 内的显式回包。事故的固化手段见 3.5。

### 3.3 三档基座

| 基座 | 适用 | 状态 | 关键能力 |
| --- | --- | --- | --- |
| `Plain` | 无状态模块 | 不持有任何数据 | `IsStateless` 恒 true；HandleR + 错误回包 |
| `Thin` | 薄模块（有状态但无私有持久化） | 状态在 `Context.State` | Discardable 注册即可丢弃；guard 前置校验；事件钩子 |
| `Stateful[T]` | 有私有状态的模块 | Ext 槽位 + Persister | 见 [05-持久化Stateful](./05-持久化Stateful.md) |

`Thin` 的适用边界写在文件头（`thin.go:13-20`）：默认把已注册协议全部声明为可丢弃（前提是重放无第二次副作用——只读，或业务状态自带幂等如签到按「今天已领」去重）；**关系型模块（好友/公会/队伍）不建议用 Thin**——它们的复杂度在事务而不在接口样板。

`HandleR` 是 Thin 的核心便利（`thin.go:184-204`）：注册处理器的同时声明响应协议，错误回包由基座按「响应号 + 空原型」自动构造——`withCode` 克隆零值原型并用反射置 code 字段（`thin.go:222-236`），字段缺失时原样返回。这样模块不必再写一个「按 msgID 分支的工厂函数」。

### 3.4 发放与扣费：失败零改动

```go
// Grant 发奖：溢出预检 → 容量预检 → 入包。
// 返回 nil, nil 表示没有任何有效道具可发（配置为空），调用方不必当失败处理。
// 失败时保证**没有修改任何状态**——预检全过才入包。
func Grant(state PlayerState, grants []ItemGrant) ([]BagChange, error) {   // grant.go:63
```

`Grant` 的两级预检各有理由（`grant.go:71-74`）：int32 溢出预检（协议 Count 宽度，脏数据负数同样拒绝）与容量预检（按「新增种类」算，入包后再判就得回滚）。`Spend` 与它对称（`spend.go:29-43`）：count<=0 视为免费、`RemoveItem` 返回值必须接——注释点明了历史故障形态：「升级成功但钱没扣」。

两个哨兵错误（`ErrBagFull`/`ErrNotEnough`/`ErrItemLimit`）只给语义不映射业务码——「基座不持有业务码表，避免协议/错误码变更牵动框架层」。

### 3.5 convention_test：把 P0 教训固化成静态检查

P0 事故后新增 `convention_test.go`，用 `go/ast` 静态检查守住约定（`internal/game/module/convention_test.go`）：

1. 解析模块包源码，找到 `module.Typed` 绑定的 handler 名；
2. 检查这些函数体内是否有「回本请求响应号」的调用（`reply`/`ctx.Reply`/`m.reply*`）；
3. `isResponseReply` 只认响应不认推送（推送是另一回事，允许）；
4. 末尾自检 `scanned > 10`——防止 AST 解析悄悄失效后测试空跑。

这是「用测试守护约定」的典范：编译器和类型系统管不了「同一个协议回两次包」，但 AST 可以。

## 4. 关键代码解析：Context 与 ApplyOnce

`Context` 每条消息新建一个（`module.go:137-153`），是模块与玩家层之间的唯一通道。三个可注入 nil 的字段（Send/Emit/AddExp）让模块可在无玩家层环境下单测。

`ApplyOnce`（`module.go:174-189`）是幂等投放的标准写法：

```go
// 键在 fn 成功后才占用 —— 一次配置错误或校验失败不能把业务键永久占死。
//
// 键、资产、模块状态最终都在同一次快照 CAS 里提交：Redis 失败则三者一起回滚，
// 不会出现「领取事实已记、奖励没到账」。主动领取类业务（任务/活动/副本）不需要
// 键，用业务状态里的「已领取」事实做幂等即可，避免 applied_grants 无限膨胀。
```

「键在 fn 成功后才占用」与 `ErrBagFull` 的注释形成呼应（`module.go:24-26`）：发奖被拒不占用业务键，玩家清空背包后可重新领取，不会被「已申请」永久卡死。

## 5. 演进记录

| 提交 | 内容 |
| --- | --- |
| Sprint 4 | PlayerActor 与基础模块 |
| P6 | 模块路由与账号绑定；背包/邮件/创角陆续落地 |
| Thin 系列提交 | 引入 `module.Thin` 薄模块基座；五个简单模块迁 Thin；八个模块迁 Thin；三个事件型模块迁 Thin（ThinOnEvent） |
| `dcd8354` | fix(module)：修复重复回包导致响应串号（quest/mail/bag/shop/battle/copy 六模块删显式回包，scene 同步补 4005） |
| `07a6a1d` | test(module)：静态检查「Typed handler 不得自己回包」 |
| `dff3b7a` 等 7 个提交 | 七个模块迁 Stateful[Data]（见 [05-持久化Stateful](./05-持久化Stateful.md)） |

模块基座的三级演进（Plain → Thin → Stateful）每次都把一类样板推平：Thin 消灭接口样板，Stateful 消灭持久化样板。每次演进后旧模块分批迁移，而不是一次性重写——这也是 7 个独立提交的原因。

## 6. 测试情况

模块包的单测矩阵（均已核实存在）：

| 文件 | 用例数 | 守护内容 |
| --- | --- | --- |
| `context_test.go` | 4 | Context 构造/Reply 空安全/ApplyOnce 语义 |
| `plain_test.go` | 12 | Plain 基座全行为 |
| `thin_test.go` | 14 | Thin 基座全行为（含 HandleR/errorResp/guard） |
| `proto_test.go` | 4 | Typed 自动回包/解码失败不回包 |
| `grant_test.go` | 6 | Grant/PrecheckGrant 的预检与零改动 |
| `spend_test.go` | 4 | Spend 的免费/不足/成功 |
| `convention_test.go` | 1 | AST 静态检查 Typed handler 不回包 |

`convention_test.go` 的测试名即文档：`TestTypedHandlersDoNotReplyManually`。

## 7. 小结

模块系统可以浓缩为三句话：

1. **契约极薄，基座极厚**：Module 只有 7 个方法，其余全是可选接口；Plain/Thin/Stateful 三档基座按模块复杂度选用；
2. **回包只有一条路**：Typed 自动回包是唯一正解，P0 串号事故后用 AST 静态检查永久固化；
3. **发放扣费失败零改动**：预检全过才动状态，哨兵错误只给语义——业务码映射永远在模块层。

模块私有状态如何落库，见 [05-持久化Stateful](./05-持久化Stateful.md)。
