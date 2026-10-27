# scenemap：RPG 场景（地图）框架

Go 实现的 RPG 场景（地图）框架，与 `internal/worldmap`（SLG 大地图）
并列且互不依赖。规格来源：《场景大地图设计.md》。

## 1. 架构总则

```
                    ┌──────────────────────────────┐
    玩家 Actor ────▶│ AOI Actor（视野/广播）        │
   （N 个）          │  · 九宫格视野               │
        │           │  · 出现/消失/移动包          │
        │           │  · 视野快照 atomic.Pointer  │
        │           └──────────┬───────────────────┘
        │                      │ on_load 拉起 / after_term 停止
        │           ┌──────────▼───────────────────┐
        └──────────▶│ MGR Actor（场景唯一权威）     │
   "到 MGR 执行"     │  · 格子索引/站位             │
                    │  · 怪物 AI（不占进程）        │
                    │  · 四定时器/倍速/关图计时      │
                    │  · 技能结算                  │
                    └──────────┬───────────────────┘
                               │ 关图通知
                    ┌──────────▼───────────────────┐
                    │ RoomManager（全局单例）       │
                    │  · 房间登记表                │
                    │  · 关房唯一收口               │
                    └──────────────────────────────┘
```

**一张可玩的图 = 1 个 AOI + 1 个 MGR + N 个玩家 Actor。怪物没有独立进程。**

- AOI：高频、只读、可并行 → 独立 Actor，不与结算抢串行队列；
- MGR：站位/结算必须串行、强一致 → 收口单 Actor；
- 怪物：数百只时一怪一 goroutine 会产生 mail 风暴 → 降级为 MGR 私有数据 +
  MGR 自投递的定时命令。

## 2. 核心概念与 Go 实现

| 概念 | Go 实现 |
|---|---|
| 隔离执行单元 | goroutine + 单写者 mailbox |
| 全局可寻址 | `Registry`（sync.Map，只读路径无锁） |
| 异步投递 | `Mailbox.TrySend` |
| 同步调用 | `Call`（带超时与缓冲校验） |
| Actor 私有状态 | `mgr.State` 字段 |
| 跨 Actor 只读快照 | `aoi.Store`（atomic.Pointer 整份替换） |
| 闭包投回 MGR 执行 | `mgr.Command` 接口 + `FuncCommand` |
| 延时投递 | `time.Timer` 自续期（间隔过倍速） |
| 崩溃防护 | `SafeRun` + RoomManager 收口 |

## 3. 目录

```
internal/scenemap/
  types.go      UUID/MapKey/GridPos/PixelPos/Dir/LeaveReason
  errors.go     哨兵错误
  actor.go      Mailbox / Registry / Call / SafeRun
  grid/         纯函数：像素↔格、距离、范围、阻挡、A*、环形落点
  mgr/          场景权威：State/Index/命令/四定时器/关图/进出图/换格
  aoi/          视野：九宫格/快照/广播/房间池
  room/         全局单例：登记表/关房收口/节点宕机
  entity/       怪物：创建/销毁/AI(GUARD-FIGHT-RETURN)/刷怪器
  gameplay/     玩法插件：Registry + 主城 + 副本示例
  bridge/       mgr.AOIHandle ↔ aoi.AOI 的翻译层
  examples/     装配 + P0 集成测试（可跑的"两张图"）
```

依赖方向：`gameplay → mgr → {grid, scenemap}`，`aoi → {grid, scenemap}`，
`bridge → {mgr, aoi}`，`examples → 全部`。`mgr` **不** import `aoi`
（靠 `AOIHandle` 接口倒置），`room` **不** import `mgr`（靠 `Closer`/`Factory`）。

## 4. 进图时序（关键顺序不可乱）

```
玩家 Actor                          MGR
   │ 1. CheckEnter（配置/玩法校验）
   │ 2. 若已在图：同步离旧图，等消失包排空
   │ 3. 延迟 30ms（delayEnterMap 比对，防快速切图错序）
   │ 4. AOI.EnterMap（同步）────────▶ 占视野格、发全量出现包
   │ 5. EntityEnter ────────────────▶ 上格子索引、Users 加入、
   │                                  PlayerEnter、CloseTime=0
   │ 6. 回 13000（含真实分线）
```

先 AOI 后 MGR：反过来会出现"MGR 有实体、AOI 没有"的不可见实体。

## 5. 位置一致性

```
MGR 格子索引（权威，技能/站位/寻路用）
      │ 换格时才触发 EntitySync / 触发器 COND_2
      ▼
AOI 像素位置（广播范围，客户端插值用）
      ▼
玩家存档（像素）
      ▼
客户端表现（移动包 / 纠偏包）
```

- 移动校验在玩家 Actor（轻量，只查阻挡），MGR 二次校验；
- **同格微调只更新像素与朝向，不触发任何判定**（光环/触发器/区域机制都依赖换格）；
- 三者通过同一次移动处理里的串行投递保持一致。

## 6. 不变量与测试映射

| # | 不变量 | 测试 |
|---|---|---|
| 1 | 场景权威态只在 MGR，玩家 Actor 不直接改 | `TestMoveNPE*` / `TestConcurrentPost*` |
| 2 | 命令必须改变状态（revision 检测） | `TestNoEffectCommand` |
| 3 | 30ms 延迟 + delayEnterMap 比对 | 本版把门控放在玩家 Actor（未实现），已记录为缺口 |
| 4 | 先 AOI.Enter 再 MGR.EntityEnter | `TestHarnessAssembly` |
| 5 | 死亡处理幂等 | `TestDamageIdempotent` |
| 6 | 关图走 RoomManager 收口 | `TestConcurrentCloseOnlyOne` |
| 7 | 光环/触发器只在换格时检测 | `TestSameCellNoEntitySync` |
| 8 | AI 不用全局轮询，CLIENT/FIXED 不排定时器 | `TestFixedDoesNotSeek` |
| 12 | 单人副本不广播玩家自身 | `TestBroadcastSkipsSelf` |
| 15 | 镜像双白名单 | 未实现（镜像代理属 P3） |
| 13 | 类型缺失显式报错 | `TestRegistryMissingType` |

## 7. 关键设计决策

1. **地图类型查不到 → `ErrMapTypeMissing`**（不按主城兜底）：
   默认主城会让"看起来能进图、结算全错"，宁可开图失败。
2. **命令无效果 → `ErrNoEffect`**（只记 warning 会让状态静默不更新）：
   用 `State.Revision` 检测，开发期就炸。
3. **玩家可重叠、怪物不可重叠**：占位语义需要显式定义；
   这里显式区分（玩家挤开由客户端表现，怪物占位失败则挪邻格）。
4. **AOI 广播默认异步**：同步回执只用于开房首批与关房；
   每帧移动等 AOI 回执会把两个 Actor 耦合成同步链。

## 8. 已知缺口（明确不做假实现）

- 玩家 Actor 侧：30ms 延迟进图、`delayEnterMap` 比对、存档位置回写（依赖玩家框架）；
- 技能链：前摇/命中分离、子弹/火墙、伤害榜（P3）；
- 镜像代控白名单、跨节点 MapKey 投递（P3）；
- 触发器 COND_0/1/2 只留接口，`oneSec` 无触发器时自动停表；
- 小地图、光环、倍速三处写入：接口与字段已留，结算内容未实现。

## 9. 如何验证

```bash
go test -race ./internal/scenemap/...        # 全部单元与集成测试
go test -race -run TestTwoPlayersSeeEachOther ./internal/scenemap/examples/
```

`examples.Harness` 是最小装配示例：AOI → bridge → MGR → gameplay →
RoomManager，两个模拟玩家可以互相看见、移动、被墙挡住、离图、空房自动关。
