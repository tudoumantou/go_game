# RPG 战斗系统设计与接入

> 规格来源：`战斗.md`（实时场景 + 帧同步战斗框架）
> 实现位置：`internal/scenemap/battle`
> 接入方式：`battle.NewEnsureCommand` / `battle.NewCastCommand` / `mgr.Deps.BuffTick`
> 验收：`internal/scenemap/battle/*_test.go`、`internal/scenemap/examples/battle_flow_test.go`

## 1. 范围

本实现对应规格的 **P3 战斗核心**，并以最小可用形态覆盖 P4 的一部分：

| 能力 | 状态 | 说明 |
|---|---|---|
| LAUNCH/CAST 分离、前摇、弹道延迟、多段 | 已实现 | 延时走 `time.AfterFunc`（生产）或本地队列（测试） |
| 选目标四分支 + 阵营/射程/人数截断 | 已实现 | 主目标优先、其余按"距离→UUID"排序 |
| 伤害公式链（攻防/暴击/会心/元素/格挡/增伤/歧视/宠物/地图/PK 压制/真实攻防/百分比） | 已实现 | 整数定点，每步显式截断 |
| Buff（属性/DoT/HoT/时停/禁攻/无敌/免死/护盾/减速） | 已实现 | 叠层、顶替、到期、护盾按条扣减 |
| 死亡链统一入口 + 幂等 | 已实现 | `Runtime.dead` + 场景权威双写 |
| 双回路（飘字广播 + 结果事件） | 已实现 | 广播走 AOI 视野，事件走 Notify |
| 子弹 / 火墙 | 最小实现 | 子弹按格追击、命中结算子技能；火墙周期伤害 |
| 被动技能 | 部分 | 触发登记 + 内部 CD；未实现"按类型逐条释放" |
| 光环 / 小地图 / 跨节点 / 时停横幅 | 未实现 | 见 §10 |

## 2. 架构位置

```
玩家 Actor ──CastCommand──▶ MGR Actor（唯一权威）
                              │  ├─ battle.Runtime（State.Ext，单写者）
                              │  ├─ Sprite 投影（NPC 权威 / 玩家只读镜像）
                              │  └─ Buff/冷却/死亡标记/延时队列
                              │
                 ┌────────────┴────────────┐
                 ▼                         ▼
        AOI 视野广播                 玩家 Actor（结果事件）
        HarmBroadcast                SkillResultEvent
        BuffSync                     MonDeadEvent
        CastStart
```

三条铁律：

1. **battle 不是 Actor**：它没有 goroutine，全部入口都在 MGR 的串行队列里；
2. **MGR 不 import battle**：battle 通过 `mgr.Command` 与 `Deps.BuffTick` 挂进来，依赖方向始终是 `battle → mgr`；
3. **玩家属性不在 MGR 结算**：玩家的 HP/Buff 由 `SkillResultEvent` 回写，MGR 只维护结算用的投影。

## 3. 一次出手的时序

```
C2S 16000（battle.Cast）
  → CastCommand.Apply（MGR goroutine）
      ① 主城? → 拒绝（ErrCodeNoBattle）
      ② 技能配置缺失? → 拒绝（ErrCodeMissingSkill）
      ③ 施法者存活/未暂停/未禁攻/未时停?
      ④ 镜像白名单（Options.MirrorMaps）
      ⑤ 冷却检查 → 通过即刻写 CD（"受理即扣 CD"）
      ⑥ LAUNCH（delay=0）：释放 Buff/被动，不产生伤害、不飘字
      ⑦ 广播 CastStart（客户端播前摇）
      ⑧ 计算前摇 → Schedule(CAST)
  → CAST（前摇到期）
      ⑨ 远程技：再延迟"格子距离 × per_grid_ms"
      ⑩ 选目标（四分支）→ 逐个 calcSkill
      ⑪ 每个目标：飘字广播 + 结果事件
      ⑫ 死亡链（统一入口、幂等）
      ⑬ 子弹/火墙生成
      ⑭ 多段：按攻速缩放间隔排下一段
```

前摇公式（`prefixMS`）：

```
prefix = base_prefix × 10000 / (speed_atk × (1 + distance × 3%))
```

再统一按地图倍速缩放（`×1000 / SpeedPermille`）。倍速只作用于"玩法节奏"（技能/子弹/刷怪/Buff tick），触发器秒表不缩放。

## 4. 数据与配置

### 4.1 三个核心结构

| 结构 | 作用 | 权威位置 |
|---|---|---|
| `Sprite` | 参与结算的属性投影（HP/攻防/命中/暴击/格挡/增伤…） | NPC：双写 `NpeAIInfo.HP`；玩家：玩家 Actor |
| `SkillContext` | 一次出手的完整上下文（目标/段号/飘字/吸血/死亡） | 结算过程中临时持有 |
| `Buff` | 已施加的 Buff 实例（配置快照 + 层数 + 护盾剩余） | MGR（NPC）/ 玩家 Actor（玩家） |

### 4.2 系数清单（全部万分比，10000 = 1.0）

| 字段 | 语义 | 默认 |
|---|---|---|
| `Coeff` | 技能系数 `skill_coe` | 必填 |
| `Harm` | 固定加伤 `skill_harm` | 0 |
| `IgnoreDef` | 无视防御 | 0 |
| `Crit` / `ReCrit` | 暴击率 / 抗暴 | 0 |
| `CritDmgInc` / `CritDmgDec` | 暴伤增减（底数 1.3） | 0 |
| `LuckyHit` / `ReLuckyHit` | 会心率 / 抗会心（底数 1.2） | 0 |
| `ElementCoe` | 元素克制 | 1.0 |
| `Bok` / `IgnoreBok` / `BokDmgInc` / `BokDmgDec` | 格挡（减伤上限 90%） | 0 |
| `AddDmgCoe` / `FinalAddDmgCoe` | 增伤 / 终伤（下限 0.1） | 1.0 |
| `Discriminate` | 歧视增伤 | 1.0 |
| `AddPetDmg` / `DecPetDmg` | 宠物增伤 / 减伤 | 0 |
| `MapTypeDmg` | 地图类型增伤 | 1.0 |
| `PkPress` | PK 压制（仅玩家/宠物来源） | 0 |
| `RealAtk` / `RealDef` | 真实攻防（公式末尾） | 0 |
| `PercentDmg` | 目标当前生命百分比伤害 | 0 |
| `SuckPermille` | 吸血比例 | 0 |
| `ThornsRate` / `ThornsCoe` | 反伤概率 / 系数 | 0 |

### 4.3 默认演示表

`battle.DefaultTable()` 提供：

| ID | 名称 | 说明 |
|---|---|---|
| 2001 | 普攻 | 前摇 300ms、单体、系数 1.0、固定 +10 |
| 2002 | 烈焰风暴 | 半径 2、最多 6 人、命中挂灼烧 |
| 2003 | 火球 | 子弹（速度 8 格/秒），命中结算 2004 |
| 2005 | 战吼 | 纯效果技，给自己加护盾 |
| 3001 | 灼烧 | DoT：3 秒，每 500ms 掉 10，最多 3 层 |
| 3002/3003/3004/3005/3006 | 时停/禁攻/护盾/无敌/免死 | — |

## 5. 伤害公式链

按规格 §10.6 的赋值顺序（整数运算，每步 `/10000` 截断）：

```
1  base = max(atk - def×(1-ignore_def), 0)
2  val  = base × coeff + harm                     （下限 1）
3  暴击：val × max(1.3 + inc - dec, 1.0)
4  会心：val × max(1.2 + inc - dec, 1.0)
5  元素：val × element_coe
6  格挡：val × (1 - min(max(inc-dec,0), 0.9))
7  增伤：val × add_dmg；终伤：val × max(final, 0.1)
8  歧视：val × discriminate
9  宠物：val × max(1 + add_pet - dec_pet, 0.1)
10 地图：val × map_type_dmg
11 PK： val × max(1 - pk_press, 0.1)（仅玩家/宠物来源）
12 真实：val + real_atk - real_def
13 百分比：val + hp × percent_dmg
14 保底 1 点
```

随后 `applyRealHarm`：**灵力（1 点抵 2 伤）→ 护盾（按条扣 Remain）→ 免死（压到 1 点）→ 扣血**。

### 手算示例（`TestDamageFormulaFixedValue`）

攻击 100、防御 20、系数 1.0、固定 +10、暴击率 100%、暴伤 +0：

```
(100-20)×1.0 + 10 = 90 → ×1.3 = 117 → 终伤 ×1.0 = 117
HP 1000 → 883
```

## 6. 双回路

| 回路 | 内容 | 去向 |
|---|---|---|
| 表现 | `HarmBroadcast`（飘字/血条）、`BuffSync`、`CastStart` | AOI 视野广播（半径 8 格） |
| 结果 | `SkillResultEvent`（HP/Buff/死亡）、`MonDeadEvent`、`PlayerDeadEvent` | `Options.Notify` → 目标主人的玩家 Actor |

**MGR 直写**只发生在 NPC：`Sprite.HP` 结算后回写 `NpeAIInfo.HP`，并同步 `AI=Dead/DeadAt` 与清仇恨。玩家侧永远只发事件——这是"单写者 + 属性归属"的边界。

## 7. 死亡链

```
任何来源致死（技能/Dot/火墙/反伤）
  → applyRealHarm 返回 dead=true
  → handleDead（统一入口）
       ├─ Runtime.dead[uuid] 已存在? → return（幂等）
       ├─ 写场景权威：HP=0 / AI=Dead / DeadAt / 清仇恨
       ├─ 清 Buff、撤护盾
       ├─ Notify(击杀者主人, MonDeadEvent)   ← 掉落主流程
       └─ OnNpcDead(s, uuid, npcID)          ← 装配层：Despawn + 玩法 HandleNpcDead
```

幂等标记必须与 `sprites` 同生命周期：怪物被 `Despawn` 后 UUID 会被复用（`entity.allocUUID` 从 `len(NPES)` 倒推），残留标记会把新怪判成尸体。`spriteOf` 检测到"场景里活着但战斗记录已死"时会重建投影并清标记。

## 8. 接入方式（三步）

```go
// ① 开图后初始化（MGR goroutine 内）
m.Post(battle.NewEnsureCommand(battle.Options{
    Table:      battle.DefaultTable(),
    NoBattle:   false,             // 主城设 true
    MapType:    1001,              // 副本
    MirrorMaps: nil,               // 镜像施法白名单，缺省全拒
    Notify:     func(uuid scenemap.UUID, evt any) { /* 投递玩家 Actor */ },
    OnNpcDead:  func(s *mgr.State, uuid scenemap.UUID, npcID int32) {
        // entity.Despawn + gameplay.HandleNpcDead + 删 AOI
    },
}))

// ② 500ms 战斗推进（吃倍速）
deps.BuffTick = battle.Tick

// ③ 客户端报技能
m.Call(ctx, battle.NewCastCommand(battle.Cast{
    Caster: uuid, SkillID: 2001, Target: targetUUID,
}))
```

`examples.Harness` 提供了 `WithBattle(...)`、`h.Cast(...)`、`h.TickCombat()` 与 `h.Events` 记录器，端到端测试见 `battle_flow_test.go`。

## 9. 不变量与测试映射（规格 §13）

| # | 不变量 | 实现点 | 测试 |
|---|---|---|---|
| 1 | 场景权威态只在 MGR | 所有入口要求 MGR goroutine；`battle` 无 goroutine | `-race` 全包；`examples` 端到端 |
| 3 | 30ms 延迟进图 + delayEnterMap 比对 | 由 `mgr` 既有实现保证 | `scenemap/examples` 既有用例 |
| 5 | 死亡统一入口 + 幂等 | `Runtime.dead` + `handleDead` | `TestKillIsIdempotent`、`TestDeadTargetIgnoredByNewCast` |
| 6 | 关图走 RoomManager 收口 | `mgr` 既有实现 | `scenemap/room` 既有用例 |
| 7 | 光环/区域只在换格判定 | `mgr.syncMap` 既有实现 | `scenemap/mgr` 既有用例 |
| 8 | AI 不用全局轮询 | `entity.TickAll` 由 MGR 每 500ms 统一驱动 | `scenemap/entity` 既有用例 |
| 9 | 主城不结算 | `Options.NoBattle` | `TestCastRejectsCityAndMissingSkill`、`TestCityBlocksSkill` |
| 15 | 镜像两个白名单 | `Options.MirrorMaps`（施法侧） | `TestMirrorWhitelist` |
| 18 | LAUNCH 不飘字 | `calcSkill` 的 `ActionLaunch` 分支只挂 Buff | `TestLaunchDoesNotHarmUntilCast`、`TestLaunchDoesNotBroadcast` |
| 21 | 前摇吃攻速与距离 | `prefixMS` | `TestLaunchDoesNotHarmUntilCast` 断言前摇 > 0 |
| 22 | 远程弹道再延迟 | `runCast` 的 `part==0` 分支 | 默认表 2003/2004（子弹路径） |
| 13 | 类型缺失显式报错 | `Table.Skill` 返回 ErrNotFound → `ErrCodeMissingSkill` | `TestCastRejectsCityAndMissingSkill` |
| 19 | 被动 CD 在 MGR 内 | `Runtime.passiveCD`（key=uuid:pasvID） | 公式/施法用例覆盖结构 |

## 10. 关键设计决策

| 决策点 | 默认/常见做法 | 本实现 | 理由 |
|---|---|---|---|
| 技能类型缺失 | 默认按主城处理 | `ErrCodeMissingSkill` 显式拒绝 | "看起来正常、结算全错"比开图失败难查 |
| 命中率公式 | `(hit/10000 - ...) + 1` 的浮点混算 | 统一万分比，基础命中写进 `Hit=10000` | 整数可复现；避免 ±1 的浮点校正 |
| 子弹移动 | 按朝向直线 + 碰撞盒 | 每跳朝目标格走 1 格 | 演示框架不引入连续碰撞；接口不变 |
| 时停 | 冻结 AI + 子弹时间复位 | `BuffStun` 禁施法 + AI 置 `Stopped`，到期恢复 | 子弹时间复位未接入（见下） |
| 被动 | 全类型逐条释放 | 只做触发登记 + 内部 CD | P4 范围，接口已留 `TriggerSkills` |

## 11. 未实现（后续）

1. 被动技能按类型（释放/命中/受击/击杀/闪避/死亡…）逐条 `cast_skill`；
2. 光环（aureole）与区域机制、小地图增量推送；
3. 时停横幅（16030）与子弹时间复位；
4. 跨节点 MapKey 的施法投递与存在性 RPC（当前战斗只在单 MGR 内）；
5. AI 主动施法（`entity` 侧读取 `NpcConfig.Skills` 并投递 CastCommand）；
6. Buff 互斥组与"攻击 N 次解除"这类条件解除；
7. 伤害榜（`RecordHurt`）与玩家属性同步的正式接线。
