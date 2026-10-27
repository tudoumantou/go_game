# FlatPersister：模块持久化第三轮简化设计方案

> 状态：**一期已落地**（Querier 下沉 / LoadExt·SaveExt / FlatRow + RegisterFlat / gendb @flat 标注）；二期（FlatSet）、三期（model 生成裁剪）待定
> 前置：8c50d27 Persister 扩展点 → 9ac0c57 gendb 批量落库 → 1d78ef0 签到 SQL 清零
> 目标读者：一人公司，评估「新增业务功能还要手写多少机械代码」

---

## 1. 背景：前两轮解决了什么，还剩什么

| 轮次 | 成果 | 消灭的东西 |
|---|---|---|
| 第一轮（Persister 扩展点） | 模块持久化改为四方法接口，按 Ext 键分组落库 | 8 处横切改动 → 0 |
| 第二轮（gendb 批量生成） | 表 CRUD/批量 UPSERT/按玩家读写全部生成 | 全部 SQL 字符串 |
| **第三轮（本文）** | **Data↔表行映射的机械部分泛型化** | **每模块约 120~180 行胶水代码 |

第二轮之后，新增一个签到式模块仍要手写 6 个文件。逐行拆解签到的 458 行非测试代码后，机械与业务的构成是：

| 组成 | 行数 | 性质 |
|---|---|---|
| signin.go 协议入口 + 业务规则（周期/奖励/幂等/背包联动） | ~150 | 业务，不可消灭 |
| signin.go 的 MsgIDs / Rollbackable / CacheFailureReply 声明 | ~60 | 机械 |
| persist.go 的 Validate 结构校验 | ~25 | 机械 |
| persist.go 的 PersistBatch / Load 行映射 | ~70 | 一半机械一半业务 |
| repo.go 的 Ext 编解码 read/write | ~35 | 纯机械 |
| repo.go 的 view 周期投影 | ~25 | 业务 |
| calendar.go 周期计算 | 19 | 纯业务 |

**结论：机械代码约 180 行，与业务代码几乎各占一半。** 第三轮的目标就是这 180 行，且不动业务那一半。

同时修正上一轮口头估计：FlatPersister 的真实覆盖面比"70% 模块"小得多（见第 6 节适用范围实测），它的主要价值在未来新表与「所有模块立即可得的 Ext 泛型瘦身」，而非改造存量。

---

## 2. 设计总纲：FlatPersister 是语法糖，不是新链路

三条不可动摇的原则：

1. **不新建落库链路。** FlatPersister 内部仍然构造一个标准 `Persister` 注册进现有 Registry，worker / snapshot / cache_load / Redis 提交 Lua 一行不改。它只是「注册期的代码量压缩」。
2. **逃逸路径永远开放。** 任何模块随时可以从 Flat 切换成手写 Persister（删 bind.go、写 persist.go），无数据迁移、无格式变更。判定树（第 6 节）决定起步用哪种，演进时单向流动：Flat → 手写容易，手写 → Flat 不可能（也没必要）。
3. **不扭曲领域模型。** 绝不要求「Data 结构 = 表结构」来成全生成器。签到的 `Claims map → 流水行 + cycle_start_day 派生列` 就是反例：强推 1:1 直映，要么把 `Data` 掰成表的形状（贫血模型），要么生成器堆特例（比不生成更糟）。ORM 路线在游戏业务上的翻车基本都在这一条。

---

## 3. 组件设计

### 3.1 依赖方向调整：Querier 下沉到 flush

现状 `Querier` 接口定义在 `pkg/model/querier_gen.go`，而 model 依赖 flush。Flat 的函数签名需要 Querier，因此把权威定义下沉到 `pkg/flush`，model 保留类型别名（`type Querier = flush.Querier`），既有 30 个生成文件零改动。

```
flush（SQL 基础设施：Querier / Chunk / BuildBatchUpsert / FlatOps）
  ↑                    ↑
model（生成：struct + CRUD + XxxFlat spec）    writebehind（框架：Persister / RegisterFlat / LoadExt）
```

### 3.2 FlatOps：表级读写适配（pkg/flush 新增，~20 行）

```go
// FlatOps 单行状态表的读写适配。两个字段均由 gendb 生成，业务零手写。
type FlatOps[T any] struct {
    Upsert func(ctx context.Context, tx *sql.Tx, rows []T) error      // = model.UpsertXxxBatch
    Load   func(ctx context.Context, q Querier, playerID int64) (*T, error) // = model.FindXxxByPK
}

// FlatSetOps 集合状态表（整包替换语义）的读写适配。
type FlatSetOps[T any] struct {
    Upsert func(ctx context.Context, tx *sql.Tx, rows []T) error        // = model.UpsertXxxBatch
    List   func(ctx context.Context, q Querier, playerID int64) ([]T, error) // = model.ListXxxByPlayer
    Delete func(ctx context.Context, tx *sql.Tx, playerID int64) error  // = model.DeleteXxxByPlayer
}
```

关键设计：字段类型与 gendb 已生成的函数签名**逐一精确匹配**，生成物可以直接传函数值，零包装 lambda。例如 `Upsert: UpsertPlayerSigninBatch` 直接编译通过。

### 3.3 RegisterFlat / RegisterFlatSet（pkg/writebehind/flat.go 新增，~90 行）

```go
// RegisterFlat 注册「玩家级单行状态表」模块。
// validate 为可选的业务校验钩子；不传则只做结构校验（解码成功且非 nil）。
func RegisterFlat[T any](name string, ops flush.FlatOps[T], validate ...func(*T) error)

// RegisterFlatSet 注册「有界小集合、整包替换」模块。
// PersistBatch = 同一事务内 DeleteByPlayer + UpsertBatch（原子，无删除窗口）。
func RegisterFlatSet[T any](name string, ops flush.FlatSetOps[T], validate ...func(*T) error)
```

内部构造的 `flatPersister[T]` 实现标准四方法：

| 方法 | 行为 |
|---|---|
| `Name()` | 返回模块名（= Ext 槽位键） |
| `Validate(raw)` | `Decode[T]` 结构校验 + 可选钩子；与手写 Persister 契约一致——损坏即报错，宁可整批回滚不静默丢数据 |
| `PersistBatch` | FlatRow：decode 全部行 → `ops.Upsert`；FlatSet：decode → 按涉及玩家批量删 → `ops.Upsert` |
| `Load` | FlatRow：`ops.Load`，`sql.ErrNoRows` 由框架转换为 `(nil, nil)`；FlatSet：`ops.List` → 编码返回 |

FlatSet 的批量删除需要 model 补充生成 `DeleteXxxByPlayers(ctx, tx, ids []int64)`（第二期，避免 N 个玩家 N 次单行 DELETE）。

### 3.4 LoadExt / SaveExt：泛型状态访问器（pkg/writebehind/ext.go 新增，~20 行）

```go
// ExtState 是模块私有状态读写的最小依赖（player.State 已实现）。
type ExtState interface {
    Ext(name string) json.RawMessage
    SetExt(name string, raw json.RawMessage)
}

// LoadExt 读并解码模块状态；从未写入过返回 (nil, nil)。
func LoadExt[T any](state ExtState, name string) (*T, error) { return Decode[T](state.Ext(name)) }

// SaveExt 编码并写回（标脏与落库调度由框架负责）。
func SaveExt[T any](state ExtState, name string, v *T) error
```

这一个组件**立即对包括 signin 在内的所有模块生效**：signin 的 repo.go 可删掉自定义 `stateAccess` 接口和 read/write 共 13 行；未来所有模块（无论 Flat 还是手写 Persister）都省这 13 行 + 不再各自定义窄接口。

### 3.5 gendb 标注扩展（tools/gendb，~40 行）

建表注释中显式标注：

```sql
-- sql/schema/game.sql
-- @flat:settings          ← 标注：本表是 settings 模块的单行状态表
CREATE TABLE `player_settings` (
  `player_id` BIGINT NOT NULL,
  ...
  PRIMARY KEY (`player_id`)
```

gendb 行为：
- 解析时**先提取 `@flat:<name>` 标注，再剥离注释**（现状是先剥离，需调整顺序并补单测）；
- 标注表额外生成 spec 变量：`var PlayerSettingsFlat = flush.FlatOps[PlayerSettings]{...}`；
- 标注即校验：**PK 不是 `player_id` 单列 → gendb 直接报错退出**（防误标）；
- 框架表 player_writebehind 不标注，天然排除。

---

## 4. 业务侧最终形态（before / after）

### 4.1 单行状态模块（如未来的 settings / 引导进度 / 开关位图）

持久化相关代码从 persist.go 130 行 + repo.go 中 35 行编解码 → **bind.go 5 行**：

```go
// internal/game/module/settings/bind.go
package settings

import "game_study/pkg/writebehind"

func init() { writebehind.RegisterFlat("settings", model.PlayerSettingsFlat) }
```

repo.go 瘦身为：

```go
d, err := writebehind.LoadExt[Data](state, "settings")   // 取代 read()
writebehind.SaveExt(state, "settings", d)                 // 取代 write()
```

### 4.2 有界集合模块（未来以 (player_id, 业务键) 复合主键设计的小集合，如任务进度）

```go
func init() { writebehind.RegisterFlatSet("quest", model.PlayerQuestFlat) }
```

PersistBatch 的差异删除由 FlatSet 的「同事务 Delete + Upsert」承担，业务零代码。

### 4.3 复杂模块（signin 及一切多表/派生列/无界集合）

维持手写 Persister 不变，仅把 repo.go 的 read/write 换成 LoadExt/SaveExt。

---

## 5. 收益核算

| 模块形态 | 现状手写 | 第三轮后 | 消灭 |
|---|---|---|---|
| 单行状态表 | persist.go 130 + repo.go 91 | bind.go 5 + repo.go ~30 | ~185 行 |
| 有界集合（复合 PK） | persist.go 130 + repo.go 91 | bind.go 5 + repo.go ~50 | ~165 行 |
| 复杂（signin 类） | persist.go 130 + repo.go 91 | persist.go 130 + repo.go ~55 | ~35 行（仅 Ext 泛型） |

新增简单玩法总账：**6 文件 ~450 行 → 4 文件 ~290 行**；横切改动维持 3 处一行；SQL 字符串维持 0。

---

## 6. 适用范围：实测数据（重要，修正乐观估计）

对现有 20 张业务表逐一判定：

| 表形态 | 数量 | 实例 | 适用档位 |
|---|---|---|---|
| PK = player_id 单列 | **2** | player_attr、player_signin | FlatRow |
| PK = id 代理键的集合表 | 15 | player_item / player_equip / mail / activity / quest… | 都不适用（代理 id 使 Data=[]T 不自然；差异删除逻辑各异，留给手写） |
| 无 player_id 的全局/账号表 | 3 | account / guild / team | 不适用 |

**诚实结论**：
- FlatRow 当前直接受益的存量只有 2 张表，且 player_signin 因双表结构整体保持手写——即存量改造价值 ≈ 0，价值全在未来按新规范设计的表；
- FlatSet 当前 0 张符合，价值同样在未来（新集合表若按 (player_id, 业务键) 复合主键设计即适用）；
- **唯一对存量立即生效的是 LoadExt/SaveExt**（所有模块省 13 行 + 消灭各自定义的窄接口）。

因此本方案的正确定位不是「改造存量」，而是「把新模块的起步成本压到最低，并保证演进时逃逸成本为零」。若未来一年新模块少于 3 个，只做第一期也成立。

---

## 7. 边界判定树（新增模块时唯一需要做的决策）

```
模块状态需要落库？
├─ 否（纯内存/纯配置驱动）→ 什么都不写
├─ 状态 = 一张玩家级单行表，字段直映、无派生列
│   → RegisterFlat（PK=player_id，sql 注释 @flat:<模块名>）
├─ 状态 = 有界小集合（行数有上界且小，如 < 500），
│   (player_id, 业务键) 复合主键，行字段直映，可整包替换
│   → RegisterFlatSet
└─ 其余（多表 / 派生列 / 无界集合 / 差异删除 / 代理 id）
    → 手写 Persister（以 signin 为参照实现）

无论走哪条路：Ext 读写一律 LoadExt/SaveExt，不再自定义窄接口。
```

判定树同时写进 `docs/新增功能开发流程（以签到为例）.md`，并把 bind.go 作为约定文件名（`grep -r RegisterFlat internal/` 即可全量发现 Flat 模块）。

---

## 8. 兼容性与迁移

1. **Flat → 手写**：删 bind.go，按 signin 模式写 persist.go。**JSON 槽位格式不变**——因为 Flat 的 T 就是 gendb 生成的 struct，其 JSON 字段名由 db tag 决定；切手写时自定义 Data 的 json tag 必须与之逐字一致（文档中标注为迁移检查项）。Redis 旧快照直接可读，无停机。
2. **列名变更**：改 schema.sql 后重新生成，JSON 键随 db tag 漂移 → 旧快照 decode 失败 → Validate 报错 → worker 整批回滚（现有防线，数据卡住但不错乱）。缓解：给生成 struct 显式增加 json tag（与 db tag 同值），使 JSON 键与列名解绑、由 schema 单点控制。
3. **FlatSet 删除语义**：Delete + Upsert 在同一 tx，与其他 Persister 一样受 worker 版本栅栏保护，无并发窗口。
4. **Validate 失败语义**：与手写 Persister 完全一致（报错回滚），不引入「跳过坏玩家」的静默路径。

---

## 9. 明确不做

- 不生成 handle（协议入口与业务规则是业务价值本身，无可生成内容）；
- 不强制 Data = 表结构；
- 不改 worker / snapshot / cache_load / Redis 提交 Lua（零横切是本轮前提）；
- 不迁移 signin 存量实现（保留为手写参照，直到至少一个新模块用 Flat 验证过）；
- 不做 handle 骨架脚手架（那是另一轮工具的事，与持久化无关）。

---

## 10. 风险与缓解

| 风险 | 缓解 |
|---|---|
| 框架内两套持久化写法并存，后人选错 | 判定树文档化 + bind.go 约定文件名 + gendb 对 PK 不符的 @flat 标注直接报错 |
| 泛型可读性下降 | 泛型全部封在 flush/writebehind 框架侧（合计 ~110 行），业务侧只见 `RegisterFlat("name", model.XxxFlat)` 一行声明 |
| gendb 注释剥离与 @flat 提取的顺序坑 | 调整为先提取后剥离，补专门单测（标注存在/不存在/标错 PK 三种用例） |
| FlatSet 批量删除性能（N 玩家 N 次 DELETE） | 第二期补 `DeleteXxxByPlayers` 批量版；在此之前 FlatSet 仅限小集合，批次数百玩家时可接受 |
| JSON 键随列名漂移（见第 8.2） | 生成 struct 显式 json tag，迁移检查项制度化 |

---

## 11. 实施计划（分期，每期独立可交付）

| 期 | 内容 | 规模 | 验证标准 |
|---|---|---|---|
| 一期 | Querier 下沉；LoadExt/SaveExt；FlatRow + RegisterFlat；gendb @flat 标注 + PK 校验；signin 的 repo.go 换用 LoadExt/SaveExt（瘦身体验） | 框架 ~130 行 + 测试 ~150 行 | 单测/集成测试全绿；signin 行为零变化；`make gen` 幂等 |
| 二期 | FlatSet + DeleteByPlayers 批量删 | 框架 ~60 行 | 同上 + 集合场景集成测试 |
| 三期 | 方案 A：model 生成收窄（Flat 表只生成被消费函数，堵死绕过写穿直接 Insert 的误用） | gendb ~40 行 | 全量 build/test 绿；diff 审查生成物收敛 |

建议顺序：一期做完先用一个真实新模块（或 player_attr 试迁）验证，再决定二三期。

---

## 12. 一页总结

- 缓存层早已全通用（Ext 槽位 + 版本 CAS），数据库层已生成 90%，**唯一剩下的胶水是 Data↔表行映射**；
- FlatPersister 不新建链路，只是把这层胶水中「1:1 直映」的部分泛型化，复杂模块逃逸路径永远开放；
- 立即受益：LoadExt/SaveExt 让所有模块省 13 行；新模块按形态最高省 ~185 行；
- 诚实预期：存量改造价值 ≈ 0，价值在未来新模块；新模块计划 < 3 个则只做一期。
