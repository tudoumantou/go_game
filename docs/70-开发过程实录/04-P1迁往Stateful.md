# 70-04 P1：七个模块迁往 Stateful[Data]

> 专题：把「内存结构 ↔ Ext JSON ↔ MySQL 行」的三层映射收进基座，业务文件不再出现持久化代码。
> 涉及提交：`db75402`（Stateful[T] 引入）、`dff3b7a` quest、`09951a4` activity、`f4d375b` partner、`50cb3b5` skill、`7798eab` shop、`3233ea2` equip、`dbcd757` copy。
> 结论：7 个模块业务文件合计净减约 490 行，持久化代码集中到 7 个 `persist.go`。

## 1. 动机：18% 的代码在做「翻译」

Thin 基座解决了模块接口七件套，但模块仍要自己维护三层映射。`db75402` 提交信息里的原话：

> 问题：Thin 解决了接口七件套，但模块仍要维护「内存结构 ↔ Ext JSON ↔ MySQL 行」三层映射。signin 里这部分 67 行（占 18%），且要求业务开发者理解 Ext 与表结构。

一个签到这个 365 行的模块，67 行在 `load/save/decode/validate` 之间倒腾数据。更要紧的是：**每个新模块都要重新学一遍「Ext 是什么、表行长什么样」**，而这段知识对业务逻辑毫无价值。

## 2. Stateful[T]：三个方法，一个约定

```go
// internal/game/module/stateful.go
type Stateful[T any] struct {
	*Thin
	newFn func() *T
}

// Read 以只读方式访问状态：解码后交给 fn，不回写。
func (s *Stateful[T]) Read(ctx *Context, fn func(*T)) error { … }

// Mutate 以读写方式访问状态：fn 返回 true 表示有改动，基座负责写回 Ext 并标脏。
func (s *Stateful[T]) Mutate(ctx *Context, fn func(*T) bool) error {
	d, err := s.decode(ctx)
	if err != nil { return err }
	if !fn(d) { return nil }
	return s.encode(ctx, d)
}
```

`Mutate` 的返回值设计是这个基座的点睛之笔，注释原文：

> 为什么让 fn 返回 bool 而不是无条件写回：很多请求是「只读 + 早退」（已领取、配置缺失、背包已满），无权改状态却触发一次序列化与标脏，会让每条消息都产生一次提交。fn 的返回值就是「我到底改没改」的答案。

业务代码的形态（signin 的 onClaim，迁移后）：

```go
func (m *Module) onClaim(ctx *module.Context, _ uint32, _ []byte) error {
	return m.Mutate(ctx, func(d *Data) bool {
		if d.Claimed[today] {
			return false // 已领取：早退，不产生提交
		}
		d.Claimed[today] = true
		return true     // 有改动 → 写回 Ext 并标脏
	})
}
```

## 3. persist.go：持久化代码的去处

迁移不是「删掉」三层映射，而是把它集中到模块的 `persist.go`，与业务彻底分离。以 quest 为例（`internal/game/module/quest/persist.go`）：

```go
// init 注册持久化：Ext 槽位存 Data（业务结构），落库时转成 player_quest 行。
func init() {
	writebehind.RegisterStateful(stateKey, model.PlayerQuestFlat,
		writebehind.StateCodec[model.PlayerQuest, Data]{
			ToRow:   toRow,
			FromRow: fromRow,
		},
		validate,
	)
}

// toRow 把业务结构转成表行；任务状态整体存一个 JSON 列。
func toRow(playerID int64, d *Data) (*model.PlayerQuest, error) {
	raw, err := json.Marshal(d.Quests)
	if err != nil { return nil, fmt.Errorf("encode quest state: %w", err) }
	return &model.PlayerQuest{PlayerId: playerID, Quests: string(raw)}, nil
}

// validate 落库前校验状态取值与进度范围，坏数据整批回滚。
func validate(d *Data) error {
	for id, rec := range d.Quests {
		if id <= 0 { return fmt.Errorf("invalid quest id %d", id) }
		if rec.Status < statusDoing || rec.Status > statusClaimed {
			return fmt.Errorf("invalid quest status %d", rec.Status)
		}
		if rec.Progress < 0 { return fmt.Errorf("invalid quest progress %d", rec.Status) }
	}
	return nil
}
```

三个函数各有分工：`toRow/fromRow` 管「业务结构 ↔ 表行」，`validate` 是**落库前最后一道闸**——坏数据宁可让这批写回失败，也不把非法状态带进数据库（partner 迁移时把不变量校验也挪进了这里，`f4d375b`）。

`RegisterStateful` 带四项 panic 级校验（重复注册、codec 缺失等），把「注册表配错」从运行时的灵异 bug 变成启动即炸，细节见 [50-游戏服/05](../50-游戏服/05-持久化Stateful.md)。

## 4. 迁移实录

| 模块 | 提交 | 业务文件行数 | 净变化 |
| --- | --- | --- | --- |
| quest | `dff3b7a` | 365 → 299 | −66 |
| activity | `09951a4` | 510 → 332 | −178 |
| partner | `f4d375b` | 386 → 316 | −70 |
| skill | `50cb3b5` | 360 → 300 | −60 |
| shop | `7798eab` | 287 → 268 | −19（同时补了 7 个单测） |
| equip | `3233ea2` | 360 → 328 | −32（加成改闭包外结算） |
| copy | `dbcd757` | 275 → 210 | −65 |
| **合计** | | **2543 → 2053** | **约 −490** |

迁移是**逐模块提交、逐模块可回滚**的：每个提交只动一个模块 + 它的测试，任何一个出问题 `git revert` 单个提交即可，不需要整体回滚。这是「演进不搞大爆炸」原则的标准执行。

equip 那次迁移还顺手修了一个真 bug：穿戴/卸下的属性加成原来在闭包内结算，闭包提前 return 时会漏加——改为闭包外结算（`3233ea2`）。

## 5. 升级注意与测试

两个写入提交信息的风险点：

1. **Ext 的 JSON 从行结构变为业务结构，旧数据需一次性迁移**（demo 环境未涉及，生产升级要写迁移脚本）；
2. `RegisterFlat` 保留——业务结构恰好等于表结构的模块继续用它，不强行统一。

测试侧：`TestStateRoundTrip` 改为验证两段编解码（业务↔Ext、业务↔行）；全量 57 包通过。

## 6. 收益不止是行数

- **新人友好**：写业务模块不需要先理解 Ext 和表结构，`Data` 结构体 + `Read/Mutate` 就是全部接口；
- **bug 半径缩小**：三层映射只有一份实现（基座），七个模块的编解码 bug 一次修复全员受益；
- **与补测试形成正循环**：Stateful 之后，模块测试连「状态怎么造」都简单了——`newData()` 给个空结构体就行，这也解释了为什么 shop 能在同一个提交里从 0% 到 74%。

## 7. 经验沉淀

1. **样板代码的归宿是基座，不是文档**。写「请按 X 模式编写」的文档，不如把 X 变成编译期强制的泛型基座。
2. **「只读 + 早退」必须便宜**。`Mutate` 的 bool 返回值看起来是小细节，实际决定了每条只读消息要不要付一次序列化 + 标脏 + 提交的成本。
3. **持久化迁移一定要逐模块提交**，并且先有测试网（[70-03](03-补齐12个零测试模块.md)）再动手。
