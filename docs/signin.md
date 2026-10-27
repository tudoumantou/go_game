# 七日签到

## 玩法

- 一周固定为北京时间 UTC+8 的**周一至周日**，进程或容器的系统时区不影响判断。
- 每天只能领取**当天**对应的一档奖励；漏签不可补，同一天重复请求返回 `already=true`，不重复发奖。
- 状态只有两份：已领取天数列表（1=周一…7=周日）与已领取奖励汇总（`{道具ID: 数量}`）。
- 奖励直接进入背包，第七天示例包含金币和经验药水，实际数值修改 XML。
- 查询与领取都在玩家 Actor 内存完成，在线不执行 SQL。

**周重置尚未接入**：清零动作属于周期回调模块的职责（由它在周一零点调用 `signin.Reset`），本模块自身不做定时重置。在回调模块上线前，跨周后上周的领取记录仍会被视为本周已领——这是已知缺口，不是行为承诺。

## 协议

定义源：`api/proto/game/v1/game.proto`（6600 段），占用 6600–6699 号段中的 6601–6604。

| 请求 / 响应 | 功能 |
| --- | --- |
| 6601 / 6602 | `SigninInfoReq` / `SigninInfoResp`，返回今天周几、今天是否已领、已领取天数、已领取奖励汇总 |
| 6603 / 6604 | `SigninClaimReq` / `SigninClaimResp`，领取今天的奖励 |
| 3010 | 已有 `BagChangeNotify`，成功发奖后推送背包最新数量 |

客户端不传角色、日期、档位或数量。错误码：0 成功；1 系统错误（允许重试）；6601 奖励配置错误；6602 背包道具数量超过 INT32 上限。

## 源文件与生成

| 文件 | 说明 |
| --- | --- |
| `api/proto/game/v1/game.proto`（6600 段） | → `pkg/pb/game/v1/game.pb.go` |
| `configs/gamedata/xml/signin_reward.xml` | `day` 为周几 1–7，同一天可配多种道具 → `pkg/gamedata/signin_reward_gen.go` |
| `sql/schema/game.sql`（`player_signin` 表） | 单表，主键 `(player_id)`，注释 `-- @flat:signin` → `pkg/model/player_signin_gen.go`（含 `PlayerSigninFlat`） |
| `internal/game/module/signin/signin.go` | 全部业务与持久化，约 330 行 |
| `internal/game/server.go` | 一行注册 `signin.New(s.logger)` |
| `internal/game/module/register/register.go` | 一行空白导入，供 flusher 与集成测试加载持久化扩展点 |

表结构：

```sql
CREATE TABLE `player_signin` (
  `player_id` BIGINT UNSIGNED NOT NULL,
  `claimed_days` TEXT NOT NULL,    -- JSON 数组，如 [1,3]
  `claimed_rewards` TEXT NOT NULL, -- JSON 对象，如 {"1001":300}
  PRIMARY KEY (`player_id`)
);
```

修改协议、XML、DDL 后运行 `make gen`。生成文件禁止手工修改。

## 持久化：FlatPersister

签到是「玩家级单行状态表」的典型：主键恰为 `(player_id)`，一行装下全部状态。因此不需要手写 `Persister`、`Repo`、`Calendar`：

```go
// signin.go
func init() {
    writebehind.RegisterFlat[model.PlayerSignin](stateKey, model.PlayerSigninFlat, validate)
}
```

- 模块侧只做 `LoadExt[model.PlayerSignin]` / `SaveExt`，两个 JSON 列在这里与业务视图 `SignIn{Days, Rewards}` 互转。
- `validate` 在落库前校验天数 1–7、奖励数量为正，坏数据整批回滚。
- 冷启动由框架调用生成的 `FindPlayerSigninByPK` 回源；批量落库由生成的 `UpsertPlayerSigninBatch` 完成。
- `Reset(state)` 供周期回调模块在周切换时清空槽位，本模块不主动调用。

在线链路与其它 Tier-2 玩法一致：

```text
查询：Actor 内存 →（登录恢复时）Redis → 只有缓存不存在才回源 MySQL
更新：Actor 暂存修改 → Redis Lua 原子提交快照与脏任务 → 成功回包
落库：独立 flusher 定时读取脏任务 → 多玩家合批 SQL 事务 → 按版本确认
```

签到状态放在快照扩展位 `Snapshot.Ext["signin"]`。Redis key 为 `wb:{player-N}:state:PLAYER_ID`，脏队列 `wb:{player-N}:dirty`，N = `player_id % 10`。

## 部署与验证

已有数据库升级时先停旧游戏服和旧 flusher，再执行：

```sql
DROP TABLE IF EXISTS signin_claim;
DROP TABLE IF EXISTS player_signin;
```

然后按 `sql/schema/game.sql` 中新的 `player_signin` 建表。旧快照（`anchor_day`/`claims` 形状）与新结构不兼容，升级时需清空 `Ext["signin"]` 或让玩家从零开始。

```bash
go test ./internal/game/module/signin ./pkg/model ./pkg/writebehind
go test -tags=integration ./test/integration -run '^TestSigninActorFlow$' -count=1 -v
```

集成测试使用真实 Redis/MySQL 与玩家 Actor，覆盖：领取只写缓存不写 SQL、两名角色同分片合批落库、重复领取幂等、Actor 重启从缓存恢复、Redis 丢失后从 SQL 冷启动恢复。
