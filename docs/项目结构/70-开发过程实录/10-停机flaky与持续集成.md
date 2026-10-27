# 70-10 停机 flaky 与持续集成

> 专题：两个时间竞争 flaky 的结构化同步、一次「停机取消被当故障」的假告警、integration 包起死回生、CI 五步卡点。
> 涉及提交：`497e603`（shutdown flaky）、`c441ac2`（停机假告警 + msgID 注释修正）、`3b47b9a`（integration 恢复可编译）、`ec6258d`/`c629225`/`a5f369f`（测试重构）、CI 配置。
> 结论：停机用例负载下各 100 次通过；integration 从「从未编译过」到 21 个用例真跑。

## 1. 停机 flaky：两个 timer 竞争

`pkg/shutdown` 的两个用例（`TestRunStopsWhenBudgetExhausted`、`TestRunStepTimeout`）在全量并行跑（CPU 吃满）时偶发失败，根因相同（`497e603` 提交信息）：

> 「步骤阻塞 X 毫秒 vs 超时 Y 毫秒」用 select 竞争，负载高时两个 timer 同时到期，select 随机选中「睡够」分支，于是——TestRunStopsWhenBudgetExhausted：步骤正常返回，Run 返回 nil（期望报预算耗尽）；TestRunStepTimeout：同一理由，步骤不再超时。

**修法：结构化同步**。预算用例改成「手动 cancel + channel 显式放行」，步骤故意不响应 ctx（模拟「跑完了但预算恰好用尽」）：

```go
started := make(chan struct{})
release := make(chan struct{})
m.Register("drain-inflight", PhaseDraining, func(context.Context) error {
	close(started)
	<-release
	return nil // 故意不响应 ctx：真实步骤可能已经跑完，而预算恰好用尽
})
ctx, cancel := context.WithCancel(context.Background())
done := make(chan error, 1)
go func() { done <- m.Run(ctx) }()

<-started
cancel()     // 预算耗尽（生产里是 WithTimeout 到期）
close(release)

select {
case err := <-done:
	require.Error(t, err, "预算耗尽必须报错，不能静默跳过剩余步骤")
	require.Contains(t, err.Error(), "budget", …)
	require.Contains(t, err.Error(), "flush", "要告诉运维是从哪一步开始被放弃")
	require.Empty(t, rec.got(), "预算耗尽后不能再执行后续步骤")
case <-time.After(2 * time.Second):
	t.Fatal("Run 未在预期时间内返回")
}
```

单步超时用例更简单：**步骤只等 `ctx.Done()`，不再有第二个 timer 与它竞争**——只等待一个信号，才能确定地测到超时。

效果：修之前约 1/300 失败；修之后负载下各 100 次通过。

## 2. 停机取消不再当故障上报

同一场战役的另一半（`c441ac2`）：`worker.Run` 的 FlushBatch 用派生 ctx，父 ctx 取消后正在进行的 Redis/SQL 调用返回 `context.Canceled`，被 `onError` 当故障上报——全量并行跑时窗口重叠概率变大，表现为 `TestRunStaggersPartitionsAndDrains` 偶发「unexpected flush error」。

**修法**：抽出 `isShutdownCancel`，只有「父 ctx 已取消 + Canceled」才静默退出；超时（`DeadlineExceeded`）、父 ctx 存活时的取消、其他错误照常上报。

```go
// isShutdownCancel 判定「这是停机取消，不是故障」。
// 只有父 ctx 已取消且错误是 Canceled 才静默——
// 超时（DeadlineExceeded）说明这批真的写不完，必须上报。
```

区分「预期的取消」与「异常的取消」，是所有 ctx 传播链路的必修课；原则见 [50-游戏服/01](../50-游戏服/01-游戏服总览与启动.md) 的停机四阶段。

## 3. integration 包起死回生

`test/integration` 带 `integration` 构建标签，**一直不参与 `go test ./...`**，因此上一轮批量断言转换留下的 30+ 处重复声明从未暴露——整个包编译不过，P6~P14 的集成用例实际上一次都没跑过（`3b47b9a` 提交信息）。

修复内容：

- 逐处改为复用已有变量（`:=` → `=`），2 处 `int`/`int64` 冲突改用独立变量名；
- `TestSigninActorFlow` 的「Redis 丢失」只删了 state 没删脏队列，会命中 `seedScript` 的保护（脏队列里还有比 MySQL 更新的快照，拒绝用旧数据覆盖）——改为 `Del + ZRem`，与真实「Redis 整体丢失」一致；
- p12/p6b 的 seed 仍按 `player_equip`/`activity_record` 的旧列结构插入，两表已改为整份 JSON 落库——按 `sql/schema` 注释改为数组/映射形状。

修复后 **21 个用例中 18 个通过**，剩余 3 个（P6/P6B 的重复领取幂等、P12 的 PlayerInfo RPC）是长期未运行积累的既有缺陷，另行处理——其中两个正是 [70-02](02-P0响应串号与幂等失效.md) 的重复回包事故。

> 教训：**带 tag 的包要定期真跑**。`go test -tags integration ./...` 必须进 CI（现在它在，见 [60-测试体系/04](../60-测试体系/04-集成测试与CI.md)），否则「测试存在」只是幻觉。

## 4. CI 五步卡点

`.github/workflows/ci.yaml` 把「生成体系的可信度」交给 CI 强制保证：

| 步骤 | 命令 | 拦什么 |
| --- | --- | --- |
| ① gen + diff | `make gen` 后 `git diff --exit-code -- pkg/pb pkg/gamedata pkg/model` | 手改生成代码、源改了没重新生成 |
| ② buf lint | `cd api/proto && buf lint` | proto 规范问题 |
| ③ golangci-lint | v1.64.8（锁大版本） | 静态检查 |
| ④ go test | `go test -tags=integration ./... -count=1 -timeout 15m` | 单测 + 集成（testcontainers 起真中间件） |
| ⑤ 交叉编译 | 五服务 | 编译期问题 |

版本锁的理由写在 workflow 注释里：buf 锁 1.74.0 防生成输出漂移导致 gen diff 误报；golangci-lint v2 要求 go >= 1.23 与 go.mod 的 1.22 不兼容；Go 1.22 与 go.mod 一致（GOTOOLCHAIN 不触发额外下载）。

**benchmark 进 CI 的结论：只记录，不卡门禁**。三层理由（[60-测试体系/04](../60-测试体系/04-集成测试与CI.md) 有展开）：CI 机器性能漂移会让基准数字不可比；benchmark 失败与回归的映射不直接；真压测走 `test/robot` + `reports/bench-*` 归档。CI 里跑一次记录趋势即可。

## 5. 测试重构：为可维护性付的账

本轮之前还做了一轮测试自身的大扫除（`091ef50`/`ec6258d`/`a5f369f`/`bea479d`）：

- `if cond { t.Fatalf }` 断言统一为 `require`：if-断言从 **1808 处降到 215 处**（−88%），测试代码净减 1310 行；
- 机械转换 677 处比较断言后，132 处逐条比对「原条件」与「require 谓词」的**方向**，0 异常——这轮人工核对抓出过「方向写反」的实例（断言期望 `True` 实际该 `False`，被 `TestGridLifecycle` 抓到后回滚重做）；
- `server.go` 1397 → 465 行（`c629225`），新增 `make new-module` 脚手架——装配逻辑拆出去，新模块不再往同一个文件里堆。

## 6. 经验沉淀

1. **时间竞争的测试，改同步原语，不是调大超时**。调大超时只是降低失败概率，结构化同步才是归零。
2. **区分「预期的取消」与「异常的取消」**：ctx 链路上的每个错误回调都要问这一问题。
3. **tag 包必须进 CI**：不参与常规运行的测试等于不存在，而且腐烂得更快（没人发现它编译不过）。
4. **断言方向要人工核对**：机械转换省时间，但 `require.True` vs `require.False` 写反只有逐条比对或变异验证能抓。
