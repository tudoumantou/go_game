# 07 Actor运行时与并发所有权

[上一章](06-gRPC服务发现与网关转发.md) · [课程总览](00-总览与阅读顺序.md)

## 这一章做出什么

实现可投递、有界信箱、分片调度与玩家状态所有权，理解 Actor 能保证和不能保证的事。

## 前置条件与本章文件

已能把网络请求转换为内部消息；先写纯内存 Actor 测试，不接 SQL。

- [pkg/actor/actor.go](../../pkg/actor/actor.go)：PID 与 Receive 接口。
- [pkg/actor/mailbox.go](../../pkg/actor/mailbox.go)：有界队列策略。
- [pkg/actor/system.go](../../pkg/actor/system.go)：分片执行与定时器。
- [pkg/actor/context.go](../../pkg/actor/context.go)：执行上下文。
- [internal/game/player/actor.go](../../internal/game/player/actor.go)：玩家消息类型。

## 按顺序实现

第一步先做“一个 channel + 一个消费者 + 一个计数状态”。这不是最终 ActorSystem，但能证明状态只由消费者修改。完成后再加入 Actor ID、多个邮箱和分片。

```go
// 可独立运行的教学程序：保存到练习目录 cmd/actorlab/main.go。
package main

import "fmt"

type Add struct {
    N int
    Done chan int // 单次确认；发送后不要复用或关闭它。
}
func main() {
    inbox := make(chan Add, 8) // 有界容量表达系统最多接受多少待处理工作。
    stopped := make(chan struct{})
    go func() {
        defer close(stopped)
        total := 0 // 只有该 goroutine 读写，所以不需要额外 mutex。
        for msg := range inbox {
            total += msg.N
            msg.Done <- total // 调用方提供容量 1，避免它离开后生产者卡死。
        }
    }()
    ack := make(chan int, 1)
    inbox <- Add{N: 3, Done: ack}
    fmt.Println(<-ack) // 必须打印 3；入队后立即读共享变量不是同等保证。
    close(inbox) // 由唯一生产方结束；消费方处理完缓冲再退出。
    <-stopped
}
```

第二步抽象 Actor.Receive(*Context)，消息使用类型开关。内部消息是 BindMsg、PacketMsg、SaveMsg、GrantItemsMsg，不必每种内部消息都 protobuf 序列化。

第三步通过 hash(id) 选择 shard；一个分片循环调度多个 Actor，单 Actor 不同时执行两个 Receive。不同 shard 可并行；同 shard 上同步慢操作会互相影响。

第四步定义背压。当前仓库与练习版一致：Send 满时明确返回 `ErrMailboxFull`，旧消息原样保留，绝不静默丢弃经济消息；调用方（落库、发奖、定时器）各自决定重试或拒绝。*（R1.1 之前的实现是“丢最旧再放新消息”，已按此原则改掉。）*

第五步加入定时器，只投递消息，不在定时器 goroutine 直接改玩家 State。重启后要恢复的任务，截止时间与状态必须持久化；内存 ScheduleOnce 本身不会恢复。

## 核心源码与注释阅读

源码节选：[pkg/actor/system.go](../../pkg/actor/system.go)。这是现有包中的片段，依赖同文件类型，不是可单独运行的完整程序。

```go
func (s *System) Spawn(id string, actor Actor) (*PID, error) {
	sh := s.shardFor(id)
	pid := &PID{ID: id, Shard: sh.index}
	cell := newActorCell(pid, actor, s.mailboxSize, s)
	if err := sh.add(cell); err != nil {
		return nil, err
	}
	if metrics.Enabled() {
		metrics.ActorCountAdd(1)
	}
	return pid, nil
}
```

源码节选：[pkg/actor/mailbox.go](../../pkg/actor/mailbox.go)。这是现有包中的片段，依赖同文件类型，不是可单独运行的完整程序。

```go
func (m *mailbox) push(env envelope) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.closed {
		return ErrActorGone // 终止后投递：可识别，不 panic
	}
	select {
	case m.queue <- env:
		return nil
	default:
		m.rejects.Add(1)
		return ErrMailboxFull // 背压：拒绝新消息，旧消息保留
	}
}
```

源码节选：[internal/game/player/actor.go](../../internal/game/player/actor.go)。这是现有包中的片段，依赖同文件类型，不是可单独运行的完整程序。

```go
func (a *Actor) Receive(ctx *actor.Context) {
	if a.repo.CacheEnabled() {
		if _, purePush := ctx.Message().(*PushMsg); !purePush {
			a.receiveCached(ctx)
			return
		}
	}
	a.receive(ctx)
}

func (a *Actor) receive(ctx *actor.Context) {
	if a.needsReload {
		switch ctx.Message().(type) {
		case *SaveMsg, *UnbindMsg:
			// 不能将可能过期的背包写回。解绑仍需清除会话。
			if _, ok := ctx.Message().(*UnbindMsg); ok {
				a.online = false
				a.sessionID = 0
				return
			}
			// 保存请求必须回执失败：否则调用方只能等到超时，无法区分
			// 「在途」和「被拒绝」。
			if msg, ok := ctx.Message().(*SaveMsg); ok {
				replySave(msg.Done, fmt.Errorf("player %d needs reload; refusing to overwrite", a.playerID))
			}
			return
		default:
			reloadCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			err := a.repo.ReloadItems(reloadCtx, a.state)
			cancel()
			if err != nil {
				a.logger.Error("reload after uncertain commit failed", "err", err)
				switch msg := ctx.Message().(type) {
				case *BindMsg:
					_ = ctx.Respond(&BindResult{Code: 1, Err: err.Error()})
				case *GrantItemsMsg:
					if msg.Done != nil {
						msg.Done <- err
					}
				}
				return
			}
			a.needsReload = false
		}
	}
	switch msg := ctx.Message().(type) {
	case *BindMsg:
		a.onBind(ctx, msg)
	case *UnbindMsg:
		a.onUnbind(ctx, msg)
	case *PacketMsg:
		a.onPacket(ctx, msg)
	case *SaveMsg:
		a.onSave(ctx, msg)
	case *GrantItemsMsg:
		a.onGrantItems(ctx, msg)
	case *PushMsg:
		_ = a.send(msg.MsgID, msg.Body)
	default:
		a.logger.Debug("unknown message", "type", fmt.Sprintf("%T", msg))
	}
}
```

## 逐段解释与知识点

PID 包含逻辑 ID 和 shard，但实际 Send 仍按 ID 查找分片；不要从 PID 结构猜测调用。当前 shardFor 使用 FNV 取模，Config 对分片数仍要求 2 的幂，这是一处约束与算法描述需要分开的例子。

有界队列控制内存，但队列策略决定业务可靠性。丢旧策略可能使保存已不在邮箱里，而新消息返回 nil；只有明确允许丢弃的状态快照才适合合并/覆盖。

终止期间 deliver 先取 cell 再 push，remove 会 close mailbox。需要检查二者并发是否可能 send on closed channel；给 map 加锁后立刻释放，不能把邮箱生命周期一起自动保护。

Recover 能保护分片循环继续跑，但不能回滚消息处理前半段对状态的修改。panic 后角色是否还能安全继续，需要领域状态与恢复策略。

## 怎么验证与定位错误

执行 `go test ./pkg/actor -v`。教学程序用 `go run ./cmd/actorlab`，打印 3 后退出；随后把 N=1 的消息发送一百次，最终值必须为 100。

进一步测满信箱、单玩家顺序、热玩家与冷玩家公平性、定时器取消和 Terminate/Send 竞争。race 只报告运行到的竞态，不是并发安全证明。

## 独立练习与通过标准

把教学 Add 改为带 ItemID 和 Count 的增加物品消息，再增加 ReadBag 查询消息，禁止消费者之外访问 map。通过标准：只有一个写入归属，有超时与停止通知，能说明每次确认代表什么。

下一章：[第 08 章 角色创建与登录预加载](08-角色创建与登录预加载.md)。
