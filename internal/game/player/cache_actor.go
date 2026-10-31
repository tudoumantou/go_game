// internal/game/player/cache_actor.go —— 内存权威模式的消息提交流程（R2.1 / R2.5 / R14）
//
// 架构职责：内存权威（Actor 内存）下，每条业务消息走同一条流水线：
//
//	清改动标记 → 执行业务 → 有改动则**编码并交给异步副本通道** → 立刻刷回包
//
// 协作关系：由 actor.go 的 Receive 分流进入（纯下行消息与无状态模块不进这里）；
// 副本回写由 pkg/writebehind.Replica 在后台批量完成；落库由 pkg/flush 从
// Actor 内存取数——两条异步链路互不相干。
//
// 设计取舍：
//   - **不回写 Redis 的同步等待**（R14）：内存是权威，Redis 只服务「别人读我」，
//     它的新鲜度要求是百毫秒级而不是提交级。所以消息处理完立刻回包，脏字段
//     编码后交给 Replica 合并、200ms 批量刷一次。旧实现在这里同步 Sync，
//     等于让每条改数据的消息都付一次 Redis RTT——内存权威最大的收益被抵消。
//   - 不 clone 备份：Actor 独占数据（同一玩家只有一个执行者），进程内不存在
//     「改到一半发现别人也改了」。跨会话的陈旧写由会话令牌（token）拦截。
//   - 不逐消息 diff：脏标记（State.changed）已经是「改没改」的答案，序列化
//     两份快照做字节比较是重复劳动，还占掉一条消息 60% 的开销。
//   - 失败不回滚：副本同步失败只告警，脏字段保留到下一条消息重试；内存里的
//     改动由 flusher 落库，副本落后由加载时的库版本校验发现并重建。
//   - 回包在提交之后：这里的「提交」已经不含网络等待，缓冲只为了保持
//     「先算完再回包」的顺序（gate 仍然管着下行顺序）。
//
// 历史注记：旧实现有「不可重放消息提交失败 → 保留 cachePending、下条消息
// 先 finishPending 补提」这条路径。内存权威（v3.5）之后副本同步失败只告警
// 不回滚，不再产生 pending，该字段与方法已于 v3.6 删除。
package player

import (
	"context"
	"fmt"
	"time"

	"game_study/internal/common/constant"
	"game_study/internal/common/errcode"
	"game_study/pkg/actor"
	gatev1 "game_study/pkg/pb/gate/v1"
	"game_study/pkg/writebehind"
	"google.golang.org/protobuf/proto"
)

// replicaSyncTimeout 副本写入的超时。
//
// 两条路径共用：异步入队后不需要等（无超时），只有「关服/下线强制刷」与
// 「未装配异步通道时退回同步写」会真的等——2s 足够一次 HSET，超时即放弃，
// 副本落后由加载侧的版本校验兜住，绝不为了副本拖住业务线程。
const replicaSyncTimeout = 2 * time.Second

// enqueueReplica 把本次脏字段交给副本回写通道（R14：异步回写）。
//
// 三段职责，顺序不能换：
//  1. **在本线程编码**（EncodeFields）：后台 goroutine 刷写时 Actor 可能已经
//     在改同一份 State，把 FieldSource 传出去就是数据竞争；
//  2. 交给异步通道（Replica.Enqueue）：O(1) 入队 + 按玩家合并，不等 RTT；
//  3. 通道不可用时退回同步写（SyncArgs）：宁可这条消息慢一次，也不能让
//     改动既不进副本也不落库（脏标记已被下面清掉，丢了就真丢了）。
func (a *Actor) enqueueReplica(fields, removed []string) error {
	if a.repo == nil || a.repo.cache == nil {
		return nil
	}
	args, err := writebehind.EncodeFields(a.state, fields, removed)
	if err != nil {
		return err
	}
	if len(args) == 0 {
		return nil
	}
	if a.repo.replica != nil {
		if err := a.repo.replica.Enqueue(writebehind.ReplicaRecord{
			PlayerID: a.playerID,
			Token:    a.token,
			Args:     args,
		}); err == nil {
			return nil
		}
		// 走到这里只有两种可能：通道未启动 / 已在关闭中。
		// 降级为同步写，不让改动悬空。
		a.logger.Debug("replica enqueue unavailable, fallback to sync", "player_id", a.playerID)
	}
	ctx, cancel := context.WithTimeout(context.Background(), replicaSyncTimeout)
	defer cancel()
	version, err := a.repo.cache.SyncArgs(ctx, a.playerID, a.token, args)
	if err == nil {
		// 同步路径能拿到版本，顺手记下；异步路径不等结果，cacheVersion
		// 停在「加载/重建时的副本版本」，它只用于日志与落库确认。
		a.cacheVersion = version
	}
	return err
}

// flushReplica 立刻把本玩家积压的副本记录刷进 Redis（不等 200ms 定时器）。
//
// 调用时机只有两个：下线（让别的玩家马上读到最新）与 A 类原子落库后
// 「Drop + Seed 重建」之前（否则旧记录会在 Seed 之后落地，把重建好的
// hash 改回旧值并把 version 抬高）。
func (a *Actor) flushReplica() error {
	if a.repo == nil || a.repo.replica == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), replicaSyncTimeout)
	defer cancel()
	return a.repo.replica.FlushPlayer(ctx, a.playerID)
}

// receiveCached 内存权威模式下一条消息的完整生命周期。
//
// 顺序即正确性：冷/热加载 → 过期重载 → 执行业务 → 脏标记判定 → 交给副本通道。
// 任何一步失败都走 cacheFailure，由它按消息类型给出下行错误。
func (a *Actor) receiveCached(ctx *actor.Context) {
	if msg, ok := ctx.Message().(*BindMsg); ok {
		// 会话路由独立于热状态加载，即使 Redis 故障也要能向当前连接回报错误。
		a.sessionID = msg.SessionID
		a.online = true
	}
	if !a.loaded {
		switch ctx.Message().(type) {
		case *SaveMsg, *UnbindMsg:
			return
		}
		if err := a.preload(ctx); err != nil {
			a.cacheFailure(ctx, err)
			return
		}
	}
	if a.cacheReload {
		// 强制冷启动：跳过 Redis，直接从库重建。
		//
		// 走到这里的原因只有「内存可能处于半改状态」（panic 恢复）。内存权威
		// 模式下不能拿 Redis 副本覆盖内存——副本可能落后于库，用它重建等于
		// 把已落库的改动倒回去。
		loadCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		state, version, err := a.repo.LoadCachedForce(loadCtx, a.playerID)
		cancel()
		if err != nil {
			a.cacheFailure(ctx, err)
			return
		}
		// 保留在线派生加成，库与缓存都只保存持久化基础属性。
		state.bonus = a.state.bonus
		a.setState(state)
		a.cacheVersion = version
		a.cacheReload = false
	}
	// 提交窗口：清改动标记 → 业务 → 按 HasChanged 决定是否写 Redis。
	//
	// R2.5：不再 clone 状态、不再序列化两份做字节比较。依据是 Actor 独占数据
	// （同一玩家只有一个执行者），跨会话的陈旧写由会话令牌（token）拦截——
	// 令牌失效时提交直接被拒，不需要「先备份、失败回滚」这套保险。
	a.state.ClearChanged()
	a.gate.Begin()
	// panic 时内存可能处于半改状态：不回滚（重载必然覆盖），标记重载即可。
	defer func() {
		a.gate.Discard()
		if r := recover(); r != nil {
			a.cacheReload = true
			panic(r)
		}
	}()
	a.receive(ctx)

	var commitErr error
	// 提交条件看「有没有未同步的字段」，而不是「这条消息改没改」。内存权威
	// 模式下副本允许失败，失败时脏字段保留；如果只按 HasChanged 判断，
	// 下一条消息本身没有改动就不会触发提交，累积的脏字段永远同步不出去。
	// 只写变化的字段（R2.5-B）：签到不重写整个背包；字段由 State 现取现
	// 序列化，不再先构造整份快照（省一次全量复制）。
	fields, removed := a.state.DirtyFields()
	if a.state.HasChanged() || len(fields) > 0 || len(removed) > 0 {
		if len(fields) == 0 && len(removed) == 0 {
			// 防御：有改动却没有字段标记，说明字段映射漏了。
			// 宁可多写一次整份，也不能让改动不落库。
			fields = a.state.Fields()
		}
		// 异步回写（R14）：编码在本线程完成（拷贝字节），入队即返回——
		// 不再在消息处理函数里等一次 Redis RTT。这是内存权威应得的收益：
		// 业务正确性只依赖内存，副本由 200ms 的批量刷写跟进。
		commitErr = a.enqueueReplica(fields, removed)
		if commitErr == nil {
			// 只清「副本同步」用的字段标记，**不清 ClearItemsDirty**：
			// 后者是落库用的增量道具标记，由 repo.Save 在写库成功后清理。
			// 在这里清掉会让 flusher 以为没有道具要写，改动只活到进程退出。
			a.state.ClearDirtyFields()
		}
	}
	// 内存权威：Redis 只是副本，入队/同步失败都不影响本次操作的正确性——
	// 落库走 flusher（数据源是内存，不是 Redis），副本落后由加载时的
	// 库版本校验兜住（见 Repository.LoadCached）。
	//
	// 唯一必须失败的是 BindMsg：它本身就是「加载玩家」，加载不到就没有
	// 后续操作可言，此时报错比放一个空状态进去安全。
	if commitErr != nil {
		if _, isBind := ctx.Message().(*BindMsg); isBind {
			a.gate.Discard()
			a.cacheFailure(ctx, commitErr)
			return
		}
		// 只告警：这是「副本同步失败」，不是「业务失败」。内存里的改动会由
		// flusher 在本轮或下轮落库；玩家重登时若副本确实落后，加载侧会用
		// player_writebehind.version 发现并以库为准重建。
		a.logger.Warn("cache replica sync failed", "err", commitErr)
	}
	// 刷出暂存的下行。发送失败只记日志——数据已经落定（内存 + 落库队列），
	// 因为一条推送失败去回滚一次已生效的操作没有意义。
	for _, err := range a.gate.Commit() {
		a.logger.Warn("cached reply failed", "err", err)
	}
}

// cacheFailure 缓存路径失败时的统一下行：日志 + 按消息类型回错误。
func (a *Actor) cacheFailure(ctx *actor.Context, err error) {
	a.logger.Warn("player cache operation failed", "err", err)
	switch msg := ctx.Message().(type) {
	case *GrantItemsMsg:
		if msg.Done != nil {
			msg.Done <- err
		}
	case *BindMsg:
		_ = ctx.Respond(&BindResult{Code: 1, Err: err.Error()})
	case *PacketMsg:
		// 模块实现 CacheFailureAware 时由它给出带错误码的业务响应；
		// 否则退回网关通用错误，避免把协议号写死在玩家层。
		if a.registry != nil {
			if id, body, ok := a.registry.CacheFailureReply(msg.MsgID); ok {
				_ = a.send(id, body)
				return
			}
		}
		body, _ := proto.Marshal(&gatev1.ErrorNotify{
			Code:    int32(errcode.ErrServerBusy),
			Msg:     errcode.Message(errcode.ErrServerBusy),
			Context: fmt.Sprintf("related_msg_id=%d", msg.MsgID),
		})
		_ = a.send(constant.MsgIDErrorNotify, body)
	}
}
