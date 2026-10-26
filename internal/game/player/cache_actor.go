// internal/game/player/cache_actor.go —— 缓存权威模式的消息提交流程（R2.1 / R2.5）
//
// 架构职责：缓存权威（Redis 快照）下，每条业务消息走同一条流水线：
//
//	清改动标记 → 执行业务 → 有改动则凭会话令牌提交 → 成功刷回包 / 失败标记重载
//
// 协作关系：由 actor.go 的 Receive 分流进入（纯下行消息与无状态模块不进这里）；
// 提交、重载全部在本文件，业务 handler（各模块）对此无感。
//
// 设计取舍（R2.5 简化，逐条对应旧实现）：
//   - 不 clone 备份：Actor 独占数据（同一玩家只有一个执行者），进程内不存在
//     「改到一半发现别人也改了」。跨会话的陈旧写由会话令牌（token）拦截。
//     原先 clone 出的备份在失败路径上先回滚、下一条消息立刻被 LoadCached 覆盖
//     ——只活了一瞬间。
//   - 不逐消息 diff：脏标记（State.changed）已经是「改没改」的答案，序列化
//     两份快照做字节比较是重复劳动，还占掉一条消息 60% 的开销。
//   - 失败不回滚而重载：重置比修补可靠；重载只在失败时发生一次，
//     而 clone 是每条消息都付。
//   - 回包缓冲到提交之后：否则客户端先看到「成功回包」再收到「提交失败」，
//     业务状态与客户端认知长期分叉。缓冲由 commitGate 统一管理。
//
// 历史注记：旧实现有「不可重放消息提交失败 → 保留 cachePending、下条消息
// 先 finishPending 补提」这条路径。内存权威（v3.5）之后副本同步失败只告警
// 不回滚，不再产生 pending，该字段与方法已于 v3.6 删除。
package player

import (
	"context"
	"fmt"
	"time"

	"game_study/pkg/actor"
	gatev1 "game_study/pkg/pb/gate/v1"
	"google.golang.org/protobuf/proto"
)

// receiveCached 缓存权威模式的一条消息的完整生命周期。
//
// 顺序即正确性：冷/热加载 → 过期重载 → 执行业务 → 脏标记判定 → 令牌提交。
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
		saveCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		var version int64
		if len(fields) == 0 && len(removed) == 0 {
			// 防御：有改动却没有字段标记，说明字段映射漏了。
			// 宁可多写一次整份，也不能让改动不落库。
			fields = a.state.Fields()
		}
		// Sync 而不是 Commit：内存权威下落库走 flusher（数据源是内存），
		// Redis 只更新副本、不登记脏队列——入队会引来 writebehind worker
		// 用可能落后的副本与 flusher 争写同一批表。
		version, commitErr = a.repo.cache.Sync(saveCtx, a.playerID, a.state, fields, removed, a.token)
		cancel()
		if commitErr == nil {
			a.cacheVersion = version
			// 只清「副本同步」用的字段标记，**不清 ClearItemsDirty**：
			// 后者是落库用的增量道具标记，由 repo.Save 在写库成功后清理。
			// 在这里清掉会让 flusher 以为没有道具要写，改动只活到进程退出。
			a.state.ClearDirtyFields()
		}
	}
	// 内存权威：Redis 只是副本，同步失败不影响本次操作的正确性——
	// 落库走 flusher（数据源是内存，不是 Redis），副本落后由加载时的
	// 库版本校验兜住（见 Repository.LoadCached）。
	//
	// 唯一必须失败的是 BindMsg：它本身就是「从 Redis 加载玩家」，加载不到
	// 就没有后续操作可言，此时报错比放一个空状态进去安全。
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
		body, _ := proto.Marshal(&gatev1.ErrorNotify{Code: 1005, Msg: "缓存暂不可用，请重试", Context: fmt.Sprintf("related_msg_id=%d", msg.MsgID)})
		_ = a.send(20, body)
	}
}
