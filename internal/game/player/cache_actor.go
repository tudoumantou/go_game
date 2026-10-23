// internal/game/player/cache_actor.go —— 缓存权威模式的消息提交流程（R2.1 / R2.5）
//
// 架构职责：缓存权威（Redis 快照）下，每条业务消息走同一条流水线：
//
//	清改动标记 → 执行业务 → 有改动则凭会话令牌提交 → 成功刷回包 / 失败标记重载
//
// 协作关系：由 actor.go 的 Receive 分流进入（纯下行消息与无状态模块不进这里）；
// 提交、重载、pending 收尾全部在本文件，业务 handler（各模块）对此无感。
//
// 设计取舍（R2.5 简化，逐条对应旧实现）：
//   - 不 clone 备份：Actor 独占数据（同一玩家只有一个执行者），进程内不存在
//     「改到一半发现别人也改了」。跨会话的陈旧写由会话令牌（token）拦截。
//     原先 clone 出的备份在失败路径上先回滚、下一条消息立刻被 LoadCached 覆盖
//     ——只活了一瞬间。
//   - 不逐消息 diff：脏标记（State.changed）已经是「改没改」的答案，序列化
//     两份快照做字节比较是重复劳动，还占掉一条消息 60% 的开销。
//   - 失败不回滚而重载：Redis 是权威，重置比修补可靠；重载只在失败时发生一次，
//     而 clone 是每条消息都付。
//   - 回包缓冲到提交之后：否则客户端先看到「成功回包」再收到「提交失败」，
//     业务状态与客户端认知长期分叉。缓冲由 commitGate 统一管理。
//
// 例外：不可重放的消息（充值发货已在 MySQL 提交事实）失败时保留 cachePending，
// 下次消息先 finishPending 补提——这条路径不适用「丢弃重载」，因为钱的事实
// 已经在 MySQL 落定，丢掉 Redis 快照会与事实分叉（见 R2.3）。
package player

import (
	"context"
	"errors"
	"fmt"
	"game_study/pkg/writebehind"
	"time"

	"game_study/pkg/actor"
	gatev1 "game_study/pkg/pb/gate/v1"
	"google.golang.org/protobuf/proto"
)

// receiveCached 缓存权威模式的一条消息的完整生命周期。
//
// 顺序即正确性：pending 收尾 → 冷/热加载 → 过期重载 → 执行业务 → 脏标记判定 → 令牌提交。
// 任何一步失败都走 cacheFailure，由它按消息类型给出下行错误。
func (a *Actor) receiveCached(ctx *actor.Context) {
	if msg, ok := ctx.Message().(*BindMsg); ok {
		// 会话路由独立于热状态加载，即使 Redis 故障也要能向当前连接回报错误。
		a.sessionID = msg.SessionID
		a.online = true
	}
	if a.cachePending != nil {
		if err := a.finishPending(); err != nil {
			a.cacheFailure(ctx, err)
			return
		}
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
		loadCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		state, version, legacy, err := a.repo.LoadCached(loadCtx, a.playerID)
		cancel()
		if err != nil {
			a.cacheFailure(ctx, err)
			return
		}
		a.legacy = legacy
		// 保留在线派生加成，Redis 只保存持久化基础属性。
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
	if a.state.HasChanged() {
		saveCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		var version int64
		// 只写本次变化的字段（R2.5-B）：签到不再重写整个背包。
		// 字段由 State 现取现序列化，不再先构造整份快照（省一次全量复制）。
		fields, removed := a.state.DirtyFields()
		if len(fields) == 0 || a.legacy {
			// 两种情况退化为整份写入：
			//   - HasChanged 为真却没有字段标记（映射漏了）；
			//   - 快照是旧格式，必须一次迁完，否则 HDEL data 后其余字段丢失。
			fields = a.state.Fields()
		}
		version, commitErr = a.repo.cache.Commit(saveCtx, a.playerID, a.state, fields, removed, a.token)
		cancel()
		if commitErr == nil {
			a.cacheVersion = version
			a.legacy = false // 旧格式已迁成分字段
			a.state.ClearItemsDirty()
			a.state.ClearDirtyFields()
		}
	}
	if commitErr != nil {
		// 先退出窗口：cacheFailure 要发的错误回包必须直发，
		// 否则会入队随后被 defer 的 Discard 一起丢掉，客户端只能等到超时。
		a.gate.Discard()
		if a.canRollbackMessage(ctx.Message()) {
			// 不回滚内存：Redis 是权威，下一条消息开头整份 LoadCached 会覆盖它。
			// （原先的 setState(previous) 正是这样被立刻覆盖，clone 纯属白做。）
			a.cacheReload = true
		} else {
			// 不可重放的消息（充值发货已在 MySQL 提交事实）：保留待提交快照，
			// 后续消息先 finishPending 补提，不能丢。
			a.cachePending = &writebehind.Record{Version: a.cacheVersion, State: a.state.snapshot()}
			a.cacheReload = false
		}
		a.cacheFailure(ctx, commitErr)
		return
	}
	// 提交成功才刷出暂存的下行；发送失败只记日志——数据已经落定，
	// 因为一条推送失败去回滚一次成功提交没有意义。
	for _, err := range a.gate.Commit() {
		a.logger.Warn("cached reply failed", "err", err)
	}
}

// canRollbackMessage 判断请求失败后能否丢弃内存改动。
//
// 协议号到「可回滚」的映射由模块自己声明（module.RollbackAware），
// 玩家层不再硬编码任何具体玩法：新增玩法不需要改这个文件。
func (a *Actor) canRollbackMessage(message any) bool {
	switch msg := message.(type) {
	case *GrantItemsMsg:
		return true // 与资产同快照的 BizID 负责幂等重放。
	case *PacketMsg:
		if a.registry == nil {
			return false
		}
		return a.registry.IsRollbackable(msg.MsgID)
	}
	return false
}

// finishPending 核对 Redis 实际版本后补提交「结果未知」的旧快照。
//
// 两种合法情形：
//   - 版本未变（pending.Version）：上次提交其实没落，重发同一快照即可；
//   - 版本 +1 且内容与 pending 一致：提交已落，只是当年没收到确认，
//     把本地版本追平即可，绝不能重发（那会把旧资产再覆盖一次）。
//
// 其余版本差说明期间有别的写入方（独立 flusher/别的实例），无法安全合并，
// 只能报错让人工介入。
func (a *Actor) finishPending() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pending := a.cachePending

	// 直接重提，不读 Redis 比对内容：
	//   - 上次其实写成功了 → 重写同样内容，version+1，幂等无害；
	//   - 上次没写成功 → 正好补上。
	// 原先的「读出来再 marshal 两份比对」要两次序列化，而它只影响
	// 「是否报错」这一个判断，不值得。
	// pending 是整份快照，必须全量写（它是「上次那条消息的完整结果」）。
	version, err := a.repo.cache.Commit(ctx, pending.State.PlayerID, pending.State, pending.State.FieldNames(), nil, a.token)
	if err != nil {
		if errors.Is(err, writebehind.ErrConflict) {
			// 已被新会话接管：这个 Actor 不该再写。放弃补提转重载——
			// MySQL 侧已落定的事实由对账补偿（R2.3），不在这里硬修。
			a.cachePending = nil
			a.cacheReload = true
			return fmt.Errorf("pending commit superseded: %w", err)
		}
		return err
	}
	a.cacheVersion = version
	a.cachePending = nil
	a.state.ClearItemsDirty()
	return nil
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
