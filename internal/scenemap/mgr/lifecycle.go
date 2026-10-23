package mgr

import (
	"game_study/internal/scenemap"
)

// 本文件是关图计时状态机（规格 4.4）。
//
// # 状态机的全部迁移
//
//	开图                 CloseTime = now + enter_time        （空房保护）
//	玩家进图             CloseTime = 0                        （有人在，不关）
//	玩家离图且 Users 空：
//	  LeaveReasonQuit    QuitClose ? 立即 : now + enter_time
//	  LeaveReasonOffline now + offline_time                   （给重连留时间）
//	  其他（切图/踢人）  now + enter_time
//	CanCheckStopMap=false  CloseTime = 0                      （玩法说不能关）
//	loop 发现 now >= CloseTime  → 发 OnClose（只发一次）+ CloseTime = 0
//
// # 为什么 MGR 不自己关自己
//
// 关图要做的清理很多（房间登记表注销、AOI 实例回收、分线人数统计、
// 玩家重定向）。这些资源的所有者是 RoomManager，MGR 只有"我该关了"的
// 信息。如果 MGR 在自己的 goroutine 里调用 Stop 并等待自己退出，就是
// 典型的自引用死锁；就算不等待，也会出现"MGR 已死但登记表还指着它"的
// 中间态。所以 MGR 只发通知，由 RoomManager 收口。

// onPlayerEnter 处理"玩家进图成功"后的关图计时。
func (m *Mgr) onPlayerEnter(s *State) {
	// 有人进来就取消关图。哪怕只进来一个人，图也有存在价值。
	s.CloseTime = 0
}

// onPlayerLeave 处理"玩家离图后"的关图计时。
//
// 只有 Users 为空时才考虑关图：宠物/召唤不算"人"，它们会随主人一起走，
// 不该让一张只有宠物的图无限存活。
func (m *Mgr) onPlayerLeave(s *State, reason scenemap.LeaveReason, now int64) {
	if len(s.Users) > 0 {
		return
	}
	// 玩法说"现在不能关"（例如战斗结算中）：清空关闭时间并保留空房。
	// 注意这是"清空"而不是"保留原值"——原值可能是上一轮的过期时间，
	// 留着它会在玩法放开限制的瞬间立刻关图（规格 11.13 的坑）。
	if !m.gameplay.CanCheckStopMap(s) {
		s.CloseTime = 0
		return
	}
	switch reason {
	case scenemap.LeaveReasonQuit:
		if m.cfg.QuitClose {
			// 立即关：置为 now，下一次 loop（now >= CloseTime）就会收割。
			// 不直接在这里调 notifyClose，是为了让"关图"只有 loop 一个出口，
			// 顺序稳定、可测试。
			s.CloseTime = now
		} else {
			s.CloseTime = now + m.cfg.EnterProtect.Milliseconds()
		}
	case scenemap.LeaveReasonOffline:
		// 掉线给足重连时间：重连进的是同一张图，提前关掉会让玩家
		// 重连后落到一张新图（副本进度全丢）。
		s.CloseTime = now + m.cfg.OfflineGrace.Milliseconds()
	default:
		// 切图/踢人/强制关闭：都按"空房保护"处理。
		s.CloseTime = now + m.cfg.EnterProtect.Milliseconds()
	}
}

// collectClose 是 loop 里的"收割关图"。now >= CloseTime 时发通知。
//
// 用 >= 而不是 >：QuitClose 场景下 CloseTime 被置为 now，若用 > 就要等
// 到下一个毫秒，虽然只差 1ms，但测试里"立即关"会变成"等一拍"，
// 语义上不干净。
func (m *Mgr) collectClose(s *State, now int64) {
	if s.CloseTime == 0 || s.closeNotified {
		return
	}
	if now < s.CloseTime {
		return
	}
	m.notifyClose(s, "empty_timeout")
	// 关掉计时，避免每一拍都进来做一次无意义判断。
	s.CloseTime = 0
}

// notifyClose 发出关图通知（幂等，全图只发一次）。
//
// 为什么要异步：OnClose 属于 RoomManager，可能要去注销注册表、停止 AOI、
// 甚至给其他节点发消息。这些都可能阻塞；MGR 在它的 goroutine 里同步等
// 会直接卡住整张图的结算。用 SafeRun 起 goroutine 并对回调再兜一层 panic
// （它已经不是 MGR 的栈了，出错不应该影响 MGR 退出）。
func (m *Mgr) notifyClose(s *State, reason string) {
	if s != nil {
		if s.closeNotified {
			return
		}
		s.closeNotified = true
	}
	m.closeOnce.Do(func() {
		m.deps.Logger.Info("scenemap/mgr: 关图", "key", m.cfg.Key.Key(), "reason", reason)
		fn := m.deps.OnClose
		if fn == nil {
			return
		}
		key := m.cfg.Key
		scenemap.SafeRun("scenemap/mgr/onclose:"+key.Key(), nil, func() {
			fn(key, reason)
		})
	})
}

// terminate 执行"关图前"的玩法收尾，并发通知。
//
// 只执行一次：Stop 与 CommandClose 可能先后到达。
func (m *Mgr) terminate(s *State, reason string) {
	if s == nil {
		m.notifyClose(nil, reason)
		return
	}
	if !s.terminated {
		s.terminated = true
		if err := m.gameplay.OnTerminate(s); err != nil {
			// 收尾失败不阻止关图：房间已经决定要关了，卡在这里只会泄漏。
			m.deps.Logger.Error("scenemap/mgr: OnTerminate 失败",
				"key", m.cfg.Key.Key(), "err", err)
		}
		s.Touch()
	}
	m.notifyClose(s, reason)
}
