// internal/cross/battle/manager.go —— 房间管理器（建房/摘除/优雅关闭）
//
// 架构职责：Manager 是房间 Actor 的「 registry + 生命周期管家」——
// 分配/校验 room_id、Spawn 与终止 Actor、维护 roomID → PID 索引。
// 协作关系：对上被 rpc.Server 调用（Create/Join/RelayBattle/Report），
// 对下经 actor.System 驱动 Room，Room 结算后经 Sink.RoomClosed 回调摘除。
// 设计取舍：房间状态全在 Actor 内（无锁串行），Manager 只持 PID——
// 索引表极轻，崩溃重启后房间自然消失（纯内存语义，10.1）。
package battle

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"game_study/pkg/actor"
	"game_study/pkg/log"

	internalv2 "game_study/pkg/pb/internalv2"
)

// Manager 房间管理器。
//
// 职责：分配 room_id、Spawn 房间 Actor、维护 roomID → PID 索引、优雅关闭时批量兜底结算。
// 房间状态本身在 Room Actor 内，管理器只持有 PID 与关房句柄。
//
// 并发模型：RWMutex 保护 rooms map（读多写少：Join/Relay 全是读）；
// seq 与 closing 用 atomic（创建计数与关闭标志在锁外读，避免热路径抢锁）。
type Manager struct {
	sys    *actor.System
	logger *log.Logger
	sink   Sink

	mu    sync.RWMutex
	rooms map[uint64]*roomEntry

	seq     atomic.Uint64
	closing atomic.Bool
}

// roomEntry 索引项：PID 用于投递消息，Room 用于 Stats 读快照。
// 同时持两者而不是只持 PID：Stats 要读房间状态（Snapshot 方法），
// 只持 PID 就得给 Actor 加查询消息、多一次异步往返——管理接口
// 是低频只读路径，直接读对象的竞态由 Actor 单线程写入保证可接受
// （快照读取可能滞后一拍，无正确性影响）。
type roomEntry struct {
	pid  *actor.PID
	room *Room
}

// NewManager 创建房间管理器。
// sys/sink 为运行期必需依赖：sys 驱动 Actor，sink 是房间下行出口
// （由 cross.Server 的两段式初始化保证非 nil，见 AttachRooms）。
func NewManager(sys *actor.System, sink Sink, logger *log.Logger) *Manager {
	return &Manager{
		sys:    sys,
		logger: logger,
		sink:   sink,
		rooms:  make(map[uint64]*roomEntry),
	}
}

// Create 创建房间并返回 roomID。
//
// roomID 生成规则：时间戳低 16 位 << 32 | 进程内自增序号，避免节点重启后复用旧号
// （客户端可能用旧 room_id 上报，重号会导致串房）。
//
// 时间戳段只取 16 位：room_id 是 64 位，低位 32 位给自增——同秒最多
// 建 2^32 间远超实际；16 位秒级时间约 18 小时回环，配合自增段
// 跨重启也不撞（回环重合的同一秒内还要自增号也相同才串房）。
func (m *Manager) Create(mode string, expectPlayers int, timeout time.Duration) (uint64, error) {
	if m.closing.Load() {
		return 0, fmt.Errorf("cross server is draining, reject new room")
	}
	n := m.seq.Add(1)
	roomID := uint64(time.Now().Unix()&0xFFFF)<<32 | n
	if err := m.CreateWithID(roomID, mode, expectPlayers, timeout); err != nil {
		return 0, err
	}
	return roomID, nil
}

// CreateWithID 用指定 roomID 创建房间（P8：中心服分配的号）。
//
// 同一 roomID 重复创建会失败 —— 中心服分配器保证唯一，这里做防御性校验。
//
// 实际语义是「幂等成功」：同房间多个参战游戏服会各自带着同一个
// room_id 来建房，首个真正建、其余静默返回成功——否则第二个玩家
// 永远拿不到房间（一局匹配两个发起方是常态）。
func (m *Manager) CreateWithID(roomID uint64, mode string, expectPlayers int, timeout time.Duration) error {
	if m.closing.Load() {
		return fmt.Errorf("cross server is draining, reject new room")
	}
	if roomID == 0 {
		return fmt.Errorf("room id must be non-zero")
	}

	// 中心服分配 room_id 时，同一房间的多个参战游戏服会各自请求建房 ——
	// 首个真正建，其余幂等返回成功（否则第二个玩家永远拿不到房间）。
	// 先 RLock 快查再 Spawn：重复创建是常态（每局 N 次），不能为此
	// 把整个创建路径串行化；窗口内两个并发首建由 Spawn 后的 map 写入
	// 兜底（后写覆盖先写，PID 以先建者为准——极端竞态下玩家进的是
	// 同一间房，可接受）
	m.mu.RLock()
	_, dup := m.rooms[roomID]
	m.mu.RUnlock()
	if dup {
		m.logger.Debug("room already exists, idempotent create", "room_id", roomID)
		return nil
	}

	room := NewRoom(roomID, mode, expectPlayers, timeout, m.sink, m.logger)
	// Actor 名带 roomID：进程内唯一，崩溃栈/日志里能直接定位到房间
	pid, err := m.sys.Spawn(fmt.Sprintf("room:%d", roomID), room)
	if err != nil {
		return fmt.Errorf("spawn room actor: %w", err)
	}
	// Spawn 后才注入 PID：Actor 自己要靠它 ScheduleOnce 超时 tick，
	// 而 ScheduleOnce 要求 PID 已存在——鸡生蛋问题用「先 Spawn 后 SetPID」
	// 破解，窗口内到达的消息进邮箱排队，不丢
	room.SetPID(pid)

	m.mu.Lock()
	m.rooms[roomID] = &roomEntry{pid: pid, room: room}
	m.mu.Unlock()

	m.logger.Info("room created", "room_id", roomID, "mode", mode, "expect", expectPlayers)
	return nil
}

// Join 玩家入房（快照入队，满员自动开战）。
// Send 是异步投递：入队结果由 Actor 串行处理，这里只保证「消息进了邮箱」；
// 房间不存在（已结算/从未创建）才返回错误。
func (m *Manager) Join(nodeID string, snap *internalv2.PlayerSnapshot) error {
	e := m.get(snap.GetRoomId())
	if e == nil {
		return fmt.Errorf("room %d not found", snap.GetRoomId())
	}
	return m.sys.Send(e.pid, &JoinMsg{NodeID: nodeID, Snapshot: snap})
}

// RelayBattle 转发战斗消息到房间（房间再扇出给其他参战者）。
// 管理器不解析 body、不改字段：纯路由，解析是游戏服的职责——
// 跨服节点只认识 room_id 一个业务字段。
func (m *Manager) RelayBattle(nodeID string, msg *internalv2.BattleMsg) error {
	e := m.get(msg.GetRoomId())
	if e == nil {
		return fmt.Errorf("room %d not found", msg.GetRoomId())
	}
	return m.sys.Send(e.pid, &BattleMsgIn{NodeID: nodeID, Msg: msg})
}

// Report 转发结算上报到房间。
// 与 RelayBattle 同路径：上报也是普通消息，由 Actor 的 onReport 做
// 令牌校验与全员到齐判定——管理器不参与业务判定。
func (m *Manager) Report(nodeID string, res *internalv2.BattleResult) error {
	e := m.get(res.GetRoomId())
	if e == nil {
		return fmt.Errorf("room %d not found", res.GetRoomId())
	}
	return m.sys.Send(e.pid, &ReportMsg{NodeID: nodeID, Result: res})
}

// RoomClosed 实现 Sink：房间结算后从索引摘除并终止 Actor。
//
// 摘除与终止分两步：先摘索引（新的 Join/Relay 立即得到「房间不存在」），
// 再延迟终止 Actor。顺序重要——反过来会让「索引还有、Actor 已死」的
// 消息黑洞出现。
func (m *Manager) RoomClosed(roomID uint64) {
	m.mu.Lock()
	e, ok := m.rooms[roomID]
	if ok {
		delete(m.rooms, roomID)
	}
	m.mu.Unlock()

	if !ok {
		return
	}
	// 延迟终止：当前消息还在 Actor 内处理，直接 Terminate 会打断自身循环
	// 100ms 经验值：settle 推送（若干次 SendToGame）远小于它；
	// 不等待直接杀会丢最后几帧结算帧。起 goroutine 而不阻塞回调方——
	// RoomClosed 由 Actor 消息处理路径同步调用，阻塞它会卡住房间邮箱
	go func() {
		time.Sleep(100 * time.Millisecond)
		m.sys.Terminate(e.pid)
		m.logger.Info("room destroyed", "room_id", roomID)
	}()
}

// Count 当前房间数。
func (m *Manager) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.rooms)
}

// Stats 房间状态快照（管理接口/指标用）。
// 直接读 Room 对象而不是发消息问 Actor：管理接口是低频只读，
// 异步问询要引入「请求-响应」消息对，复杂度不值。读到的是近似值
// （可能与 Actor 内实时状态差一拍），指标场景可接受。
func (m *Manager) Stats() map[string]int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := map[string]int{"total": len(m.rooms), "waiting": 0, "running": 0, "settled": 0}
	for _, e := range m.rooms {
		state, _ := e.room.Snapshot()
		switch state {
		case StateWaiting:
			out["waiting"]++
		case StateRunning:
			out["running"]++
		case StateSettled:
			out["settled"]++
		}
	}
	return out
}

// Drain 优雅关闭：拒绝新房 → 存量房间兜底结算（超时强制）。
//
// 三步：置 closing（拒绝新请求）→ 快照房间列表 → 群发 CloseMsg 等结算。
// 快照后遍历：等待期间新消息可能改 map，而 for range map 期间修改
// 是未定义行为；发完 CloseMsg 用轮询等 Count 归零而不是 WaitGroup——
// 房间结算是 Actor 异步完成的，没有可 wait 的 goroutine 组。
func (m *Manager) Drain(timeout time.Duration) {
	m.closing.Store(true)

	m.mu.RLock()
	entries := make([]*roomEntry, 0, len(m.rooms))
	for _, e := range m.rooms {
		entries = append(entries, e)
	}
	m.mu.RUnlock()

	if len(entries) == 0 {
		return
	}
	m.logger.Info("draining rooms", "count", len(entries), "timeout", timeout.String())

	deadline := time.Now().Add(timeout)
	// 先群发再等待：CloseMsg 触发各房间的兜底结算，结算结果仍要经
	// CrossService 流推给游戏服——所以这里不能跳过等待直接杀
	for _, e := range entries {
		_ = m.sys.Send(e.pid, &CloseMsg{Reason: "server_drain"})
	}

	for time.Now().Before(deadline) {
		// 200ms 轮询粒度：结算推送是毫秒级，无需更细；
		// 强停前至少给每间房一次 settle 的机会
		if m.Count() == 0 {
			m.logger.Info("all rooms drained")
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	// 超时仍有房间：强制放行，由进程退出兜底——
	// 玩家侧会因断线重连/超时平局机制自愈，好过整个关闭流程卡死
	m.logger.Warn("drain timeout, remaining rooms", "count", m.Count())
}

// get 按 roomID 取索引项（读路径公共出口）。
// RLock + defer RUnlock 的标准读锁范式：返回指针给调用方用——
// roomEntry 内容（pid/room）本身不变（变更走换指针），指针安全。
func (m *Manager) get(roomID uint64) *roomEntry {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.rooms[roomID]
}
