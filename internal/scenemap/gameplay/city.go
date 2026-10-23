package gameplay

import (
	"sync/atomic"

	"game_study/internal/scenemap"
	"game_study/internal/scenemap/mgr"
)

// City 是主城玩法（多线公共图，规格 3.2 / 4.1）。
//
// 主城几乎不参与玩法：没有波次、没有通关、也不因空房关闭（关图由
// 动态分线的平衡器决定）。这里只做两件事：
//   - 记录在线人数（监控分线负载、动态选线用）；
//   - 明确"主城永不因为没人而关图"。
type City struct {
	// 嵌入空实现：只覆盖关心的方法。Go 没有"默认方法"，嵌入
	// mgr.Unimplemented 是标准解法（漏实现会变成编译错误，但错误信息
	// 指向嵌入点，排查费劲——所以这里显式覆盖了全部有语义差异的方法）。
	mgr.Unimplemented

	mapID int32
	// players 在线人数。用原子量而不是普通 int：PlayerEnter/Leave 跑在
	// MGR goroutine，但监控/选线可能从任意 goroutine 读，普通 int 会被
	// -race 抓到，加锁又让一个纯统计字段产生锁竞争。
	players atomic.Int64
}

// 编译期断言：主城必须满足玩法接口。
var _ mgr.Gameplay = (*City)(nil)

// NewCity 构造主城玩法。mapID 只用于日志与快照标识。
func NewCity(mapID int32) *City {
	return &City{mapID: mapID}
}

// MapID 返回地图配置 ID。
func (c *City) MapID() int32 {
	if c == nil {
		return 0
	}
	return c.mapID
}

// Players 返回当前在线人数（近似值，读的瞬间可能已变）。
func (c *City) Players() int64 {
	if c == nil {
		return 0
	}
	return c.players.Load()
}

// MgrInit 主城没有需要初始化的玩法状态。
func (c *City) MgrInit(*mgr.State, ...any) error { return nil }

// Loop 主城没有秒级玩法逻辑。
//
// 规格 7.3 要求 Loop 只做"阶段推进/倒计时/胜负判定"这类秒级全局逻辑；
// 主城一条都没有，空实现是正确行为，不是待办。
func (c *City) Loop(*mgr.State) error { return nil }

// PlayerEnter 记录进图人数。
func (c *City) PlayerEnter(_ scenemap.UUID, _ *mgr.State) error {
	c.players.Add(1)
	return nil
}

// PlayerLeave 维护人数（没有其他清理动作）。
func (c *City) PlayerLeave(_ scenemap.UUID, _ scenemap.LeaveReason, _ *mgr.State) error {
	// 用 CAS 递减并夹在 0 以上：PlayerLeave 理论上与 PlayerEnter 成对，
	// 但"进图回滚/重复离图"会让计数漂移，一个负的在线人数出现在监控里
	// 会让人误判为"有 bug 的人在刷进出图"，先夹住比追查更省事。
	for {
		n := c.players.Load()
		if n <= 0 {
			return nil
		}
		if c.players.CompareAndSwap(n, n-1) {
			return nil
		}
	}
}

// CanCheckStopMap 恒 false：主城永不因无人关闭。
//
// 主城的存在意义是"任何时刻都能进来"，所以关图只由分线平衡器（把 2 线
// 的人合到 1 线）主动发起，不交给"空房计时"。返回 false 后 MGR 会把
// CloseTime 归零（见 mgr/lifecycle.go），即使一个玩家都没有也不关。
func (c *City) CanCheckStopMap(*mgr.State) bool { return false }

// AllowSkill 报告主城是否允许结算技能。
//
// 规格 11.9：主城不结算技能，且要**玩家侧 + MGR 侧双重拦截**——
// 玩家 Actor 收到施法请求先按地图参数拒绝，MGR 侧再按同一份配置拒绝，
// 避免"客户端伪造 + 服务端只拦一层"漏过去。
//
// 这里只是**配置查询**：真正的拦截点在玩家 Actor 与 MGR 的施法命令里。
// 本方法返回常量 false 而不是按 mapID 查表，是因为主城就是"不结算"的
// 定义本身；留着 mapID 参数是为了让调用方不必区分"问的是哪张图"。
func (c *City) AllowSkill(mapID int32) bool {
	_ = mapID
	return false
}
