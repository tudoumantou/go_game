package scene

import (
	"fmt"

	"game_study/pkg/actor"
	"game_study/pkg/gamedata"
	"game_study/pkg/log"
)

// Manager 场景 Actor 的创建与查找。
//
// 场景数量固定（配置表规模），启动时一次性 Spawn，之后只读——
// 因此查找不需要加锁，也不存在「并发创建同一场景」的问题。
type Manager struct {
	sys    *actor.System
	logger *log.Logger
	pids   map[int32]*actor.PID
}

// NewManager 创建场景管理器。
func NewManager(sys *actor.System, logger *log.Logger) *Manager {
	return &Manager{sys: sys, logger: logger, pids: make(map[int32]*actor.PID)}
}

// Boot 按配置启动全部场景 Actor。配置非法时返回错误（启动期暴露，不留到运行期）。
func (m *Manager) Boot() error {
	for _, cfg := range gamedata.SceneCfgList() {
		sc, err := fromConfig(cfg)
		if err != nil {
			return err
		}
		pid, err := m.sys.Spawn(fmt.Sprintf("scene:%d", sc.ID), NewActor(sc, m.sys, m.logger))
		if err != nil {
			return fmt.Errorf("spawn scene %d: %w", sc.ID, err)
		}
		m.pids[sc.ID] = pid
		// 启动心跳扫描：自调度，之后由 Actor 自己续期。
		if err := m.sys.Send(pid, &SweepMsg{}); err != nil {
			return fmt.Errorf("start sweep for scene %d: %w", sc.ID, err)
		}
	}
	m.logger.Info("scenes booted", "count", len(m.pids))
	return nil
}

// Heartbeat 刷新玩家在场景内的在场时间（实现 player.SceneLink）。
//
// 玩家 Actor 每 30 秒调一次；缺了它，挂机玩家会在 90 秒后被兜底清理误踢。
func (m *Manager) Heartbeat(playerID int64, sceneID int32) {
	pid := m.PID(sceneID)
	if pid == nil {
		return
	}
	_ = m.sys.Send(pid, &HeartbeatMsg{PlayerID: playerID})
}

// Resync 请求场景推送一次全量视野（实现 player.SceneLink）。
//
// 重连时由服务端主动发起：断线期间视野里可能有人进出，客户端并不知道；
// 等客户端自己发现断档再拉，会先经历一段「看不到人」的窗口。
func (m *Manager) Resync(playerID int64, sceneID int32, playerPID *actor.PID) {
	pid := m.PID(sceneID)
	if pid == nil {
		return
	}
	_ = m.sys.Send(pid, &SyncMsg{PlayerID: playerID, PID: playerPID})
}

// PID 场景 Actor 的 PID；场景不存在返回 nil。
func (m *Manager) PID(sceneID int32) *actor.PID {
	return m.pids[sceneID]
}

// Count 已启动的场景数（健康检查/日志用）。
func (m *Manager) Count() int { return len(m.pids) }

// fromConfig 校验并转换配置。
//
// 宽高为 0、格宽为 0、人数上限为 0 都会让场景完全不可用（钳制到 (0,0)、
// 一进就满），必须在启动时失败而不是上线后才发现。
func fromConfig(cfg gamedata.SceneCfg) (Config, error) {
	if cfg.Id <= 0 {
		return Config{}, fmt.Errorf("scene: invalid id %d", cfg.Id)
	}
	if cfg.Col2 <= 0 || cfg.Col3 <= 0 {
		return Config{}, fmt.Errorf("scene %d: width/height must be positive (got %d/%d)", cfg.Id, cfg.Col2, cfg.Col3)
	}
	if cfg.Col4 <= 0 {
		return Config{}, fmt.Errorf("scene %d: cell size must be positive (got %d)", cfg.Id, cfg.Col4)
	}
	if cfg.Col5 <= 0 {
		return Config{}, fmt.Errorf("scene %d: max players must be positive (got %d)", cfg.Id, cfg.Col5)
	}
	return Config{
		ID:         int32(cfg.Id),
		Name:       cfg.Col1,
		Width:      int32(cfg.Col2),
		Height:     int32(cfg.Col3),
		Cell:       int32(cfg.Col4),
		MaxPlayers: int32(cfg.Col5),
	}, nil
}
