// internal/game/seat/cache.go —— 跨服座位状态的本服缓存
//
// 跨服房间在 cross 节点上，查询不能同步等跨服回包：cross 节点在每次
// 状态变更时广播全量快照（CrossFrame_SeatState），本文件把它缓存下来，
// 玩家查询直接读本地缓存。快照只读且每次整体替换，浅拷贝切片即可。
package seat

import (
	"sync"

	pkgseat "game_study/pkg/seat"
)

// Cache 跨服座位快照缓存。
type Cache struct {
	mu    sync.RWMutex
	rooms []pkgseat.RoomSnapshot
}

// NewCache 创建缓存。
func NewCache() *Cache { return &Cache{} }

// Set 整体替换缓存（由 crossHandler.OnSeatState 调用）。
func (c *Cache) Set(rooms []pkgseat.RoomSnapshot) {
	c.mu.Lock()
	c.rooms = rooms
	c.mu.Unlock()
}

// Snapshots 返回当前快照副本（元素不可变，浅拷贝切片即可）。
func (c *Cache) Snapshots() []pkgseat.RoomSnapshot {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if len(c.rooms) == 0 {
		return nil
	}
	out := make([]pkgseat.RoomSnapshot, len(c.rooms))
	copy(out, c.rooms)
	return out
}
