// Package writebehind 实现 Redis 权威热状态、可靠脏队列和独立合批落库。
package writebehind

import (
	"encoding/json"
	"fmt"
	"math"
)

// Partitions 是共享持久队列协议的一部分，不能在单个进程中单独调整。
const Partitions = 10

type Profile struct {
	Name    string
	Level   int32
	Exp     int64
	Coin    int64
	SceneID int32
}

type Attr struct {
	Career int32
	Hp     int32
	Mp     int32
	Atk    int32
	Def    int32
	Spd    int32
}

type ItemGrant struct {
	ItemID int32 `json:"item_id"`
	Count  int64 `json:"count"`
}

// Snapshot 不保存装备等派生加成，只保存持久化基础属性。
//
// 框架只认识 Profile/Attr/Items 这类所有玩法共用的资产；模块私有状态一律放 Ext，
// 由各自的 Persister 负责序列化、校验与落库。这样新增玩法不再需要改动本结构体。
type Snapshot struct {
	PlayerID      int64
	Profile       Profile
	Attr          Attr
	Items         map[int32]int64
	Ext           map[string]json.RawMessage // 模块名 -> 模块快照
	AppliedGrants map[string]bool

	// LegacySignin 仅用于识别旧版快照里的内建签到字段。
	//
	// 字段名刻意与旧 JSON 键一致（"Signin"），Load 时被迁移进 Ext["signin"] 后清空，
	// 避免升级瞬间把线上签到数据当成不存在而重新开始周期。
	LegacySignin json.RawMessage `json:"Signin,omitempty"`
}

type Record struct {
	Version int64
	State   Snapshot
}

func (s Snapshot) Validate() error {
	if s.PlayerID <= 0 {
		return fmt.Errorf("invalid player id")
	}
	for id, n := range s.Items {
		if id <= 0 || n < 0 || n > math.MaxInt32 {
			return fmt.Errorf("invalid item %d count %d", id, n)
		}
	}
	for name, raw := range s.Ext {
		if name == "" {
			return fmt.Errorf("empty module state name")
		}
		p, ok := LookupPersister(name)
		if !ok {
			return fmt.Errorf("unknown module state %q", name)
		}
		if len(raw) == 0 {
			return fmt.Errorf("module %q has empty state", name)
		}
		if err := p.Validate(raw); err != nil {
			return fmt.Errorf("module %q: %w", name, err)
		}
	}
	return nil
}

// migrateLegacy 把旧版内建字段搬进 Ext，返回是否发生了迁移。
//
// 旧版 Snapshot 的 Signin 字段 JSON 结构与 Ext["signin"] 完全一致，
// 迁移只是换个存放位置，不需要改写数据本身。
func (s *Snapshot) migrateLegacy() bool {
	if len(s.LegacySignin) == 0 || string(s.LegacySignin) == "null" {
		s.LegacySignin = nil
		return false
	}
	if s.Ext == nil {
		s.Ext = make(map[string]json.RawMessage, 1)
	}
	if _, exists := s.Ext["signin"]; !exists {
		s.Ext["signin"] = s.LegacySignin
	}
	s.LegacySignin = nil
	return true
}

func Partition(playerID int64) int { return int(uint64(playerID) % Partitions) }
