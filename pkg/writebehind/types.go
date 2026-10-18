// Package writebehind 实现 Redis 权威热状态、可靠脏队列和独立合批落库。
package writebehind

import (
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

type SigninData struct {
	AnchorDay int64                 `json:"anchor_day"`
	Claims    map[int64][]ItemGrant `json:"claims"`
}

// Snapshot 不保存装备等派生加成，只保存持久化基础属性。
type Snapshot struct {
	PlayerID      int64
	Profile       Profile
	Attr          Attr
	Items         map[int32]int64
	Signin        *SigninData
	AppliedGrants map[string]bool
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
	if s.Signin != nil {
		if s.Signin.AnchorDay <= 0 {
			return fmt.Errorf("invalid signin anchor")
		}
		for day, rewards := range s.Signin.Claims {
			if day < s.Signin.AnchorDay || len(rewards) == 0 {
				return fmt.Errorf("invalid signin day")
			}
			for _, g := range rewards {
				if g.ItemID <= 0 || g.Count <= 0 || g.Count > math.MaxInt32 {
					return fmt.Errorf("invalid signin reward")
				}
			}
		}
	}
	return nil
}

func Partition(playerID int64) int { return int(uint64(playerID) % Partitions) }
