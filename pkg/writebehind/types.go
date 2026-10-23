// Package writebehind 实现 Redis 权威热状态、可靠脏队列和独立合批落库。
//
// 架构位置：本包定义玩家快照的**线上协议**（Snapshot）与缓存侧存储
// （store.go），落库 worker（worker.go）把 Redis 脏队列异步投影到
// MySQL。与 pkg/flush 的关系：flush 是「进程内分片落库框架」，
// writebehind 是「缓存权威玩家的可靠落库通道」，两者互补——
// 非缓存玩家走 flush.MarkDirty，缓存玩家走本包的脏 ZSET。
package writebehind

import (
	"encoding/json"
	"fmt"
	"math"
)

// Partitions 是共享持久队列协议的一部分，不能在单个进程中单独调整。
//
// 10 个脏队列分区被所有游戏服进程与独立落库进程共享：改这个值等于
// 改协议，必须全网同步（老数据按新分区算会落到错误队列/错误 slot）。
// 与 flush 分片数的关系：两者都是「按玩家分桶」，但一个分 Redis
// 队列、一个分进程内 worker，独立演进、各自配置。
const Partitions = 10

// Profile 玩家基础档案（玩法进度类的核心资产）。
//
// 字段直接以字段名做 JSON 键（无 tag）：这些是稳定核心字段，
// 默认命名即可；改字段名 = 破坏线上格式，禁止。
type Profile struct {
	Name    string
	Level   int32
	Exp     int64
	Coin    int64
	SceneID int32
}

// Attr 持久化基础属性（不含装备等派生加成——那些按需现算，
// 存了反而要在每次换装时同步两份真相，必然漂移）。
type Attr struct {
	Career int32
	Hp     int32
	Mp     int32
	Atk    int32
	Def    int32
	Spd    int32
}

// ItemGrant 一条发放记录（item_id + count）。
//
// 显式 json tag：snake_case 是「新格式」的约定，与 Profile 等老字段的
// 默认命名形成对比——新结构体一律带 tag，避免日后改字段名时
// JSON 键跟着漂移（tag 是序列化契约，与字段名解耦）。
type ItemGrant struct {
	ItemID int32 `json:"item_id"`
	Count  int64 `json:"count"`
}

// Snapshot 不保存装备等派生加成，只保存持久化基础属性。
//
// 框架只认识 Profile/Attr/Items 这类所有玩法共用的资产；模块私有状态一律放 Ext，
// 由各自的 Persister 负责序列化、校验与落库。这样新增玩法不再需要改动本结构体。
//
// 字段分组：
//   - PlayerID：主键；Load 时与 key 中的 id 互相校验（防串号）；
//   - Profile/Attr/Items：核心资产，由 worker.persistSnapshots 直接写
//     player / player_attr / player_item 三张表；
//   - Ext：模块名 → 模块快照 JSON。框架只搬运不解释，RawMessage 延迟
//     解析到 Persister；键名即模块名（注册表契约），map 无 tag；
//   - AppliedGrants：已应用的一次性发放流水号（防重发栅栏）。
//
// Go 细节：Validate 用值接收者（只读）——值接收杜绝方法内意外修改
// 调用方的快照；结构体在 KB 级，按值传递的拷贝成本可接受。
type Snapshot struct {
	PlayerID      int64
	Profile       Profile
	Attr          Attr
	Items         map[int32]int64
	Ext           map[string]json.RawMessage // 模块名 -> 模块快照
	AppliedGrants map[string]bool
}

// Record 是「版本 + 快照」的组合：落库与 Ack 的最小单位。
//
// version 是 Redis 侧的提交版本（Commit 的返回值），同时充当下次
// Commit 的 expected 与数据库版本栅栏（player_writebehind.version）。
type Record struct {
	// Version 快照版本（单调递增；0/负值非法，worker.Persist 会拒绝）。
	Version int64
	// State 快照本体。
	State Snapshot
}

// FieldSource 提交时的字段来源。
//
// 为什么不让调用方直接传 Snapshot：构造 Snapshot 要把背包 map、Ext map、
// 幂等键表全部复制一遍（100 件道具约 4.3µs），而一次提交通常只改一个字段。
// 按需取字段后，这份复制就没了。
//
// 实现者：Snapshot（冷启动种子 / pending 补提）与 player.State（常规提交）。
type FieldSource interface {
	// Field 返回字段的 JSON；nil 表示该字段没有内容（调用方跳过写入）。
	Field(name string) ([]byte, error)
	// Fields 返回全部字段名，用于必须整份写入的场景。
	Fields() []string
}

// FieldNames 返回快照的全部字段名（Redis Hash 的 field）。
//
// 用于「必须整份写入」的场景：冷启动 Seed、pending 补提。
// 常规消息用 State.DirtyFields() 只写变化的部分。
func (s Snapshot) FieldNames() []string {
	fields := []string{"profile", "attr", "items", "grants"}
	for name := range s.Ext {
		fields = append(fields, "ext:"+name)
	}
	return fields
}

// Field 实现 FieldSource。
func (s Snapshot) Field(name string) ([]byte, error) { return marshalField(s, name) }

// Fields 实现 FieldSource。
func (s Snapshot) Fields() []string { return s.FieldNames() }

// Validate 校验快照自洽性（Seed 落库前的公共闸门）。
//
// 检查项：PlayerID 正数；Items 数量合法（id 正、数量在 int32 列范围内）；
// Ext 每个键非空、模块已注册、且模块自己的 Validate 通过。
// 这样新增模块只需实现 Persister.Validate，框架闸门无需改动。
//
// Go 细节：map 遍历顺序随机，但校验是全量扫描，返回第一个发现的错误，
// 顺序不影响结论（要么全过要么报错）。
func (s Snapshot) Validate() error {
	if s.PlayerID <= 0 {
		return fmt.Errorf("invalid player id")
	}
	for id, n := range s.Items {
		// 数量上限 math.MaxInt32：数据库列是 int，超范围值会在驱动层
		// 溢出成负数（静默数据损坏），必须在入口挡住
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

// Partition 玩家 → 脏队列分区（取模）。
//
// Go 细节：int64 先转 uint64 再取模——Go 的取模符号跟随被除数，
// 负数直接 % 会得到负分区（key 形态异常、ZSET 错队），转无符号后
// 任何 int64 输入都得到 [0, Partitions) 内的确定分区。
func Partition(playerID int64) int { return int(uint64(playerID) % Partitions) }
