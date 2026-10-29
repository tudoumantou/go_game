// pkg/writebehind/fields.go —— 快照字段名（Redis Hash 的 field）
//
// 为什么这四个名字必须只有一份定义：它们此前在六处各写一遍——
//
//	pkg/writebehind/types.go    FieldNames 的 []string{"profile","attr","items","grants"}
//	pkg/writebehind/store.go    marshalField 的 switch（编码）
//	pkg/writebehind/store.go    parseRecord 的目标 map（解码）
//	internal/game/player/snapshot.go  State.Field 的 switch（编码）
//	internal/game/player/snapshot.go  State.Fields 的字段列表
//	internal/game/player/state.go     fieldOf 的模块名 → field 名映射
//
// 六份必须同时改。漏改的后果不是均匀的：
//
//	漏在 parseRecord  → Redis 里写了，重启后读不出来，玩家数据静默回滚
//	漏在 marshalField → 报 "unknown field"（会炸，能发现）
//	漏在 fieldOf      → 改了状态但永不标脏，永远不落库
//
// 中间那条最贵：它不报错，只是「昨天还在的东西今天没了」。
//
// 判断标准不是「字符串出现在哪」，而是「有几个人能在不沟通的前提下
// 各自给出正确答案」。一份常量表 + 一条覆盖测试，答案是 0 个人需要沟通。
package writebehind

// 核心字段名。这是线上兼容契约：Redis 里的 Hash field 已是这些值，
// 改名等于让所有在线玩家的存量快照读不出来——只能新增，不能重命名。
const (
	FieldProfile = "profile"
	FieldAttr    = "attr"
	FieldItems   = "items"
	FieldGrants  = "grants"

	// FieldExtPrefix 模块私有状态的字段前缀（ext:<Persister.Name>）。
	//
	// 与核心字段分开命名空间：模块名是业务自由命名的，前缀保证它
	// 永远不会撞上 profile/attr/items/grants。
	FieldExtPrefix = "ext:"
)

// CoreFields 核心字段名，顺序固定。
//
// 用数组而非切片：长度是编译期常量，也杜绝调用方 append 改到全局那份。
// 顺序用于日志与测试比对，不参与协议语义。
//
// 用途：整份写入（冷启动 Seed、pending 补提）遍历它；增量写入只写
// DirtyFields 报上来的那几个。
var CoreFields = [...]string{FieldProfile, FieldAttr, FieldItems, FieldGrants}
