package spatial

import (
	"fmt"
	"sort"

	"game_study/internal/worldmap/domain"
)

// OccupancyKey 是静态占位格的键。对应 SQL map_occupancy 唯一键
// (world_id, cell_x, cell_y, layer) 去掉 world 的部分（内存索引本身
// 就是单世界的，world 由 SceneGrid/Topology 在上一层隔离）。
//
// 为什么能直接用结构体做 map 键：Go 要求结构体键的所有字段可比较，
// CellX/CellY 是 int64、Layer 是 int32，全部可比较，因此
// map[OccupancyKey]domain.ObjectKey 合法且高效（编译期展开为
// 逐字段比较，不需要手写 "x:y:layer" 字符串拼接——字符串键既有
// 分配开销又可能因分隔符碰撞出 bug，如负数坐标 "-1:-1"）。
type OccupancyKey struct {
	CellX int64
	CellY int64
	// Layer 层级。第一版所有静态对象都在 layer 0；预留字段是为了
	// 将来多层格/联盟建筑不必改键结构（文档 5.3：扩展 layer/footprint，
	// 不默认绕过唯一键）。
	Layer int32
}

// OccupancyIndex 是静态占位的内存预检索引（文档 5.3）。
//
// 定位：最终权威是 SQL map_occupancy 唯一键 (world,cell_x,cell_y,layer)，
// 两个创建同时争格时只有一个事务能插入成功，另一个收到
// ErrPositionOccupied。本索引的作用是在规划阶段（MutationPlan 生成时）
// 就地把绝大多数冲突挡掉，避免每个冲突都走到数据库才失败。
// 因此它的全部写操作都必须"可能被数据库否决"：Claim 成功不代表
// 最终成功，只代表内存预检通过。
//
// 数据结构：正索引 + 反索引。
//
//	occupants map[OccupancyKey]domain.ObjectKey      格 → 占用者
//	byObject  map[domain.ObjectKey]map[OccupancyKey]struct{} 对象 → 占的格集合
//
// 反索引用 map[OccupancyKey]struct{} 而不是切片：对象占的格要去重
// （重复 Claim 同一格是幂等成功，不能进集合两次），map 天然去重，
// 且删除时 O(1)。
//
// 并发：SceneLoop 私有，非并发安全（文档 4.2）。其他 goroutine 通过
// Snapshot 拿深拷贝。
type OccupancyIndex struct {
	occupants map[OccupancyKey]domain.ObjectKey
	byObject  map[domain.ObjectKey]map[OccupancyKey]struct{}
}

// NewOccupancyIndex 构造空索引。
func NewOccupancyIndex() *OccupancyIndex {
	return &OccupancyIndex{
		occupants: make(map[OccupancyKey]domain.ObjectKey),
		byObject:  make(map[domain.ObjectKey]map[OccupancyKey]struct{}),
	}
}

// Claim 声明占用一格。已被占用时返回包装 domain.ErrPositionOccupied
// 的错误，错误信息里带占用者 key——客户端需要知道"被谁占了"
// 才能决定换点还是攻击。
//
// 同一对象重复 Claim 同一格是幂等成功（no-op）：命令重试/计划重放
// 时会产生完全相同的 Claim，把它判成冲突会让重试永远失败。
// 不同对象争格才是冲突。
//
// 注意本函数不校验坐标是否在世界范围内：拓扑校验属于上层
// （调用方持有 Topology）。在本层做范围检查要么冗余、要么需要反向
// 依赖 config，违背包边界设计。
func (idx *OccupancyIndex) Claim(key OccupancyKey, obj domain.ObjectKey) error {
	if cur, ok := idx.occupants[key]; ok {
		if cur == obj {
			// 幂等重占：直接成功，不写反索引（集合里已有）。
			return nil
		}
		return fmt.Errorf("%w: 格 (%d,%d,layer=%d) 已被对象 %s 占用",
			domain.ErrPositionOccupied, key.CellX, key.CellY, key.Layer, cur)
	}
	idx.occupants[key] = obj
	set := idx.byObject[obj]
	if set == nil {
		set = make(map[OccupancyKey]struct{})
		idx.byObject[obj] = set
	}
	set[key] = struct{}{}
	return nil
}

// Release 释放自己占用的格。
//
// 只允许"占用者本人"释放：防止对象 A 的删除流程误释放对象 B 的占位
// （A、B 恰好规划了同一格时，B 的事务会以为格还空着，两个建筑叠在
// 一起——这类 bug 在只按格删除的代码里极其常见）。因此占用者不匹配
// 返回 domain.ErrNotOwner，格本就空闲返回 domain.ErrObjectNotFound：
// 双重释放说明计划层有 bug，静默成功会让内存状态与 SQL 真相漂移，
// 必须显式炸出来。
func (idx *OccupancyIndex) Release(key OccupancyKey, obj domain.ObjectKey) error {
	cur, ok := idx.occupants[key]
	if !ok {
		return fmt.Errorf("%w: 格 (%d,%d,layer=%d) 未被占用, 对象 %s 的释放是重复操作",
			domain.ErrObjectNotFound, key.CellX, key.CellY, key.Layer, obj)
	}
	if cur != obj {
		return fmt.Errorf("%w: 格 (%d,%d,layer=%d) 占用者是 %s, 不是 %s（禁止释放他人占位）",
			domain.ErrNotOwner, key.CellX, key.CellY, key.Layer, cur, obj)
	}
	delete(idx.occupants, key)
	if set := idx.byObject[obj]; set != nil {
		delete(set, key)
		if len(set) == 0 {
			delete(idx.byObject, obj)
		}
	}
	return nil
}

// Occupant 返回格的占用者。空闲时 ok=false。
func (idx *OccupancyIndex) Occupant(key OccupancyKey) (domain.ObjectKey, bool) {
	obj, ok := idx.occupants[key]
	return obj, ok
}

// ObjectsOn 反查对象占用的全部格（删除对象时逐个释放用），按键排序
// 保证输出确定。
//
// 为什么必须有反索引、不能扫描正索引：对象删除是高频路径
// （城池摧毁、资源点刷新），正索引有 O(格数) 条记录（世界上限
// 5000×5000 格），每删一个对象就全表扫一遍不可接受；反索引把代价
// 降到 O(该对象占的格数)（第一版主堡/资源点 1×1，通常就是 1）。
// 空间上多一份 map 是指级交换，值得。
func (idx *OccupancyIndex) ObjectsOn(obj domain.ObjectKey) []OccupancyKey {
	set := idx.byObject[obj]
	if len(set) == 0 {
		return nil
	}
	out := make([]OccupancyKey, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	// 排序消除 map 随机顺序：删除流程按确定顺序释放，日志与测试可复现。
	sort.Slice(out, func(i, j int) bool {
		if out[i].Layer != out[j].Layer {
			return out[i].Layer < out[j].Layer
		}
		if out[i].CellX != out[j].CellX {
			return out[i].CellX < out[j].CellX
		}
		return out[i].CellY < out[j].CellY
	})
	return out
}

// Snapshot 返回正索引的深拷贝，供测试/调试/跨 goroutine 只读使用。
//
// 为什么这里是"真深拷贝"：OccupancyKey 只含 int64/int32、
// domain.ObjectKey 只含两个 uint64，全部是标量，map 逐项复制后
// 新旧两份数据完全独立。如果哪天值类型里出现切片或 map，
// 这个函数就必须升级为递归拷贝——到那时请先问"为什么不给
// OccupancyIndex 加只读接口"。
//
// 反索引不拷：调用方要的是"哪格被谁占了"的真相，对象→格的倒排
// 是内部实现细节。
func (idx *OccupancyIndex) Snapshot() map[OccupancyKey]domain.ObjectKey {
	out := make(map[OccupancyKey]domain.ObjectKey, len(idx.occupants))
	for k, v := range idx.occupants {
		out[k] = v
	}
	return out
}

// Len 返回已占用格数。
func (idx *OccupancyIndex) Len() int { return len(idx.occupants) }
