// internal/game/module/index_decl.go —— 模块对归属索引的声明
//
// 索引原来只由配置文件的 idtab.mappings 声明。模块要索引，改 yaml；漏了
// 那一行，`s.ownerIdx["mail"]` 就是 nil，模块静默回落 SQL——服务能跑、
// 性能掉一半、日志里一个字都没有。索引是「代码用得到的东西」，声明就该
// 和用它的代码在一起，配置文件只做部署开关。
//
// 仓储基座本身不在这里：它对「无库怎么办、事务怎么起」的判断与游戏模块
// 系统无关，已上提到 pkg/sqlstore（见该包说明）。
package module

import "game_study/pkg/cache"

// IndexSpec 一个归属索引的声明。
type IndexSpec struct {
	// Table 表名。
	Table string
	// By 归属者列（ZSET 的 score，如 player_id）。
	By string
	// Key 实体 id 列（ZSET 的 member，通常是主键）。
	Key string
}

// IndexDeclarer 可选接口：模块声明自己需要哪些归属索引。
//
// Registry 在组装期遍历模块收集声明，缺的补建（yaml 没配也能工作），
// 建好的再经 IndexInjector 交回模块。未实现该接口的模块不会被问第二次。
type IndexDeclarer interface {
	IndexSpecs() []IndexSpec
}

// IndexInjector 可选接口：组装层把建好的归属索引交给模块。
//
// 传 table 而不是「就是我的索引」：一个模块可能声明多个索引
// （主表和明细表各自成索引），按表名分发比按顺序对位安全。
type IndexInjector interface {
	AttachIndex(table string, oi *cache.OwnerIndex)
}
