// pkg/writebehind/ext.go —— 模块私有状态（Ext 槽位）的编解码助手
//
// Snapshot 只保存全玩法通用资产；玩法私有状态放在 Ext[name] 的 JSON 槽位里。
// 本文件把「槽位读写」封装成泛型助手，模块只需声明自己的状态结构体 T，
// 不用关心 json.RawMessage 的细节（LoadExt/SaveExt 即 Decode/Encode +
// 取/放槽位的一步到位包装）。
//
// 协作关系：ExtState 由 player.State 实现（框架侧），模块通过
// LoadExt/SaveExt 访问自己的槽位；标脏与落库调度仍由框架负责
// （SaveExt 本身不触发 flush）。
package writebehind

import (
	"encoding/json"
	"fmt"
)

// Decode 解码模块私有状态。
//
// raw 为空表示该模块还没有任何数据（从未写入过），返回 (nil, nil) 而不是报错——
// 「首次使用」和「数据损坏」必须区分开：前者是正常冷启动路径，
// 后者要报错让人工介入。
//
// Go 细节：var v T 先拿零值（大结构体会逃逸到堆，由编译器决策），
// json.Unmarshal(raw, &v) 必须传指针——Unmarshal 要通过指针修改 v；
// 返回 &v 让调用方拿到与函数内局部变量解耦的副本。
func Decode[T any](raw json.RawMessage) (*T, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var v T
	if err := json.Unmarshal(raw, &v); err != nil {
		return nil, fmt.Errorf("decode module state: %w", err)
	}
	return &v, nil
}

// Encode 编码模块私有状态；v 为 nil 时返回 nil（等价于清空该模块槽位）。
//
// 与 Decode 严格对称：nil 指针入、nil 出，使 SaveExt(state, name, nil)
// 成为合法的「清除模块」操作，不需要单独的 Clear API。
//
// Go 细节：json.RawMessage 本质是 []byte 的类型别名，Marshal 产物可直接
// 当 RawMessage 用（无需转换拷贝）；%w 保留底层序列化错误链。
func Encode[T any](v *T) (json.RawMessage, error) {
	if v == nil {
		return nil, nil
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("encode module state: %w", err)
	}
	return raw, nil
}

// ExtState 是模块私有状态读写的最小依赖（player.State 实现它）。
//
// 由框架提供之后，模块不必再各自声明同形窄接口；单测给几行内存实现即可。
//
// Go 细节：接口隔离原则的落地——模块只依赖自己用得上的两个方法，
// 而不是整个 player.State（几十个方法）。窄接口让单测的假实现只有
// 几行，也让模块与框架的类型依赖解耦。
type ExtState interface {
	// Ext 取模块槽位；从未写入过返回 nil/空。
	Ext(name string) json.RawMessage
	// SetExt 写模块槽位；raw 为 nil 即清除。
	SetExt(name string, raw json.RawMessage)
}

// LoadExt 读取并解码模块私有状态；从未写入过时返回 (nil, nil)。
//
// 与直接调 Decode + state.Ext 相比，它把「取槽位」和「解码」合成一步，
// 模块的 read 辅助函数不再需要手写。泛型 T 由返回类型推断：
//
//	st, err := writebehind.LoadExt[SigninState](state, "signin")
//
// 调用点自描述状态类型。
func LoadExt[T any](state ExtState, name string) (*T, error) {
	return Decode[T](state.Ext(name))
}

// SaveExt 编码并写回模块私有状态；标脏与落库调度由框架负责。
//
// 等价于 Encode + state.SetExt，模块的 write 辅助函数不再需要手写。
// 注意保存后模块通常还要调框架的标脏入口（如 player.MarkDirty），
// 本函数只保证槽位内容被更新，不承诺一定落库。
func SaveExt[T any](state ExtState, name string, v *T) error {
	raw, err := Encode(v)
	if err != nil {
		return err
	}
	state.SetExt(name, raw)
	return nil
}
