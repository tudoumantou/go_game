package writebehind

import (
	"encoding/json"
	"fmt"
)

// Decode 解码模块私有状态。
//
// raw 为空表示该模块还没有任何数据（从未写入过），返回 (nil, nil) 而不是报错——
// 「首次使用」和「数据损坏」必须区分开。
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
type ExtState interface {
	Ext(name string) json.RawMessage
	SetExt(name string, raw json.RawMessage)
}

// LoadExt 读取并解码模块私有状态；从未写入过时返回 (nil, nil)。
//
// 与直接调 Decode + state.Ext 相比，它把「取槽位」和「解码」合成一步，
// 模块的 read 辅助函数不再需要手写。
func LoadExt[T any](state ExtState, name string) (*T, error) {
	return Decode[T](state.Ext(name))
}

// SaveExt 编码并写回模块私有状态；标脏与落库调度由框架负责。
//
// 等价于 Encode + state.SetExt，模块的 write 辅助函数不再需要手写。
func SaveExt[T any](state ExtState, name string, v *T) error {
	raw, err := Encode(v)
	if err != nil {
		return err
	}
	state.SetExt(name, raw)
	return nil
}
