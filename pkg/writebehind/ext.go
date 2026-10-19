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
