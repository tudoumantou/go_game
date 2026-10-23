package writebehind

import (
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
)

// memState 是模块私有状态槽位的内存实现（等价于 player.State 的那两个方法）。
type memState struct{ ext map[string]json.RawMessage }

func (s *memState) Ext(name string) json.RawMessage { return s.ext[name] }

func (s *memState) SetExt(name string, raw json.RawMessage) {
	if s.ext == nil {
		s.ext = make(map[string]json.RawMessage)
	}
	s.ext[name] = raw
}

func TestLoadSaveExt(t *testing.T) {
	st := &memState{}

	// 未写过：必须是 (nil, nil)，与「数据损坏」区分。
	v, err := LoadExt[flatRow](st, "settings")
	require.NoError(t, err, fmt.Sprintf("empty slot: %v %v", v, err))
	require.Nil(t, v)

	in := &flatRow{PlayerID: 1, Anchor: 9}
	err = SaveExt(st, "settings", in)
	require.NoError(t, err)
	out, err := LoadExt[flatRow](st, "settings")
	require.NoError(t, err, fmt.Sprintf("round trip: %+v %v", out, err))
	require.NotNil(t, out)
	require.EqualValues(t, 9, out.Anchor)
	require.EqualValues(t, 1, out.PlayerID)

	// nil 表示清空槽位（等价于 SetExt(name, nil)）。
	err = SaveExt[flatRow](st, "settings", nil)
	require.NoError(t, err)
	raw, ok := st.ext["settings"]
	require.True(t, ok, fmt.Sprintf("nil save must clear slot: ok=%v raw=%v", ok, raw))
	require.Nil(t, raw)
}
