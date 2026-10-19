package writebehind

import (
	"encoding/json"
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
	if v, err := LoadExt[flatRow](st, "settings"); err != nil || v != nil {
		t.Fatalf("empty slot: %v %v", v, err)
	}

	in := &flatRow{PlayerID: 1, Anchor: 9}
	if err := SaveExt(st, "settings", in); err != nil {
		t.Fatal(err)
	}
	out, err := LoadExt[flatRow](st, "settings")
	if err != nil || out == nil || out.Anchor != 9 || out.PlayerID != 1 {
		t.Fatalf("round trip: %+v %v", out, err)
	}

	// nil 表示清空槽位（等价于 SetExt(name, nil)）。
	if err := SaveExt[flatRow](st, "settings", nil); err != nil {
		t.Fatal(err)
	}
	if raw, ok := st.ext["settings"]; !ok || raw != nil {
		t.Fatalf("nil save must clear slot: ok=%v raw=%v", ok, raw)
	}
}
