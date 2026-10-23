// internal/game/scene/bounds_test.go —— 边界钳制与启动期配置校验单测
package scene

import (
	"testing"

	"game_study/pkg/gamedata"
	"github.com/stretchr/testify/require"
)

// TestClamp 钳制语义：下界钉 0、上界开区间（max-1）、非法宽度退化为 0。
// 用例覆盖负数/边界/远超上界与 max<=0 兜底——后者是配置非法时运行期的最后防线。
func TestClamp(t *testing.T) {
	cases := []struct{ v, max, want int32 }{
		{-5, 100, 0},
		{0, 100, 0},
		{50, 100, 50},
		{99, 100, 99},
		{100, 100, 99}, // 上界是开区间
		{9999, 100, 99},
		{5, 0, 0}, // 非法地图宽度退化为 0，避免返回负数
	}
	for _, c := range cases {
		if got := Clamp(c.v, c.max); got != c.want {
			t.Fatalf("Clamp(%d,%d) = %d, want %d", c.v, c.max, got, c.want)
		}
	}
}

// TestInBounds 开区间语义 [0,width)：四角在内，贴界与负值在外。
func TestInBounds(t *testing.T) {
	if !InBounds(0, 0, 100, 100) || !InBounds(99, 99, 100, 100) {
		t.Fatal("corners must be inside")
	}
	if InBounds(100, 99, 100, 100) || InBounds(-1, 0, 100, 100) {
		t.Fatal("out of range must be rejected")
	}
}

// TestFromConfigRejects 配置非法必须启动失败：
// 宽高为 0 会让所有坐标钳制到 (0,0)，人数为 0 会让场景一进就满。
func TestFromConfigRejects(t *testing.T) {
	bad := []gamedata.SceneCfg{
		{Id: 0, Col1: "x", Col2: 100, Col3: 100, Col4: 10, Col5: 30},
		{Id: 1, Col1: "x", Col2: 0, Col3: 100, Col4: 10, Col5: 30},
		{Id: 1, Col1: "x", Col2: 100, Col3: 0, Col4: 10, Col5: 30},
		{Id: 1, Col1: "x", Col2: 100, Col3: 100, Col4: 0, Col5: 30},
		{Id: 1, Col1: "x", Col2: 100, Col3: 100, Col4: 10, Col5: 0},
	}
	for _, cfg := range bad {
		if _, err := fromConfig(cfg); err == nil {
			t.Fatalf("config %+v must be rejected", cfg)
		}
	}
}

// TestFromConfigLoadsRealConfig 现有 scene.xml 必须全部合法。
func TestFromConfigLoadsRealConfig(t *testing.T) {
	for _, cfg := range gamedata.SceneCfgList() {
		sc, err := fromConfig(cfg)
		require.NoError(t, err)
		if sc.Width <= 0 || sc.Height <= 0 || sc.MaxPlayers <= 0 {
			t.Fatalf("scene %d not normalized: %+v", cfg.Id, sc)
		}
	}
}
