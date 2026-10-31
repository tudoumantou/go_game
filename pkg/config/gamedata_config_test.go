// pkg/config/gamedata_config_test.go —— 热更配置段的解析契约
//
// 为什么单独测这一段：`gamedata.dir` 是「配了不生效」风险最高的字段——
// 它决定进程去哪个目录读表，写错（或者 yaml tag 拼错）不会报错，只会
// 静默地永远不热更。所以这里既验 tag，也验出厂配置的安全默认。
package config

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// TestGameDataConfigYAMLTags 直接对 yaml tag 做断言：
// 字段名与 yaml 键不一致时，Load 不会报错（未知键被忽略），只有这里能拦住。
func TestGameDataConfigYAMLTags(t *testing.T) {
	const doc = `
gamedata:
  dir: /etc/game/gamedata/json
  watch: true
  debounce: 500ms
`
	var wrap struct {
		GameData GameDataConfig `yaml:"gamedata"`
	}
	require.NoError(t, yaml.Unmarshal([]byte(doc), &wrap))

	require.Equal(t, "/etc/game/gamedata/json", wrap.GameData.Dir)
	require.True(t, wrap.GameData.Watch)
	require.Equal(t, "500ms", wrap.GameData.Debounce)
}

// TestShippedConfigsDisableGameDataByDefault 出厂配置必须保持「不热更」：
// dir 为空时进程只用编译期内嵌快照。一旦有人把某个环境变量或本地路径
// 写进出厂配置，生产就可能读到运维机器上的半成品配置——这是配置事故，
// 不是功能开关，所以用测试锁住。
func TestShippedConfigsDisableGameDataByDefault(t *testing.T) {
	t.Setenv("MYSQL_PASSWORD", "test-password")

	paths, err := filepath.Glob("../../configs/game/*.yaml")
	require.NoError(t, err)
	require.NotEmpty(t, paths)

	for _, p := range paths {
		t.Run(filepath.Base(p), func(t *testing.T) {
			cfg, err := Load(p)
			require.NoError(t, err)
			require.Empty(t, cfg.GameData.Dir,
				"出厂配置不应默认打开热更目录（会读到部署环境的残留文件）")
			require.NotEmpty(t, cfg.GameData.Debounce,
				"去抖窗口应显式给出，否则改动会被静默回落到代码默认值")
		})
	}
}
