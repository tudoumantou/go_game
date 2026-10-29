// pkg/config/load_test.go —— 出厂配置必须能被加载
//
// 守的是一类「改了结构没改配置」的事故：删字段、改必填项、加校验之后，
// yaml 里残留的键或缺失的键会让进程起不来，而单元测试全绿——因为没有任何
// 测试真的读过 configs/ 下的文件。
//
// 故意跑全部环境而不是挑一份：prod 用 ${VAR} 占位、docker 用服务名，
// 三种形态的差异本身就是会被改坏的地方。
package config

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestShippedGameConfigsLoad(t *testing.T) {
	// ${MYSQL_PASSWORD} 之类的占位在真实环境由部署注入；这里给个非空值，
	// 让校验走「正常路径」而不是「空密码的降级分支」。
	t.Setenv("MYSQL_PASSWORD", "test-password")

	paths, err := filepath.Glob("../../configs/game/*.yaml")
	require.NoError(t, err)
	require.NotEmpty(t, paths, "configs/game 下应有配置")

	for _, p := range paths {
		t.Run(filepath.Base(p), func(t *testing.T) {
			cfg, err := Load(p)
			require.NoError(t, err, "配置必须能加载并通过校验——删字段/改结构后最先炸的应该是这里")
			require.NotNil(t, cfg)

			// 出厂配置里 mail 必须声明了归属索引：它是唯一真正消费
			// by_owner 映射的功能，漏配会让邮件列表静默退回 SQL。
			var found bool
			for _, m := range cfg.IDTab.Mappings {
				if m.Table == "mail" {
					found = true
					require.Equal(t, "player_id", m.By)
					require.Equal(t, "id", m.Key)
				}
			}
			require.True(t, found, "configs/game 的 idtab.mappings 应包含 mail")

			// idtab 的两个调参项必须显式给出：它们 v3.21 才接线，
			// 配成空/0 会静默走代码默认值——等于配置又变回摆设。
			require.Positive(t, cfg.IDTab.BuildPageSize, "出厂配置应显式给出构建分页")
			require.NotEmpty(t, cfg.IDTab.ReconcileInterval, "出厂配置应显式给出对账周期")
		})
	}
}
