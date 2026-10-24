// internal/common/errcode/errcheck_test.go —— 错误码口径的守护测试
//
// 现状（务必先读）：错误码有两层，各自的边界是——
//  1. proto 的 ErrCode（本包镜像）：跨模块共性语义，客户端与 proto 同源生成，
//     码值→文案的对应关系在客户端；
//  2. 各业务模块的 codeXxx：按模块分段（41xx 场景、5xxx 邮件、6xxx 战斗、
//     7xxx 任务…），模块内自洽，客户端按段兜底。
//
// 两层并存本身不是问题，问题是**数值撞车**：模块用了 proto 已定义的某个值，
// 但语义不同，客户端会按 proto 的文案提示，玩家看到的就是错的。
// 本轮就修过 4 处：scene 的「不在场景」取了 4001（CHANNEL_CLOSED）、
// 「场景繁忙」取了 4003（CHAT_TOO_FAST）、battle 的「已在队列」取了 6001
// （BATTLE_NOT_FOUND）、「玩法未开放」取了 6004（BATTLE_FULL）。
//
// 这个测试把「允许的语义复用」写成白名单，其余撞车一律失败。
package errcode

import (
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"testing"

	commonv1 "game_study/pkg/pb/common/v1"
	"github.com/stretchr/testify/require"
)

// semanticReuse 允许与 proto 共用码值的情形：模块 → 码值 → 期望的 proto 名。
//
// 只有**语义真的相同**才能登记。新增一条等于承诺「客户端按 proto 文案提示
// 这个码时，玩家看到的话是对的」。
var semanticReuse = map[string]map[int32]string{
	"bag": {
		3001: "ERR_CODE_ITEM_NOT_EXIST",
		3002: "ERR_CODE_ITEM_NOT_ENOUGH",
		3004: "ERR_CODE_ITEM_NOT_USABLE",
	},
	"chat": {
		4001: "ERR_CODE_CHANNEL_CLOSED",
	},
	"mail": {
		5001: "ERR_CODE_MAIL_NOT_FOUND",
		5002: "ERR_CODE_MAIL_TAKEN",
		5003: "ERR_CODE_NO_ATTACHMENT",
	},
	"battle": {
		6002: "ERR_CODE_BATTLE_TOKEN_INVALID",
		6003: "ERR_CODE_RESULT_REPORTED",
	},
}

// codeDecl 匹配模块里的业务码常量声明（缩进的 codeXxx int32 = N）。
var codeDecl = regexp.MustCompile(`(?m)^\t(code\w+)\s+int32\s*=\s*(\d+)`)

// TestModuleCodesDoNotClashWithProto 模块自建码撞上 proto 已定义值时必须语义一致。
func TestModuleCodesDoNotClashWithProto(t *testing.T) {
	dirs, err := os.ReadDir("../../game/module")
	require.NoError(t, err, "工作目录不对：应该在 internal/common/errcode 下")
	require.NotEmpty(t, dirs)

	scanned := 0
	for _, d := range dirs {
		if !d.IsDir() {
			continue
		}
		mod := d.Name()
		files, err := filepath.Glob(filepath.Join("../../game/module", mod, "*.go"))
		require.NoError(t, err)
		for _, f := range files {
			if filepath.Ext(f) != ".go" || hasTestSuffix(f) {
				continue
			}
			scanned++
			src, err := os.ReadFile(f)
			require.NoError(t, err)
			for _, m := range codeDecl.FindAllStringSubmatch(string(src), -1) {
				name := m[1]
				val, err := strconv.Atoi(m[2])
				require.NoError(t, err)
				if val == 0 {
					continue // 0 恒为成功，各模块都有 codeOK，不参与冲突检查
				}
				protoName, defined := commonv1.ErrCode_name[int32(val)]
				if !defined {
					continue // 模块自有码段，与 proto 无交集
				}
				want, allowed := semanticReuse[mod][int32(val)]
				require.True(t, allowed,
					"%s/%s 的 %s = %d 与 proto 的 %s 撞车且未登记语义复用："+
						"客户端会按 proto 文案提示，玩家看到的是错的",
					mod, filepath.Base(f), name, val, protoName)
				require.Equal(t, want, protoName,
					"%s/%s 的 %s = %d 登记的语义是 %s，但 proto 该值是 %s",
					mod, filepath.Base(f), name, val, want, protoName)
			}
		}
	}
	require.Greater(t, scanned, 30, "只扫到 %d 个模块文件，扫描逻辑可能失效", scanned)
}

// TestNoCrossModuleCodeReuse 不同模块不能共用同一个码值。
//
// 共用意味着客户端无法从码值反推是哪个模块出错——同一个数字在两个模块
// 表示不同失败，文案只能写一个，另一个必然错。
func TestNoCrossModuleCodeReuse(t *testing.T) {
	dirs, err := os.ReadDir("../../game/module")
	require.NoError(t, err)

	owner := make(map[int32]string)
	for _, d := range dirs {
		if !d.IsDir() {
			continue
		}
		mod := d.Name()
		files, err := filepath.Glob(filepath.Join("../../game/module", mod, "*.go"))
		require.NoError(t, err)
		for _, f := range files {
			if filepath.Ext(f) != ".go" || hasTestSuffix(f) {
				continue
			}
			src, err := os.ReadFile(f)
			require.NoError(t, err)
			for _, m := range codeDecl.FindAllStringSubmatch(string(src), -1) {
				val, _ := strconv.Atoi(m[2])
				if val == 0 {
					continue
				}
				if prev, ok := owner[int32(val)]; ok && prev != mod {
					t.Errorf("码值 %d 同时被 %s 和 %s 使用（%s）", val, prev, mod, m[1])
				}
				owner[int32(val)] = mod
			}
		}
	}
}

func hasTestSuffix(p string) bool {
	const suf = "_test.go"
	return len(p) >= len(suf) && p[len(p)-len(suf):] == suf
}
