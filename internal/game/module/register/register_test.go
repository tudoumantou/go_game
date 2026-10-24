// internal/game/module/register/register_test.go —— 持久化注册的完整性检查
//
// 这个包本身没有逻辑，只有一串空白导入。但它漏一行就是线上事故：
// 独立落库进程（cmd/flusher）和不导入业务模块的集成测试，靠这里
// 触发各模块 init 里的 writebehind.RegisterFlat/RegisterStateful；
// 漏了哪个，框架遇到未注册的 Ext 槽位会拒绝整批落库（宁可不写也不丢数据）。
//
// 所以测试要能回答两个问题：
//  1. 该注册的是否都注册了；
//  2. 以后新增模块时，漏加一行能否被发现。
package register

import (
	"os"
	"path/filepath"
	"testing"

	"game_study/pkg/writebehind"
	"github.com/stretchr/testify/require"
)

// nameExceptions 目录名 → 注册名（Ext 槽位键）。
//
// stateKey 是线上数据契约，改了会让存量快照读不出来；所以出现「目录叫 A、
// 槽位叫 B」时保留 B，并在这里登记例外。新增例外必须同时确认兼容性。
var nameExceptions = map[string]string{
	"loginreward": "login", // 登录礼包：槽位沿用 "login"，不改存量数据
}

// registered 当前已注册的槽位名集合。
func registered() map[string]bool {
	out := make(map[string]bool)
	for _, p := range writebehind.Persisters() {
		out[p.Name()] = true
	}
	return out
}

// TestKnownModulesRegistered 逐个点名，防止某行空白导入被误删。
func TestKnownModulesRegistered(t *testing.T) {
	// 顺序与 register.go 的导入顺序一致，方便对照
	want := []string{
		"signin",   // 七日签到
		"quest",    // 任务
		"skill",    // 技能
		"activity", // 活动
		"copy",     // 副本
		"shop",     // 商店
		"equip",    // 装备
		"partner",  // 伙伴
		"login",    // 登录礼包（目录 loginreward）
	}
	got := registered()
	for _, name := range want {
		require.True(t, got[name],
			"模块 %q 的持久化未注册：flusher 遇到该槽位会拒绝整批落库", name)
	}
}

// TestEveryPersistPackageImported 目录扫描 + 注册表对照。
//
// 这是唯一能发现「新增了 persist.go 但忘了在 register.go 补一行」的机制：
// 上面那条用例的 want 是手写列表，漏加导入时它自己也不会知道。
func TestEveryPersistPackageImported(t *testing.T) {
	entries, err := os.ReadDir("..")
	require.NoError(t, err)
	require.NotEmpty(t, entries, "工作目录不对：应该在 internal/game/module/register 下")

	got := registered()
	checked := 0
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		// 只检查「按约定把持久化放在 persist.go」的模块；
		// loginreward 的注册写在自己文件里，由上面的显式列表兜底。
		if _, err := os.Stat(filepath.Join("..", e.Name(), "persist.go")); err != nil {
			continue
		}
		checked++

		want := e.Name()
		if alt, ok := nameExceptions[want]; ok {
			want = alt
		}
		require.True(t, got[want],
			"模块目录 %q 有 persist.go 却没在 register.go 导入，flusher 会拒绝落库；"+
				"若它的 Ext 槽位名与目录名不同，请把例外登记到 nameExceptions", e.Name())
	}

	// 防止「扫空目录导致永远通过」：模块数只会增加，少于 8 说明路径或判断失效了
	require.GreaterOrEqual(t, checked, 8,
		"只扫到 %d 个带 persist.go 的模块，预期至少 8 个——扫描逻辑可能失效", checked)
}

// TestNoDuplicateSlotNames 槽位名必须唯一（RegisterPersister 会 panic，
// 这里再确认一次运行期注册表里没有重名条目）。
func TestNoDuplicateSlotNames(t *testing.T) {
	seen := make(map[string]bool)
	for _, p := range writebehind.Persisters() {
		require.NotEmpty(t, p.Name(), "注册项不能有空的槽位名")
		require.False(t, seen[p.Name()], "槽位名重复: %s", p.Name())
		seen[p.Name()] = true
	}
}
