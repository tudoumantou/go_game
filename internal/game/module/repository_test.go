// internal/game/module/repository_test.go —— Repository 契约的单元测试
//
// 这一组用例守的是「基座自己不出错」，与业务规则无关：
//
//   - 没改就不写（否则每条只读请求都赔一次序列化 + 一次提交）；
//   - 出错就不写（否则半成品状态会被编进快照）；
//   - 幂等键「失败不占、成功才占」（否则一次失败被当成永久已应用）。
//
// 全部跑在 moduletest.State 内存桩上，不碰数据库、不碰 Actor。
package module_test

import (
	"encoding/json"
	"errors"
	"testing"

	"game_study/internal/game/module"
	"game_study/internal/game/module/moduletest"
	"github.com/stretchr/testify/require"
)

// demo 是被测状态的形状：一个计数 + 一个 map，够覆盖「值改」与「引用改」。
type demo struct {
	N     int            `json:"n"`
	Items map[string]int `json:"items"`
}

func newDemo() *demo { return &demo{Items: map[string]int{}} }

// newRepo 组装「模块 + 内存状态 + 业务上下文」，返回仓储与状态桩。
//
// 走 Define/NewCtx 而不是直接构造 Repo：Repo 的字段未导出，业务也拿不到，
// 测试用同一个入口才能保证测的是真实路径。
func newRepo(t *testing.T) (module.Repository[demo], *moduletest.State) {
	t.Helper()
	st := moduletest.NewState(1)
	m := module.Define[demo]("demo", newDemo)
	return m.NewCtx(moduletest.NewContext(st, moduletest.NewRecorder())).Repo, st
}

// decodeDemo 从 Ext 槽位读回落库形状（空槽位返回 nil）。
func decodeDemo(t *testing.T, st *moduletest.State) *demo {
	t.Helper()
	raw := st.Ext("demo")
	if len(raw) == 0 {
		return nil
	}
	d := &demo{}
	require.NoError(t, json.Unmarshal(raw, d))
	return d
}

// TestViewDiscardsWrites 只读路径上的修改必须被丢弃。
//
// 为什么这条重要：View 的语义是查询。如果它悄悄把修改写回去，
// 「查列表时顺手改了个统计字段」这种代码会活得很好，直到某次
// 有人发现只读接口产生了写放大。
func TestViewDiscardsWrites(t *testing.T) {
	repo, st := newRepo(t)

	called := false
	require.NoError(t, repo.View(func(d *demo) error {
		called = true
		d.N = 42 // 应被丢弃
		return nil
	}))
	require.True(t, called, "View 必须真的执行闭包")
	require.Nil(t, decodeDemo(t, st), "View 不该写回状态")
	require.Zero(t, st.SetExtCount(), "View 不该产生 SetExt")

	// 再读一次拿到的仍是空状态，说明上一步的修改没有残留。
	require.NoError(t, repo.View(func(d *demo) error {
		require.Zero(t, d.N)
		return nil
	}))
}

// TestMutateSkipsWriteWhenUnchanged changed=false 时不编码、不标脏。
func TestMutateSkipsWriteWhenUnchanged(t *testing.T) {
	repo, st := newRepo(t)

	require.NoError(t, repo.Mutate(func(d *demo) (bool, error) {
		return false, nil
	}))
	require.Nil(t, decodeDemo(t, st), "changed=false 却写了状态")
	require.Zero(t, st.SetExtCount(), "changed=false 却标了脏")
	require.Empty(t, st.DirtyModules())
}

// TestMutateWritesWhenChanged changed=true 时编码回写并标脏一次。
func TestMutateWritesWhenChanged(t *testing.T) {
	repo, st := newRepo(t)

	require.NoError(t, repo.Mutate(func(d *demo) (bool, error) {
		d.N = 7
		d.Items["a"] = 3
		return true, nil
	}))
	back := decodeDemo(t, st)
	require.NotNil(t, back)
	require.Equal(t, 7, back.N)
	require.Equal(t, 3, back.Items["a"])
	require.Equal(t, 1, st.SetExtCount(), "一次 Mutate 只该写一次")
}

// TestMutateDropsChangesOnError fn 出错时状态不回写，错误原样上抛。
//
// 这是「发奖失败就不置领取位」这类逻辑的地基：闭包返回 error 后，
// 前面在 *D 上做的一半修改不能漏进快照。
func TestMutateDropsChangesOnError(t *testing.T) {
	repo, st := newRepo(t)

	boom := errors.New("boom")
	err := repo.Mutate(func(d *demo) (bool, error) {
		d.N = 99 // 半成品：不该落库
		return true, boom
	})
	require.ErrorIs(t, err, boom, "错误必须原样上抛")
	require.Nil(t, decodeDemo(t, st), "出错却把半成品写进了快照")
	require.Zero(t, st.SetExtCount())
}

// TestViewReportsDecodeError 槽位里是坏 JSON 时报错，不静默当空状态。
//
// 静默当空状态的后果：一次编码 bug 会把玩家进度整体清零，而且没有任何日志。
func TestViewReportsDecodeError(t *testing.T) {
	repo, st := newRepo(t)
	st.SetExt("demo", json.RawMessage(`{"n":`)) // 截断的 JSON

	require.Error(t, repo.View(func(*demo) error { return nil }))
}

// TestOnceAppliesOnce 首次执行，第二次直接 ErrApplied 且不再进闭包。
func TestOnceAppliesOnce(t *testing.T) {
	repo, st := newRepo(t)

	runs := 0
	first := repo.Once("order-1", func(d *demo) (bool, error) {
		runs++
		d.N++
		return true, nil
	})
	require.NoError(t, first)
	require.Equal(t, 1, runs)

	second := repo.Once("order-1", func(d *demo) (bool, error) {
		runs++
		d.N++
		return true, nil
	})
	require.ErrorIs(t, second, module.ErrApplied, "重复的幂等键必须返回 ErrApplied")
	require.Equal(t, 1, runs, "幂等命中时不该再执行闭包")
	require.Equal(t, 1, decodeDemo(t, st).N, "幂等命中不该重复改状态")
}

// TestOnceDoesNotClaimOnError fn 失败时键不被占用，重投还能重试。
//
// 顺序反过来（先占键再执行）会把一次偶发失败变成永久丢失：
// 键已落库，重投全部命中断言「已应用」，玩家永远拿不到奖励。
func TestOnceDoesNotClaimOnError(t *testing.T) {
	repo, st := newRepo(t)

	boom := errors.New("boom")
	require.ErrorIs(t, repo.Once("order-2", func(d *demo) (bool, error) {
		d.N = 5
		return true, boom
	}), boom)
	require.False(t, st.HasBiz("order-2"), "失败的请求不该占住幂等键")

	// 重投：必须真的执行，并且这次能成功。
	ran := false
	require.NoError(t, repo.Once("order-2", func(d *demo) (bool, error) {
		ran = true
		d.N = 5
		return true, nil
	}))
	require.True(t, ran, "上次失败后重投必须重新执行")
	require.Equal(t, 5, decodeDemo(t, st).N)
	require.True(t, st.HasBiz("order-2"), "成功后键必须落下")
}

// TestOnceClaimsKeyEvenWhenUnchanged 「只发奖不改状态」的场景也必须占键。
//
// 纯发奖请求（changed=false）如果因为「没改状态」就不落键，
// 重投时会二次发奖——这正是 ClaimBiz 内部要自己标脏的原因。
func TestOnceClaimsKeyEvenWhenUnchanged(t *testing.T) {
	repo, st := newRepo(t)

	require.NoError(t, repo.Once("grant-x", func(*demo) (bool, error) { return false, nil }))
	require.True(t, st.HasBiz("grant-x"), "changed=false 也要占键")
	require.Contains(t, st.DirtyModules(), "grants", "占键必须标脏，否则键不会被提交")
}

// TestOnceEmptyKeyAlwaysRuns 空键等价于 Mutate（不是「永久幂等」）。
func TestOnceEmptyKeyAlwaysRuns(t *testing.T) {
	repo, _ := newRepo(t)

	for i := 0; i < 3; i++ {
		require.NoError(t, repo.Once("", func(*demo) (bool, error) { return false, nil }))
	}
}

// TestTouchSkipsEmptySlot 槽位为空时不标脏（不为没玩过的玩家写空状态）。
func TestTouchSkipsEmptySlot(t *testing.T) {
	repo, st := newRepo(t)

	require.NoError(t, repo.Touch())
	require.Empty(t, st.DirtyModules(), "空槽位不该标脏")
	require.Nil(t, decodeDemo(t, st))
}

// TestTouchMarksExistingSlot 槽位已存在时只标脏、不重新编码。
func TestTouchMarksExistingSlot(t *testing.T) {
	repo, st := newRepo(t)
	require.NoError(t, repo.Mutate(func(d *demo) (bool, error) { d.N = 1; return true, nil }))
	beforeExt := st.SetExtCount()
	beforeDirty := len(st.DirtyModules())

	require.NoError(t, repo.Touch())
	require.Len(t, st.DirtyModules(), beforeDirty+1, "Touch 必须新增一次标脏")
	require.Equal(t, beforeExt, st.SetExtCount(), "Touch 不该重新编码状态")
}

// TestResetClearsSlot Reset 后回到 newFn 的空状态（不是 nil 指针）。
func TestResetClearsSlot(t *testing.T) {
	repo, st := newRepo(t)
	require.NoError(t, repo.Mutate(func(d *demo) (bool, error) {
		d.N = 9
		d.Items["a"] = 1
		return true, nil
	}))
	require.NotNil(t, decodeDemo(t, st))

	require.NoError(t, repo.Reset())
	require.Nil(t, decodeDemo(t, st), "Reset 后槽位必须为空")

	require.NoError(t, repo.View(func(d *demo) error {
		require.Zero(t, d.N)
		require.NotNil(t, d.Items, "空状态必须来自 newFn，map 要初始化")
		return nil
	}))
}

// TestRepositorySatisfiedByRepo 编译期契约已由 repository.go 断言，
// 这里再确认「从 Ctx 拿到的东西确实满足接口」——防止有人把字段类型改回去。
func TestRepositorySatisfiedByRepo(t *testing.T) {
	repo, _ := newRepo(t)
	require.NotNil(t, repo)
}
