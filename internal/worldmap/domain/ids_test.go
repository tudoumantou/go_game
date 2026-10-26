package domain

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestSafeMulDiv 覆盖移动位置推导 (to-from)*elapsed/duration 的溢出判定。
//
// 实现要点：|a|、|b| 先取绝对值走 mul128 得到 128 位真实积（输入非负，
// 高低位拼接即数学真值，不涉及补码截断），再按符号定合法域——正积必须
// < 2^63，负积可取到 -2^63（uq==2^63 的唯一合法情形，返回 MinInt64）。
// 符号单独处理，不依赖旧实现「128 位无符号长除 + int64 截断」对负积的
// 隐式还原（to-from 为负的向左移动依赖这条路径，必须显式正确）。
//
// 用例边界：MaxInt64/MinInt64 两侧的合法上/下界、两个方向各一档越界即
// 报错、负积非整除除数的向零取整、除数为零/负归入 ErrInvalidArgument
// （与 ErrOverflow 分流，调用方降级策略不同）。
func TestSafeMulDiv(t *testing.T) {
	tests := []struct {
		name    string
		a, b, c int64
		want    int64
		wantErr error // 期望的错误类别；nil 表示不报错
	}{
		{"正常推导", 3000, 1500, 1000, 4500, nil},
		{"移动推导真实量级", 5_000_000, 3_600_000, 3_600_000, 5_000_000, nil},
		{"乘积恰为 MaxInt64（合法上界）", math.MaxInt64, 1, 1, math.MaxInt64, nil},
		{"乘积越过 2^63 但未过 2^64", math.MaxInt64, 2, 1, 0, ErrOverflow},
		{"乘积越过 2^64（hi 非零）", math.MaxInt64 / 2, 5, 1, 0, ErrOverflow},
		{"负积恰为 MinInt64（合法下界）", math.MinInt64, 1, 1, math.MinInt64, nil},
		{"负积非整除除数（向零取整）", -7, 3, 2, -10, nil},
		{"负积越过 -2^63", -5_000_000_000_000_000_000, 2, 1, 0, ErrOverflow},
		{"负积越过 2^64（hi 非零）", math.MinInt64, 2, 1, 0, ErrOverflow},
		{"除数为零", 1, 1, 0, 0, ErrInvalidArgument},
		{"除数为负", 1, 1, -1, 0, ErrInvalidArgument},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := SafeMulDiv(tt.a, tt.b, tt.c)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr, "越界/非法入参必须报错，不能静默给出错值")
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

// TestSafeMulDivOverflowErrorForBadDivisor 区分两类错误：
// 除非法是入参错误，乘积越界才是 ErrOverflow——调用方的降级策略不同。
func TestSafeMulDivOverflowErrorForBadDivisor(t *testing.T) {
	_, err := SafeMulDiv(1, 1, 0)
	require.ErrorIs(t, err, ErrInvalidArgument)
	require.NotErrorIs(t, err, ErrOverflow)
}

// TestIsqrtCommonDomain 锁 Isqrt 在段长平方和量级（约 5e13）内的正确性。
// 该量级远低于 MaxInt64/2 的安全域边界，修正循环不会平方溢出。
func TestIsqrtCommonDomain(t *testing.T) {
	tests := []struct {
		n    int64
		want int64
	}{
		{0, 0},
		{1, 1},
		{2, 1},
		{3, 1},
		{4, 2},
		{99, 9},
		{100, 10},
		{101, 10},
		{50_000_000_000_000, 7_071_067}, // ≈ 5000 格 × 1000 units 的段长平方和量级
	}
	for _, tt := range tests {
		got, err := Isqrt(tt.n)
		require.NoError(t, err)
		require.Equal(t, tt.want, got, "Isqrt(%d)", tt.n)
	}
	_, err := Isqrt(-1)
	require.ErrorIs(t, err, ErrInvalidArgument, "负入参必须报错")
}
