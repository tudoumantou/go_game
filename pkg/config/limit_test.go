package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestLimitDefaultsWhenUnset 漏配（零值）时必须落到内置默认，而不是「不限流」。
//
// 这是限流最容易被关掉的方式：新增一个配置文件、忘了写 limit 段，
// 然后线上毫无防护。零值代表「用默认」，只有显式负数才表示关闭。
func TestLimitDefaultsWhenUnset(t *testing.T) {
	var c LimitConfig

	rate, burst := c.PacketLimit()
	require.EqualValues(t, DefaultPacketRate, rate)
	require.EqualValues(t, DefaultPacketBurst, burst)

	require.Equal(t, time.Duration(DefaultChatIntervalMS)*time.Millisecond, c.ChatInterval())
	require.EqualValues(t, DefaultLoginPerMinute, c.LoginLimit())
}

// TestLimitDisabledByNegative 负数是唯一的关闭方式（排查问题时的临时开关）。
func TestLimitDisabledByNegative(t *testing.T) {
	c := LimitConfig{PacketRate: -1, ChatIntervalMS: -1, LoginPerMin: -1}

	rate, _ := c.PacketLimit()
	require.EqualValues(t, 0, rate, "负数必须表示关闭")
	require.EqualValues(t, 0, c.ChatInterval())
	require.EqualValues(t, 0, c.LoginLimit())
}

// TestLimitCustomValues 显式配置按原样生效。
func TestLimitCustomValues(t *testing.T) {
	c := LimitConfig{PacketRate: 5, PacketBurst: 8, ChatIntervalMS: 2000, LoginPerMin: 3}

	rate, burst := c.PacketLimit()
	require.EqualValues(t, 5, rate)
	require.EqualValues(t, 8, burst)
	require.Equal(t, 2*time.Second, c.ChatInterval())
	require.EqualValues(t, 3, c.LoginLimit())
}
