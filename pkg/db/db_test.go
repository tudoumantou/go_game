// pkg/db/db_test.go —— DSN 时区归一
//
// 这组测试守的是一个「不报错但会错」的问题：驱动按 loc 解析时间列，
// MySQL 按会话 time_zone 生成时间，两者不一致时所有时间戳整体偏移。
// 没有断言的话，这种事只能靠「玩家说时间不对」发现。
package db

import (
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

// TestNormalizeDSNInjectsUTCTimeZone 缺省时补 time_zone='+00:00'。
func TestNormalizeDSNInjectsUTCTimeZone(t *testing.T) {
	out := normalizeDSN("game:pw@tcp(127.0.0.1:3306)/game_study?parseTime=true")
	cfg, err := mysql.ParseDSN(out)
	require.NoError(t, err)
	require.True(t, cfg.ParseTime, "parseTime 不能被弄丢")
	require.Equal(t, "'+00:00'", cfg.Params["time_zone"],
		"会话时区必须钉死 UTC，与驱动默认 loc 对齐")
}

// TestNormalizeDSNKeepsExplicitTimeZone 显式配了就不覆盖。
//
// 显式选择代表调用方知道自己在做什么（例如连的是 +08:00 的历史库），
// 替它改会安静地改变语义。
func TestNormalizeDSNKeepsExplicitTimeZone(t *testing.T) {
	in := "game:pw@tcp(127.0.0.1:3306)/game_study?parseTime=true&time_zone=%27%2B08%3A00%27"
	cfg, err := mysql.ParseDSN(normalizeDSN(in))
	require.NoError(t, err)
	require.Equal(t, "'+08:00'", cfg.Params["time_zone"])
}

// TestNormalizeDSNInvalidPassthrough 解析不了就原样返回。
//
// 不在这一层吞掉语法错误：让 sql.Open/Ping 报出真实的 DSN 问题，
// 否则「参数拼错」会变成「连不上但不知道为什么」。
func TestNormalizeDSNInvalidPassthrough(t *testing.T) {
	const bad = "not-a-dsn"
	require.Equal(t, bad, normalizeDSN(bad))
}

// TestNormalizeDSNInjectsQueryTimeouts 缺省注入读写超时。
//
// 守的是「一个卡住的查询永久占住玩家 Actor」这个可用性问题：Actor 串行，
// 一条没有超时的 SQL 会让该玩家的所有后续消息排队等待。驱动层的
// readTimeout/writeTimeout 是唯一能「零遗漏」兜住所有仓储调用位置的地方。
func TestNormalizeDSNInjectsQueryTimeouts(t *testing.T) {
	cfg, err := mysql.ParseDSN(normalizeDSN("game:pw@tcp(127.0.0.1:3306)/game_study?parseTime=true"))
	require.NoError(t, err)
	require.Equal(t, defaultQueryTimeout, cfg.ReadTimeout)
	require.Equal(t, defaultQueryTimeout, cfg.WriteTimeout)
}

// TestNormalizeDSNKeepsExplicitTimeouts 显式配置优先（例如压测环境想放宽到 30s）。
func TestNormalizeDSNKeepsExplicitTimeouts(t *testing.T) {
	in := "game:pw@tcp(127.0.0.1:3306)/game_study?readTimeout=30s&writeTimeout=20s"
	cfg, err := mysql.ParseDSN(normalizeDSN(in))
	require.NoError(t, err)
	require.Equal(t, 30*time.Second, cfg.ReadTimeout)
	require.Equal(t, 20*time.Second, cfg.WriteTimeout)
}
