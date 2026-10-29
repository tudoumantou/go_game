// pkg/cache/keygen_test.go —— key 格式的兼容性钉子
//
// 这里不测「函数返回了拼接好的字符串」（读一遍实现就知道），只钉两个
// **线上兼容契约**：
//
//   - token key 的格式：多个进程共用，改一个字符 = 全体在线会话立即失效
//     （网关 Get 拿到 redis.Nil，校验失败，玩家掉线）；
//   - key 里不能出现未转义的分隔符：token 是 hex，accuid 是数字，但
//     ServInfoKey 这种固定 key 也要守住「域:实体」的形状，否则运维用
//     `SCAN serv_info:*` 清理时会漏。
package cache

import (
	"strings"
	"testing"
)

// TestTokenKeyExactFormat 钉死 token key 的字面格式。
//
// 改这条用例等于「主动决定让所有在线会话失效」——评审时必须说清楚。
func TestTokenKeyExactFormat(t *testing.T) {
	if got := TokenKey("abc123"); got != "token:abc123" {
		t.Fatalf("TokenKey = %q, want %q", got, "token:abc123")
	}
}

// TestLoginKeysShape 登录服三个 key 的域前缀与分隔符。
func TestLoginKeysShape(t *testing.T) {
	cases := []struct {
		name string
		got  string
		want string
	}{
		{"serv_info", ServInfoKey(), "serv_info:all"},
		{"acc_summary", AccSummaryKey(42), "acc_summary:42"},
		{"user_role", UserRoleListKey(7), "user_role:list:7"},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("%s = %q, want %q", c.name, c.got, c.want)
		}
		if strings.Count(c.got, ":") == 0 {
			t.Errorf("%s = %q, 缺少 <域>: 分隔符", c.name, c.got)
		}
	}
}
