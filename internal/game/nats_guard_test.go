package game

import (
	"testing"

	"game_study/pkg/log"
)

// TestAcceptSender 验证接收侧的发送者校验：subject 里的发送者与载荷自称的
// 发送者不一致时必须拒收。
//
// 这条校验是 NATS publish 权限之外的第二道：权限配漏时（例如运维把
// relay.group.*.from.* 写成通配），伪造的消息仍会被这里拦下并且留下 warn
// 日志与 metrics 计数，配漏这件事因此可见。
func TestAcceptSender(t *testing.T) {
	s := &Server{logger: log.New("test")}

	cases := []struct {
		name    string
		subject string
		claimed string
		want    bool
	}{
		{"一致", "relay.node.game-01.from.game-02", "game-02", true},
		{"带环境前缀", "prod.relay.node.game-01.from.game-02", "game-02", true},
		{"冒充别人", "relay.node.game-01.from.game-02", "game-03", false},
		{"载荷没填", "relay.node.game-01.from.game-02", "", false},
		{"subject 缺发送者", "relay.node.game-01", "game-02", false},
		{"发送者是通配符", "relay.node.game-01.from.*", "*", false},
	}
	for _, c := range cases {
		if got := s.acceptSender(c.subject, c.claimed, "test"); got != c.want {
			t.Fatalf("%s: acceptSender(%q, %q) = %v, want %v",
				c.name, c.subject, c.claimed, got, c.want)
		}
	}
}
