package bootstrap

import (
	"testing"

	"game_study/pkg/config"
)

// TestListenAddr 启动日志里的 listen 必须是「业务端口」，不是运维端口。
//
// 地图进程曾经配着 http_addr（从未被读取的死配置）而只开 gRPC；把 HTTP
// 排在前面会让启动日志报一个谁都没在听的端口——运维照日志去连，连不上。
func TestListenAddr(t *testing.T) {
	cases := []struct {
		name string
		cfg  config.Config
		want string
	}{
		{
			name: "gateway 报 TCP 不报运维 HTTP",
			cfg:  config.Config{Server: config.ServerConfig{TCPAddr: "0.0.0.0:9000", HTTPAddr: "0.0.0.0:9001"}},
			want: "0.0.0.0:9000",
		},
		{
			name: "map 报 GRPC 不报 HTTP",
			cfg:  config.Config{Server: config.ServerConfig{GRPCAddr: ":9400", HTTPAddr: ":9401"}},
			want: ":9400",
		},
		{
			name: "login 只有 HTTP",
			cfg:  config.Config{Server: config.ServerConfig{HTTPAddr: "0.0.0.0:8080"}},
			want: "0.0.0.0:8080",
		},
		{
			name: "全空给占位符",
			cfg:  config.Config{},
			want: "-",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := listenAddr(&c.cfg); got != c.want {
				t.Fatalf("listenAddr = %q, want %q", got, c.want)
			}
		})
	}
}

// TestShutdownBudgetIsPositive 兜底关闭预算必须为正。
//
// 预算为 0 会让 Shutdown 的第一步就撞上「预算耗尽」，所有关闭步骤变成
// 「被放弃」并各记一条错误——比不关更糟（既没清理，又让退出码非零）。
// Run 本身走 os.Exit 不好在进程内测，端到端验证靠跑二进制
// （cross 与 map 的实跑记录见提交说明）。
func TestShutdownBudgetIsPositive(t *testing.T) {
	if defaultShutdownBudget <= 0 {
		t.Fatalf("defaultShutdownBudget 必须为正，实际 %v", defaultShutdownBudget)
	}
}
