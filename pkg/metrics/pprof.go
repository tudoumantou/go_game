// pkg/metrics/pprof.go —— 把 net/http/pprof 挂到内部 metrics 端口
//
// 默认关闭：profile 能读出内存里的业务数据（堆快照含玩家状态），不该
// 在默认配置下对外可达。需要时用环境变量打开：
//
//	PPROF_ENABLE=1 ./bin/game -config configs/game/game-dev.yaml
//	go tool pprof http://127.0.0.1:9102/debug/pprof/profile?seconds=30
//
// 为什么不用 import _ "net/http/pprof"：那个包的 init 会把处理器注册到
// DefaultServeMux，等于在进程里凭空多开一组路由；这里显式挂到自己的 mux，
// 谁能访问由监听地址决定，不依赖 http.DefaultServeMux 有没有被别处占用。
package metrics

import (
	"net/http"
	"net/http/pprof"
	"os"
)

// pprofEnabled 是否开启 profiling 端点。
func pprofEnabled() bool { return os.Getenv("PPROF_ENABLE") == "1" }

// registerPprof 注册 pprof 路由（未开启时什么也不做）。
func registerPprof(mux *http.ServeMux) {
	if !pprofEnabled() {
		return
	}
	// Index 覆盖 allocs/block/goroutine/heap/mutex/threadcreate 等子页面
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
}
