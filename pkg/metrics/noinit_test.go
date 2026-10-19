// pkg/metrics/noinit_test.go —— 未初始化时调用指标函数必须安全（回归测试）
//
// 背景：曾出现 SIGSEGV —— 网关在 metrics.enable=false 时仍调用 ConnInc()，
// 此时 connGauge 为 nil 直接崩溃。本测试用子进程隔离「从未 Init」的进程状态，
// 验证全部指标函数在未初始化时都是安全的 no-op。
package metrics

import (
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

const childEnv = "METRICS_NOINIT_CHILD"

// TestNoInitIsSafeInChild 父进程拉起一个从未 Init 的子进程跑全部指标调用。
func TestNoInitIsSafeInChild(t *testing.T) {
	if os.Getenv(childEnv) == "1" {
		runAllMetricCalls()
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=TestNoInitIsSafeInChild", "-test.v=false")
	cmd.Env = append(os.Environ(), childEnv+"=1")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("子进程在未初始化指标时崩溃（这正是要防的 SIGSEGV）:\n%s", out)
	}
	if strings.Contains(string(out), "panic") {
		t.Fatalf("子进程出现 panic:\n%s", out)
	}
}

// runAllMetricCalls 覆盖所有导出指标函数（新增指标时同步补充）。
func runAllMetricCalls() {
	// 未初始化断言
	if Enabled() {
		panic("child process should start with metrics disabled")
	}

	ConnInc()
	ConnDec()
	ConnRejected("max_conns")

	MsgInc("in", "ok")
	MsgInc("out", "error")
	MsgObserve("in", 15*time.Millisecond, 256)
	MsgInflightInc()
	MsgInflightDec()

	ActorMailbox(3, 10)
	ActorProcessed(true)
	ActorObserve(time.Millisecond)
	ActorPanicInc()
	ActorRejectedAdd(2)
	ActorCountAdd(1)
	ActorCountAdd(-1)

	FlushDirty(1, 5)
	FlushObserve(10 * time.Millisecond)
	FlushRows(true, 5)
	FlushFail(2)
	FlushLag(0, 1.2)

	ChannelStreams(4)
	ChannelPoolSize(2)
	ChannelError("send")

	KafkaLag("gs.mail", 10)
	KafkaConsumed("gs.mail", "ok")
	KafkaProduced("gs.mail", true)

	// 服务名在未初始化时返回占位值，不 panic
	if Service() == "" {
		panic("Service() should return a placeholder, not empty")
	}
}
