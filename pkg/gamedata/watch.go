// pkg/gamedata/watch.go —— 配置表热更的触发入口（R16）
//
// # 为什么需要这个文件
//
// tables.go 提供了 Replace / LoadDir / Rollback，但它们是**能力**不是**入口**：
// 在此之前全仓没有一个生产调用点，改一张策划表必须重新编译发版，写好的
// 「构造新快照 + 原子指针替换」一直在空转。
//
// 本文件补的就是入口，两条：
//
//   - Watch：监听目录，文件变化后去抖 → LoadDir（无人值守，运维/AI 下发用）；
//   - 手动触发：进程层收到 SIGHUP 时调一次 LoadDir（Watch 不可用的兜底，
//     例如容器 bind mount 上 inotify 不生效的场景）。
//
// # 三条必须守住的语义
//
//  1. **失败保留旧快照**：LoadDir 全链路在 current.Store 之前失败即返回错误，
//     本文件只负责记日志与回调，绝不「清理」当前配置——一个写错的 JSON
//     不能把线上配置打没。
//  2. **去抖**：编辑器保存一个文件通常产生 2~3 个事件（写临时文件 + rename），
//     不去抖会把同一批内容加载多次，日志和指标都被放大成假的「多次热更」。
//  3. **事件不可靠**：inotify 在 NFS / 部分 bind mount 上会丢事件，所以监听
//     失败不能让进程起不来（返回错误由调用方降级），且必须保留 SIGHUP 兜底。
package gamedata

import (
	"context"
	"strings"
	"time"

	"github.com/fsnotify/fsnotify"
)

// DefaultDebounce 事件去抖窗口。
//
// 200ms 的依据：编辑器保存一次的最长事件间隔通常在几十毫秒内，200ms 足以
// 合并掉临时文件与 rename；再长会让「下发完等生效」的操作手感变钝。
const DefaultDebounce = 200 * time.Millisecond

// Logger 热更链路的日志接口。
//
// 依赖倒置：pkg/gamedata 不 import 具体日志实现（与 pkg/actor 的做法一致），
// 由进程装配层注入 pkg/log.Logger（它的 Info/Warn 签名正好满足）。
type Logger interface {
	Info(msg string, keysAndValues ...any)
	Warn(msg string, keysAndValues ...any)
}

// WatchOptions 目录监听参数。
type WatchOptions struct {
	// Dir 热更目录：读取其中的 `<表名>.json`，文件名即表名。
	Dir string
	// Debounce 去抖窗口；0 取 DefaultDebounce。
	Debounce time.Duration
	// Logger 可选；nil 时静默（单测与 CLI 工具）。
	Logger Logger
	// OnReload 成功回调：版本号 + 耗时。用于指标与审计日志。
	OnReload func(version string, took time.Duration)
	// OnError 失败回调：用于指标（result="fail"）。错误已保证不改变当前快照。
	OnError func(err error)
	// OnReady 监听已就绪（w.Add 成功、事件开始投递）。
	//
	// 存在的理由有两个：进程层需要在启动日志里给出「热更已生效」的确定信号；
	// 测试需要一个同步点——否则「起 goroutine → 立刻改文件」会和 w.Add
	// 抢跑，事件在注册前就发完了（漏事件，不是配置写错）。
	OnReady func()
}

// Watch 监听目录下的 *.json 变化并自动应用，阻塞直到 ctx 取消。
//
// 返回 nil 表示正常退出（ctx 取消或 watcher 关闭）；返回错误只发生在
// 「监听器建不起来」（目录不存在、inotify 句柄耗尽）——调用方应记录并
// 降级为「只能手动热更」，而不是让进程起不来。
//
// 注意：首次进入不会自动加载一次。启动期应当是「编译期内嵌快照」（见 init），
// 否则一次误放的半成品 JSON 会让新起的进程带着它上线。
func Watch(ctx context.Context, opt WatchOptions) error {
	if opt.Dir == "" {
		return nil
	}
	if opt.Debounce <= 0 {
		opt.Debounce = DefaultDebounce
	}
	w, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	defer w.Close()
	if err := w.Add(opt.Dir); err != nil {
		return err
	}
	if opt.Logger != nil {
		opt.Logger.Info("gamedata watcher started", "dir", opt.Dir, "debounce", opt.Debounce)
	}
	// 就绪回调放在 Add 之后、事件循环之前：调用方（与测试）据此确定
	// 「此后的文件改动一定会被看到」。
	if opt.OnReady != nil {
		opt.OnReady()
	}

	// trigger 容量 1：事件风暴只留一个待处理标记。加载是全量读目录，
	// 多攒几个标记并不会更快，只会多读几遍磁盘。
	trigger := make(chan struct{}, 1)

	// 事件聚合协程：把 fsnotify 的两个 channel 收成一个「要重载」的信号。
	// 单独起协程是为了让下面的 select 能同时等 ctx 与 trigger——若在同一个
	// select 里直接读 w.Events，去抖期间的定时等待会漏掉 ctx 取消。
	eventsDone := make(chan struct{})
	go func() {
		defer close(eventsDone)
		for {
			select {
			case <-ctx.Done():
				return
			case ev, ok := <-w.Events:
				if !ok {
					return
				}
				if !isTableChange(ev) {
					continue
				}
				select {
				case trigger <- struct{}{}:
				default: // 已有待处理标记：合并
				}
			case err, ok := <-w.Errors:
				if !ok {
					return
				}
				// 监听器级错误不致命：记一笔继续跑。inotify 队列溢出后
				// 会短暂丢事件，此时 SIGHUP 兜底仍可用。
				if opt.Logger != nil {
					opt.Logger.Warn("gamedata watcher error", "dir", opt.Dir, "err", err)
				}
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			<-eventsDone
			return nil
		case <-trigger:
			if !sleepCtx(ctx, opt.Debounce) {
				<-eventsDone
				return nil
			}
			// 抽干去抖窗口内累积的标记：一次保存只加载一次。
		drain:
			for {
				select {
				case <-trigger:
				default:
					break drain
				}
			}
			start := time.Now()
			version, err := LoadDir(opt.Dir)
			took := time.Since(start)
			if err != nil {
				// 关键：这里不碰 current。LoadDir 内部保证失败不改快照。
				if opt.OnError != nil {
					opt.OnError(err)
				}
				if opt.Logger != nil {
					opt.Logger.Warn("gamedata reload failed, keeping current snapshot",
						"dir", opt.Dir, "err", err)
				}
				continue
			}
			if opt.OnReload != nil {
				opt.OnReload(version, took)
			}
			if opt.Logger != nil {
				opt.Logger.Info("gamedata reloaded", "dir", opt.Dir, "version", version, "took", took)
			}
		}
	}
}

// isTableChange 判断事件是否属于「表文件变了」。
//
// 只认三种操作：Write（改内容）、Create（新增/rename 落位）、Rename（覆盖式
// 保存）。Chmod 不算——权限变更不改变内容，加载一次纯属浪费。
// 后缀必须 .json：目录里允许混放 README、.bak、.swp 等说明与临时文件。
func isTableChange(ev fsnotify.Event) bool {
	if ev.Op&(fsnotify.Write|fsnotify.Create|fsnotify.Rename) == 0 {
		return false
	}
	return strings.HasSuffix(ev.Name, ".json")
}

// sleepCtx 可被取消的等待。返回 false 表示 ctx 已取消。
//
// 不用 time.Sleep：去抖窗口 200ms 期间进程可能在关服，Sleep 会让关服
// 白等这一段时间。
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
