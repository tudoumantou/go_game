// pkg/log/log.go —— 基于 zap 的结构化日志封装
// 设计目标：替代 slog，提供统一的服务字段、TraceID 支持、高性能输出；
// 所有服务入口通过 log.New("svc") 初始化，禁止直接 import zap。
//
// 架构职责：五进程（gateway/game/center/cross/login）日志的唯一出口，负责
// 级别归一化、编码器选择、service 标签注入与调用位置开关。
// 协作关系：被 pkg/mq、pkg/rpc、pkg/shutdown 及各进程 main 依赖；
// shutdown 的 FLUSH 阶段调用 Sync() 完成最后的刷盘。
// 设计取舍：
//   - 用 zap 而非标准库 slog：热路径（网关每秒数万包）下 slog 的反射与
//     interface{} 分配开销不可忽略；zap 的编码器 + LevelEnabler 组合在
//     低于级别的日志上直接短路，序列化都不会发生。
//   - 封装 SugaredLogger 而非 Logger：键值对 API 调用点更简洁，
//     反射开销集中在 fmt 风格参数上，对本项目量级可接受。
//   - service 字段在 New 时通过 zap.Fields 固化，每条日志自动带上，
//     多进程日志汇到同一套采集系统后可按 service 分流，无需调用方手动传。
package log

import (
	"fmt"
	"os"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Logger 是 zap.SugaredLogger 的薄封装，保留键值对风格 API。
//
// 契约：零值 *Logger(nil) 不可用，必须经 New/NewDev 构造；所有方法并发安全
// （zap 内部已保证），可在多个 goroutine 间共享同一个实例。
type Logger struct {
	logger *zap.SugaredLogger
}

// Option 配置项。
//
// 函数选项（functional options）模式：New 只接受 ...Option，新增配置项
// 不需要改函数签名，也不产生「半打默认值参数」的构造函数。
type Option func(*options)

// options New 的内部配置载体，仅本包可见。
type options struct {
	// level 级别字符串，parseLevel 归一化
	level string
	// json true 走 JSON 编码器，false 走控制台编码器
	json bool
	// service 注入每条日志的 service 字段值
	service string
	// addCaller 是否附加调用文件:行号（有性能开销，debug 专用）
	addCaller bool
	// traceIDKey 预留：链路追踪字段名（如 trace_id），当前版本未启用
	traceIDKey string
}

// WithLevel 设置日志级别：debug/info/warn/error，默认 info。
// 非法值不报错，parseLevel 兜底为 info——日志配置写错不该阻塞进程启动。
func WithLevel(lv string) Option {
	return func(o *options) { o.level = lv }
}

// WithJSON 启用 JSON 输出（默认即 JSON，保留以兼容未来纯文本模式）。
func WithJSON() Option {
	return func(o *options) { o.json = true }
}

// WithCaller 开启调用位置（debug 阶段可用，生产默认关闭）。
// 代价：每条日志多一次 runtime.Caller 栈回溯，热路径上明显，故默认关闭。
func WithCaller() Option {
	return func(o *options) { o.addCaller = true }
}

// New 创建 Logger，service 字段会注入每条日志。
//
// opts 按传入顺序应用，后应用的覆盖先应用的（同名配置最后者胜）。
func New(service string, opts ...Option) *Logger {
	o := &options{
		level:      "info",
		json:       true,
		service:    service,
		traceIDKey: "trace_id",
	}
	for _, op := range opts {
		op(o)
	}

	// 生产编码器基线：ISO8601 时间（人眼可读、日志检索友好）+ 大写级别
	// （INFO/WARN，避免与正文小写单词混淆）。
	cfg := zap.NewProductionEncoderConfig()
	cfg.EncodeTime = zapcore.ISO8601TimeEncoder
	cfg.EncodeLevel = zapcore.CapitalLevelEncoder

	enc := zapcore.NewJSONEncoder(cfg)
	if !o.json {
		enc = zapcore.NewConsoleEncoder(cfg)
	}

	level := parseLevel(o.level)
	// Core = 编码器 + 输出目标 + 级别开关三件套；低于 level 的日志
	// 在 Core 层被短路，序列化与系统调用都不会发生（级别判断短路）。
	core := zapcore.NewCore(enc, zapcore.AddSync(os.Stdout), level)

	// zap.Fields 预置字段：写死在每条日志里，调用方无感。
	zcfg := []zap.Option{zap.Fields(zap.String("service", o.service))}
	if o.addCaller {
		zcfg = append(zcfg, zap.AddCaller())
	}

	z := zap.New(core, zcfg...)
	return &Logger{logger: z.Sugar()}
}

// NewDev 创建开发模式 Logger（彩色、调用位置、debug 级别）。
// 仅本地开发使用：生产禁彩色（转义符污染日志采集）。
func NewDev(service string) *Logger {
	cfg := zap.NewDevelopmentConfig()
	cfg.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	z, err := cfg.Build(zap.Fields(zap.String("service", service)))
	if err != nil {
		// 日志初始化失败直接 panic，属于启动致命错误：
		// 没有日志的系统等于盲飞，不存在「降级继续跑」的意义。
		panic(fmt.Sprintf("build dev logger: %v", err))
	}
	return &Logger{logger: z.Sugar()}
}

// parseLevel 级别字符串 → zapcore.Level。
// 大小写归一化 + 未知名兜底 info：配置里手滑写成 "INFO"/"Info" 也能工作。
func parseLevel(s string) zapcore.Level {
	switch strings.ToLower(s) {
	case "debug":
		return zapcore.DebugLevel
	case "info":
		return zapcore.InfoLevel
	case "warn", "warning":
		return zapcore.WarnLevel
	case "error":
		return zapcore.ErrorLevel
	default:
		// 未知级别按 info 处理：宁可多打，不可因配置错误静默丢日志
		return zapcore.InfoLevel
	}
}

// With 返回附加固定字段的新 Logger。
//
// 返回新实例而非改实例：原 Logger 可继续无字段使用，避免共享污染；
// 典型用法是按请求维度派生 log.With("trace_id", tid) 沿调用链传递。
func (l *Logger) With(keysAndValues ...any) *Logger {
	return &Logger{logger: l.logger.With(keysAndValues...)}
}

// Named 返回带子名称的新 Logger。
// 与 With 的区别：Named 追加 logger name（点分级），With 加结构化字段；
// 例：Named("mq") 后输出 name=xxx.mq，可按名字过滤。
func (l *Logger) Named(name string) *Logger {
	return &Logger{logger: l.logger.Named(name)}
}

// Sync flushes any buffered log entries.
// stdout 通常无用户态缓冲，Sync 主要兑现接口契约；shutdown FLUSH 阶段调用，
// 某些平台（如 macOS 的 stdout）fsync 会返回 EINVAL，调用方应忽略该错误。
func (l *Logger) Sync() error {
	return l.logger.Sync()
}

// Debug~Error 四级封装。
// 注意 keysAndValues 必须是偶数个（k/v 交替），奇数时 zap 会补一个
// 空键占位——调用方拼错不会 panic，但会打出脏字段，靠 code review 兜。
func (l *Logger) Debug(msg string, keysAndValues ...any) { l.logger.Debugw(msg, keysAndValues...) }
func (l *Logger) Info(msg string, keysAndValues ...any)  { l.logger.Infow(msg, keysAndValues...) }
func (l *Logger) Warn(msg string, keysAndValues ...any)  { l.logger.Warnw(msg, keysAndValues...) }
func (l *Logger) Error(msg string, keysAndValues ...any) { l.logger.Errorw(msg, keysAndValues...) }

// Fatal 输出致命日志并 os.Exit(1)，仅用于启动期不可恢复错误。
//
// 注意：os.Exit 不执行任何 defer（包括未 Sync 的日志缓冲之外的收尾），
// 所以 Fatal 只允许出现在「进程本来就活不下去」的启动路径，禁止用于
// 运行期错误——那会绕过 shutdown 编排，造成数据没刷盘就退出。
func (l *Logger) Fatal(msg string, keysAndValues ...any) { l.logger.Fatalw(msg, keysAndValues...) }
