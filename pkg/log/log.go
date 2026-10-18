// pkg/log/log.go —— 基于 zap 的结构化日志封装
// 设计目标：替代 slog，提供统一的服务字段、TraceID 支持、高性能输出；
// 所有服务入口通过 log.New("svc") 初始化，禁止直接 import zap。
package log

import (
	"fmt"
	"os"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Logger 是 zap.SugaredLogger 的薄封装，保留键值对风格 API。
type Logger struct {
	logger *zap.SugaredLogger
}

// Option 配置项。
type Option func(*options)

type options struct {
	level      string
	json       bool
	service    string
	addCaller  bool
	traceIDKey string
}

// WithLevel 设置日志级别：debug/info/warn/error，默认 info。
func WithLevel(lv string) Option {
	return func(o *options) { o.level = lv }
}

// WithJSON 启用 JSON 输出（默认即 JSON，保留以兼容未来纯文本模式）。
func WithJSON() Option {
	return func(o *options) { o.json = true }
}

// WithCaller 开启调用位置（debug 阶段可用，生产默认关闭）。
func WithCaller() Option {
	return func(o *options) { o.addCaller = true }
}

// New 创建 Logger，service 字段会注入每条日志。
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

	cfg := zap.NewProductionEncoderConfig()
	cfg.EncodeTime = zapcore.ISO8601TimeEncoder
	cfg.EncodeLevel = zapcore.CapitalLevelEncoder

	enc := zapcore.NewJSONEncoder(cfg)
	if !o.json {
		enc = zapcore.NewConsoleEncoder(cfg)
	}

	level := parseLevel(o.level)
	core := zapcore.NewCore(enc, zapcore.AddSync(os.Stdout), level)

	zcfg := []zap.Option{zap.Fields(zap.String("service", o.service))}
	if o.addCaller {
		zcfg = append(zcfg, zap.AddCaller())
	}

	z := zap.New(core, zcfg...)
	return &Logger{logger: z.Sugar()}
}

// NewDev 创建开发模式 Logger（彩色、调用位置、debug 级别）。
func NewDev(service string) *Logger {
	cfg := zap.NewDevelopmentConfig()
	cfg.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	z, err := cfg.Build(zap.Fields(zap.String("service", service)))
	if err != nil {
		// 日志初始化失败直接 panic，属于启动致命错误
		panic(fmt.Sprintf("build dev logger: %v", err))
	}
	return &Logger{logger: z.Sugar()}
}

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
		return zapcore.InfoLevel
	}
}

// With 返回附加固定字段的新 Logger。
func (l *Logger) With(keysAndValues ...any) *Logger {
	return &Logger{logger: l.logger.With(keysAndValues...)}
}

// Named 返回带子名称的新 Logger。
func (l *Logger) Named(name string) *Logger {
	return &Logger{logger: l.logger.Named(name)}
}

// Sync flushes any buffered log entries.
func (l *Logger) Sync() error {
	return l.logger.Sync()
}

func (l *Logger) Debug(msg string, keysAndValues ...any) { l.logger.Debugw(msg, keysAndValues...) }
func (l *Logger) Info(msg string, keysAndValues ...any)  { l.logger.Infow(msg, keysAndValues...) }
func (l *Logger) Warn(msg string, keysAndValues ...any)  { l.logger.Warnw(msg, keysAndValues...) }
func (l *Logger) Error(msg string, keysAndValues ...any) { l.logger.Errorw(msg, keysAndValues...) }

// Fatal 输出致命日志并 os.Exit(1)，仅用于启动期不可恢复错误。
func (l *Logger) Fatal(msg string, keysAndValues ...any) { l.logger.Fatalw(msg, keysAndValues...) }
