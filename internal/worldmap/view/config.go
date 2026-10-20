package view

import (
	"time"
)

// Config 是视野流子系统的可调参数。
//
// 刻意不 import pkg/config：view 会被 server 层依赖，如果 view 反过来
// import 配置包，就形成 "底层视图 → 配置" 的倒置依赖，还会让 view 的
// 单元测试被迫构造一整份 MapConfig（包含几十个与本包无关的字段），
// 配置的默认值/校验也会变成两处维护。因此由调用方把字段填进来
// （见 ConfigFromMap），view 只认识自己的运行参数。
type Config struct {
	// TickInterval 是 SceneView 的兜底刷新周期。批次到达时会立刻
	// Apply+Flush；ticker 只用于"没有新批次但行军线等外部状态变了"
	// 的情况（LineWorker 不经过 ProjectionBatch）。
	TickInterval time.Duration
	// ViewPendingBytes 单个订阅允许积压的事件字节数上限（文档 12.6）。
	// 超过则丢增量并改发 Resync，而不是无限占用内存。
	ViewPendingBytes int64
	// PayloadLimitBytes 单个 ViewEvent 序列化后的上限（文档 12.3）。
	// 目前只对行军线事件生效：折线顶点太多时会按顶点截断；普通对象
	// 事件只有几十字节，不会触及。
	PayloadLimitBytes int
	// MaxResyncGap 订阅者 revision 落后超过这个值时直接 Resync。
	// 投影不保存历史增量，缺口过大时"补发"已不可能，只能全量重来。
	// 0 表示使用默认值（withDefaults 补齐）。
	MaxResyncGap uint64
	// QueueSize 是每个 SceneView 的投影批次队列容量，也是每个订阅
	// 事件 channel 的容量。场景循环满队列即丢弃本批（不阻塞），
	// 订阅满 channel 即走 Resync 路径。
	QueueSize int
	// MaxViewWidth/MaxViewHeight 单次订阅视野上限（格，文档 12.1）。
	// 超限按左上角裁剪，而不是拒绝：客户端传大视野不应导致断流，
	// 但服务端必须给单次负载一个上界。0 表示只按世界边界裁剪。
	MaxViewWidth  int64
	MaxViewHeight int64
}

// DefaultConfig 返回与 MapDefaults 对齐的默认值。
//
// 与 pkg/config 的默认值保持数值一致是"调用方约定"，不是编译期依赖；
// 如果两者漂移，MapDefaults 显式映射过来的字段会覆盖这里，只有测试
// 或未显式配置时才用默认值。
func DefaultConfig() Config {
	return Config{
		TickInterval:      time.Second,
		ViewPendingBytes:  1 << 20, // 1 MiB，文档 12.6 的 2 MiB 上限留了余量
		PayloadLimitBytes: 48 << 10,
		MaxResyncGap:      64,
		QueueSize:         64,
		MaxViewWidth:      2560,
		MaxViewHeight:     2560,
	}
}

// withDefaults 补齐非正数字段。返回副本，调用方的 Config 不被改写。
func (c Config) withDefaults() Config {
	d := DefaultConfig()
	if c.TickInterval <= 0 {
		c.TickInterval = d.TickInterval
	}
	if c.ViewPendingBytes <= 0 {
		c.ViewPendingBytes = d.ViewPendingBytes
	}
	if c.PayloadLimitBytes <= 0 {
		c.PayloadLimitBytes = d.PayloadLimitBytes
	}
	if c.MaxResyncGap == 0 {
		c.MaxResyncGap = d.MaxResyncGap
	}
	if c.QueueSize <= 0 {
		c.QueueSize = d.QueueSize
	}
	if c.MaxViewWidth < 0 {
		c.MaxViewWidth = 0
	}
	if c.MaxViewHeight < 0 {
		c.MaxViewHeight = 0
	}
	return c
}

// MapConfigValues 是 pkg/config.MapConfig 中与本包相关字段的镜像。
//
// 为什么不用 pkg/config.MapConfig 直接做 ConfigFromMap 的参数：
// 见 Config 的注释——那会制造分层倒置。调用方（server 层）只写一行
// 映射：
//
//	vcfg := view.ConfigFromMap(view.MapConfigValues{
//	    TickInterval:      mapCfg.TickInterval, // yaml 里是字符串
//	    ViewPendingBytes:  mapCfg.ViewPendingBytes,
//	    PayloadLimitBytes: mapCfg.AOIPayloadLimitBytes,
//	    MaxViewWidth:      mapCfg.MaxViewWidth,
//	    MaxViewHeight:     mapCfg.MaxViewHeight,
//	})
//
// QueueSize/MaxResyncGap 在 MapConfig 里没有对应字段，由调用方按
// 部署规模决定（文档 12.6 建议最多 256 个待推批次）。
type MapConfigValues struct {
	// TickInterval 与 MapConfig.TickInterval 一致，是 "1s" 这样的字符串。
	// 放在这里解析，调用方不必为了一个时长字段 import time.ParseDuration。
	TickInterval string

	ViewPendingBytes  int64
	PayloadLimitBytes int
	MaxViewWidth      int64
	MaxViewHeight     int64

	QueueSize    int
	MaxResyncGap uint64
}

// ConfigFromMap 把配置镜像转换为 Config，零值/非法时长回退默认值。
func ConfigFromMap(v MapConfigValues) Config {
	c := Config{
		ViewPendingBytes:  v.ViewPendingBytes,
		PayloadLimitBytes: v.PayloadLimitBytes,
		MaxResyncGap:      v.MaxResyncGap,
		QueueSize:         v.QueueSize,
		MaxViewWidth:      v.MaxViewWidth,
		MaxViewHeight:     v.MaxViewHeight,
	}
	// 空串或非法串都回退默认：配置错误不应该让整个世界起不来，
	// 视野刷新周期退化一秒无业务影响。
	if v.TickInterval != "" {
		if d, err := time.ParseDuration(v.TickInterval); err == nil && d > 0 {
			c.TickInterval = d
		}
	}
	return c.withDefaults()
}
