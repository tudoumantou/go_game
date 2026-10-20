// pkg/pool/buffer.go —— 字节缓冲池，供网关 TCP 收发包复用
//
// 职责：为网关读写路径提供可复用的字节缓冲，把「每包一次 make」变成
// 「取-用-还」。TCP 网关每秒数万包，堆分配是 GC 压力的主要来源，
// sync.Pool 是标准解法。
//
// 协作关系：net/网关在读包、组包、加密后写入时 Acquire/Release；
// 池内对象跨 goroutine 安全（sync.Pool 自身并发安全），但单个取出
// 对象不共享——Acquire 到 Release 之间独占使用。
//
// 设计取舍：sync.Pool 不保证命中（GC 会清空私有/共享队列），所以 New
// 必须能造出可用对象；池只降低分配频率，不是对象缓存。归还前统一
// Reset：防止脏数据被下一个使用者看到，也防止大数组长期驻留。
package pool

import (
	"bytes"
	"sync"
)

var (
	// defaultPool 复用常见大小的 bytes.Buffer，减少 GC 压力。
	//
	// Go 细节：sync.Pool.New 是惰性工厂——Get 命中为空闲池空时才调用；
	// Get 返回 any 需要类型断言取回具体类型。Pool 中的对象随时可能被
	// GC 回收（victim 缓存机制），不能假设对象一直活着。
	defaultPool = sync.Pool{
		New: func() any { return &bytes.Buffer{} },
	}
	// writerPool 复用 sizedWriter，避免网关写路径频繁分配。
	//
	// 与 defaultPool 分开：bytes.Buffer 内部有 off/增长策略，适合长度
	// 频繁变化的场景；sizedWriter 直接管 []byte，适合已知大致大小的
	// 发包路径（配合 hint 与 Grow）。
	writerPool = sync.Pool{
		New: func() any { return &sizedWriter{} },
	}
)

// AcquireBuffer 获取一个重置过的 bytes.Buffer，用完后必须 ReleaseBuffer。
//
// 在 Get 后立即 Reset：池中对象可能残留上一次的内容（归还时虽也
// Reset，但双保险），脏 buffer 直接写会把上个连接的字节拼进这个包。
// 契约：Acquire 与 Release 必须严格配对，且 Release 后不得再持有
// buffer（下一个 Acquire 会复用同一对象并覆盖内容）。
func AcquireBuffer() *bytes.Buffer {
	b := defaultPool.Get().(*bytes.Buffer)
	b.Reset()
	return b
}

// ReleaseBuffer 归还 bytes.Buffer。
//
// nil 入参直接返回：配对释放常写在 defer 里，对象可能已被提前置空
// （防御性 nil 检查是 Go API 的常见宽容姿态）。
// Put 前 Reset 清空内容——注意 Reset 保留已分配的容量，大包之后的
// buffer 会带着大数组继续在池里循环，这是容量复用与内存驻留的取舍。
func ReleaseBuffer(b *bytes.Buffer) {
	if b == nil {
		return
	}
	b.Reset()
	defaultPool.Put(b)
}

// sizedWriter 包装 []byte 并复用。
//
// buf 归还后不得继续持有 Bytes() 的结果：下一个 AcquireWriter
// 会复用同一对象并覆盖底层数组，旧切片会「看到」别人的数据。
//
// Go 细节：切片是「指针+len+cap」三段头，结构体按值拷贝时三者一起
// 拷、底层数组仍共享——本类型通过池保证「同一时刻只有一个使用者」，
// 不提供写时拷贝；谁 Acquire 谁负责在使用期间独占。
type sizedWriter struct {
	buf []byte
}

// AcquireWriter 获取一个复用 writer（初始容量 hint）。
//
// hint 语义：只是**首次/扩容的参考下限**。池里的 writer 反复复用
// 已长大的 buffer，cap 够用时直接 buf[:0]（len 归零、容量保留），
// 不够才 make 新数组——避免了「每次按 hint 重新分配」的浪费，
// 也让 buffer 容量随历史峰值自适应增长。
//
// 契约：与 ReleaseWriter 严格配对；Bytes() 结果在 Release 前有效。
func AcquireWriter(hint int) *sizedWriter {
	w := writerPool.Get().(*sizedWriter)
	// 容量不够才重新分配：池里的 writer 反复复用已长大的 buffer，
	// hint 只是首次/扩容的参考值，不是每次都 make。
	if cap(w.buf) < hint {
		w.buf = make([]byte, 0, hint)
	} else {
		w.buf = w.buf[:0]
	}
	return w
}

// ReleaseWriter 归还 writer。
//
// 同样 nil 宽容；buf[:0] 保留容量、清空长度，与 AcquireWriter 的
// 快路径对称。不要把外部切片直接赋给 w.buf 再 Release（会破坏池内
// buffer 的容量记忆，且外部切片可能仍被引用）。
func ReleaseWriter(w *sizedWriter) {
	if w == nil {
		return
	}
	w.buf = w.buf[:0]
	writerPool.Put(w)
}

// Bytes 返回 writer 当前内容（归还前有效）。
//
// 返回内部切片本身（非拷贝）：零拷贝供 write syscall 直读；
// 调用方不得修改内容，也不得越过 Release 继续持有。
func (w *sizedWriter) Bytes() []byte { return w.buf }

// Write 实现 io.Writer。
//
// append 语义：容量不足时自动扩容（翻倍策略，摊薄多次 Write 的
// 数组搬运）。内存 writer 不会写失败，永远返回 (len(p), nil)，
// 因此可安全用于 io.Copy 等上层组件。
//
// Go 细节：append 只拷贝 p 的内容不保留引用——调用方事后修改 p
// 不影响已写入内容（与 DecodePacket 的零拷贝取舍相反，这里选拷贝
// 保安全）。
func (w *sizedWriter) Write(p []byte) (int, error) {
	w.buf = append(w.buf, p...)
	return len(p), nil
}

// Grow 预扩容。
//
// 新容量取「翻倍」与「恰好够用」的较大值：翻倍摊薄连续多次 Grow
// 的数组搬运成本，下限保证本次请求一定被满足。
//
// Go 细节：make + copy 手工扩容而不是 append(nil...)——Grow 的契约
// 是「只扩容量不改长度」，append 会改 len；newCap 至少 len+n，
// 调用后 cap-len >= n 一定成立（调用方可安全再 Write n 字节不触发
// 二次扩容）。
func (w *sizedWriter) Grow(n int) {
	if cap(w.buf)-len(w.buf) < n {
		newCap := cap(w.buf) * 2
		if newCap < len(w.buf)+n {
			newCap = len(w.buf) + n
		}
		buf := make([]byte, len(w.buf), newCap)
		copy(buf, w.buf)
		w.buf = buf
	}
}
