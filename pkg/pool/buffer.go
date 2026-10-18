// pkg/pool/buffer.go —— 字节缓冲池，供网关 TCP 收发包复用
package pool

import (
	"bytes"
	"sync"
)

var (
	// defaultPool 复用常见大小的 bytes.Buffer，减少 GC 压力。
	defaultPool = sync.Pool{
		New: func() any { return &bytes.Buffer{} },
	}
	// writerPool 复用 sizedWriter，避免网关写路径频繁分配。
	writerPool = sync.Pool{
		New: func() any { return &sizedWriter{} },
	}
)

// AcquireBuffer 获取一个重置过的 bytes.Buffer，用完后必须 ReleaseBuffer。
func AcquireBuffer() *bytes.Buffer {
	b := defaultPool.Get().(*bytes.Buffer)
	b.Reset()
	return b
}

// ReleaseBuffer 归还 bytes.Buffer。
func ReleaseBuffer(b *bytes.Buffer) {
	if b == nil {
		return
	}
	b.Reset()
	defaultPool.Put(b)
}

// sizedWriter 包装 []byte 并复用。
type sizedWriter struct {
	buf []byte
}

// AcquireWriter 获取一个复用 writer（初始容量 hint）。
func AcquireWriter(hint int) *sizedWriter {
	w := writerPool.Get().(*sizedWriter)
	if cap(w.buf) < hint {
		w.buf = make([]byte, 0, hint)
	} else {
		w.buf = w.buf[:0]
	}
	return w
}

// ReleaseWriter 归还 writer。
func ReleaseWriter(w *sizedWriter) {
	if w == nil {
		return
	}
	w.buf = w.buf[:0]
	writerPool.Put(w)
}

// Bytes 返回 writer 当前内容（归还前有效）。
func (w *sizedWriter) Bytes() []byte { return w.buf }

// Write 实现 io.Writer。
func (w *sizedWriter) Write(p []byte) (int, error) {
	w.buf = append(w.buf, p...)
	return len(p), nil
}

// Grow 预扩容。
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
