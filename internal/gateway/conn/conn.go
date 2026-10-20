// internal/gateway/conn/conn.go —— 单个 TCP 连接处理
//
// Package conn 网关接入连接层：每个客户端连接对应一个 Conn（读写循环 +
// 幂等关闭语义），Manager 负责全局登记与 ID 分配。本包不识别业务语义，
// 只把解码后的 Packet 与关闭事件交给 conn.Handler（由 router 实现）。
package conn

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"game_study/pkg/codec"
	"game_study/pkg/log"
	"game_study/pkg/metrics"
)

const (
	// readBufferSize 单次 read 缓存。
	readBufferSize = 8192
	// defaultWriteTimeout 写超时。
	defaultWriteTimeout = 10 * time.Second
)

// Event 连接事件。
type Event struct {
	// Packet 已解码的逻辑包（外层长度字段已剥离，MsgID/Seq/Body 可用）
	Packet codec.Packet
}

// Handler 处理连接事件。
type Handler interface {
	// OnPacket 每收到一个完整逻辑包回调一次（读循环线程串行调用，
	// 实现方不应做阻塞操作，否则拖慢同连接的后续收包）
	OnPacket(c *Conn, p codec.Packet)
	// OnClose 连接关闭时回调一次（closeOnce 保证唯一）；
	// err 为关闭原因（对端断开/超时/主动关闭），主动正常关闭时为 nil
	OnClose(c *Conn, err error)
}

// Conn 网关侧 TCP 连接封装。
//
// 生命周期：New 创建 → Serve 起读写循环 → Close 幂等收尾。
// 并发约定：processReadBuf/OnPacket 只在读循环线程执行；Send/SendRaw 可被
// 任意 goroutine 调用（closed 原子位 + 写超时兜底）；Close 可重入，
// 首次调用才关 socket 并触发 OnClose。
type Conn struct {
	// id 连接 ID（Manager 分配，进程内唯一，同时作为会话数值 ID）
	id      int64
	raw     net.Conn
	handler Handler
	logger  *log.Logger

	// close 控制
	closed    atomic.Bool
	closeCh   chan struct{}
	closeOnce sync.Once
	closeErr  error

	// 读缓存
	readBuf []byte
}

// New 创建 Conn。
func New(id int64, raw net.Conn, handler Handler, logger *log.Logger) *Conn {
	return &Conn{
		id:      id,
		raw:     raw,
		handler: handler,
		logger:  logger.With("conn_id", id, "remote", raw.RemoteAddr().String()),
		closeCh: make(chan struct{}),
		readBuf: make([]byte, 0, readBufferSize),
	}
}

// ID 返回连接 ID。
func (c *Conn) ID() int64 { return c.id }

// RemoteAddr 返回对端地址。
func (c *Conn) RemoteAddr() net.Addr { return c.raw.RemoteAddr() }

// LocalAddr 返回本地地址。
func (c *Conn) LocalAddr() net.Addr { return c.raw.LocalAddr() }

// Serve 阻塞运行读写循环。
//
// 读循环在当前 goroutine，写循环另起一个（当前版本写是同步直写，
// writeLoop 仅作为退出占位，见 writeLoop 注释）。返回时连接必定已 Close。
func (c *Conn) Serve(ctx context.Context) {
	defer c.Close(nil)

	go c.writeLoop(ctx)
	c.readLoop(ctx)
}

// readLoop 读循环：阻塞读 socket → 累积到 readBuf → 切出完整包交给 Handler。
//
// ctx 取消或连接关闭都会退出；读错误（含 EOF）统一走 Close 路径，
// 保证 OnClose 只触发一次。
func (c *Conn) readLoop(ctx context.Context) {
	buf := make([]byte, readBufferSize)
	for {
		select {
		case <-ctx.Done():
			c.Close(ctx.Err())
			return
		case <-c.closeCh:
			return
		default:
		}

		n, err := c.raw.Read(buf)
		if err != nil {
			if err != io.EOF {
				c.logger.Debug("read error", "err", err)
			}
			c.Close(err)
			return
		}
		if n == 0 {
			continue
		}
		c.readBuf = append(c.readBuf, buf[:n]...)
		c.processReadBuf()
	}
}

// processReadBuf 从累积缓冲切出完整包（处理 TCP 粘包/半包）。
//
// 外层格式：4B 长度 + length 字节的包体（4.2）。长度非法（<=0 或超上限）
// 直接断连——宁可错杀也不解析，防止恶意长度字段耗尽内存。
func (c *Conn) processReadBuf() {
	for {
		if len(c.readBuf) < codec.LengthFieldSize {
			return
		}
		length, err := codec.ReadLength(c.readBuf)
		if err != nil {
			c.logger.Error("invalid length", "err", err)
			c.Close(err)
			return
		}
		if length <= 0 || length > 65535-codec.LengthFieldSize {
			c.logger.Error("invalid packet length", "len", length)
			c.Close(fmt.Errorf("invalid packet length %d", length))
			return
		}
		total := codec.LengthFieldSize + length
		if len(c.readBuf) < total {
			return
		}

		pkt, err := codec.DecodePacket(c.readBuf[codec.LengthFieldSize:total])
		if err != nil {
			c.logger.Error("decode packet", "err", err)
			c.Close(err)
			return
		}

		c.handler.OnPacket(c, pkt)
		if metrics.Enabled() {
			metrics.MsgInc("in", "ok")
			metrics.MsgObserve("in", 0, len(pkt.Body))
		}
		copy(c.readBuf, c.readBuf[total:])
		c.readBuf = c.readBuf[:len(c.readBuf)-total]
	}
}

// writeLoop 处理出站包。当前版本同步写，简化实现。
func (c *Conn) writeLoop(ctx context.Context) {
	<-c.closeCh
}

// Send 同步发送一个逻辑包（已加密）。
//
// 编码后单次 Write 发出，并设置写超时（defaultWriteTimeout）防止对端
// 不读时无限阻塞。注意：本方法未对并发写加锁，调用方需自行避免同一
// 连接上的多写方交叉（现状：读循环回系统包、后端 recvLoop 回业务包）。
func (c *Conn) Send(p codec.Packet) error {
	if c.closed.Load() {
		return fmt.Errorf("conn closed")
	}
	data, err := codec.EncodePacket(p)
	if err != nil {
		return err
	}
	_ = c.raw.SetWriteDeadline(time.Now().Add(defaultWriteTimeout))
	_, err = c.raw.Write(data)
	return err
}

// SendRaw 发送原始字节。
func (c *Conn) SendRaw(data []byte) error {
	if c.closed.Load() {
		return fmt.Errorf("conn closed")
	}
	_ = c.raw.SetWriteDeadline(time.Now().Add(defaultWriteTimeout))
	_, err := c.raw.Write(data)
	return err
}

// Close 关闭连接。
//
// 幂等：closeOnce 保证「标记 closed → 关 socket → 回调 OnClose」只执行一次，
// 后续调用直接返回首次记录的 closeErr。OnClose 在回调内同步执行，
// 实现方（router）不应再调用本连接的方法。
func (c *Conn) Close(err error) error {
	c.closeOnce.Do(func() {
		c.closed.Store(true)
		c.closeErr = err
		close(c.closeCh)
		if c.raw != nil {
			_ = c.raw.Close()
		}
		c.handler.OnClose(c, err)
	})
	return c.closeErr
}

// IsClosed 返回是否已关闭。
func (c *Conn) IsClosed() bool { return c.closed.Load() }
