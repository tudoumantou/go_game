// internal/gateway/conn/conn.go —— 单个 TCP 连接处理
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
	Packet codec.Packet
}

// Handler 处理连接事件。
type Handler interface {
	OnPacket(c *Conn, p codec.Packet)
	OnClose(c *Conn, err error)
}

// Conn 网关侧 TCP 连接封装。
type Conn struct {
	id      int64
	raw     net.Conn
	handler Handler
	logger  *log.Logger

	// close 控制
	closed   atomic.Bool
	closeCh  chan struct{}
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
func (c *Conn) Serve(ctx context.Context) {
	defer c.Close(nil)

	go c.writeLoop(ctx)
	c.readLoop(ctx)
}

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
