// internal/gateway/conn/conn.go —— 单个 TCP 连接处理
//
// Package conn 网关接入连接层：每个客户端连接对应一个 Conn（读写循环 +
// 幂等关闭语义），Manager 负责全局登记与 ID 分配。本包不识别业务语义，
// 只把解码后的 Packet 与关闭事件交给 conn.Handler（由 router 实现）。
// 设计取舍：goroutine per conn 模型——读循环阻塞在 socket 上，另起一个
// 写循环 goroutine；泄漏防护靠「读错误/ctx 取消 → Close → closeCh 关闭」
// 的闭环，任何退出路径都汇合到 Close，保证 fd 与 goroutine 同时释放。
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
	// readBufferSize 单次 read 的系统调用缓存，8KB 兼顾系统调用次数与
	// 栈/堆占用；readBuf 的初始容量同此值，避免前几个包反复扩容
	readBufferSize = 8192
	// defaultWriteTimeout 写超时：对端只收不发（慢连接/恶意占位）时，
	// 写 goroutine 最多阻塞 10s 即报错断开，防止连接与内存被长期占死
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
// 知识点：net.Conn 是接口（*net.TCPConn 的实现之一），因此测试可注入
// fake conn；SetReadDeadline/SetWriteDeadline 是接口方法，用于防慢连接。
type Conn struct {
	// id 连接 ID（Manager 分配，进程内唯一，同时作为会话数值 ID）
	id      int64
	raw     net.Conn
	handler Handler
	logger  *log.Logger

	// close 控制：closed 原子位供 Send/SendRaw 无锁快速判断；
	// closeCh 广播退出信号（读循环 select + writeLoop 等待）；
	// closeOnce 保证「标记→关 socket→回调」只执行一次
	closed    atomic.Bool
	closeCh   chan struct{}
	closeOnce sync.Once
	closeErr  error

	// 读缓存：TCP 是字节流，readBuf 累积半包直到切出完整逻辑包，
	// 粘包/半包的拆分逻辑全部在 processReadBuf（codec 只认完整包）
	readBuf []byte
}

// New 创建 Conn。
// 注意 closeCh 是无缓冲 channel：Close 时 close(closeCh) 即向所有
// 等待方广播，不需要发送数据；readBuf 预分配容量减少早期 append 扩容。
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
// 知识点：defer 的参数在 defer 语句执行时求值——此处 defer c.Close(nil)
// 的 nil 立即绑定，若读循环内部以错误原因关过连接，closeOnce 会保证
// 首次原因不被这里的 nil 覆盖。
func (c *Conn) Serve(ctx context.Context) {
	defer c.Close(nil)

	go c.writeLoop(ctx)
	c.readLoop(ctx)
}

// readLoop 读循环：阻塞读 socket → 累积到 readBuf → 切出完整包交给 Handler。
//
// ctx 取消或连接关闭都会退出；读错误（含 EOF）统一走 Close 路径，
// 保证 OnClose 只触发一次。
// 知识点：select + default 构成非阻塞检查——三个退出源（ctx/closeCh/
// 读错误）任一触发即收敛到 Close，避免读循环在连接已死后仍阻塞 Read。
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
			// EOF（对端正常关闭）不计错误日志，其余（超时/重置）记 Debug
			if err != io.EOF {
				c.logger.Debug("read error", "err", err)
			}
			c.Close(err)
			return
		}
		if n == 0 {
			continue
		}
		// 读到的是裸字节流：先累积再切包，buf 复用避免每轮分配
		c.readBuf = append(c.readBuf, buf[:n]...)
		c.processReadBuf()
	}
}

// processReadBuf 从累积缓冲切出完整包（处理 TCP 粘包/半包）。
//
// 外层格式：4B 长度 + length 字节的包体（4.2）。长度非法（<=0 或超上限）
// 直接断连——宁可错杀也不解析，防止恶意长度字段耗尽内存。
// 知识点：粘包/半包是 TCP 流式协议的固有现象（无消息边界），必须由
// 应用层长度字段重组；这里用「头部够→解长度→整体够→解码→前移缓冲」
// 的标准状态机处理，copy 前移比重新分配更省。
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
		// 长度上限与协议最大包一致（65535 含 4B 头）：越界即视为攻击/错位，
		// 断连而不是尝试分配，防止恶意长度把 readBuf 撑爆
		if length <= 0 || length > 65535-codec.LengthFieldSize {
			c.logger.Error("invalid packet length", "len", length)
			c.Close(fmt.Errorf("invalid packet length %d", length))
			return
		}
		total := codec.LengthFieldSize + length
		if len(c.readBuf) < total {
			// 半包：等后续字节到达，下轮 read 后继续切
			return
		}

		pkt, err := codec.DecodePacket(c.readBuf[codec.LengthFieldSize:total])
		if err != nil {
			c.logger.Error("decode packet", "err", err)
			c.Close(err)
			return
		}

		// 同步回调：同一连接的包严格保序交给你 handler
		c.handler.OnPacket(c, pkt)
		if metrics.Enabled() {
			metrics.MsgInc("in", "ok")
			metrics.MsgObserve("in", 0, len(pkt.Body))
		}
		// 粘包情形：把已消费部分前移，剩余半包留在头部继续累积
		copy(c.readBuf, c.readBuf[total:])
		c.readBuf = c.readBuf[:len(c.readBuf)-total]
	}
}

// writeLoop 处理出站包。当前版本同步写，简化实现。
// 占位语义：Send/SendRaw 直接在调用方 goroutine 写 socket，本循环只等
// closeCh 退出——保留 goroutine 是为将来改异步写队列（chan 缓冲）时
// 不动 Serve 的结构；目前写路径的并发安全由调用方约定保证（见 Send）。
func (c *Conn) writeLoop(ctx context.Context) {
	<-c.closeCh
}

// Send 同步发送一个逻辑包（已加密）。
//
// 编码后单次 Write 发出，并设置写超时（defaultWriteTimeout）防止对端
// 不读时无限阻塞。注意：本方法未对并发写加锁，调用方需自行避免同一
// 连接上的多写方交叉（现状：读循环回系统包、后端 recvLoop 回业务包）。
// 知识点：SetWriteDeadline 是绝对时间点而非时长，超过后未写完的 Write
// 返回 timeout 错误；对已关闭连接先查 closed 原子位，避免无效系统调用。
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
// 用于握手期明文系统消息（如 ServerHello）；与 Send 共用写超时与
// closed 快检，同样不提供多写方互斥。
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
// 知识点：sync.Once 内部用 atomic + mutex 实现一次性语义，适合
// 「关连接/关资源」这类必须精确执行一次的场景；closeErr 在 Once 内写入、
// Once 外读取，靠 Once 的 happens-before 保证可见性，无需额外加锁。
func (c *Conn) Close(err error) error {
	c.closeOnce.Do(func() {
		c.closed.Store(true)
		c.closeErr = err
		// close(channel) 是广播语义：读循环 select 与 writeLoop 同时被唤醒
		close(c.closeCh)
		if c.raw != nil {
			_ = c.raw.Close()
		}
		c.handler.OnClose(c, err)
	})
	return c.closeErr
}

// IsClosed 返回是否已关闭。
// 原子读，供 server/manager 等路径无锁查询连接活性。
func (c *Conn) IsClosed() bool { return c.closed.Load() }
