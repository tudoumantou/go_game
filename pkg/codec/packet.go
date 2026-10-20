// pkg/codec/packet.go —— 外层包编解码
// 客户端 ↔ 网关物理包格式（设计文档 4.2）：
//
//	Length(2B) | MsgID(2B) | Seq(4B) | AES-GCM Body
//
// Length = 2 + 4 + len(Body)（不含 Length 自身）。
package codec

import (
	"encoding/binary"
	"fmt"

	"game_study/internal/common/constant"
)

const (
	// HeaderSize 外层包头固定长度（MsgID + Seq）。
	// 不含 Length 字段自身——Length 在包体外层单独占 LengthFieldSize 字节，
	// 与「Length = 2 + 4 + len(Body)」的约定互为倒数（见文件头协议图）。
	HeaderSize = 6
	// LengthFieldSize Length 字段长度。
	// 2 字节大端无符号整数，因此单包上限 65535 字节，与 internal/common/constant
	// 的 MaxPacketSize（64KB）对齐；超限的包应在业务层分帧，而不是硬发。
	LengthFieldSize = 2
)

// Packet 解码后的逻辑包（body 为 AES-GCM 加密后的二进制，需 SessionCipher 解密）。
//
// 字段语义：
//   - MsgID：协议消息号，唯一声明点是 proto 文件的 `// @msg <id>` 注解，
//     生成物见 msgids_gen.go；已下发客户端的号不可更改（兼容基线测试见
//     msgids_test.go 的 wireBaseline）；
//   - Seq：发送序号，由 pkg/crypto 的 SessionCipher 单调递增分配，
//     接收端据此做防重放检测；
//   - Body：加密体，布局为 nonce||ciphertext||tag（AES-256-GCM），
//     解密后才能得到 proto 序列化后的明文。
//
// 本结构体是纯数据载体，无并发状态，可跨 goroutine 传递；但 Body 切片
// 直接引用入参缓冲区（见 DecodePacket），调用方复用读缓冲时必须自行拷贝。
type Packet struct {
	MsgID uint16
	Seq   uint32
	Body  []byte // 加密体
}

// EncodePacket 把 Packet 编码为带 Length 前缀的字节流（用于 TCP 发送）。
//
// 返回的字节流 = Length(2B) || MsgID(2B) || Seq(4B) || Body，可直接 write 到
// 连接；TCP 是字节流没有边界，接收端须先用 ReadLength 取长度、再按长度切包
// （DecodePacket 只处理「已切好的一整包」）。
// 错误语义：Body 超过 MaxPacketSize-HeaderSize-LengthFieldSize 时返回 error，
// 不做截断——静默截断会产生收端解不开的包，难排查。
// 并发：纯函数，无共享可变状态，可多 goroutine 并发调用。
func EncodePacket(p Packet) ([]byte, error) {
	if len(p.Body) > constant.MaxPacketSize-HeaderSize-LengthFieldSize {
		return nil, fmt.Errorf("body too large: %d", len(p.Body))
	}
	length := HeaderSize + len(p.Body)
	buf := make([]byte, LengthFieldSize+length)
	binary.BigEndian.PutUint16(buf[0:], uint16(length))
	binary.BigEndian.PutUint16(buf[LengthFieldSize:], p.MsgID)
	binary.BigEndian.PutUint32(buf[LengthFieldSize+2:], p.Seq)
	copy(buf[LengthFieldSize+HeaderSize:], p.Body)
	return buf, nil
}

// DecodePacket 从完整包数据（不含 Length）解码 Packet。
//
// 入参 data 必须是「去掉 Length 前缀后的完整一包」，即 buf[LengthFieldSize:]
// 且长度等于 ReadLength 读到的值——切多切少都会在这里体现为 Body 错位。
// 返回的 Body 不拷贝，直接引用 data 的尾部子切片：零拷贝是有意的性能取舍，
// 代价是调用方若复用读缓冲，必须在 Decrypt 之前完成拷贝或使用。
// 错误语义：len(data) < HeaderSize 时返回 error（连固定包头都放不下，
// 后续字段必错，早失败）。
func DecodePacket(data []byte) (Packet, error) {
	if len(data) < HeaderSize {
		return Packet{}, fmt.Errorf("packet too short: %d", len(data))
	}
	return Packet{
		MsgID: binary.BigEndian.Uint16(data[0:]),
		Seq:   binary.BigEndian.Uint32(data[2:]),
		Body:  data[HeaderSize:],
	}, nil
}

// ReadLength 从 2 字节大端读取包体长度。
//
// 配套用法：先从连接头部读满 LengthFieldSize 字节，调用本函数得到 length，
// 再继续读 length 字节作为 DecodePacket 的入参。length 不含 Length 字段自身。
// 错误语义：len(b) < LengthFieldSize 时返回 error（缓冲区还没攒够包头）。
func ReadLength(b []byte) (int, error) {
	if len(b) < LengthFieldSize {
		return 0, fmt.Errorf("length buffer too short")
	}
	return int(binary.BigEndian.Uint16(b)), nil
}
