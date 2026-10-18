// pkg/codec/packet.go —— 外层包编解码
// 客户端 ↔ 网关物理包格式（设计文档 4.2）：
//   Length(2B) | MsgID(2B) | Seq(4B) | AES-GCM Body
// Length = 2 + 4 + len(Body)（不含 Length 自身）。
package codec

import (
	"encoding/binary"
	"fmt"

	"game_study/internal/common/constant"
)

const (
	// HeaderSize 外层包头固定长度（MsgID + Seq）。
	HeaderSize = 6
	// LengthFieldSize Length 字段长度。
	LengthFieldSize = 2
)

// Packet 解码后的逻辑包（body 为 AES-GCM 加密后的二进制，需 SessionCipher 解密）。
type Packet struct {
	MsgID uint16
	Seq   uint32
	Body  []byte // 加密体
}

// EncodePacket 把 Packet 编码为带 Length 前缀的字节流（用于 TCP 发送）。
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
func ReadLength(b []byte) (int, error) {
	if len(b) < LengthFieldSize {
		return 0, fmt.Errorf("length buffer too short")
	}
	return int(binary.BigEndian.Uint16(b)), nil
}
