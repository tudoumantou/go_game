// pkg/codec/packet.go —— 外层包编解码
//
// 客户端 ↔ 网关物理包格式（设计文档 4.2）：
//
//	Length(2B) | MsgID(2B) | Seq(4B) | AES-GCM Body
//
// Length = 2 + 4 + len(Body)（不含 Length 自身）。
// 本文件只做「字节流 ↔ 逻辑包」的转换：切包、读长度、拼长度前缀。
// 加密/解密由 pkg/crypto 的 SessionCipher 负责，本层不接触密钥。
package codec

import (
	"encoding/binary"
	"fmt"

	"game_study/internal/common/constant"
)

const (
	// HeaderSize 外层包头固定长度（MsgID 2B + Seq 4B）。
	// 不含 Length 字段自身——Length 在包体外层单独占 LengthFieldSize 字节，
	// 与「Length = 2 + 4 + len(Body)」的约定互为倒数（见文件头协议图）。
	// 无类型整型常量：与 byte/uint16/int 运算时按需适配，切片下标处按 int 用。
	HeaderSize = 6
	// LengthFieldSize Length 字段长度。
	// 2 字节大端无符号整数，因此单包上限 65535 字节，与 internal/common/constant
	// 的 MaxPacketSize（64KB）对齐；超限的包应在业务层分帧，而不是硬发——
	// 长度超限若静默截断，会变成另一个「合法」长度，是协议层最难排查的故障。
	LengthFieldSize = 2
	// maxWireMsgID wire 上 msg_id 能表示的最大值（2 字节）。
	//
	// 比 MaxPacketSize 更硬的上限：分段号段上限 9999 已经够用，这里的
	// 65535 是传输格式的天花板——把内部类型放宽到 uint32 不代表线上的包
	// 也跟着变宽，包格式是已下发的客户端契约。
	maxWireMsgID = 0xFFFF
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
// 并发与生命周期：本结构体是纯数据载体，无并发状态，可跨 goroutine 传递；
// 但 Body 是切片（引用类型），直接引用入参缓冲区（见 DecodePacket 的零拷贝
// 取舍），调用方复用读缓冲时必须自行拷贝，否则后一个包会覆盖前一个包的 Body。
// 结构体按值传递时只拷贝三个字加切片头，成本可忽略。
type Packet struct {
	// MsgID 消息号。
	//
	// 类型是 uint32（与协议表、模块注册表、路由表同宽），wire 上仍写 2 字节：
	// 窄化只发生在 EncodePacket 一处，超范围直接报错而不是截断——截断会把
	// 3001 变成 3001-65536 这样的另一个「合法」号，收端解成完全不相干的协议。
	MsgID uint32
	// Seq 发送序号；接收端防重放检测的输入。
	Seq uint32
	// Body 加密体；注意切片别名语义（见结构体注释），非拷贝。
	Body []byte // 加密体
}

// EncodePacket 把 Packet 编码为带 Length 前缀的字节流（用于 TCP 发送）。
//
// 返回的字节流 = Length(2B) || MsgID(2B) || Seq(4B) || Body，可直接 write 到
// 连接；TCP 是字节流没有边界，接收端须先用 ReadLength 取长度、再按长度切包
// （DecodePacket 只处理「已切好的一整包」）。
// 错误语义：Body 超过 MaxPacketSize-HeaderSize-LengthFieldSize 时返回 error，
// 不做截断——静默截断会产生收端解不开的包，难排查。
// 并发：纯函数，无共享可变状态，可多 goroutine 并发调用。
//
// Go 细节：binary.BigEndian 保证多字节字段按网络字节序（大端）落盘，
// 与收发双方 CPU 的字节序无关；make 一次性按精确长度分配，避免 append
// 反复扩容与数组搬运；每个字段写入前重新切片定位，偏移全部由常量拼死，
// 不存在运行期算错的可能。
func EncodePacket(p Packet) ([]byte, error) {
	if len(p.Body) > constant.MaxPacketSize-HeaderSize-LengthFieldSize {
		return nil, fmt.Errorf("body too large: %d", len(p.Body))
	}
	// 唯一的窄化点：内部 uint32 → wire 2 字节。超范围必须报错，不能截断
	// （见 Packet.MsgID 的说明）。
	if p.MsgID > maxWireMsgID {
		return nil, fmt.Errorf("msg id %d exceeds wire limit %d", p.MsgID, maxWireMsgID)
	}
	length := HeaderSize + len(p.Body)
	// 总长 = Length 字段自身（LengthFieldSize）+ header + body；
	// Length 字段里记录的只是后两部分，与接收端 ReadLength 的约定对齐
	buf := make([]byte, LengthFieldSize+length)
	binary.BigEndian.PutUint16(buf[0:], uint16(length))
	binary.BigEndian.PutUint16(buf[LengthFieldSize:], uint16(p.MsgID))
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
//
// Go 细节：data[HeaderSize:] 是与入参共享底层数组的子切片（切片别名），
// 并非拷贝——底层数组被复用（如连接读缓冲）时 Body 内容随之改变。
// 包头字段按定长偏移读取，与 EncodePacket 的写入顺序严格互为逆操作。
func DecodePacket(data []byte) (Packet, error) {
	if len(data) < HeaderSize {
		return Packet{}, fmt.Errorf("packet too short: %d", len(data))
	}
	return Packet{
		MsgID: uint32(binary.BigEndian.Uint16(data[0:])),
		Seq:   binary.BigEndian.Uint32(data[2:]),
		Body:  data[HeaderSize:],
	}, nil
}

// ReadLength 从 2 字节大端读取包体长度。
//
// 配套用法：先从连接头部读满 LengthFieldSize 字节，调用本函数得到 length，
// 再继续读 length 字节作为 DecodePacket 的入参。length 不含 Length 字段自身。
// 错误语义：len(b) < LengthFieldSize 时返回 error（缓冲区还没攒够包头）。
//
// Go 细节：int(...) 是无符号到有符号的显式转换，length 上限 65535，
// 在 32/64 位 int 上都不会溢出；配合 io.ReadFull 使用可正确处理 TCP 半包
// （一次 Read 只拿到部分包头/包体是常态）。
func ReadLength(b []byte) (int, error) {
	if len(b) < LengthFieldSize {
		return 0, fmt.Errorf("length buffer too short")
	}
	return int(binary.BigEndian.Uint16(b)), nil
}
