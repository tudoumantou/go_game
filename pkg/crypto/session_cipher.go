// pkg/crypto/session_cipher.go —— 会话加密 + Seq 计数 + 防重放窗口
// 外层包格式：Length(2B) | MsgID(2B) | Seq(4B) | AES-GCM Body
// Seq 单调递增，接收端用 64 位滑动窗口检测重放。
package crypto

import (
	"encoding/binary"
	"fmt"
	"sync"
)

// SessionCipher 维护一条连接的加解密与 Seq 状态。
//
// 生命周期：一条连接一个实例（握手完成后由握手状态机的 SessionKey
// 构造），连接断开即丢弃。
// 并发约定：可被多 goroutine 使用（内部 mu 保护 Seq 状态）；但锁只保证
// Seq 分配原子、防重放状态一致，**不保证多写者之间的业务顺序**——
// 同一连接的多写者应自行排序（如actor 模型天然串行）。
// 不变量：
//   - sendSeq 从 1 开始严格递增、不复用（0 被保留为「非法值」，见 checkSeq）；
//   - recvSeq 单调不回退，window 只随高水位前进；
//   - gcm 实例不可更换。
type SessionCipher struct {
	gcm     *AESGCM
	sendSeq uint32
	recvSeq uint32
	window  uint64 // 64-bit 滑动窗口，低位对应最新 seq-63 到最新 seq
	mu      sync.Mutex
}

// NewSessionCipher 从 32 字节会话密钥创建 SessionCipher。
//
// sendSeq 初值 1：0 不用作合法序号（checkSeq 显式拒绝 seq=0），
// 保留 0 作为「未初始化/非法」的判据，与网关旧版行为兼容。
// 错误语义：密钥长度不为 32 时返回 error（来自 NewAESGCM）。
func NewSessionCipher(key []byte) (*SessionCipher, error) {
	gcm, err := NewAESGCM(key)
	if err != nil {
		return nil, err
	}
	return &SessionCipher{gcm: gcm, sendSeq: 1}, nil
}

// Encrypt 加密明文并附带发送 Seq。
// plaintext 不含外层 MsgID/Seq，调用方应传入 proto marshal 后的 body。
//
// 返回 (密文, 本次分配的 seq, error)。内部布局：先把 msgID(2B)|seq(4B)|
// 明文拼成 body，再整体 GCM 加密——MsgID/Seq 在密文保护范围内，
// 篡改任一字段都会导致认证失败（比放明文 AAD 更强，且不需要调用方
// 在解密端重新提供这些字段）。
// 拿到的密文直接作为外层 Packet.Body 上帧（外层还会再包一层
// Length|MsgID|Seq，两层的 MsgID/Seq 相同，接收端以外层为准路由、
// 以内层为准校验，见设计文档 4.2/15）。
// Seq 分配与加密在同一把锁内完成顺序取出，但加密本身在锁外——
// 允许并发加密不同包，Seq 仍严格不重复。
func (s *SessionCipher) Encrypt(msgID uint16, plaintext []byte) ([]byte, uint32, error) {
	s.mu.Lock()
	seq := s.sendSeq
	s.sendSeq++
	s.mu.Unlock()

	// body = msgID(2B) | seq(4B) | plaintext
	body := make([]byte, 6+len(plaintext))
	binary.BigEndian.PutUint16(body[0:], msgID)
	binary.BigEndian.PutUint32(body[2:], seq)
	copy(body[6:], plaintext)

	ct, err := s.gcm.Encrypt(body)
	if err != nil {
		return nil, 0, err
	}
	return ct, seq, nil
}

// Decrypt 解密并校验 Seq，返回 msgID / seq / 明文 body。
//
// 顺序固定：先 GCM 解密（失败即返回，不进入 Seq 检查——伪造包在
// 认证层就该被拒，不该有资格更新防重放窗口），再切出 msgID/seq，
// 最后 checkSeq 防重放。
// 返回值：msgID 供路由、seq 供对账/调试、plaintext 是内层明文
// （即调用方 Encrypt 时传入的 proto body，**不含** msgID/seq 头）。
// 错误语义：解密失败（密钥错/被篡改）或 Seq 非法（0/过老/重放）
// 均返回 error；网关侧应累计失败次数并按设计文档 15.2 断连。
func (s *SessionCipher) Decrypt(ciphertext []byte) (msgID uint16, seq uint32, plaintext []byte, err error) {
	body, err := s.gcm.Decrypt(ciphertext)
	if err != nil {
		return 0, 0, nil, fmt.Errorf("decrypt: %w", err)
	}
	if len(body) < 6 {
		return 0, 0, nil, fmt.Errorf("body too short")
	}
	msgID = binary.BigEndian.Uint16(body[0:])
	seq = binary.BigEndian.Uint32(body[2:])
	plaintext = body[6:]

	if err := s.checkSeq(seq); err != nil {
		return 0, 0, nil, err
	}
	return msgID, seq, plaintext, nil
}

// checkSeq 64 位滑动窗口防重放检测。
//
// 规则：
//   - seq == 0：非法（0 不用作合法序号）；
//   - seq > recvSeq：窗口左移 diff 位后置最低位（前进高水位）；
//     diff >= 64 时整窗清零——中间缺口超过窗口容量，全部视为过期；
//   - seq <= recvSeq：查 window 对应 bit，命中即重放拒绝，未命中则
//     置位接受（容忍 63 个序号的乱序，覆盖网关内多路径投递的乱序场景）。
//
// 取舍：窗口 64 位是「乱序宽容度」与「每连接内存/计算开销」的折中
// （一个 uint64，加锁一次完成全部判断）。设计文档 15.2 写的窗口为
// 1024，实现取 64——以本文件为准，文档差异已记录待同步。
// 调用方持锁进入，窗口与 recvSeq 的更新原子完成。
func (s *SessionCipher) checkSeq(seq uint32) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if seq == 0 {
		return fmt.Errorf("seq must be > 0")
	}

	if seq > s.recvSeq {
		// 前进窗口
		diff := seq - s.recvSeq
		if diff >= 64 {
			s.window = 0
		} else {
			s.window <<= diff
		}
		s.window |= 1
		s.recvSeq = seq
		return nil
	}

	// seq <= recvSeq，落在窗口内才接受
	diff := s.recvSeq - seq
	if diff >= 64 {
		return fmt.Errorf("seq %d too old (window base %d)", seq, s.recvSeq-63)
	}
	bit := uint64(1) << diff
	if s.window&bit != 0 {
		return fmt.Errorf("seq %d replay detected", seq)
	}
	s.window |= bit
	return nil
}
