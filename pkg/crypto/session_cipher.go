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
type SessionCipher struct {
	gcm     *AESGCM
	sendSeq uint32
	recvSeq uint32
	window  uint64 // 64-bit 滑动窗口，低位对应最新 seq-63 到最新 seq
	mu      sync.Mutex
}

// NewSessionCipher 从 32 字节会话密钥创建 SessionCipher。
func NewSessionCipher(key []byte) (*SessionCipher, error) {
	gcm, err := NewAESGCM(key)
	if err != nil {
		return nil, err
	}
	return &SessionCipher{gcm: gcm, sendSeq: 1}, nil
}

// Encrypt 加密明文并附带发送 Seq。
// plaintext 不含外层 MsgID/Seq，调用方应传入 proto marshal 后的 body。
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
