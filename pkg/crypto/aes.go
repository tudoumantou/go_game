// pkg/crypto/aes.go —— AES-256-GCM 会话对称加密
package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"fmt"
	"io"
)

// AESGCM 封装 AES-256-GCM 加解密。
//
// 实例创建后即可被多个 goroutine 并发使用（cipher.AEAD 的 Seal/Open
// 本身线程安全）；但底层 gcm 一经构造不可更换——轮换密钥的正确的是
// 新建一个 AESGCM 实例，而不是改字段。
type AESGCM struct {
	gcm cipher.AEAD
}

// NewAESGCM 从 32 字节密钥创建 AES-GCM。
//
// 密钥必须是 32 字节（AES-256）；GCM 默认参数 nonce 12 字节、tag 16 字节，
// 可通过 NonceSize()/Overhead() 查询（上层估算包体大小时用）。
// 错误语义：密钥长度不符或底层构造失败返回 error，不会返回半成品实例。
func NewAESGCM(key []byte) (*AESGCM, error) {
	if len(key) != 32 {
		return nil, fmt.Errorf("AES-256 key must be 32 bytes, got %d", len(key))
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &AESGCM{gcm: gcm}, nil
}

// Encrypt 加密明文，内部生成随机 nonce，返回 nonce||ciphertext||tag。
// 输出长度 = nonceSize + len(plaintext) + tagSize。
//
// nonce 每次调用都从 crypto/rand 重新生成，**绝不复用**：GCM 的安全前提
// 是同一密钥下 nonce 不重复，复用会泄漏认证密钥（H 多项式可逆），
// 属于密码学层面的致命错误，不是「概率问题」。
// 返回值布局固定为 nonce 在前，解密端（Decrypt）按同一布局切分；
// 调用方不需要自己管理 nonce，也不要把两次加密的输出做任何拼接假设。
// 并发：可多 goroutine 并发调用（nonce 随机、无共享计数器）。
func (a *AESGCM) Encrypt(plaintext []byte) ([]byte, error) {
	nonce := make([]byte, a.gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}
	return a.gcm.Seal(nonce, nonce, plaintext, nil), nil
}

// Decrypt 解密 nonce||ciphertext||tag，返回明文。
//
// 输入布局必须与 Encrypt 输出一致（nonce 在前）。失败包括两种：长度不足
// （连 nonce 都放不下）与 GCM 认证失败（密文被篡改 / 密钥不匹配 /
// 用错了密钥解密）。认证失败应被视为「连接异常」而非普通业务错误——
// 网关按设计文档 15.2 的约定累计失败次数并断连，**不要在应用层重试
// 同一密文**（重试不会改变结果，只会放大爆破面）。
func (a *AESGCM) Decrypt(ciphertext []byte) ([]byte, error) {
	ns := a.gcm.NonceSize()
	if len(ciphertext) < ns {
		return nil, fmt.Errorf("ciphertext too short")
	}
	nonce, ct := ciphertext[:ns], ciphertext[ns:]
	return a.gcm.Open(nil, nonce, ct, nil)
}

// NonceSize 返回 nonce 长度。
// GCM 标准值 12；供上层（如 flush/metrics 估算字节预算）计算加密开销，
// 不要硬编码 12——换 AEAD 算法时只改这里。
func (a *AESGCM) NonceSize() int { return a.gcm.NonceSize() }

// Overhead 返回认证 tag 长度。
// GCM 标准值 16；加密后包体增量 = NonceSize()+Overhead()，
// 用于校验「明文 + 开销」是否超出外层 MaxPacketSize。
func (a *AESGCM) Overhead() int { return a.gcm.Overhead() }
