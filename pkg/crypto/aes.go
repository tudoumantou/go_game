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
type AESGCM struct {
	gcm cipher.AEAD
}

// NewAESGCM 从 32 字节密钥创建 AES-GCM。
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
func (a *AESGCM) Encrypt(plaintext []byte) ([]byte, error) {
	nonce := make([]byte, a.gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}
	return a.gcm.Seal(nonce, nonce, plaintext, nil), nil
}

// Decrypt 解密 nonce||ciphertext||tag，返回明文。
func (a *AESGCM) Decrypt(ciphertext []byte) ([]byte, error) {
	ns := a.gcm.NonceSize()
	if len(ciphertext) < ns {
		return nil, fmt.Errorf("ciphertext too short")
	}
	nonce, ct := ciphertext[:ns], ciphertext[ns:]
	return a.gcm.Open(nil, nonce, ct, nil)
}

// NonceSize 返回 nonce 长度。
func (a *AESGCM) NonceSize() int { return a.gcm.NonceSize() }

// Overhead 返回认证 tag 长度。
func (a *AESGCM) Overhead() int { return a.gcm.Overhead() }
