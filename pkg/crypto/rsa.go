// pkg/crypto/rsa.go —— RSA 非对称加密（用于握手阶段交换 AES 会话密钥）
package crypto

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"
)

// RSAKeyPair 保存 RSA 公私钥。
type RSAKeyPair struct {
	Public  *rsa.PublicKey
	Private *rsa.PrivateKey
}

// LoadRSAKeyPair 从 PEM 文件加载公私钥。
// 约定：私钥 = <base>.pem，公钥 = <base>_pub.pem（与 keygen 工具输出一致）。
func LoadRSAKeyPair(publicPath, privatePath string) (*RSAKeyPair, error) {
	pubPEM, err := os.ReadFile(publicPath)
	if err != nil {
		return nil, fmt.Errorf("read public key: %w", err)
	}
	pub, err := ParsePublicKey(pubPEM)
	if err != nil {
		return nil, fmt.Errorf("parse public key: %w", err)
	}

	privPEM, err := os.ReadFile(privatePath)
	if err != nil {
		return nil, fmt.Errorf("read private key: %w", err)
	}
	priv, err := ParsePrivateKey(privPEM)
	if err != nil {
		return nil, fmt.Errorf("parse private key: %w", err)
	}

	// 提前完成 CRT 预计算：rsa.PrivateKey.Precompute() 是惰性的，若留到并发
	// 解密时首次触发，多个 goroutine 会同时写 Precomputed 字段（data race），
	// 结果是**偶发地返回错误明文而不是报错** —— 表现为握手阶段 GCM 校验失败。
	priv.Precompute()

	return &RSAKeyPair{Public: pub, Private: priv}, nil
}

// ParsePublicKey 解析 PEM 公钥。
func ParsePublicKey(pemData []byte) (*rsa.PublicKey, error) {
	block, _ := pem.Decode(pemData)
	if block == nil {
		return nil, fmt.Errorf("invalid public key PEM")
	}
	pub, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, err
	}
	rsaPub, ok := pub.(*rsa.PublicKey)
	if !ok {
		return nil, fmt.Errorf("not RSA public key")
	}
	return rsaPub, nil
}

// ParsePrivateKey 解析 PEM 私钥。
func ParsePrivateKey(pemData []byte) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode(pemData)
	if block == nil {
		return nil, fmt.Errorf("invalid private key PEM")
	}
	priv, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	if err != nil {
		// 尝试 PKCS8
		key, err2 := x509.ParsePKCS8PrivateKey(block.Bytes)
		if err2 != nil {
			return nil, fmt.Errorf("parse private key: %w", err)
		}
		var ok bool
		priv, ok = key.(*rsa.PrivateKey)
		if !ok {
			return nil, fmt.Errorf("not RSA private key")
		}
	}
	// 同上：避免并发首解密时的写竞争
	priv.Precompute()
	return priv, nil
}

// RSAEncrypt 使用 RSA-OAEP-SHA256 加密。
func RSAEncrypt(pub *rsa.PublicKey, plaintext []byte) ([]byte, error) {
	return rsa.EncryptOAEP(sha256.New(), rand.Reader, pub, plaintext, nil)
}

// RSADecrypt 使用 RSA-OAEP-SHA256 解密。
func RSADecrypt(priv *rsa.PrivateKey, ciphertext []byte) ([]byte, error) {
	return rsa.DecryptOAEP(sha256.New(), rand.Reader, priv, ciphertext, nil)
}
