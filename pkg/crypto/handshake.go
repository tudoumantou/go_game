// pkg/crypto/handshake.go —— RSA/AES 握手协议状态机
// 协议流程（设计文档 4.4 + 4.9）：
//   1. ServerHello:    网关下发 RSA 公钥 + 随机盐
//   2. ClientKeyExchange: 客户端生成 pre-master，用 RSA 公钥加密后发回
//   3. 双方用 HKDF(pre-master, salt) 派生 AES-256 会话密钥
//   4. Finished:       客户端用会话密钥加密 finished 标记，网关验证后握手完成
package crypto

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/pem"
	"fmt"

	"golang.org/x/crypto/hkdf"
)

const (
	// PreMasterSize 预主密钥长度（256 bit）。
	PreMasterSize = 32
	// SaltSize 随机盐长度。
	SaltSize = 32
)

// HandshakeServer 服务端（网关）握手状态机。
type HandshakeServer struct {
	publicKey  *rsa.PublicKey
	privateKey *rsa.PrivateKey
	salt       []byte
	secret     []byte // pre-master，解密后得到
	derivedKey []byte
	finished   bool
}

// NewHandshakeServer 创建服务端握手状态机。
func NewHandshakeServer(publicKey *rsa.PublicKey, privateKey *rsa.PrivateKey) *HandshakeServer {
	salt := make([]byte, SaltSize)
	_, _ = rand.Read(salt) // 失败概率极低，忽略
	return &HandshakeServer{
		publicKey:  publicKey,
		privateKey: privateKey,
		salt:       salt,
	}
}

// ServerHello 返回网关 RSA 公钥 + 随机盐。
func (h *HandshakeServer) ServerHello() (pubKeyPEM []byte, salt []byte) {
	return PublicKeyToPEM(h.publicKey), h.salt
}

// OnClientKeyExchange 处理 ClientKeyExchange，解密 pre-master 并派生会话密钥。
func (h *HandshakeServer) OnClientKeyExchange(encryptedPreMaster []byte) error {
	if h.secret != nil {
		return fmt.Errorf("pre-master already received")
	}
	pm, err := RSADecrypt(h.privateKey, encryptedPreMaster)
	if err != nil {
		return fmt.Errorf("decrypt pre-master: %w", err)
	}
	if len(pm) != PreMasterSize {
		return fmt.Errorf("invalid pre-master size %d", len(pm))
	}
	h.secret = pm
	h.derivedKey = deriveKey(pm, h.salt)
	return nil
}

// VerifyFinished 验证客户端发来的 Finished 消息。
func (h *HandshakeServer) VerifyFinished(encryptedFinished []byte) error {
	if h.derivedKey == nil {
		return fmt.Errorf("session key not derived")
	}
	gcm, err := NewAESGCM(h.derivedKey)
	if err != nil {
		return err
	}
	plain, err := gcm.Decrypt(encryptedFinished)
	if err != nil {
		return fmt.Errorf("decrypt finished: %w", err)
	}
	if string(plain) != "finished" {
		return fmt.Errorf("invalid finished marker")
	}
	h.finished = true
	return nil
}

// SessionKey 返回派生后的 AES-256 会话密钥（握手完成后可用）。
func (h *HandshakeServer) SessionKey() []byte {
	return h.derivedKey
}

// IsFinished 返回握手是否完成。
func (h *HandshakeServer) IsFinished() bool { return h.finished }

// HandshakeClient 客户端（机器人/真实客户端）握手状态机。
type HandshakeClient struct {
	preMaster  []byte
	salt       []byte
	pubKey     *rsa.PublicKey
	derivedKey []byte
}

// NewHandshakeClient 创建客户端握手状态机。
func NewHandshakeClient() (*HandshakeClient, error) {
	pm := make([]byte, PreMasterSize)
	if _, err := rand.Read(pm); err != nil {
		return nil, err
	}
	return &HandshakeClient{preMaster: pm}, nil
}

// OnServerHello 收到 ServerHello 后保存公钥和盐。
func (c *HandshakeClient) OnServerHello(pubKeyPEM, salt []byte) error {
	if len(salt) != SaltSize {
		// 长度不对说明切分错了，继续下去只会得到「密钥不一致 → finished 认证失败」，
		// 早失败比晚失败好排查
		return fmt.Errorf("invalid salt size %d, want %d", len(salt), SaltSize)
	}
	pub, err := ParsePublicKey(pubKeyPEM)
	if err != nil {
		return err
	}
	c.pubKey = pub
	c.salt = salt
	c.derivedKey = deriveKey(c.preMaster, c.salt)
	return nil
}

// ClientKeyExchange 返回加密后的 pre-master。
func (c *HandshakeClient) ClientKeyExchange() ([]byte, error) {
	if c.pubKey == nil {
		return nil, fmt.Errorf("server hello not received")
	}
	return RSAEncrypt(c.pubKey, c.preMaster)
}

// Finished 返回加密后的 finished 标记。
func (c *HandshakeClient) Finished() ([]byte, error) {
	if c.derivedKey == nil {
		return nil, fmt.Errorf("session key not derived")
	}
	gcm, err := NewAESGCM(c.derivedKey)
	if err != nil {
		return nil, err
	}
	return gcm.Encrypt([]byte("finished"))
}

// SessionKey 返回派生后的 AES-256 会话密钥。
func (c *HandshakeClient) SessionKey() []byte {
	return c.derivedKey
}

// PublicKeyToPEM 把 RSA 公钥序列化为 PEM（PKIX 格式）。
func PublicKeyToPEM(pub *rsa.PublicKey) []byte {
	b, _ := x509.MarshalPKIXPublicKey(pub)
	return pem.EncodeToMemory(&pem.Block{Type: "RSA PUBLIC KEY", Bytes: b})
}

func deriveKey(secret, salt []byte) []byte {
	hk := hkdf.New(sha256.New, secret, salt, []byte("game-study-session-key"))
	key := make([]byte, 32)
	if _, err := hk.Read(key); err != nil {
		// hkdf.Read 在合法参数下不会失败
		panic(err)
	}
	return key
}
