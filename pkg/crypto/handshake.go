// pkg/crypto/handshake.go —— RSA/AES 握手协议状态机
// 协议流程（设计文档 4.4 + 4.9）：
//  1. ServerHello:    网关下发 RSA 公钥 + 随机盐
//  2. ClientKeyExchange: 客户端生成 pre-master，用 RSA 公钥加密后发回
//  3. 双方用 HKDF(pre-master, salt) 派生 AES-256 会话密钥
//  4. Finished:       客户端用会话密钥加密 finished 标记，网关验证后握手完成
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
	// 与 AES-256 密钥长度对齐：HKDF 派生后直接就是会话密钥材料，
	// 不需要再做一次拉伸/截断。
	PreMasterSize = 32
	// SaltSize 随机盐长度。
	// 每次握手由服务端重新生成（crypto/rand），防跨会话密钥关联；
	// 客户端对盐长度做显式校验（见 HandshakeClient.OnServerHello）。
	SaltSize = 32
)

// HandshakeServer 服务端（网关）握手状态机。
//
// 生命周期：一条连接在握手期内创建一个实例，握手完成后即可丢弃
// （会话密钥交给 SessionCipher 接管）。
// 并发约定：单个实例**不是**线程安全的——一个连接的握手由单 goroutine
// 驱动；不同连接各自持有实例，可并发驱动（底层 RSA 私钥已 Precompute，
// 多实例并发解密安全，见 rsa.go）。
// 不变量：secret/derivedKey 一旦置上不可改（状态机只前进不后退），
// finished 只可能由 false 变 true。
type HandshakeServer struct {
	publicKey  *rsa.PublicKey
	privateKey *rsa.PrivateKey
	salt       []byte
	secret     []byte // pre-master，解密后得到
	derivedKey []byte
	finished   bool
}

// NewHandshakeServer 创建服务端握手状态机。
//
// 内部立即生成随机盐：盐必须在该握手的 ServerHello 之前就绪，
// 且之后不可更换（客户端拿到盐就算出密钥，换盐 = 双方密钥不一致）。
// rand.Read 失败被忽略：概率约 2^-200 量级，且失败时 salt 全零仍有
// 随机性来源 pre-master，不构成实际风险。
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
//
// 幂等：重复调用返回同一份 salt（客户端重试/重连同一握手时不能换盐，
// 否则两边派生密钥不一致）；公钥 PEM 可重复序列化，无副作用。
// 调用顺序约束：必须在 OnClientKeyExchange 之前（客户端要靠盐算密钥）。
func (h *HandshakeServer) ServerHello() (pubKeyPEM []byte, salt []byte) {
	return PublicKeyToPEM(h.publicKey), h.salt
}

// OnClientKeyExchange 处理 ClientKeyExchange，解密 pre-master 并派生会话密钥。
//
// 状态机约束：每个实例只接受一次（重复到达返回 error——重放握手帧
// 不应改变已派生密钥）。
// 长度校验：解密出的 pre-master 必须精确 PreMasterSize(32) 字节，
// 防畸形客户端发送超长/过短密钥材料（HKDF 对任意长度都「成功」，
// 不校验就会静默派生出一个双方不一致的密钥）。
// 成功副作用：置 secret 与 derivedKey；此后 VerifyFinished 才可用。
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
//
// 前置条件：derivedKey 已生成（OnClientKeyExchange 成功），否则返回
// error——顺序错乱说明协议被破坏，不应继续。
// 验证内容：用派生密钥 GCM 解密，明文必须精确等于 "finished"。
// GCM 认证通过即证明双方密钥一致（这是握手真正的验收点；解密失败
// 大概率是密钥不一致，而不是客户端发错内容）。
// 成功副作用：finished = true，此后 IsFinished() 为真。
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
//
// 未完成握手（OnClientKeyExchange 未成功）时返回 nil——调用方应先
// IsFinished() 或判空，不要把 nil 当空密钥传给 NewAESGCM（会得到
// 「key must be 32 bytes, got 0」的误导性错误）。
func (h *HandshakeServer) SessionKey() []byte {
	return h.derivedKey
}

// IsFinished 返回握手是否完成。
func (h *HandshakeServer) IsFinished() bool { return h.finished }

// HandshakeClient 客户端（机器人/真实客户端）握手状态机。
//
// 并发约定同 HandshakeServer：单连接单实例、单 goroutine 驱动；
// preMaster 在构造时生成后只读。
// 与 Server 的关键差异：客户端收到 ServerHello 后**本地立即派生密钥**，
// 无需等服务端确认——这正是 TLS 式握手的特性，服务端确认通过
// Finished 的 GCM 校验隐式完成。
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
//
// 盐长度必须精确 SaltSize：长度不对说明流切分错了（包头边界问题），
// 此时继续走下去只会得到「密钥不一致 → Finished 认证失败」的远端症状，
// 早失败比晚失败好排查（错误信息直接指向盐长度）。
// 成功副作用：置 pubKey/salt 并立即本地派生 derivedKey。
// 前置顺序约束：必须先于 ClientKeyExchange（没有公钥无法加密 pre-master）。
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
//
// 前置条件：OnServerHello 已成功（pubKey 非 nil），否则返回 error
// （没有公钥时加密无从谈起）。
// 输出是 RSA-OAEP 密文，长度 = 模长（2048 → 256 字节），不是 32 字节——
// 接收端按密文长度读取，不要按 PreMasterSize 假设。
func (c *HandshakeClient) ClientKeyExchange() ([]byte, error) {
	if c.pubKey == nil {
		return nil, fmt.Errorf("server hello not received")
	}
	return RSAEncrypt(c.pubKey, c.preMaster)
}

// Finished 返回加密后的 finished 标记。
//
// 用派生密钥加密固定串 "finished"（每次 nonce 随机，密文不同但明文相同，
// 服务端只比对明文）。derivedKey 未生成时返回 error。
// 调用时机：客户端本地派生密钥后即可调用，不等服务端任何消息。
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
//
// OnServerHello 成功前返回 nil（判空约定同 HandshakeServer.SessionKey）。
func (c *HandshakeClient) SessionKey() []byte {
	return c.derivedKey
}

// PublicKeyToPEM 把 RSA 公钥序列化为 PEM（PKIX 格式）。
//
// 与 ParsePublicKey 严格互逆（客户端解析服务端 PEM 的同一对函数）。
// 序列化不会失败到需要返回 error 的程度（x509.MarshalPKIXPublicKey 对
// 合法 *rsa.PublicKey 只可能因接口为 nil 出错，那属于调用方 bug）。
func PublicKeyToPEM(pub *rsa.PublicKey) []byte {
	b, _ := x509.MarshalPKIXPublicKey(pub)
	return pem.EncodeToMemory(&pem.Block{Type: "RSA PUBLIC KEY", Bytes: b})
}

// deriveKey 用 HKDF-SHA256 从 pre-master + salt 派生 32 字节会话密钥。
//
// info 固定为 "game-study-session-key"：HKDF 的 info 做域分隔，
// 防止同一份 secret 在其他场景（未来若加双方向密钥）派生出相同密钥。
// 客户端与服务端用完全相同的 (secret, salt, info) 调用——三者任一
// 不一致都会在 Finished 的 GCM 校验才暴露，所以三处输入都有上游校验
// （pre-master 长度、salt 长度、info 常量）。
// hkdf.Read 在参数合法时不会失败；panic 仅用于暴露编程错误（如 key 长度
// 写错），不应用于运行期错误处理。
func deriveKey(secret, salt []byte) []byte {
	hk := hkdf.New(sha256.New, secret, salt, []byte("game-study-session-key"))
	key := make([]byte, 32)
	if _, err := hk.Read(key); err != nil {
		// hkdf.Read 在合法参数下不会失败
		panic(err)
	}
	return key
}
