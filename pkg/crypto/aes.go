// pkg/crypto/aes.go —— AES-256-GCM 会话对称加密
//
// 架构职责：握手完成后所有业务报文体的加解密原语（每连接一个密钥）。
// 协作关系：密钥由 handshake.go 的 HKDF 派生；session_cipher.go 在本
// 原语之上叠加 Seq/防重放与帧布局；网关 packet 层用 NonceSize/Overhead
// 估算加密后的包体预算（MaxPacketSize 校验）。
// 关键取舍：选 GCM（AEAD）而非 CBC+HMAC——一次计算同时完成加密与
// 认证，Go 标准库对 GCM 有 AES-NI 硬件加速；nonce 用随机生成而非
// 计数器，免去跨连接/重启的计数器同步问题，代价是 96bit 随机数的
// 生日界（约 2^32 条/密钥后需轮换，本场景远够）。
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
// Go 知识点：cipher.AEAD 是接口，标准库实现无内部可变状态，因此
// 「只读共享 = 并发安全」，不需要额外加锁；反之后若自己包一层可变的
// nonce 计数器，就必须同步（参见 session_cipher.go 的 mu）。
type AESGCM struct {
	gcm cipher.AEAD
}

// NewAESGCM 从 32 字节密钥创建 AES-GCM。
//
// 密钥必须是 32 字节（AES-256）；GCM 默认参数 nonce 12 字节、tag 16 字节，
// 可通过 NonceSize()/Overhead() 查询（上层估算包体大小时用）。
// 错误语义：密钥长度不符或底层构造失败返回 error，不会返回半成品实例。
// Go 知识点：先校验再构造，失败即 (nil, err)——调用方拿到的非 nil
// 实例一定可用，不会出现「半个初始化对象」被当成正常返回值。
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
// 并发：可多 goroutine 并发调用（nonce 随机、无共享计数器）——若改用
// 递增计数器 nonce，这里就必须加锁或上原子操作。
// Go 知识点：Seal(dst=nonce, nonce, plaintext, nil) 把密文**追加**到
// dst 之后——把 nonce 同时当 dst 前缀，一次分配得到 nonce||ct||tag；
// 若先 Seal(nil,...) 再 append(nonce, ct...) 则多一次全量拷贝。
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
// Go 知识点：len < ns 的特判必须先做——否则 ciphertext[:ns] 切片越界
// 会直接 panic（运行时错误，不可 recover）；Open 的 dst 传 nil，返回
// 的明文是新分配的数组，与输入密文不共享底层数组，调用方改写明文
// 不会影响原密文。
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
