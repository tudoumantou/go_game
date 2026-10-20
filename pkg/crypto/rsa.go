// pkg/crypto/rsa.go —— RSA 非对称加密（用于握手阶段交换 AES 会话密钥）
//
// 架构职责：握手期的密钥运输工具——服务端持私钥，客户端用公钥加密
// pre-master；同时负责 PEM 密钥文件的加载与解析。
// 协作关系：handshake.go 的状态机调用 RSAEncrypt/RSADecrypt；keygen
// 工具产出的 PEM 文件由 LoadRSAKeyPair 读取；RSAKeyPair 在网关启动时
// 加载一次、全连接共享。
// 关键取舍：OAEP-SHA256 而非 PKCS#1 v1.5——v1.5 的填充可被
// Bleichenbacher 类填充预言攻击逐步破解，OAEP 可证明安全；加载即
// Precompute——把惰性 CRT 预计算挪到单线程启动期，避免并发首解密时
// 的 data race（详见 LoadRSAKeyPair 注释）。
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
//
// 部署约定：Public 会通过握手 ServerHello 下发给客户端，Private 只存在于
// 网关进程内存（PEM 文件权限 600，不进镜像 layer、不打日志）。
// 线程安全：Precompute 后两个字段均为只读，可被所有连接并发共享——
// 这也是网关用一把密钥服务全部连接的原因。
// Go 知识点：「初始化后只读」的对象并发共享是安全的——没有写就没有
// 竞争，race detector 也安静；一旦某天加了可写字段（如使用计数），
// 就必须重新评估同步方案。
type RSAKeyPair struct {
	Public  *rsa.PublicKey
	Private *rsa.PrivateKey
}

// LoadRSAKeyPair 从 PEM 文件加载公私钥。
// 约定：私钥 = <base>.pem，公钥 = <base>_pub.pem（与 keygen 工具输出一致）。
//
// 两个路径独立加载、独立报错：公钥配置错与私钥配置错要能区分
// （公钥还可能来自配置中心而非本地文件）。
// 关键副作用：加载即 Precompute()——rsa.PrivateKey 的 CRT 预计算是惰性的，
// 若留到并发解密时首次触发，多个 goroutine 会同时写 Precomputed 字段，
// 症状是「解密不报错但返回错明文」，最终表现为握手 GCM 校验失败（详见
// handshake_concurrency_test.go 的回归背景）。因此**任何拿到私钥的路径
// 都必须先 Precompute**，本函数是唯一入口。
// 错误语义：读取/解析任一文件失败即返回 error，不会返回半成品 KeyPair。
// Go 知识点：这个 race 是 Go data race 的典型「安静腐败」——不 panic、
// 不报错，只是结果偶发错误，且离开 -race 后极难复现；修复靠「把初始化
// 挪到并发开始之前」，而不是加锁。
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
//
// 只接受 PKIX/SPKI 格式（BEGIN RSA PUBLIC KEY 之外的通用 X.509 公钥格式），
// 与 PublicKeyToPEM 的输出互为逆运算；客户端拿到的 PEM 就是这几种之一。
// 错误语义：PEM 解码失败、非 X.509 公钥、非 RSA 公钥分别报不同错误，
// 便于区分「文件不对」与「算法不对」。
// Go 知识点：x509.ParsePKIXPublicKey 返回 any（算法无关），需用
// comma-ok 类型断言收窄到 *rsa.PublicKey——`pub.(*rsa.PublicKey)` 的
// ok 模式失败时返回零值而非 panic，正好转成错误返回。
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
//
// PKCS1（BEGIN RSA PRIVATE KEY）优先，失败再试 PKCS8（BEGIN PRIVATE KEY）：
// 兼容 openssl genrsa 的新旧默认输出与其他工具链产物。
// 同样在返回前 Precompute()——与 LoadRSAKeyPair 同一不变量：任何
// 并发使用私钥的路径，拿到的实例必须已预计算（见该函数注释）。
// 错误语义：两种格式都解析失败时报 PKCS1 的错误（更常见，信息更有用）。
// Go 知识点：内层错误用 err2 而非 := 遮蔽 err——外层 err 要在最终错误
// 信息里用 %w 呈现「主尝试（PKCS1）为什么失败」。
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
//
// 明文长度受模长限制：RSA-2048 下单次最多约 190 字节——只够放密钥材料
// （pre-master 32B），不放业务数据。OAEP 自带随机填充，同一明文两次
// 加密输出不同，不能把密文当去重键。
// 并发：只读公钥，可并发；rand.Reader 阻塞时会体现在 error 上。
// Go 知识点：长度上限 = 模长字节数 - 2*hashLen - 2（2048bit=256B，
// SHA256=32B → 256-66=190）——这是 OAEP 的结构性开销，超长明文直接
// error；握手约定 32B 远在下界内，不会触界。
func RSAEncrypt(pub *rsa.PublicKey, plaintext []byte) ([]byte, error) {
	return rsa.EncryptOAEP(sha256.New(), rand.Reader, pub, plaintext, nil)
}

// RSADecrypt 使用 RSA-OAEP-SHA256 解密。
//
// 并发安全的前提：传入的私钥已 Precompute（见 ParsePrivateKey）。
// 错误语义：密文损坏/长度不符/填充校验失败均返回 error；调用方
// （握手状态机）应把它当握手失败处理，不要重试同一密文。
func RSADecrypt(priv *rsa.PrivateKey, ciphertext []byte) ([]byte, error) {
	return rsa.DecryptOAEP(sha256.New(), rand.Reader, priv, ciphertext, nil)
}
