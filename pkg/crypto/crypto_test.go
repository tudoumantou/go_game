// pkg/crypto/crypto_test.go —— crypto 包核心逻辑单测
package crypto

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"github.com/stretchr/testify/require"
	"testing"
)

// generateTestKeyPair 生成测试用 RSA-2048 密钥对。
//
// 与生产同规格（2048 位）——用更小位数会改变 OAEP 最大明文长度等
// 边界行为，测不出真实问题。注意此处不走 LoadRSAKeyPair，
// 因此没有 Precompute 副作用；涉及并发的用例见
// handshake_concurrency_test.go（那里显式 Precompute）。
func generateTestKeyPair(t *testing.T) *RSAKeyPair {
	t.Helper()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err, "generate key")
	return &RSAKeyPair{Public: &priv.PublicKey, Private: priv}
}

// TestRSAOAEPRoundTrip 覆盖 RSA-OAEP-SHA256 加解密往返。
//
// 构造说明：明文取 32 字节——与握手中 pre-master 的真实长度一致，
// 确保加密容量（2048 位约 190 字节）覆盖真实使用场景；
// 2048 位密钥与生产同规格。
// 断言：解密结果与明文逐字节相等（OAEP 填充正确剥离）。
func TestRSAOAEPRoundTrip(t *testing.T) {
	pair := generateTestKeyPair(t)

	plain := []byte("pre-master-secret-32bytes-long!!")
	ct, err := RSAEncrypt(pair.Public, plain)
	require.NoError(t, err, "encrypt")
	got, err := RSADecrypt(pair.Private, ct)
	require.NoError(t, err, "decrypt")
	if !bytes.Equal(got, plain) {
		t.Fatalf("roundtrip mismatch")
	}
}

// TestAESGCMRoundTrip 覆盖 AES-256-GCM 加解密往返。
//
// 构造说明：密钥用确定性模式 key[i]=byte(i) 而非随机——测试可复现，
// 失败时能稳定重跑；安全性与随机性由 crypto/rand 与标准库保证，
// 不属于本包测试职责。断言：解密后与明文逐字节相等。
func TestAESGCMRoundTrip(t *testing.T) {
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}
	gcm, err := NewAESGCM(key)
	require.NoError(t, err, "new aes")
	plain := []byte("hello game study")
	ct, err := gcm.Encrypt(plain)
	require.NoError(t, err, "encrypt")
	got, err := gcm.Decrypt(ct)
	require.NoError(t, err, "decrypt")
	if !bytes.Equal(got, plain) {
		t.Fatalf("roundtrip mismatch")
	}
}

// TestHandshakeRoundTrip 覆盖完整握手四步流程：
// ServerHello → OnServerHello → ClientKeyExchange → OnClientKeyExchange →
// Finished → VerifyFinished。
//
// 关键断言有两个，缺一不可：
//  1. server.IsFinished() —— 状态机走完；
//  2. 双方 SessionKey 逐字节相同 —— HKDF 派生一致性，这是本测试的核心：
//     派生若不一致，症状要等到 Finished 的 GCM 校验才出现，且错误信息
//     只表现为「解密失败」，无法直接指向密钥派生，所以必须在这里显式断言。
func TestHandshakeRoundTrip(t *testing.T) {
	pair := generateTestKeyPair(t)

	server := NewHandshakeServer(pair.Public, pair.Private)
	client, err := NewHandshakeClient()
	require.NoError(t, err, "new client")

	pub, salt := server.ServerHello()
	if err := client.OnServerHello(pub, salt); err != nil {
		t.Fatalf("client server hello: %v", err)
	}

	cek, err := client.ClientKeyExchange()
	require.NoError(t, err, "client key exchange")
	if err := server.OnClientKeyExchange(cek); err != nil {
		t.Fatalf("server key exchange: %v", err)
	}

	finished, err := client.Finished()
	require.NoError(t, err, "client finished")
	if err := server.VerifyFinished(finished); err != nil {
		t.Fatalf("server verify finished: %v", err)
	}

	if !server.IsFinished() {
		t.Fatalf("server not finished")
	}
	if !bytes.Equal(server.SessionKey(), client.SessionKey()) {
		t.Fatalf("session key mismatch")
	}
}

// TestSessionCipherRoundTripAndReplay 覆盖 SessionCipher 的三类行为：
//
//  1. 基础往返：msgID/seq/明文解密后一致；首个 seq == 1（0 保留语义）；
//  2. 防重放：同一条密文第二次 Decrypt 必须被拒（窗口 bit 命中）；
//  3. Seq 单调：后续 3 次加密 seq 严格 +1（2、3、4）。
//
// 构造说明：密文里的 seq 有 GCM tag 保护，无法直接改写注入「seq=0」
// 或「乱序包」——那类场景需要能构造明文的专用向量，这里不做；
// bad 副本是为此预留的（保持原样，未使用）。
func TestSessionCipherRoundTripAndReplay(t *testing.T) {
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}
	s, err := NewSessionCipher(key)
	require.NoError(t, err, "new session cipher")

	plain := []byte("payload")
	ct1, seq1, err := s.Encrypt(1001, plain)
	require.NoError(t, err, "encrypt")
	if seq1 != 1 {
		t.Fatalf("first seq should be 1, got %d", seq1)
	}

	gotID, gotSeq, gotPlain, err := s.Decrypt(ct1)
	require.NoError(t, err, "decrypt")
	if gotID != 1001 || gotSeq != 1 || !bytes.Equal(gotPlain, plain) {
		t.Fatalf("decrypt mismatch")
	}

	// 重放同一条应被拒绝
	if _, _, _, err := s.Decrypt(ct1); err == nil {
		t.Fatalf("replay should be rejected")
	}

	// seq 0 不合法
	bad := make([]byte, len(ct1))
	copy(bad, ct1)
	// 这里无法直接改密文里的 seq，但可测 seq 单调递增
	for i := 0; i < 3; i++ {
		_, seq, err := s.Encrypt(uint16(1001+i), plain)
		if err != nil {
			t.Fatalf("encrypt %d: %v", i, err)
		}
		if seq != uint32(i+2) {
			t.Fatalf("seq should be %d, got %d", i+2, seq)
		}
	}
}
