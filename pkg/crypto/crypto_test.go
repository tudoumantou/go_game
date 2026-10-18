// pkg/crypto/crypto_test.go —— crypto 包核心逻辑单测
package crypto

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"testing"
)

func generateTestKeyPair(t *testing.T) *RSAKeyPair {
	t.Helper()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	return &RSAKeyPair{Public: &priv.PublicKey, Private: priv}
}

func TestRSAOAEPRoundTrip(t *testing.T) {
	pair := generateTestKeyPair(t)

	plain := []byte("pre-master-secret-32bytes-long!!")
	ct, err := RSAEncrypt(pair.Public, plain)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	got, err := RSADecrypt(pair.Private, ct)
	if err != nil {
		t.Fatalf("decrypt: %v", err)
	}
	if !bytes.Equal(got, plain) {
		t.Fatalf("roundtrip mismatch")
	}
}

func TestAESGCMRoundTrip(t *testing.T) {
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}
	gcm, err := NewAESGCM(key)
	if err != nil {
		t.Fatalf("new aes: %v", err)
	}
	plain := []byte("hello game study")
	ct, err := gcm.Encrypt(plain)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	got, err := gcm.Decrypt(ct)
	if err != nil {
		t.Fatalf("decrypt: %v", err)
	}
	if !bytes.Equal(got, plain) {
		t.Fatalf("roundtrip mismatch")
	}
}

func TestHandshakeRoundTrip(t *testing.T) {
	pair := generateTestKeyPair(t)

	server := NewHandshakeServer(pair.Public, pair.Private)
	client, err := NewHandshakeClient()
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	pub, salt := server.ServerHello()
	if err := client.OnServerHello(pub, salt); err != nil {
		t.Fatalf("client server hello: %v", err)
	}

	cek, err := client.ClientKeyExchange()
	if err != nil {
		t.Fatalf("client key exchange: %v", err)
	}
	if err := server.OnClientKeyExchange(cek); err != nil {
		t.Fatalf("server key exchange: %v", err)
	}

	finished, err := client.Finished()
	if err != nil {
		t.Fatalf("client finished: %v", err)
	}
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

func TestSessionCipherRoundTripAndReplay(t *testing.T) {
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}
	s, err := NewSessionCipher(key)
	if err != nil {
		t.Fatalf("new session cipher: %v", err)
	}

	plain := []byte("payload")
	ct1, seq1, err := s.Encrypt(1001, plain)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	if seq1 != 1 {
		t.Fatalf("first seq should be 1, got %d", seq1)
	}

	gotID, gotSeq, gotPlain, err := s.Decrypt(ct1)
	if err != nil {
		t.Fatalf("decrypt: %v", err)
	}
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
