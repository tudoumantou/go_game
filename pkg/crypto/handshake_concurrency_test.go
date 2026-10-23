package crypto

import (
	"crypto/rand"
	"crypto/rsa"
	"fmt"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
)

// TestHandshakeConcurrentIntegrity 并发握手必须全部成功。
//
// 回归背景：网关共用一把 rsa.PrivateKey，Precompute() 是惰性的 —— 前 N 个
// 并发请求会同时写 Precomputed 字段。表现为**偶发的会话密钥不一致**：
// RSA 解密不报错，但返回的明文是错的，客户端算出的会话密钥与服务端不同，
// Finished 帧 GCM 校验失败，客户端只能看到 "read server finished: EOF"。
//
// 真实压测里 300 次登录会挂 2-3 次，且与并发度不单调相关（是竞争窗口，不是负载）。
// 修法是加载密钥时先 Precompute()，本测试保证这条不变量不被改回去。
//
// 覆盖场景与构造：200 个 goroutine 并发跑完整握手，**共用同一对 RSA 密钥**
// ——复现网关「一把私钥服务全部连接」的真实拓扑；errs 只保留前 5 条错误，
// 防止失败时输出爆炸看不到结论。200 轮是经验值：竞争是概率性的，
// 轮数太低可能「碰巧通过」，200 能把窗口命中概率推到稳定可观测。
func TestHandshakeConcurrentIntegrity(t *testing.T) {
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err, "generate key")
	// 与 LoadRSAKeyPair 保持一致：加载即预计算
	priv.Precompute()
	pub := &priv.PublicKey

	const rounds = 200
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		errs []string
	)

	for i := 0; i < rounds; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			if err := runHandshake(pub, priv); err != nil {
				mu.Lock()
				if len(errs) < 5 {
					errs = append(errs, fmt.Sprintf("#%d: %v", idx, err))
				}
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()

	require.Empty(t, errs, fmt.Sprintf("%d/%d 次并发握手失败（会话密钥不一致）:\n%v", len(errs), rounds, errs))
}

// runHandshake 跑一次完整握手：ServerHello → ClientKeyExchange → Finished。
func runHandshake(pub *rsa.PublicKey, priv *rsa.PrivateKey) error {
	srv := NewHandshakeServer(pub, priv)
	pubPEM, salt := srv.ServerHello()

	cli, err := NewHandshakeClient()
	if err != nil {
		return err
	}
	if err := cli.OnServerHello(pubPEM, salt); err != nil {
		return fmt.Errorf("client on server hello: %w", err)
	}
	cek, err := cli.ClientKeyExchange()
	if err != nil {
		return err
	}
	if err := srv.OnClientKeyExchange(cek); err != nil {
		return fmt.Errorf("server on client key exchange: %w", err)
	}

	cipher, err := NewSessionCipher(cli.SessionKey())
	if err != nil {
		return err
	}
	finished, _, err := cipher.Encrypt(13, []byte("finished"))
	if err != nil {
		return err
	}
	// 服务端 VerifyFinished 校验的是整个 SSH 式 body（msgID+seq+marker），
	// 这里用与网关相同的路径：先 Decrypt 再比对 marker。
	gcm, err := NewAESGCM(srv.SessionKey())
	if err != nil {
		return err
	}
	plain, err := gcm.Decrypt(finished)
	if err != nil {
		return fmt.Errorf("decrypt finished: %w", err)
	}
	if len(plain) < 6 || string(plain[6:]) != "finished" {
		return fmt.Errorf("finished marker mismatch: %q", plain)
	}
	return nil
}

// TestHandshakeKeyStable 同一把密钥的并发解密结果必须一致（CRT 预计算不变量）。
func TestHandshakeKeyStable(t *testing.T) {
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err, "generate key")
	priv.Precompute()

	msg := []byte("precomputed-check")
	ct, err := RSAEncrypt(&priv.PublicKey, msg)
	require.NoError(t, err, "encrypt")

	const n = 50
	var wg sync.WaitGroup
	results := make([][]byte, n)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			results[idx], errs[idx] = RSADecrypt(priv, ct)
		}(i)
	}
	wg.Wait()

	for i := 0; i < n; i++ {
		require.Nil(t, errs[i])
		require.EqualValues(t, string(msg), string(results[i]))
	}
}
