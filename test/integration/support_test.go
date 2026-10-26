//go:build integration

// test/integration/support_test.go —— 集成测试的共用前置条件
//
// 这里只放「每个系统级测试都要做、漏做就起不来」的准备动作。目前只有一件：
// 切到项目根目录，并保证 keys/ 下的 RSA 密钥对存在。
//
// 为什么需要它：网关按**相对路径** `keys/gateway_rsa.pem` 加载握手密钥，而
// `keys/*.pem` 被 .gitignore 排除（私钥不入库，这是对的）。于是本地有 keys/
// 就能跑、CI 全新 checkout 没有 keys/ 就全挂——表现是一堆测试报
//
//	gateway start failed: load gateway rsa keys: read public key:
//	open keys/gateway_rsa_pub.pem: no such file or directory
//
// 这类「本地绿、CI 红」的缺口最难查，因为失败信息看起来像产品 bug。测试自己
// 生成密钥（每次随机、用完即弃）比「CI 里多跑一条命令」更可靠：测试不再依赖
// 外部准备步骤，谁在本地删了 keys/ 也照样能跑。
package integration

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// keyNames 网关与登录服各自需要的两对 PEM（私钥 + 公钥）。
//
// 四件都要：网关用 gateway_rsa 做握手，登录服签发 Token 用 login_rsa，
// 而网关验 Token 要 login_rsa_pub。缺任何一件都会在某个服务的启动阶段炸，
// 报错位置和真正缺失的文件往往不是同一个，所以一次性全检查。
var keyNames = []string{
	"gateway_rsa.pem", "gateway_rsa_pub.pem",
	"login_rsa.pem", "login_rsa_pub.pem",
}

// chdirProjectRoot 切到项目根，并确保测试密钥就位。
//
// 所有系统级测试（起 gateway/login/center 的）都该用它而不是裸 os.Chdir：
// 两件事必须一起做，分开做就一定会有人漏掉后半件。
func chdirProjectRoot(t *testing.T) {
	t.Helper()
	root := projectRoot(t)
	ensureTestKeys(t, filepath.Join(root, "keys"))
	require.NoError(t, os.Chdir(root), "chdir 到项目根")
}

// ensureTestKeys 缺失时现场生成 RSA 密钥对（已存在则不动）。
//
// 不覆盖已有文件：本地开发者可能用 `make keygen` 生成的密钥做了别的联调，
// 每次跑测试换一套会让那些会话失效。
func ensureTestKeys(t *testing.T, dir string) {
	t.Helper()
	missing := false
	for _, name := range keyNames {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			missing = true
			break
		}
	}
	if !missing {
		return
	}
	require.NoError(t, os.MkdirAll(dir, 0o755), "创建 keys 目录")

	// 2048 位是当前安全基线，也是生产配置的默认值；测试用更短的会让
	// 「密钥长度不足」这类校验在测试里失去意义。
	for _, base := range []string{"gateway_rsa", "login_rsa"} {
		writeTestKeyPair(t, dir, base)
	}
	t.Logf("已生成测试密钥对到 %s（keys/*.pem 不入库，CI 每次现场生成）", dir)
}

// writeTestKeyPair 生成一对密钥并写盘。
//
// 编码与 tools/keygen 一致：私钥 PKCS#8（BEGIN PRIVATE KEY）、公钥 PKIX
// （BEGIN PUBLIC KEY）。必须一致——pkg/crypto 的解析器只认这几种，测试用
// 别的格式会得到一个「看起来像密钥加载失败」的假故障。
func writeTestKeyPair(t *testing.T, dir, base string) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err, "生成 %s 密钥", base)

	privDER, err := x509.MarshalPKCS8PrivateKey(key)
	require.NoError(t, err, "编码私钥")
	privPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privDER})
	require.NoError(t, os.WriteFile(filepath.Join(dir, base+".pem"), privPEM, 0o600),
		"写私钥（0600：私钥不该被同机其他用户读到）")

	pubDER, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	require.NoError(t, err, "编码公钥")
	pubPEM := pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pubDER})
	require.NoError(t, os.WriteFile(filepath.Join(dir, base+"_pub.pem"), pubPEM, 0o644),
		"写公钥")
}
