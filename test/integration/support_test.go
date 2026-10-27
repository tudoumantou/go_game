//go:build integration

// test/integration/support_test.go —— 集成测试的共用前置条件
//
// 这里只放「每个系统级测试都要做、漏做就起不来」的准备动作：
//   - 切到项目根目录 + 保证 keys/ 下的 RSA 密钥对存在；
//   - 给需要用独立数据库的用例分配一个全新库（isolatedDSN）。
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
	"strings"
	"testing"

	"context"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
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

// isolatedDSN 为单个用例准备一个全新数据库，返回该库的 DSN。
//
// 为什么每个用例要独立库：sql/schema 的建表语句不带 IF NOT EXISTS，
// 同一个库上跑第二次就报「table already exists」。过去每个用例各自起一个
// MySQL 容器来绕开这件事——代价是 10 个用例 10 次容器启动，CI 上把
// 15 分钟超时打满、docker daemon 也被拖垮（exec inspect 超时）。
//
// 现在改成「容器共享 + 库独立」：DROP+CREATE 一个按用例名命名的库，
// 既保证干净，也让有 COUNT(*) 断言的用例不会看到别人留下的数据。
//
// 库名由测试函数名派生（TestP12LoginPreload → t_p12loginpreload）：
// 固定可预测，失败后可以直接连上去看现场。
func isolatedDSN(t *testing.T) string {
	t.Helper()
	require.NotNil(t, testRootDB, "TestMain 未初始化 root 连接")

	name := isolatedDBName(t)
	cfg, err := mysqlDriver.ParseDSN(testDSN)
	require.NoError(t, err, "解析 testDSN")

	// 先删后建：重跑测试也拿到空库，不需要手工清理。
	//
	// GRANT 不能省：testcontainers 的默认用户只在容器初始化时被授予了
	// MYSQL_DATABASE 指定的那一个库，新建的库连不进去（1044 Access denied）。
	for _, stmt := range []string{
		"DROP DATABASE IF EXISTS `" + name + "`",
		"CREATE DATABASE `" + name + "`",
		"GRANT ALL PRIVILEGES ON `" + name + "`.* TO '" + cfg.User + "'@'%'",
	} {
		_, err := testRootDB.Exec(stmt)
		require.NoError(t, err, stmt)
	}

	cfg.DBName = name
	dsn := cfg.FormatDSN()

	initSchema(t, dsn)
	return dsn
}

// isolatedDBName 由测试函数名生成合法库名（只留小写字母数字，加 t_ 前缀）。
//
// 过滤而不是转义：库名会被拼进 DDL，而 t.Name() 可能带斜杠（子测试）。
// 只保留 [a-z0-9] 是最省事的安全做法，最长也不会超过 MySQL 的 64 字符限制。
func isolatedDBName(t *testing.T) string {
	var b strings.Builder
	for _, r := range strings.ToLower(t.Name()) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		}
	}
	return "t_" + b.String()
}

// freshEnv 为一个用例准备干净环境：独立数据库 + 清空共享 Redis 与 etcd。
//
// 三者必须一起做。只给独立库是不够的——Redis 里还有 IDTable 的发号游标和
// 玩家快照，etcd 里还有上一个用例残留的 /services 注册与 leader key。
// 残留的典型症状：新用例拿到一个 account，而库里已经有这个 account 的角色
// （发号游标被前一个用例推进过）；或者 center 一启动就 lost leadership
// （读到了别人留下的 leader key）。
//
// 过去每个用例起一套容器，天然没有这个问题；容器共享之后必须显式清理。
func freshEnv(t *testing.T) string {
	t.Helper()

	dsn := isolatedDSN(t)
	resetSharedRedis(t)
	resetSharedEtcd(t)
	return dsn
}

// resetSharedRedis 清空共享 Redis（串行执行，不会踩到别的用例）。
func resetSharedRedis(t *testing.T) {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: testRedisAddr})
	defer func() { _ = rdb.Close() }()
	require.NoError(t, rdb.FlushAll(context.Background()).Err(), "清空共享 Redis")
}

// resetSharedEtcd 清空共享 etcd 的全部 key。
//
// 用空前缀 + WithPrefix 删全部：用例串行，不存在误删并发用例数据的问题。
// 只删 /services 不够——leader 选举用的是另一个前缀，残留会让新的 center
// 以为自己不是 leader。
func resetSharedEtcd(t *testing.T) {
	t.Helper()
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{testEtcdAddr},
		DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err, "连接 etcd")
	defer func() { _ = cli.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err = cli.Delete(ctx, "", clientv3.WithPrefix())
	require.NoError(t, err, "清空共享 etcd")
}
