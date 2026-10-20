// tools/keygen —— 生成 RSA 密钥对（设计文档 19.5 部署清单第 2 步）
//
// 工程链位置：一次性部署工具，对应 Makefile target `make keygen`
// （= `go run ./tools/keygen -out deploy/keys`）。注意它不在 `make gen` 链上 ——
// 生成链每次运行必须幂等且产物可从源码重建，而密钥只能生成一次。
//
// 产出两对密钥（PEM）：
//   - gateway_rsa.pem / gateway_rsa_pub.pem：网关握手密钥。
//     私钥进网关配置（crypto.rsa_private_key），公钥由登录服随 Token 下发；
//   - login_rsa.pem / login_rsa_pub.pem：Token 签发密钥。
//     私钥进登录服配置（token.rsa_private_key），公钥给网关验签。
//
// 输入：无（密码学随机源即输入）。输出：-out 目录下 4 个 PEM 文件。
// 密钥严禁入库（.gitignore 已排除 deploy/keys/*.pem）。
package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
)

// main 入口：生成网关握手与登录 Token 两对 RSA 密钥，写入 -out 目录。
//
// 安全约束：私钥严禁入库（.gitignore 已排除 deploy/keys/*.pem）；重新生成会使
// 线上已签发 Token 全部失效，只能在部署阶段执行一次。
//
// Go 知识点：log.Fatalf = 打印到 stderr + os.Exit(1)。os.Exit 不走 defer，
// 本函数没有需要清理的资源，直接退出即可；「写一对失败就整体退出」由
// writePair 的 Fatal 策略保证，不会留下半套文件继续往下跑。
func main() {
	out := flag.String("out", "deploy/keys", "输出目录")
	// bits 默认 2048：低于 2048 不满足当前安全基线；参数暴露出来只为兼容
	// 旧环境联调，不提供下调指引
	bits := flag.Int("bits", 2048, "RSA 密钥位数")
	flag.Parse()

	if err := os.MkdirAll(*out, 0o755); err != nil {
		log.Fatalf("mkdir %s: %v", *out, err)
	}

	// 网关握手密钥对
	gwPriv, gwPub, err := genPair(*bits)
	if err != nil {
		log.Fatalf("generate gateway keypair: %v", err)
	}
	writePair(*out, "gateway_rsa", gwPriv, gwPub)

	// 登录服 Token 签发密钥对
	loginPriv, loginPub, err := genPair(*bits)
	if err != nil {
		log.Fatalf("generate login keypair: %v", err)
	}
	writePair(*out, "login_rsa", loginPriv, loginPub)

	fmt.Printf("密钥对已生成到 %s：\n", *out)
	fmt.Println("  gateway_rsa.pem     私钥 → 网关配置 crypto.rsa_private_key")
	fmt.Println("  gateway_rsa_pub.pem 公钥 → 登录服（随 Token 下发）")
	fmt.Println("  login_rsa.pem       私钥 → 登录服配置 token.rsa_private_key")
	fmt.Println("  login_rsa_pub.pem   公钥 → 网关（验签 Token）")
}

// genPair 生成一对 RSA 密钥，返回 (私钥 PEM, 公钥 PEM, error)。
//
// 编码约定：私钥用 PKCS#8（BEGIN PRIVATE KEY）、公钥用 PKIX（BEGIN PUBLIC KEY），
// 与网关/登录服配置读取的 PEM 类型一致；bits 由调用方给（默认 2048）。
//
// Go 知识点：
//   - crypto/rand.Reader 是密码学安全随机源（操作系统 CSPRNG），
//     rsa.GenerateKey 的质数生成完全依赖它 —— 换成 math/rand 会让密钥可预测；
//   - PEM = Base64(DER) + 头尾行，pem.EncodeToMemory 直接产出可写盘字节，
//     中间形态是 x509 的 DER 编码（PKCS#8/PKIX 规定了 DER 的结构）；
//   - 2048 位密钥生成是 CPU 密集操作（百毫秒级），本工具串行生成两对，可接受。
func genPair(bits int) (privPEM, pubPEM []byte, err error) {
	key, err := rsa.GenerateKey(rand.Reader, bits)
	if err != nil {
		return nil, nil, err
	}
	privDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, nil, err
	}
	privPEM = pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privDER})

	pubDER, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		return nil, nil, err
	}
	pubPEM = pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pubDER})
	return privPEM, pubPEM, nil
}

// writePair 把一对密钥落盘：私钥 0600（仅属主可读）、公钥 0644。
//
// 写失败直接 log.Fatal：半套密钥比没有密钥更危险 —— 对端会拿着失效公钥验签，
// 故障表现是「握手随机失败」而非启动即报错，极难排查。
//
// Go/Unix 知识点：os.WriteFile 的权限位受 umask 影响（实际 = 0600 & ~umask），
// 且只在「创建文件」时生效 —— 覆盖已存在的私钥文件时权限不会变，若旧文件是
// 0644，重新生成后依然是 0644。部署脚本首次创建后应 chmod 600 兜底；
// 0600 的意义：同机其他用户/服务进程读不到私钥，缩小密钥泄露面。
func writePair(dir, name string, privPEM, pubPEM []byte) {
	privPath := filepath.Join(dir, name+".pem")
	if err := os.WriteFile(privPath, privPEM, 0o600); err != nil {
		log.Fatalf("write %s: %v", privPath, err)
	}
	pubPath := filepath.Join(dir, name+"_pub.pem")
	if err := os.WriteFile(pubPath, pubPEM, 0o644); err != nil {
		log.Fatalf("write %s: %v", pubPath, err)
	}
}
