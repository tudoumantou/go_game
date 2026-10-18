// tools/keygen —— 生成 RSA 密钥对（设计文档 19.5 部署清单第 2 步）
//
// 产出两对密钥（PEM）：
//   - gateway_rsa.pem / gateway_rsa_pub.pem：网关握手密钥。
//     私钥进网关配置（crypto.rsa_private_key），公钥由登录服随 Token 下发；
//   - login_rsa.pem / login_rsa_pub.pem：Token 签发密钥。
//     私钥进登录服配置（token.rsa_private_key），公钥给网关验签。
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

func main() {
	out := flag.String("out", "deploy/keys", "输出目录")
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
