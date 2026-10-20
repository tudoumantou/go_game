// pkg/crypto/doc.go —— 包级文档

// Package crypto 安全：RSA-OAEP 握手、AES-256-GCM 会话加密、Seq 防重放。
// 实现在 rsa.go / handshake.go / aes.go / session_cipher.go（设计文档第 15 章）。
//
// 架构位置：网关进程的安全层——TCP 建连后先握手（rsa/handshake）交换
// 会话密钥，之后所有报文走 session_cipher 的帧加密；登录服的 Token
// 签发使用独立的 RSA 密钥对（与握手密钥分离，见 config.CryptoConfig）。
// 阶段划分：
//   - rsa.go：非对称原语与 PEM 解析（握手期专用，一次性）；
//   - handshake.go：ServerHello/ClientKeyExchange/Finished 状态机；
//   - aes.go：会话期 AEAD 原语（每连接一个密钥，长生命周期）；
//   - session_cipher.go：帧级 Seq 分配与 64 位滑动窗口防重放。
//
// 关键取舍：会话密钥经 HKDF-SHA256(pre-master, salt) 派生而非直接使用
// pre-master——域分隔 + 拉伸，未来扩展双方向密钥不改协议骨架。
package crypto
