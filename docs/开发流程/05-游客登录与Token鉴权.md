# 05 游客登录与Token鉴权

[上一章](04-Protobuf封包与加密握手.md) · [课程总览](00-总览与阅读顺序.md)

## 这一章做出什么

实现游客账号与随机 Token，保证网关身份来自服务器验证。

## 前置条件与本章文件

第 02 的 DB/Redis 依赖可用，第 04 理解握手。

- [internal/login/handler/auth.go](../../internal/login/handler/auth.go)：HTTP 协议。
- [internal/login/service/auth_service.go](../../internal/login/service/auth_service.go)：账号查建与 Token。
- [internal/login/repo/account_repo.go](../../internal/login/repo/account_repo.go)：账号 SQL。
- [internal/login/repo/token_repo.go](../../internal/login/repo/token_repo.go)：带 TTL 的 Token 映射。
- [internal/gateway/router/router.go](../../internal/gateway/router/router.go)：鉴权状态转移。

## 按顺序实现

先定义 HTTP 请求只需要 device_id。handler 校验方法与 JSON，service 查/建账号，Repo 负责 SQL。练习时先实现一个内存 TokenRepo，再换 Redis，理解接口不随存储变化。

Token 使用 crypto/rand 生成随机字节，hex 编码为可传输字符串。Redis 保存 token→account_id 与 TTL。这里没有用 JWT，也没有走 login_rsa 私钥签名；YAML 的字段名不能代替真实调用链。

Gateway 鉴权时取 Token 查 Redis，失败就拒绝，不接受客户端直接指定 account_id。验证通过再告诉 Game 账号是谁；角色选择由 Game 管理。

```bash
# 已启动的本地开发服务；此命令会创建/查询一个游客账号并返回凭据。
curl --fail -X POST http://127.0.0.1:8080/v1/auth/guest   -H 'Content-Type: application/json'   -d '{"device_id":"learning-device-001"}'
```

同一个 device_id 重试应指向同一账号；这不代表真实设备身份已经可信，恶意客户端仍可能伪造 device_id。正式账号体系需要另外设计认证方式，不把游客字段当作密码。

当前空 device_id 被替换成 anonymous；学习工程可以选择明确拒绝空值，但要标记这是你的接口策略。不要一面说源码是拒绝，一面复制了它的匿名回退。

## 核心源码与注释阅读

源码节选：[internal/login/service/auth_service.go](../../internal/login/service/auth_service.go)。这是现有包中的片段，依赖同文件类型，不是可单独运行的完整程序。

```go
func (s *AuthService) GuestLogin(ctx context.Context, deviceID string) (token string, accountID int64, err error) {
	if s.accountRepo != nil {
		accountID, err = s.accountRepo.FindOrCreateByDevice(ctx, deviceID)
		if err != nil {
			return "", 0, fmt.Errorf("find or create account: %w", err)
		}
		_ = s.accountRepo.TouchLogin(ctx, accountID)
	} else {
		// 降级：无 MySQL 环境（单测）用固定账号
		accountID = 1000001
	}

	token, err = generateToken()
	if err != nil {
		return "", 0, fmt.Errorf("generate token: %w", err)
	}
	if err := s.tokenRepo.Set(ctx, token, accountID, s.tokenTTL); err != nil {
		return "", 0, fmt.Errorf("save token: %w", err)
	}
	return token, accountID, nil
}
```

## 逐段解释与知识点

GuestLogin 先获取账号，再生成并持久化 Token，持久化失败不应向客户端报告成功。没有 MySQL 的固定账号回退是测试/开发路径，不能作为多用户生产实现。

account_id 是登录主体；HTTP 响应兼容字段 player_id 同值，不表示建角完成。网关某些历史 player 字段在鉴权阶段也可能存账号 ID，阅读时看赋值来源。

TTL 是存活时间，不等于抢登策略、封禁策略和撤销策略。Token 泄露后需要明确作废途径。日志不记录完整 Token，测试输出只在本地观察。

## 怎么验证与定位错误

同设备两次登录核对 account_id；Token 非空且能被 Gateway 验证；无效/过期 Token 被拒绝；Redis 写失败时 HTTP 不应返回伪成功。状态机测试还要覆盖没完成握手就发 Token 的非法顺序。

出现 no role 不是 Token 失败：先确认 account 绑定成功，再去 08 检查角色。

## 独立练习与通过标准

用 miniredis 或临时测试实例验证 TTL；构造两个设备，证明 Token 不能交换成任意角色操作。通过标准：能画出 handler→service→repo 和 Token→account 两条链。

下一章：[第 06 章 gRPC服务发现与网关转发](06-gRPC服务发现与网关转发.md)。
