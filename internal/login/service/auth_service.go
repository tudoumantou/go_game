// internal/login/service/auth_service.go —— 认证服务
//
// 架构职责：登录服的核心业务——游客登录（按设备找/建账号 + 签发
// token）与 token 校验。上行被 handler.AuthHandler 调用，下行依赖
// AccountRepo（MySQL）与 TokenRepo（Redis）两个仓储。
// 协作关系：handler.GuestLogin → GuestLogin → Redis SET token；
// 网关握手期的 TokenValidator 与 handler.CreateOrder 的
// ValidateToken → Get；两端读的是同一个 Redis key（token:xxx），
// 「登录服签发、网关校验」的信任链由此建立。
// 设计取舍：① accountRepo 允许 nil——无 MySQL 环境（单测/最小部署）
// 降级为固定账号，token 链路照常可测；② token 用 32 字节
// crypto/rand 十六进制（64 字符）：256bit 熵，不可枚举不可猜测，
// 且天然 URL/Header 安全，无需转义；③ TTL 由配置注入——策略在
// 调用方，service 只负责执行；④ TouchLogin 失败被忽略：last_login_at
// 是审计字段，不该让一次登录因它失败。
package service

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	"game_study/internal/login/repo"
)

// AuthService 认证服务。
// 依赖两个仓储 + 一个策略参数：tokenRepo 管「凭证」，accountRepo
// 管「身份」，tokenTTL 管「凭证寿命」——三者都可注入替换。
type AuthService struct {
	tokenRepo   *repo.TokenRepo
	accountRepo *repo.AccountRepo
	tokenTTL    time.Duration
}

// NewAuthService 创建 AuthService（accountRepo 可为 nil：无 MySQL 时降级为固定账号）。
// 知识点：依赖经构造函数注入而非包级单例——多测试并行时各自持
// 独立实例，无共享状态；nil 依赖是「可选协作者」的显式表达，
// 每个使用点必须判空（见 GuestLogin）。
func NewAuthService(tokenRepo *repo.TokenRepo, accountRepo *repo.AccountRepo, tokenTTL time.Duration) *AuthService {
	return &AuthService{tokenRepo: tokenRepo, accountRepo: accountRepo, tokenTTL: tokenTTL}
}

// GuestLogin 游客登录：按设备找/建账号，签发 token 并绑定 account_id。
// 顺序：账号（找/建）→ 更新登录时间 → 生成 token → 写 Redis。
// token 生成放在账号之后：账号失败就不浪费随机源与一次 Redis 写。
func (s *AuthService) GuestLogin(ctx context.Context, deviceID string) (token string, accountID int64, err error) {
	if s.accountRepo != nil {
		accountID, err = s.accountRepo.FindOrCreateByDevice(ctx, deviceID)
		if err != nil {
			return "", 0, fmt.Errorf("find or create account: %w", err)
		}
		// 忽略错误：last_login_at 是审计字段，更新失败不影响登录正确性
		_ = s.accountRepo.TouchLogin(ctx, accountID)
	} else {
		// 降级：无 MySQL 环境（单测）用固定账号
		// 固定值避开 0——0 是「未鉴权」的哨兵值，见网关 Session.PlayerID
		accountID = 1000001
	}

	token, err = generateToken()
	if err != nil {
		return "", 0, fmt.Errorf("generate token: %w", err)
	}
	// 签发即生效：写入 Redis 后网关立即可校验，无传播延迟
	// （单点 Redis 是信任根，多活部署需考虑 Redis 集群一致性窗口）
	if err := s.tokenRepo.Set(ctx, token, accountID, s.tokenTTL); err != nil {
		return "", 0, fmt.Errorf("save token: %w", err)
	}
	return token, accountID, nil
}

// ValidateToken 校验 token，返回 account_id。
// 网关握手与充值下单共用；token 过期（TTL 到）或不存在都表现为
// 错误——拒绝登录与拒绝下单是同一套语义，无需区分原因。
func (s *AuthService) ValidateToken(ctx context.Context, token string) (int64, error) {
	return s.tokenRepo.Get(ctx, token)
}

// generateToken 生成 64 字符十六进制 token（256bit 熵）。
// 知识点：crypto/rand.Read 是阻塞式密码学随机源（Linux getrandom，
// 理论上有极低概率的短暂阻塞）；math/rand 可预测，用于安全凭证
// 等于没有凭证。hex 编码保证 token 只含 [0-9a-f]，可直接进
// HTTP Header / URL / Redis key 无需转义。
func generateToken() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
