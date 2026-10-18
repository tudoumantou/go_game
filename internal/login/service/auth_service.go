// internal/login/service/auth_service.go —— 认证服务
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
type AuthService struct {
	tokenRepo   *repo.TokenRepo
	accountRepo *repo.AccountRepo
	tokenTTL    time.Duration
}

// NewAuthService 创建 AuthService（accountRepo 可为 nil：无 MySQL 时降级为固定账号）。
func NewAuthService(tokenRepo *repo.TokenRepo, accountRepo *repo.AccountRepo, tokenTTL time.Duration) *AuthService {
	return &AuthService{tokenRepo: tokenRepo, accountRepo: accountRepo, tokenTTL: tokenTTL}
}

// GuestLogin 游客登录：按设备找/建账号，签发 token 并绑定 account_id。
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

// ValidateToken 校验 token，返回 account_id。
func (s *AuthService) ValidateToken(ctx context.Context, token string) (int64, error) {
	return s.tokenRepo.Get(ctx, token)
}

func generateToken() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
