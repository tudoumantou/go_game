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
	tokenRepo *repo.TokenRepo
	tokenTTL  time.Duration
}

// NewAuthService 创建 AuthService。
func NewAuthService(tokenRepo *repo.TokenRepo, tokenTTL time.Duration) *AuthService {
	return &AuthService{tokenRepo: tokenRepo, tokenTTL: tokenTTL}
}

// GuestLogin 游客登录：生成新 token 并绑定 player_id（P1 用 player_id=0 占位，P4 读 DB）。
func (s *AuthService) GuestLogin(ctx context.Context, deviceID string) (token string, playerID int64, err error) {
	// P1 简化：每个 device 给一个固定 player_id 的 token，方便机器人测试
	_ = deviceID
	playerID = 1000001 // TODO(P4): 从 idtab / account 表生成

	token, err = generateToken()
	if err != nil {
		return "", 0, fmt.Errorf("generate token: %w", err)
	}
	if err := s.tokenRepo.Set(ctx, token, playerID, s.tokenTTL); err != nil {
		return "", 0, fmt.Errorf("save token: %w", err)
	}
	return token, playerID, nil
}

// ValidateToken 校验 token。
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
