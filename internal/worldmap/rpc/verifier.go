package rpc

// verifier.go —— 内部 token 校验（设计文档 15.5「信任边界」）。
//
// 为什么地图服必须自己再校验一次 player_id：
//
//	Game → Map 是内部调用，Map 拿到的 player_id 只应来自 Game 已绑定的
//	会话。如果 Map 无条件相信请求体里的 player_id，任何能连到内网端口的
//	进程都能冒充任意玩家（读他的部队、占他的出征槽位）。M2 先用「内部
//	token」占位，生产环境换 mTLS 或有时效的服务凭据时，只需要换一个
//	TokenVerifier 实现，map_server.go 一行都不用改——这就是把鉴权抽成
//	接口（而不是在 handler 里写死比较逻辑）的价值。
//
// 错误分层（与 map_server.go 的约定）：
//
//	本文件只返回「领域/装配错误」（ErrInvalidToken），由 handler 统一
//	翻译成 gRPC status（codes.Unauthenticated）。校验器不 import grpc，
//	这样它可以被非 gRPC 入口（MQ、测试）复用。

import (
	"context"
	"crypto/subtle"
	"errors"
)

// AnyPlayerID 是「token 不绑定具体玩家」的通配返回值。
//
// 为什么需要通配值而不是让接口返回 (playerID, allowed bool)：
// 接口要同时表达三种真实存在的语义——
//   - "这个 token 就是玩家 1001"（生产：服务凭据里带 player_id）；
//   - "这个 token 是合法的服务身份，可以代任意玩家提交"（dev 静态 token）；
//   - "dev 环境完全不校验"（NopVerifier）。
//
// 用一个哨兵常量 -1 表示后两种，handler 只需一次比较：
//
//	if bound != AnyPlayerID && bound != reqPlayer { 拒绝 }
//
// 选 -1 而不是 0：域内 PlayerID 的合法值必须为正（domain.CommandEnvelope
// 的 Validate 会拒绝 <= 0），0 在别处已经被当作「未设置」使用，复用会歧义。
const AnyPlayerID int64 = -1

// ErrInvalidToken 表示 token 非法或已过期。
//
// 用哨兵错误而不是裸 errors.New 字符串：调用方可以用 errors.Is 判定
// 「凭证问题」与「校验器内部故障」，前者翻译成 Unauthenticated，
// 后者应翻译成 Internal（让上游知道该重试而不是该换 token）。
var ErrInvalidToken = errors.New("worldmap/rpc: 内部 token 无效")

// TokenVerifier 校验内部调用凭证，返回该凭证绑定的玩家。
//
// 契约：
//   - token 非法 → 返回 ErrInvalidToken（可包装）；
//   - 凭证合法但代表「服务身份」而非具体玩家 → 返回 AnyPlayerID；
//   - 实现必须并发安全：gRPC 每个 RPC 一个 goroutine，校验器是共享实例。
//
// ctx 传入是为了将来接远程校验（例如向 auth 服务查询 token 归属）时
// 能遵守调用方超时；本地实现可以忽略它。
type TokenVerifier interface {
	Verify(ctx context.Context, token string) (playerID int64, err error)
}

// StaticTokenVerifier 是「一个固定 token，允许任意 player_id」的开发用实现。
//
// ⚠️ 仅限本地开发/联调，禁止用于任何可被外部访问的环境。
//
// 它满足的是「内网最小防线」：拒绝没有 token 的裸调用，让开发阶段就能
// 跑通 metadata 注入链路；它**不提供**玩家级隔离——知道这个 token 的
// 任何进程都能冒充任意玩家。生产必须换成校验 token↔player 绑定的实现。
type StaticTokenVerifier struct {
	token string
}

// NewStaticTokenVerifier 创建静态 token 校验器。token 为空视为配置错误，
// 返回 nil，让装配方尽早失败（而不是运行到第一个请求才发现没人能通过）。
func NewStaticTokenVerifier(token string) *StaticTokenVerifier {
	if token == "" {
		return nil
	}
	return &StaticTokenVerifier{token: token}
}

// Verify 实现 TokenVerifier。
//
// Go 知识点：字符串比较用 subtle.ConstantTimeCompare 而不是 ==。
// 普通的 == 会在第一个不同字节处提前返回，攻击者可以按响应时间逐字节
// 猜出 token（timing attack）。对内部 token 来说这条攻击面很窄，但
// 代价只是多一次长度相同的比较，没有理由不做。
//
// 长度不同时 ConstantTimeCompare 直接返回 0（长度本身会泄漏，
// 这是可接受的：token 长度不是秘密，内容才是）。
func (v *StaticTokenVerifier) Verify(_ context.Context, token string) (int64, error) {
	if subtle.ConstantTimeCompare([]byte(token), []byte(v.token)) != 1 {
		return 0, ErrInvalidToken
	}
	return AnyPlayerID, nil
}

// NopVerifier 完全不校验（开发/单测用）。
//
// 它仍然要求调用方从 metadata 里取出 token：空 token 在 handler 层已被
// 拒绝。这样「忘记注入 token」在开发环境也会立刻暴露，而不是等上线
// 换成真校验器才炸。
type NopVerifier struct{}

// Verify 实现 TokenVerifier：任何非空 token 都视为合法服务身份。
func (NopVerifier) Verify(context.Context, string) (int64, error) {
	return AnyPlayerID, nil
}
