// internal/login/repo/account_repo.go —— 账号仓储
//
// 架构职责：account 表的读写（按设备找号、建游客号、更新登录时间），
// 是登录服唯一直接写账号数据的文件。Repo 层之上是 service，
// 之下是 database/sql——本文件不出现任何 HTTP/业务概念。
// 协作关系：service.AuthService.GuestLogin 调 FindOrCreateByDevice；
// pkg/model 的 InsertAccount 生成参数化 INSERT（gorm/gen 风格代码生成）。
// 设计取舍：① 「按设备找，没有则建」放在一个方法里——调用方不关心
// 内部是 SELECT 还是 INSERT，也避免「先查后建」的竞态被放大到
// service 层（device 列有唯一索引兜底，见下）；② 游客账号密码留空
// （P7 接渠道登录再补），当前 device 即身份凭证。
package repo

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"game_study/pkg/db"
	"game_study/pkg/model"
)

// AccountRepo 账号仓储。
// 依赖 *db.DB（database/sql 的薄封装）：所有方法第一个参数都是
// ctx——DB 查询必须可取消，否则慢 SQL 会拖住请求 goroutine。
type AccountRepo struct {
	db *db.DB
}

// NewAccountRepo 创建仓储。
// 依赖注入 *db.DB：单测传 sqlmock/内存库即可，不起真 MySQL。
func NewAccountRepo(database *db.DB) *AccountRepo {
	return &AccountRepo{db: database}
}

// FindOrCreateByDevice 按设备找账号，没有则创建游客账号。
//
// 返回值带上 status：封禁（1）/注销（2）必须在登录入口拦掉，
// 否则一个被冻结的账号照样能换 token 进游戏。
// 并发语义：SELECT 未命中 → INSERT。两个相同 device 的并发首次登录
// 会都走到 INSERT，靠 account.device 唯一索引保证只有一个成功
// （另一个报 1062）；当前实现把 1062 当错误上抛，由调用方重试
// 即转到 SELECT 命中路径——「唯一索引是并发找/建的最终仲裁者」。
func (r *AccountRepo) FindOrCreateByDevice(ctx context.Context, device string) (int64, int32, error) {
	if device == "" {
		device = "anonymous"
	}

	// 占位符 ? 而非字符串拼接：SQL 注入的唯一可靠防御是参数化——
	// device 来自客户端，拼接即把注入面敞开；? 由驱动做转义
	const q = `SELECT id, status FROM account WHERE device = ? ORDER BY id LIMIT 1`
	var id int64
	var status int32
	err := r.db.QueryRowContext(ctx, q, device).Scan(&id, &status)
	if err == nil {
		return id, status, nil
	}
	// 知识点：errors.Is 判 sql.ErrNoRows——驱动可能包装错误，
	// Is 沿 %w 链查找；== 比较会漏。ErrNoRows 不是故障，是「没有」。
	if !errors.Is(err, sql.ErrNoRows) {
		return 0, 0, fmt.Errorf("query account by device: %w", err)
	}

	// 创建游客账号（密码占位，P7 接入渠道登录）
	// 账号名用随机后缀而非 device 本身：device 可能含特殊字符/
	// 超长，hex 随机串保证合法且不与已有账号撞名
	suffix, err := randomHex(6)
	if err != nil {
		return 0, 0, err
	}
	acc := &model.Account{
		Account:      "guest_" + suffix,
		PasswordHash: "",
		Salt:         "",
		Status:       0,
		Device:       device,
		// sql.NullTime：允许 NULL 的时间列——Go 的 time.Time 零值
		// 无法表达「从未登录」，NullTime 的 Valid 标志可以
		LastLoginAt: sql.NullTime{Time: time.Now(), Valid: true},
	}
	res, err := model.InsertAccount(ctx, r.db.DB, acc)
	if err != nil {
		return 0, 0, fmt.Errorf("insert account: %w", err)
	}
	id, err = res.LastInsertId()
	if err != nil {
		return 0, 0, fmt.Errorf("insert account id: %w", err)
	}
	return id, 0, nil // 新建账号必然是正常状态
}

// TouchLogin 更新最近登录时间。
// 每次登录顺带更新；失败不阻断登录（调用方 _ = 忽略错误）——
// 这是审计性质的数据，丢了不影响本次登录的正确性。
func (r *AccountRepo) TouchLogin(ctx context.Context, accountID int64) error {
	const q = `UPDATE account SET last_login_at = ? WHERE id = ?`
	_, err := r.db.ExecContext(ctx, q, time.Now(), accountID)
	return err
}

// randomHex 生成 n 字节随机数的 hex 串（2n 字符）。
// 知识点：crypto/rand 而非 math/rand——账号名可猜测 = 账号可枚举，
// 安全场景必须用密码学随机源；math/rand 的种子可预测，只配做玩具。
func randomHex(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
