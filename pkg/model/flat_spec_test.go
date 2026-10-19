package model

import (
	"testing"

	"game_study/pkg/flush"
)

// 编译期断言：gendb 为 @flat 表生成的 FlatOps 形状与真实生成函数签名兼容。
//
// player_signin 的主键恰为 (player_id)，形状与 @flat 表一致；这里手工搭一份
// 与生成物同形的 spec，一旦 UpsertXxxBatch / FindXxxByPK 的签名变化，
// 这个文件先编译失败，而不是等某个模块注册时才发现。
var _ = flush.FlatOps[PlayerSignin]{
	Upsert: UpsertPlayerSigninBatch,
	Load:   FindPlayerSigninByPK,
}

func TestFlatOpsCompiles(t *testing.T) {
	// 断言在包级变量，本测试只保证该文件被 go test 加载执行。
}
