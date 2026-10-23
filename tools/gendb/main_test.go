// tools/gendb/main_test.go —— DDL 解析与 @flat 生成的单元测试
//
// 核心不变量是「标注即契约」：@flat 只允许出现在 PK 恰为 player_id 的
// 玩家级单行状态表上；集合表（自增 id 主键）被标注必须在解析期报错，
// 而不是生成一份 FindByPK 签名与 FlatOps.Load 对不上、编译不过的代码。
package main

import (
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// 一个标注了 @flat 的单行表 + 一个未标注的多行表。
const flatDDL = `
-- sql/schema/player_settings.sql
-- 玩家设置（单行状态表）
-- @flat:settings
CREATE TABLE player_settings (
  player_id BIGINT UNSIGNED NOT NULL,
  sound TINYINT NOT NULL DEFAULT 1,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (player_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='玩家设置';

-- 未标注：普通集合表
CREATE TABLE player_quest (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  player_id BIGINT UNSIGNED NOT NULL,
  progress INT NOT NULL DEFAULT 0,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
`

// writeTempSQL 把 DDL 写入临时文件并返回路径（parseSQL 只接受文件路径）。
func writeTempSQL(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "schema.sql")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

// TestParseSQLFlatAnnotation @flat 标注的归属判定：取「本表之前、上一张表结束之后」
// 区间内的注解，因此同一文件里一张表有标注、一张表没有，互不串扰。
//
// 同时断言标注不影响列解析（仍为 3 列）—— 注释是被替换成等长空格而非删除的，
// 列定义段的字节偏移不受影响。
func TestParseSQLFlatAnnotation(t *testing.T) {
	tables, err := parseSQL(writeTempSQL(t, flatDDL))
	require.NoError(t, err)
	if len(tables) != 2 {
		t.Fatalf("want 2 tables, got %d", len(tables))
	}
	byName := map[string]table{}
	for _, tb := range tables {
		byName[tb.Name] = tb
	}
	if got := byName["player_settings"].FlatModule; got != "settings" {
		t.Fatalf("player_settings flat module = %q, want settings", got)
	}
	if got := byName["player_quest"].FlatModule; got != "" {
		t.Fatalf("player_quest must not be flat, got %q", got)
	}
	// 标注不影响列解析。
	if n := len(byName["player_settings"].Columns); n != 3 {
		t.Fatalf("columns=%d, want 3", n)
	}
}

// TestParseSQLRejectsFlatOnCollectionTable 多行集合表（PK 是自增 id）被标成
// 单行状态表：生成期必须报错，否则 FindByPK(id) 与 FlatOps.Load(playerID) 签名对不上。
func TestParseSQLRejectsFlatOnCollectionTable(t *testing.T) {
	// 多行集合表（PK 是自增 id）被标成单行状态表：生成期必须报错，
	// 否则 FindByPK 的签名与 FlatOps.Load 对不上，生成出一份编译不过的代码。
	const bad = `
-- @flat:quest
CREATE TABLE player_quest (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  player_id BIGINT UNSIGNED NOT NULL,
  progress INT NOT NULL DEFAULT 0,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
`
	if _, err := parseSQL(writeTempSQL(t, bad)); err == nil {
		t.Fatal("flat annotation on table with id PK must fail")
	}
}

// TestGenModelGeneratesFlatSpec 生成物必须含 PlayerSettingsFlat 适配
// （Upsert=Batch 写、Load=FindByPK 读）；未标注表不得出现 FlatOps，
// 防止注解泄漏到集合表。
func TestGenModelGeneratesFlatSpec(t *testing.T) {
	tables, err := parseSQL(writeTempSQL(t, flatDDL))
	require.NoError(t, err)
	out := t.TempDir()
	for _, tb := range tables {
		if err := genModel(out, tb); err != nil {
			t.Fatal(err)
		}
	}
	raw, err := os.ReadFile(filepath.Join(out, "player_settings_gen.go"))
	require.NoError(t, err)
	src := string(raw)
	for _, want := range []string{
		"var PlayerSettingsFlat = flush.FlatOps[PlayerSettings]{",
		"Upsert: UpsertPlayerSettingsBatch,",
		"Load:   FindPlayerSettingsByPK,",
	} {
		if !strings.Contains(src, want) {
			t.Fatalf("generated spec missing %q\n---\n%s", want, src)
		}
	}

	// 未标注的表不能出现 Flat spec。
	quest, err := os.ReadFile(filepath.Join(out, "player_quest_gen.go"))
	require.NoError(t, err)
	if strings.Contains(string(quest), "FlatOps") {
		t.Fatal("unannotated table must not generate FlatOps")
	}
}
