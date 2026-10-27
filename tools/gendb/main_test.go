// tools/gendb/main_test.go —— DDL 解析与 @flat 生成的单元测试
//
// 核心不变量是「标注即契约」：@flat 只允许出现在 PK 恰为 player_id 的
// 玩家级单行状态表上；集合表（自增 id 主键）被标注必须在解析期报错，
// 而不是生成一份 FindByPK 签名与 FlatOps.Load 对不上、编译不过的代码。
package main

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
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
	err := os.WriteFile(path, []byte(content), 0o644)
	require.NoError(t, err)
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
	require.Len(t, tables, 2)
	byName := map[string]table{}
	for _, tb := range tables {
		byName[tb.Name] = tb
	}
	got := byName["player_settings"].FlatModule
	require.EqualValues(t, "settings", got, fmt.Sprintf("player_settings flat module = %q, want settings", got))
	got = byName["player_quest"].FlatModule
	require.EqualValues(t, "", got, fmt.Sprintf("player_quest must not be flat, got %q", got))
	// 标注不影响列解析。
	n := len(byName["player_settings"].Columns)
	require.EqualValues(t, 3, n, fmt.Sprintf("columns=%d, want 3", n))
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
	_, err := parseSQL(writeTempSQL(t, bad))
	require.Error(t, err, "flat annotation on table with id PK must fail")
}

// TestGenModelGeneratesFlatSpec 生成物必须含 PlayerSettingsFlat 适配
// （Upsert=Batch 写、Load=FindByPK 读）；未标注表不得出现 FlatOps，
// 防止注解泄漏到集合表。
func TestGenModelGeneratesFlatSpec(t *testing.T) {
	tables, err := parseSQL(writeTempSQL(t, flatDDL))
	require.NoError(t, err)
	out := t.TempDir()
	for _, tb := range tables {
		err := genModel(out, tb)
		require.NoError(t, err)
	}
	raw, err := os.ReadFile(filepath.Join(out, "player_settings_gen.go"))
	require.NoError(t, err)
	src := string(raw)
	for _, want := range []string{
		"var PlayerSettingsFlat = flush.FlatOps[PlayerSettings]{",
		"Upsert: UpsertPlayerSettingsBatch,",
		"Load:   FindPlayerSettingsByPK,",
	} {
		require.Contains(t, src, want, fmt.Sprintf("generated spec missing %q\n---\n%s", want, src))
	}

	// 未标注的表不能出现 Flat spec。
	quest, err := os.ReadFile(filepath.Join(out, "player_quest_gen.go"))
	require.NoError(t, err)
	require.NotContains(t, string(quest), "FlatOps", "unannotated table must not generate FlatOps")
}

// annoDDL 带各类查询/更新注解的表 + 一张无注解表（用于验证注解不串表）。
const annoDDL = `
-- @findby:account_id
-- @lockby:id
-- @update:progress level,exp
-- @countby:account_id,level
-- @listby:account_id
-- @listin:id
-- @deleteby:account_id
-- @updateby:bump level account_id
CREATE TABLE player_probe (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  account_id BIGINT UNSIGNED NOT NULL,
  level INT NOT NULL DEFAULT 1,
  exp BIGINT NOT NULL DEFAULT 0,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE plain_probe (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  account_id BIGINT UNSIGNED NOT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
`

// TestParseSQLAnnotations 注解归属与解析：每类注解落到对应槽位，
// 参数按空白切分（@updateby 三段：名字/更新列/条件列），无注解表保持空。
func TestParseSQLAnnotations(t *testing.T) {
	tables, err := parseSQL(writeTempSQL(t, annoDDL))
	require.NoError(t, err)
	byName := map[string]table{}
	for _, tb := range tables {
		byName[tb.Name] = tb
	}
	probe := byName["player_probe"]
	require.Equal(t, [][]string{{"account_id"}}, probe.Anno.FindBy)
	require.Equal(t, [][]string{{"id"}}, probe.Anno.LockBy)
	require.Equal(t, [][]string{{"account_id"}}, probe.Anno.ListBy)
	require.Equal(t, [][]string{{"account_id"}}, probe.Anno.DeleteBy)
	require.Equal(t, [][]string{{"account_id", "level"}}, probe.Anno.CountBy)
	require.Len(t, probe.Anno.Updates, 1)
	require.Equal(t, "progress", probe.Anno.Updates[0].Name)
	require.Equal(t, []string{"level", "exp"}, probe.Anno.Updates[0].Cols)
	require.Len(t, probe.Anno.UpdateBys, 1)
	require.Equal(t, "bump", probe.Anno.UpdateBys[0].Name)
	require.Equal(t, []string{"level"}, probe.Anno.UpdateBys[0].SetCols)
	require.Equal(t, []string{"account_id"}, probe.Anno.UpdateBys[0].WhereCols)

	plain := byName["plain_probe"]
	require.Empty(t, plain.Anno.FindBy, "annotations must not leak into the next table")
	require.Empty(t, plain.Anno.Updates)
}

// TestParseSQLRejectsUnknownAnnoColumn 注解引用了不存在的列：生成期必须失败。
// 这是「schema 是唯一真相」的兑现点——过去手写 SQL 里列名拼错，
// 编译能过、测试不覆盖，只能等线上 1054。
func TestParseSQLRejectsUnknownAnnoColumn(t *testing.T) {
	const bad = `
-- @findby:acount_id
CREATE TABLE player_probe (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  account_id BIGINT UNSIGNED NOT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
`
	_, err := parseSQL(writeTempSQL(t, bad))
	require.Error(t, err, "unknown column in @findby must fail generation")
	require.Contains(t, err.Error(), "acount_id")
}

// TestGenModelGeneratesAnnotatedFuncs 生成物包含各类注解函数，且 SQL 片段正确：
//   - @findby 带 ORDER BY 主键 + LIMIT 1（结果确定，不随执行计划漂）
//   - @lockby 带 FOR UPDATE
//   - @update 只 SET 声明列（不全列覆盖）
//   - @updateby 的 WHERE 用非 PK 列
func TestGenModelGeneratesAnnotatedFuncs(t *testing.T) {
	tables, err := parseSQL(writeTempSQL(t, annoDDL))
	require.NoError(t, err)
	out := t.TempDir()
	for _, tb := range tables {
		require.NoError(t, genModel(out, tb))
	}
	raw, err := os.ReadFile(filepath.Join(out, "player_probe_gen.go"))
	require.NoError(t, err)
	src := string(raw)

	for _, want := range []string{
		"func FindPlayerProbeByAccountId(ctx context.Context, db Querier, accountId int64) (*PlayerProbe, error)",
		"WHERE account_id = ? ORDER BY id LIMIT 1\"",
		"func LockPlayerProbeById(",
		"LIMIT 1 FOR UPDATE",
		"func ListPlayerProbeByAccountId(",
		"func ListPlayerProbeByIds(ctx context.Context, db Querier, ids []int64) ([]PlayerProbe, error)",
		"WHERE id IN (\"+strings.Join(marks, \",\")+\")\"",
		"func DeletePlayerProbeByAccountId(",
		"func CountPlayerProbeByAccountIdLevel(ctx context.Context, db Querier, accountId int64, level int32) (int64, error)",
		"func UpdatePlayerProbeProgressByPK(",
		"UPDATE player_probe SET level = ?, exp = ? WHERE id = ?",
		"func UpdatePlayerProbeBumpByAccountId(",
		"UPDATE player_probe SET level = ? WHERE account_id = ?",
	} {
		require.Contains(t, src, want, fmt.Sprintf("generated code missing %q\n---\n%s", want, src))
	}
	// 无注解表不得出现注解函数。
	plain, err := os.ReadFile(filepath.Join(out, "plain_probe_gen.go"))
	require.NoError(t, err)
	require.NotContains(t, string(plain), "FindPlainProbeByAccountId")
}
