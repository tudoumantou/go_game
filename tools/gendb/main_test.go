package main

import (
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

func writeTempSQL(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "schema.sql")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestParseSQLFlatAnnotation(t *testing.T) {
	tables, err := parseSQL(writeTempSQL(t, flatDDL))
	if err != nil {
		t.Fatal(err)
	}
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

func TestGenModelGeneratesFlatSpec(t *testing.T) {
	tables, err := parseSQL(writeTempSQL(t, flatDDL))
	if err != nil {
		t.Fatal(err)
	}
	out := t.TempDir()
	for _, tb := range tables {
		if err := genModel(out, tb); err != nil {
			t.Fatal(err)
		}
	}
	raw, err := os.ReadFile(filepath.Join(out, "player_settings_gen.go"))
	if err != nil {
		t.Fatal(err)
	}
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
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(quest), "FlatOps") {
		t.Fatal("unannotated table must not generate FlatOps")
	}
}
