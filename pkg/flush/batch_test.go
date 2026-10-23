package flush

import (
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

// TestBuildBatchUpsert 生成的 SQL 与参数必须严格对应。
func TestBuildBatchUpsert(t *testing.T) {
	cols := []string{"player_id", "item_id", "count"}
	rows := [][]any{
		{int64(1), int32(1001), int64(5)},
		{int64(1), int32(1002), int64(3)},
	}

	sql, args, err := BuildBatchUpsert("player_item", cols, rows)
	require.NoError(t, err, "build")

	want := "INSERT INTO player_item (player_id, item_id, count) VALUES (?, ?, ?),(?, ?, ?)" +
		" ON DUPLICATE KEY UPDATE item_id=VALUES(item_id), count=VALUES(count)"
	if sql != want {
		t.Fatalf("sql =\n%s\nwant\n%s", sql, want)
	}
	if len(args) != 6 {
		t.Fatalf("args = %d, want 6", len(args))
	}
	if args[0] != int64(1) || args[1] != int32(1001) || args[5] != int64(3) {
		t.Fatalf("args order wrong: %v", args)
	}
}

// TestBuildBatchUpsertRowMismatch 行内字段数不匹配必须报错，不能生成错位 SQL。
func TestBuildBatchUpsertRowMismatch(t *testing.T) {
	_, _, err := BuildBatchUpsert("t", []string{"a", "b"}, [][]any{{1}})
	if err == nil {
		t.Fatal("expect error for column count mismatch")
	}
	if !strings.Contains(err.Error(), "row 0") {
		t.Fatalf("error should point at the bad row: %v", err)
	}
}

// TestBuildBatchUpsertEmpty 空输入报错（避免生成 `VALUES` 非法 SQL）。
func TestBuildBatchUpsertEmpty(t *testing.T) {
	if _, _, err := BuildBatchUpsert("t", nil, [][]any{{1}}); err == nil {
		t.Fatal("expect error for no columns")
	}
	if _, _, err := BuildBatchUpsert("t", []string{"a"}, nil); err == nil {
		t.Fatal("expect error for no rows")
	}
}

// TestBuildBatchUpsertSingleColumn 只有一列时更新子句不能留下悬空逗号。
func TestBuildBatchUpsertSingleColumn(t *testing.T) {
	sql, _, err := BuildBatchUpsert("t", []string{"id"}, [][]any{{1}})
	require.NoError(t, err, "build")
	if strings.Contains(sql, "UPDATE ,") || strings.HasSuffix(strings.TrimSpace(sql), "UPDATE") {
		t.Fatalf("malformed update clause: %s", sql)
	}
}

// TestBuildBatchDelete 合批删除。
func TestBuildBatchDelete(t *testing.T) {
	sql, args, err := BuildBatchDelete("mail", "id", []any{1, 2, 3})
	require.NoError(t, err, "build")
	if sql != "DELETE FROM mail WHERE id IN (?,?,?)" {
		t.Fatalf("sql = %s", sql)
	}
	if len(args) != 3 {
		t.Fatalf("args = %d", len(args))
	}
}

// TestBuildBatchDeleteWhere 带前置固定条件的删除（按玩家删道具）。
func TestBuildBatchDeleteWhere(t *testing.T) {
	sql, args, err := BuildBatchDeleteWhere("player_item", "player_id = ?", []any{int64(7)}, "item_id", []any{1001, 1002})
	require.NoError(t, err, "build")
	if sql != "DELETE FROM player_item WHERE player_id = ? AND item_id IN (?,?)" {
		t.Fatalf("sql = %s", sql)
	}
	// 条件参数必须排在 id 之前
	if len(args) != 3 || args[0] != int64(7) {
		t.Fatalf("args = %v", args)
	}
}

// TestBuildBatchDeleteEmpty 空条件/空 id 报错。
func TestBuildBatchDeleteEmpty(t *testing.T) {
	if _, _, err := BuildBatchDelete("t", "", []any{1}); err == nil {
		t.Fatal("expect error for empty pk")
	}
	if _, _, err := BuildBatchDelete("t", "id", nil); err == nil {
		t.Fatal("expect error for empty ids")
	}
	if _, _, err := BuildBatchDeleteWhere("t", "", nil, "id", []any{1}); err == nil {
		t.Fatal("expect error for empty condition")
	}
}

// TestChunkByRows 行数上限切分。
func TestChunkByRows(t *testing.T) {
	rows := make([][]any, 7)
	for i := range rows {
		rows[i] = []any{int64(i), int32(1)}
	}
	batches := Chunk(rows, 3, 0)
	if len(batches) != 3 {
		t.Fatalf("batches = %d, want 3", len(batches))
	}
	if len(batches[0]) != 3 || len(batches[1]) != 3 || len(batches[2]) != 1 {
		t.Fatalf("batch sizes = %d/%d/%d", len(batches[0]), len(batches[1]), len(batches[2]))
	}
}

// TestChunkByBytes 字节预算切分：长字符串必须提前换批。
func TestChunkByBytes(t *testing.T) {
	long := strings.Repeat("x", 400)
	rows := [][]any{
		{int64(1), long},
		{int64(2), long},
		{int64(3), long},
	}
	// 每行约 400+ 字节，预算 1000 → 每批最多 2 行
	batches := Chunk(rows, 100, 1000)
	if len(batches) < 2 {
		t.Fatalf("expect multiple batches by bytes, got %d", len(batches))
	}
	for i, b := range batches {
		if len(b) > 2 {
			t.Fatalf("batch %d has %d rows, exceeded byte budget", i, len(b))
		}
	}
}

// TestChunkSingleOversizeRow 单行超预算时单独成批（否则永远放不进去）。
func TestChunkSingleOversizeRow(t *testing.T) {
	rows := [][]any{
		{int64(1), strings.Repeat("x", 5000)},
		{int64(2), "small"},
	}
	batches := Chunk(rows, 100, 1000)
	if len(batches) != 2 {
		t.Fatalf("batches = %d, want 2", len(batches))
	}
	if len(batches[0]) != 1 {
		t.Fatalf("oversize row should be alone, got %d", len(batches[0]))
	}
}

// TestChunkEmpty 空输入返回空切片。
func TestChunkEmpty(t *testing.T) {
	if batches := Chunk(nil, 10, 100); len(batches) != 0 {
		t.Fatalf("expect no batches, got %d", len(batches))
	}
}

// TestTableName 表名解析与白名单。
func TestTableName(t *testing.T) {
	cases := map[string]string{
		"player_item:1001": "player_item",
		"mail":             "mail",
		"bad-table":        "",
		"":                 "",
	}
	for in, want := range cases {
		if got := TableName(in); got != want {
			t.Fatalf("TableName(%q) = %q, want %q", in, got, want)
		}
	}
}
