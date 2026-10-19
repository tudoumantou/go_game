package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeProto(t *testing.T, name, body string) string {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

var testSegs = []segment{
	{Name: "System", Min: 1, Max: 99},
	{Name: "Bag", Min: 3000, Max: 3999},
	{Name: "Internal", Min: 9000, Max: 9999},
}

func TestScanBindsAnnotation(t *testing.T) {
	p := writeProto(t, "item.proto", `syntax = "proto3";
package game.v1;

// @msg 3001
message UseItemReq {
  int32 id = 1;
}

// 说明注释不影响注解与 message 的邻接关系。
// @msg 3002
// 这里还能再插一行注释。
message UseItemResp {}
`)
	decls, err := scanFile(p)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if len(decls) != 2 {
		t.Fatalf("decls = %d, want 2 (%+v)", len(decls), decls)
	}
	if decls[0].ID != 3001 || decls[0].Name != "UseItemReq" {
		t.Fatalf("unexpected first decl: %+v", decls[0])
	}
	if decls[1].ID != 3002 || decls[1].Name != "UseItemResp" {
		t.Fatalf("unexpected second decl: %+v", decls[1])
	}
}

func TestScanRejectsDanglingAnnotation(t *testing.T) {
	p := writeProto(t, "bad.proto", `// @msg 3001
enum Color { RED = 0; }
`)
	_, err := scanFile(p)
	if err == nil || !strings.Contains(err.Error(), "不是 message") {
		t.Fatalf("dangling annotation must fail, got %v", err)
	}
}

func TestScanRejectsOutOfRange(t *testing.T) {
	p := writeProto(t, "bad.proto", `// @msg 70000
message TooBig {}
`)
	if _, err := scanFile(p); err == nil {
		t.Fatal("id above 65535 must be rejected at scan time")
	}
}

func TestValidateDuplicateID(t *testing.T) {
	decls := []decl{
		{ID: 3001, Name: "A", File: "a.proto", Line: 3},
		{ID: 3001, Name: "B", File: "b.proto", Line: 5},
	}
	problems := validate(decls, testSegs)
	found := false
	for _, p := range problems {
		if strings.Contains(p, "消息号 3001 重复") && strings.Contains(p, "A") && strings.Contains(p, "B") {
			found = true
		}
	}
	if !found {
		t.Fatalf("duplicate id not reported: %v", problems)
	}
}

func TestValidateDuplicateName(t *testing.T) {
	decls := []decl{
		{ID: 3001, Name: "Same", File: "a.proto", Line: 3},
		{ID: 3002, Name: "Same", File: "b.proto", Line: 3},
	}
	problems := validate(decls, testSegs)
	if len(problems) != 1 || !strings.Contains(problems[0], "message 重名 Same") {
		t.Fatalf("duplicate message name not reported: %v", problems)
	}
}

func TestValidateOutOfSegmentAndInternal(t *testing.T) {
	decls := []decl{
		{ID: 8888, Name: "Nowhere", File: "a.proto", Line: 3},
		{ID: 9001, Name: "Grpc", File: "a.proto", Line: 9},
	}
	problems := validate(decls, testSegs)
	var noSeg, internal bool
	for _, p := range problems {
		if strings.Contains(p, "不在任何号段内") {
			noSeg = true
		}
		if strings.Contains(p, "内部 gRPC 段") {
			internal = true
		}
	}
	if !noSeg || !internal {
		t.Fatalf("segment checks missing: %v", problems)
	}
}

func TestGenerateIsStable(t *testing.T) {
	decls := []decl{
		{ID: 3002, Name: "B", File: "a.proto", Line: 3},
		{ID: 3001, Name: "A", File: "a.proto", Line: 9},
	}
	first := generate(decls, testSegs)
	second := generate(decls, testSegs)
	if first != second {
		t.Fatal("generate must be deterministic")
	}
	if strings.Index(first, "MsgA") > strings.Index(first, "MsgB") {
		t.Fatal("constants should follow numeric order for stable diffs")
	}
	if !strings.Contains(first, "MsgA") || !strings.Contains(first, "func MsgID(") {
		t.Fatalf("generated source missing expected pieces:\n%s", first)
	}
}

// TestRepoProtoDeclarations 真实 proto 必须全部通过校验：
// 后续新增消息忘记加注解不会失败，但撞号、重名、越界会在这里拦住。
func TestRepoProtoDeclarations(t *testing.T) {
	segs, err := parseSegments("../../internal/common/constant")
	if err != nil {
		t.Fatalf("segments: %v", err)
	}
	decls, err := scanProtos("../../api/proto/game/v1")
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if len(decls) < 60 {
		t.Fatalf("unexpectedly few declarations: %d", len(decls))
	}
	if problems := validate(decls, segs); len(problems) > 0 {
		t.Fatalf("repo proto declarations invalid:\n%s", strings.Join(problems, "\n"))
	}
}
