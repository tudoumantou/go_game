// tools/genmsgid/main_test.go —— 消息号扫描/校验/生成的单元测试
//
// 除构造用例外，TestRepoProtoDeclarations 直接跑仓库真实 proto：
// 新增消息忘记加注解不会失败，但撞号、重名、越界在这里拦住 —— 这是「唯一声明点」
// 机制的质量网。
package main

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writeProto 把 proto 文本写入临时文件并返回路径（scanFile 只接受路径）。
func writeProto(t *testing.T, name, body string) string {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, name)
	err := os.WriteFile(p, []byte(body), 0o644)
	require.NoError(t, err)
	return p
}

// testSegs 测试专用号段；含 Internal 段（9000-9999），用于验证
// 「客户端协议不得占用内部 gRPC 号」这条规则。
var testSegs = []segment{
	{Name: "System", Min: 1, Max: 99},
	{Name: "Bag", Min: 3000, Max: 3999},
	{Name: "Internal", Min: 9000, Max: 9999},
}

// TestScanBindsAnnotation 注解与 message 的绑定：中间夹说明注释不打断邻接关系
// （策划习惯在 @msg 上方写用途说明，这条保证注解不会因此失效）。
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
	require.NoError(t, err, "scan")
	require.Len(t, decls, 2)
	require.EqualValues(t, 3001, decls[0].ID, fmt.Sprintf("unexpected first decl: %+v", decls[0]))
	require.EqualValues(t, "UseItemReq", decls[0].Name)
	require.EqualValues(t, 3002, decls[1].ID, fmt.Sprintf("unexpected second decl: %+v", decls[1]))
	require.EqualValues(t, "UseItemResp", decls[1].Name)
}

// TestScanRejectsDanglingAnnotation @msg 之后不是 message（如 enum）必须报错：
// 注解写错位置后静默丢失一条消息号，比直接失败难查得多。
func TestScanRejectsDanglingAnnotation(t *testing.T) {
	p := writeProto(t, "bad.proto", `// @msg 3001
enum Color { RED = 0; }
`)
	_, err := scanFile(p)
	require.Error(t, err, fmt.Sprintf("dangling annotation must fail, got %v", err))
	require.Contains(t, err.Error(), "不是 message")
}

// TestScanRejectsOutOfRange 号必须在 1..65535：帧头 msg_id 只有 16 位，
// 越界号在扫描期就拦下，而不是等运行时截断后撞号。
func TestScanRejectsOutOfRange(t *testing.T) {
	p := writeProto(t, "bad.proto", `// @msg 70000
message TooBig {}
`)
	_, err := scanFile(p)
	require.Error(t, err, "id above 65535 must be rejected at scan time")
}

// TestValidateDuplicateID 同一号被两个 message 声明必须报错，且错误信息同时含
// 双方名字（客户端会串消息体，报告必须能直接定位到两个声明点）。
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
	require.True(t, found)
}

// TestValidateDuplicateName message 重名必须报错：生成的 Go 常量（MsgXXX）会冲突，
// 且断言恰好一条问题，避免重复计数干扰其它规则。
func TestValidateDuplicateName(t *testing.T) {
	decls := []decl{
		{ID: 3001, Name: "Same", File: "a.proto", Line: 3},
		{ID: 3002, Name: "Same", File: "b.proto", Line: 3},
	}
	problems := validate(decls, testSegs)
	require.Len(t, problems, 1, fmt.Sprintf("duplicate message name not reported: %v", problems))
	require.Contains(t, problems[0], "message 重名 Same")
}

// TestValidateOutOfSegmentAndInternal 两类越界都要拦：不在任何号段内（无主号）、
// 落在内部 gRPC 段（客户端协议不得占用内部通道）。
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
	require.True(t, noSeg, fmt.Sprintf("segment checks missing: %v", problems))
	require.True(t, internal)
}

// TestGenerateIsStable 输出必须确定：同一输入两次生成逐字节相同，且常量按号排序
// （-check 模式靠逐字节比对，顺序不稳定会产生大量假 diff）。
func TestGenerateIsStable(t *testing.T) {
	decls := []decl{
		{ID: 3002, Name: "B", File: "a.proto", Line: 3},
		{ID: 3001, Name: "A", File: "a.proto", Line: 9},
	}
	first := generate(decls, testSegs)
	second := generate(decls, testSegs)
	require.EqualValues(t, second, first, "generate must be deterministic")
	require.False(t, strings.Index(first, "MsgA") > strings.Index(first, "MsgB"), "constants should follow numeric order for stable diffs")
	require.Contains(t, first, "MsgA", fmt.Sprintf("generated source missing expected pieces:\n%s", first))
	require.Contains(t, first, "func MsgID(")
}

// TestRepoProtoDeclarations 真实 proto 必须全部通过校验：
// 后续新增消息忘记加注解不会失败，但撞号、重名、越界会在这里拦住。
func TestRepoProtoDeclarations(t *testing.T) {
	segs, err := parseSegments("../../internal/common/constant")
	require.NoError(t, err, "segments")
	decls, err := scanProtos("../../api/proto/game/v1")
	require.NoError(t, err, "scan")
	require.False(t, len(decls) < 60, fmt.Sprintf("unexpectedly few declarations: %d", len(decls)))
	problems := validate(decls, segs)
	require.Empty(t, problems, fmt.Sprintf("repo proto declarations invalid:\n%s", strings.Join(problems, "\n")))
}

// TestRepoProtocolMessagesHaveMsgAnnotation 客户端协议消息必须带 `// @msg`。
//
// 为什么补这条：生成链只认 @msg，而 proto 里长期并存第二种写法
// `message XxxResp { // MsgID: 1011 }`——人能读到，工具读不到。后果是这批号
// 在 Go 侧没有常量，服务端只能手写数字（1011/1021 曾各写 3 处），改号时
// 只有 proto 注释会变，代码不会。
//
// 判据用命名后缀（Req/Resp/Notify）而不是「有没有 MsgID 注释」：后者正是
// 要被淘汰的那套写法，用它当判据等于让漏注解的消息继续合法存在。
// 子结构（SeatRoom/AttrDiff/MailEntry 等）不以三个后缀结尾，不会被误伤。
func TestRepoProtocolMessagesHaveMsgAnnotation(t *testing.T) {
	const dir = "../../api/proto/game/v1"
	decls, err := scanProtos(dir)
	require.NoError(t, err, "scan")
	has := make(map[string]bool, len(decls))
	for _, d := range decls {
		has[d.Name] = true
	}

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	checked := 0
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".proto") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, e.Name()))
		require.NoError(t, err)
		for i, line := range strings.Split(string(data), "\n") {
			m := reMessage.FindStringSubmatch(line)
			if m == nil {
				continue
			}
			name := m[1]
			if !strings.HasSuffix(name, "Req") && !strings.HasSuffix(name, "Resp") &&
				!strings.HasSuffix(name, "Notify") {
				continue
			}
			checked++
			require.True(t, has[name],
				"%s:%d: %s 缺 `// @msg <id>` 注解——客户端协议号只有这一个声明点，"+
					"不写就不会生成常量，调用处只能写裸数字", e.Name(), i+1, name)
		}
	}
	require.Greater(t, checked, 100, "没扫到协议消息，检查逻辑本身失效了")
}
