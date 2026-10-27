// pkg/codec/msgids_test.go —— 消息号协议兼容性测试（wire baseline）
// 守住两条不变量：① 已下发到客户端的消息号永远不变（wireBaseline 逐一比对）；
// ② 注册表内一号一义、名字↔号码正反查询一致。
// 新增消息必须同步补进 wireBaseline，否则 TestMsgIDsMatchWireBaseline 会以
// 「条数不一致」失败——这是故意的：防止有人只加号不进基线、绕过兼容审查。
package codec

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// wireBaseline 协议兼容基线（R10.1）。
//
// 这些号已经下发到客户端，改号会让旧客户端的请求落到别的 handler 上。
// 新增消息只在末尾追加；确需改号必须同步客户端版本，并在此显式更新基线。
var wireBaseline = map[string]uint32{
	"EnterGameReq":           1010,
	"CreateRoleReq":          1020,
	"PlayerInfoReq":          2001,
	"PlayerInfoResp":         2002,
	"UseItemReq":             3001,
	"UseItemResp":            3002,
	"BagListReq":             3003,
	"BagListResp":            3004,
	"BagChangeNotify":        3010,
	"EquipListReq":           3101,
	"EquipListResp":          3102,
	"EquipWearReq":           3103,
	"EquipWearResp":          3104,
	"EquipTakeOffReq":        3105,
	"EquipTakeOffResp":       3106,
	"SkillListReq":           3201,
	"SkillListResp":          3202,
	"SkillLearnReq":          3203,
	"SkillLearnResp":         3204,
	"SkillUpgradeReq":        3205,
	"SkillUpgradeResp":       3206,
	"SceneEnterReq":          4001,
	"SceneEnterResp":         4002,
	"SceneMoveReq":           4003,
	"SceneMoveResp":          4004,
	"SceneLeaveReq":          4005,
	"SceneLeaveResp":         4006,
	"SceneEntityNotify":      4010,
	"SceneSyncReq":           4020,
	"SceneSyncResp":          4021,
	"BattleStartNotify":      5001,
	"MatchReq":               5002,
	"MatchResp":              5003,
	"BattleResultReport":     5010,
	"BattleSettleNotify":     5011,
	"BattleSyncNotify":       5020,
	"BattleOpReport":         5021,
	"ChatSendReq":            6001,
	"ChatSendResp":           6002,
	"ChatMessageNotify":      6010,
	"ChatHistoryReq":         6020,
	"ChatHistoryResp":        6021,
	"GuildInfoReq":           6101,
	"GuildInfoResp":          6102,
	"GuildCreateReq":         6103,
	"GuildCreateResp":        6104,
	"GuildJoinReq":           6105,
	"GuildJoinResp":          6106,
	"GuildQuitReq":           6107,
	"GuildQuitResp":          6108,
	"TeamInfoReq":            6201,
	"TeamInfoResp":           6202,
	"TeamCreateReq":          6203,
	"TeamCreateResp":         6204,
	"TeamJoinReq":            6205,
	"TeamJoinResp":           6206,
	"TeamLeaveReq":           6207,
	"TeamLeaveResp":          6208,
	"CopyListReq":            6301,
	"CopyListResp":           6302,
	"CopySettleReq":          6303,
	"CopySettleResp":         6304,
	"ShopListReq":            6401,
	"ShopListResp":           6402,
	"ShopBuyReq":             6403,
	"ShopBuyResp":            6404,
	"ActivityListReq":        6501,
	"ActivityListResp":       6502,
	"ActivityClaimReq":       6503,
	"ActivityClaimResp":      6504,
	"SigninInfoReq":          6601,
	"SigninInfoResp":         6602,
	"SigninClaimReq":         6603,
	"SigninClaimResp":        6604,
	"RankListReq":            6701,
	"RankListResp":           6702,
	"FriendListReq":          6801,
	"FriendListResp":         6802,
	"FriendApplyReq":         6803,
	"FriendApplyResp":        6804,
	"FriendHandleReq":        6805,
	"FriendHandleResp":       6806,
	"FriendDeleteReq":        6807,
	"FriendDeleteResp":       6808,
	"PartnerListReq":         6901,
	"PartnerListResp":        6902,
	"PartnerActivateReq":     6903,
	"PartnerActivateResp":    6904,
	"PartnerStarUpReq":       6905,
	"PartnerStarUpResp":      6906,
	"PartnerFieldReq":        6907,
	"PartnerFieldResp":       6908,
	"MailListReq":            7001,
	"MailListResp":           7002,
	"MailTakeAttachmentReq":  7020,
	"MailTakeAttachmentResp": 7021,
	"MailDeleteReq":          7022,
	"MailDeleteResp":         7023,
	"QuestListReq":           7101,
	"QuestListResp":          7102,
	"QuestAcceptReq":         7103,
	"QuestAcceptResp":        7104,
	"QuestClaimReq":          7105,
	"QuestClaimResp":         7106,
	"FriendBlockReq":         6809,
	"FriendBlockResp":        6810,
	"FriendUnblockReq":       6811,
	"FriendUnblockResp":      6812,
	"EquipEnhanceReq":        3107,
	"EquipEnhanceResp":       3108,
}

// TestMsgIDsMatchWireBaseline 逐条比对 wireBaseline：生成表里的号必须与
// 基线完全一致，且总条数相等。
//
// 覆盖场景：改号（会破坏旧客户端——旧客户端把请求发到别的 handler 且不报错）、
// 漏登（新增消息没补基线）、多登（基线里删了但生成表还有）。
// 为什么用测试而不是 code review 守：号是跨进程契约，改号的后果只在
// 特定消息上偶发，人工审查拦不住，回归测试成本最低。
func TestMsgIDsMatchWireBaseline(t *testing.T) {
	for name, want := range wireBaseline {
		got, ok := MsgID(name)
		require.True(t, ok, "%s 未注册", name)
		require.EqualValues(t, want, got, "%s 号从 %d 变为 %d（会破坏旧客户端）", name, want, got)
	}
	require.EqualValues(t, MsgCount(), len(wireBaseline), "基线 %d 条，注册表 %d 条：新增消息请补进基线", len(wireBaseline), MsgCount())
}

// TestMsgNameReverse 反查必须与正查互为逆运算：MsgName(id) 回到同名。
//
// 覆盖场景：Notify 类消息只有号码没有名字（日志/调试/按号反解），
// 反查错位会把 5010 打成 5011 的名字，误导排障；另外未注册的号与名字
// 都不能命中——map 查找的零值 (0,false) 若被当成有效结果，会把
// 「未知消息」误判成「0 号消息」。
func TestMsgNameReverse(t *testing.T) {
	for name, id := range wireBaseline {
		got, ok := MsgName(id)
		require.True(t, ok, "MsgName(%d) = %q,%v，期望 %q", id, got, ok, name)
		require.EqualValues(t, name, got, "MsgName(%d) = %q,%v，期望 %q", id, got, ok, name)
	}
	_, ok1 := MsgName(999999)
	require.False(t, ok1, "未注册的号不应命中")
	_, ok2 := MsgID("NotAMessage")
	require.False(t, ok2, "未注册的名字不应命中")
}

// TestMsgIDsUnique 注册表内不允许一号多义（生成器已拦，这里守住产物）。
//
// 构造：seen map 记录「号 → 已占用它的名字」，遇到重复号即报错并指出
// 冲突双方。这是对生成器的冗余防线——生成器（tools/genmsgid）启动时
// 已拒绝重复号，但生成产物可能被手工编辑，测试守住的是文件现状。
func TestMsgIDsUnique(t *testing.T) {
	seen := map[uint32]string{}
	for name, id := range wireBaseline {
		other3, ok4 := seen[id]
		require.False(t, ok4, "号 %d 被 %s 与 %s 同时占用", id, other3, name)
		seen[id] = name
	}
}
