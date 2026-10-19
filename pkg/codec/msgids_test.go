package codec

import "testing"

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
	"QuestListReq":           7101,
	"QuestListResp":          7102,
	"QuestAcceptReq":         7103,
	"QuestAcceptResp":        7104,
	"QuestClaimReq":          7105,
	"QuestClaimResp":         7106,
}

func TestMsgIDsMatchWireBaseline(t *testing.T) {
	for name, want := range wireBaseline {
		got, ok := MsgID(name)
		if !ok {
			t.Fatalf("%s 未注册", name)
		}
		if got != want {
			t.Fatalf("%s 号从 %d 变为 %d（会破坏旧客户端）", name, want, got)
		}
	}
	if len(wireBaseline) != MsgCount() {
		t.Fatalf("基线 %d 条，注册表 %d 条：新增消息请补进基线", len(wireBaseline), MsgCount())
	}
}

func TestMsgNameReverse(t *testing.T) {
	for name, id := range wireBaseline {
		got, ok := MsgName(id)
		if !ok || got != name {
			t.Fatalf("MsgName(%d) = %q,%v，期望 %q", id, got, ok, name)
		}
	}
	if _, ok := MsgName(999999); ok {
		t.Fatal("未注册的号不应命中")
	}
	if _, ok := MsgID("NotAMessage"); ok {
		t.Fatal("未注册的名字不应命中")
	}
}

// TestMsgIDsUnique 注册表内不允许一号多义（生成器已拦，这里守住产物）。
func TestMsgIDsUnique(t *testing.T) {
	seen := map[uint32]string{}
	for name, id := range wireBaseline {
		if other, ok := seen[id]; ok {
			t.Fatalf("号 %d 被 %s 与 %s 同时占用", id, other, name)
		}
		seen[id] = name
	}
}
