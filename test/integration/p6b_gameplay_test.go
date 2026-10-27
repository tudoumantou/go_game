//go:build integration

// test/integration/p6b_gameplay_test.go —— P6 业务闭环（第二批）：
// B1 等级/任务 → B2 装备/技能 → B3 私聊 → B4 公会/组队/副本/商店/活动
package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"game_study/internal/game"
	"game_study/internal/gateway"
	"game_study/internal/login"
	"game_study/pkg/config"
	"game_study/pkg/db"
	"game_study/test/robot"

	"github.com/stretchr/testify/require"
)

// 道具/装备/技能 ID（与 configs/gamedata/xml 对齐）。
const (
	itemDiamond int32 = 1001
	itemPotion  int32 = 1002
	equipSword  int32 = 3001
	skillFire   int32 = 4002 // 火球术（等级门槛 1，升级消耗 cfg.Col3*level 钻石）
)

// TestP6BGameplayFlow P6 业务闭环（第二批）：任务/装备/技能/私聊/公会/组队/
// 副本/商店/活动，全部以错误码与终态断言。
//
// 编排：两个玩家（A 主号、B 交互方）；A 的钻石/药水/装备直接落库预置，
// 绕过业务获取路径，聚焦各模块状态机。商店购买后轮询等待 Write-Behind 落库
// （flush 周期 2s）：bagItem 走协议读内存态、itemCount 直查库，两侧一致才说明
// 缓存权威 + 异步落库闭环成立。
func TestP6BGameplayFlow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	// ① 中间件
	// 容器由 TestMain 起一次共享（见 support_test.go 的 isolatedDSN 说明），
	// 这里只取一条指向本用例独立库的 DSN：库独立才是「建表不撞车」的关键。
	mysqlDSN := freshEnv(t)
	redisAddr := testRedisAddr
	etcdEndpoint := testEtcdAddr

	database, err := db.New(db.Config{DSN: mysqlDSN, MaxOpen: 10, MaxIdle: 3})
	require.NoError(t, err, "connect mysql")
	defer func() { _ = database.Close() }()

	// ② 配置（P6-B 不需要 Kafka：私聊走在线直投分支）
	loginPort := freePort(t)
	gwPort := freePort(t)
	gwHTTPPort := freePort(t)
	gameGRPCPort := freePort(t)

	baseCfg := &config.Config{}
	baseCfg.MySQL.DSN = mysqlDSN
	baseCfg.MySQL.MaxOpenConns = 20
	baseCfg.MySQL.MaxIdleConns = 5
	baseCfg.Redis.Addrs = []string{redisAddr}
	baseCfg.Redis.PoolSize = 20
	baseCfg.Redis.MinIdleConn = 5
	baseCfg.Etcd.Endpoints = []string{etcdEndpoint}
	baseCfg.Etcd.LeaseTTL = 10
	baseCfg.Log.Level = "info"
	baseCfg.Actor.ShardCount = 64
	baseCfg.Actor.MailboxSize = 256
	baseCfg.Flush.ShardCount = 10
	baseCfg.Flush.Interval = "2s"
	baseCfg.Flush.Stagger = "200ms"
	baseCfg.Flush.BatchRows = 50
	baseCfg.Server.MaxConns = 1000

	loginCfg := *baseCfg
	loginCfg.Server.Name = "login-p6b"
	loginCfg.Server.HTTPAddr = fmt.Sprintf("127.0.0.1:%d", loginPort)
	loginCfg.Token.TTL = "24h"

	gwCfg := *baseCfg
	gwCfg.Server.Name = "gateway-p6b"
	gwCfg.Server.TCPAddr = fmt.Sprintf("127.0.0.1:%d", gwPort)
	gwCfg.Server.HTTPAddr = fmt.Sprintf("127.0.0.1:%d", gwHTTPPort)

	gameCfg := *baseCfg
	gameCfg.Server.Name = "game-p6b"
	gameCfg.Server.GRPCAddr = fmt.Sprintf("127.0.0.1:%d", gameGRPCPort)

	chdirProjectRoot(t)

	svcCtx, svcCancel := context.WithCancel(ctx)
	defer svcCancel()

	loginSrv := login.New(&loginCfg)
	startAndWait(t, "login", func() error { return loginSrv.Start(svcCtx) })
	gameSrv := game.New(&gameCfg)
	startAndWait(t, "game", func() error { return gameSrv.Start(svcCtx) })
	gwSrv := gateway.New(&gwCfg)
	startAndWait(t, "gateway", func() error { return gwSrv.Start(svcCtx) })
	time.Sleep(2 * time.Second)

	loginURL := fmt.Sprintf("http://127.0.0.1:%d", loginPort)
	gwAddr := fmt.Sprintf("127.0.0.1:%d", gwPort)

	// ③ 两个玩家：A 主号，B 用于私聊/组队/公会
	roleA, clientA := newRole(t, loginURL, gwAddr, "甲士")
	defer clientA.Close()
	roleB, clientB := newRole(t, loginURL, gwAddr, "乙士")
	defer clientB.Close()

	// ---- 预置道具：给 A 一笔钻石（商店/技能消耗源）与经验药水 ----
	if _, err := database.Exec(
		`INSERT INTO player_item (player_id, item_id, count) VALUES (?, ?, ?), (?, ?, ?)
		 ON DUPLICATE KEY UPDATE count = VALUES(count)`,
		roleA, itemDiamond, 5000, roleA, itemPotion, 10); err != nil {
		t.Fatalf("seed player_item: %v", err)
	}
	// 预置一把装备（B2 穿戴用）。equip 表是整份 JSON（player_equip.equips），
	// 没有自增主键，UID 由测试自己给。
	const equipUID = int64(7001)
	seedEquip := fmt.Sprintf(
		`[{"uid":%d,"equip_id":%d,"enhance_level":0,"quality":1,"equipped":false}]`,
		equipUID, equipSword)
	if _, err := database.Exec(
		`INSERT INTO player_equip (player_id, equips) VALUES (?, ?)`, roleA, seedEquip); err != nil {
		t.Fatalf("seed player_equip: %v", err)
	}

	// ============ B1：任务 ============
	ql, err := clientA.QuestList()
	require.NoError(t, err, "quest list")
	require.EqualValues(t, 0, ql.Code, fmt.Sprintf("quest list unexpected: code=%d n=%d", ql.Code, len(ql.Quests)))
	require.Len(t, ql.Quests, 2)
	// 接取任务 2（use_item 类型，目标 1 次）
	if resp, err := clientA.QuestAccept(2); err != nil {
		t.Fatalf("quest accept: %v", err)
	} else if resp.Code != 0 {
		t.Fatalf("quest accept code=%d", resp.Code)
	}
	// 重复接取 → 7102
	if resp, err := clientA.QuestAccept(2); err != nil {
		t.Fatalf("quest accept again: %v", err)
	} else if resp.Code != 7102 {
		t.Fatalf("expect 7102 for repeated accept, got %d", resp.Code)
	}
	// 未完成就领奖 → 7103
	if resp, err := clientA.QuestClaim(2); err != nil {
		t.Fatalf("quest claim unfinished: %v", err)
	} else if resp.Code != 7103 {
		t.Fatalf("expect 7103 for unfinished claim, got %d", resp.Code)
	}
	// 使用道具推进任务（1002 Col3=10 可使用）
	if resp, err := clientA.UseItem(itemPotion, 1); err != nil {
		t.Fatalf("use potion: %v", err)
	} else if resp.Code != 0 {
		t.Fatalf("use potion code=%d", resp.Code)
	}
	// 领奖 → 发经验 + 钻石
	claim, err := clientA.QuestClaim(2)
	require.NoError(t, err, "quest claim")
	require.False(t, claim.Code != 0 || claim.ExpGain <= 0, fmt.Sprintf("quest claim code=%d exp=%d", claim.Code, claim.ExpGain))
	// 重复领奖 → 7104
	if resp, err := clientA.QuestClaim(2); err != nil {
		t.Fatalf("quest claim again: %v", err)
	} else if resp.Code != 7104 {
		t.Fatalf("expect 7104 for repeated claim, got %d", resp.Code)
	}
	// 经验已入账（等级/经验由 LevelInfoNotify 推送，这里用 PlayerInfo 校验）
	pinfo, err := clientA.PlayerInfo()
	require.NoError(t, err, "player info")
	require.False(t, pinfo.Exp <= 0, fmt.Sprintf("expect exp > 0 after quest claim, got %d", pinfo.Exp))

	// ============ B2：装备 ============
	el, err := clientA.EquipList()
	require.NoError(t, err, "equip list")
	require.EqualValues(t, 0, el.Code, fmt.Sprintf("equip list unexpected: code=%d %+v", el.Code, el.Equips))
	require.Len(t, el.Equips, 1)
	require.EqualValues(t, equipUID, el.Equips[0].Uid)
	wear, err := clientA.EquipWear(equipUID)
	require.NoError(t, err, "equip wear")
	require.EqualValues(t, 0, wear.Code, fmt.Sprintf("equip wear code=%d attrs=%d", wear.Code, len(wear.Attrs)))
	require.NotEmpty(t, wear.Attrs)
	// 脱下
	off, err := clientA.EquipTakeOff(equipUID)
	require.NoError(t, err, "equip take off")
	require.EqualValues(t, 0, off.Code)

	// ============ B2：技能 ============
	sl, err := clientA.SkillList()
	require.NoError(t, err, "skill list")
	require.EqualValues(t, 0, sl.Code)
	learn, err := clientA.SkillLearn(skillFire)
	require.NoError(t, err, "skill learn")
	require.EqualValues(t, 0, learn.Code)
	// 重复学习 → 3203
	if resp, err := clientA.SkillLearn(skillFire); err != nil {
		t.Fatalf("skill learn again: %v", err)
	} else if resp.Code != 3203 {
		t.Fatalf("expect 3203 for repeated learn, got %d", resp.Code)
	}
	up, err := clientA.SkillUpgrade(skillFire)
	require.NoError(t, err, "skill upgrade")
	require.EqualValues(t, 0, up.Code, fmt.Sprintf("skill upgrade code=%d level=%d", up.Code, up.Level))
	require.EqualValues(t, 2, up.Level)
	// 未学习技能升级 → 3204
	if resp, err := clientA.SkillUpgrade(4001); err != nil {
		t.Fatalf("skill upgrade unlearned: %v", err)
	} else if resp.Code != 3204 {
		t.Fatalf("expect 3204 for unlearned upgrade, got %d", resp.Code)
	}

	// ============ B3：私聊（同服在线直投） ============
	sent, err := clientA.ChatSend(2, roleB, "你好，乙士")
	require.NoError(t, err, "chat send")
	require.EqualValues(t, 0, sent.Code, fmt.Sprintf("chat send code=%d msg_id=%q", sent.Code, sent.MsgId))
	require.NotEqualValues(t, "", sent.MsgId)
	notify, err := clientB.WaitChatNotify(10 * time.Second)
	require.NoError(t, err, "wait chat notify")
	require.EqualValues(t, roleA, notify.SenderId, fmt.Sprintf("chat notify mismatch: %+v", notify))
	require.EqualValues(t, "你好，乙士", notify.Content)
	// 历史（B 视角拉与 A 的会话）
	hist, err := clientB.ChatHistory(roleA, 20)
	require.NoError(t, err, "chat history")
	require.EqualValues(t, 0, hist.Code, fmt.Sprintf("chat history code=%d n=%d", hist.Code, len(hist.Messages)))
	require.NotEmpty(t, hist.Messages)

	// ============ B4：公会 ============
	gi, err := clientA.GuildInfo()
	require.NoError(t, err, "guild info")
	if gi.Code != 6101 { // 未加入
		t.Fatalf("expect 6101 before join, got %d", gi.Code)
	}
	gname := fmt.Sprintf("远征军%d", time.Now().UnixNano()%100000)
	gc, err := clientA.GuildCreate(gname)
	require.NoError(t, err, "guild create")
	require.EqualValues(t, 0, gc.Code, fmt.Sprintf("guild create code=%d id=%d", gc.Code, gc.GuildId))
	require.NotEqualValues(t, 0, gc.GuildId)
	// 重名 → 6103（由 B 建同名）
	if resp, err := clientB.GuildCreate(gname); err != nil {
		t.Fatalf("guild create dup: %v", err)
	} else if resp.Code != 6103 {
		t.Fatalf("expect 6103 for dup guild name, got %d", resp.Code)
	}
	// B 加入
	gj, err := clientB.GuildJoin(gc.GuildId)
	require.NoError(t, err, "guild join")
	require.EqualValues(t, 0, gj.Code)
	gi2, err := clientA.GuildInfo()
	require.NoError(t, err, "guild info after join")
	require.False(t, gi2.Code != 0 || gi2.MemberCount != 2 || !gi2.IsLeader, fmt.Sprintf("guild info after join unexpected: %+v", gi2))
	// B 退出
	if resp, err := clientB.GuildQuit(); err != nil {
		t.Fatalf("guild quit: %v", err)
	} else if resp.Code != 0 {
		t.Fatalf("guild quit code=%d", resp.Code)
	}

	// ============ B4：组队 ============
	ti, err := clientA.TeamInfo()
	require.NoError(t, err, "team info")
	if ti.Code != 6201 { // 未组队
		t.Fatalf("expect 6201 before team, got %d", ti.Code)
	}
	tc, err := clientA.TeamCreate(1)
	require.NoError(t, err, "team create")
	require.EqualValues(t, 0, tc.Code, fmt.Sprintf("team create code=%d id=%d", tc.Code, tc.TeamId))
	require.NotEqualValues(t, 0, tc.TeamId)
	tj, err := clientB.TeamJoin(tc.TeamId)
	require.NoError(t, err, "team join")
	require.EqualValues(t, 0, tj.Code)
	ti2, err := clientA.TeamInfo()
	require.NoError(t, err, "team info after join")
	require.EqualValues(t, 0, ti2.Code, fmt.Sprintf("team info after join unexpected: %+v", ti2))
	require.Len(t, ti2.Members, 2)
	require.False(t, !ti2.Members[0].IsLeader, fmt.Sprintf("expect first member is leader: %+v", ti2.Members))
	// B 退出 → 队伍剩 1 人
	if resp, err := clientB.TeamLeave(); err != nil {
		t.Fatalf("team leave: %v", err)
	} else if resp.Code != 0 {
		t.Fatalf("team leave code=%d", resp.Code)
	}
	info3, err := clientA.TeamInfo()
	require.NoError(t, err, "team info after leave")
	require.Len(t, info3.Members, 1)

	// ============ B4：副本 ============
	cl, err := clientA.CopyList()
	require.NoError(t, err, "copy list")
	require.False(t, cl.Code != 0 || len(cl.Copies) < 2, fmt.Sprintf("copy list unexpected: code=%d n=%d", cl.Code, len(cl.Copies)))
	// 副本 2 需要 10 级 → 6302
	if resp, err := clientA.CopySettle(2, 3); err != nil {
		t.Fatalf("copy settle lv-low: %v", err)
	} else if resp.Code != 6302 {
		t.Fatalf("expect 6302 for low level, got %d", resp.Code)
	}
	// 副本 1 首通 → first_pass=true + 掉落
	s1, err := clientA.CopySettle(1, 3)
	require.NoError(t, err, "copy settle")
	if s1.Code != 0 || !s1.FirstPass || s1.ExpGain <= 0 || len(s1.Rewards) == 0 {
		t.Fatalf("copy settle unexpected: code=%d first=%v exp=%d rewards=%d",
			s1.Code, s1.FirstPass, s1.ExpGain, len(s1.Rewards))
	}
	// 二次通关 → first_pass=false
	s2, err := clientA.CopySettle(1, 2)
	require.NoError(t, err, "copy settle again")
	require.False(t, s2.Code != 0 || s2.FirstPass, fmt.Sprintf("expect first_pass=false on second run, code=%d first=%v", s2.Code, s2.FirstPass))

	// ============ B4：商店 ============
	sgl, err := clientA.ShopList()
	require.NoError(t, err, "shop list")
	require.EqualValues(t, 0, sgl.Code, fmt.Sprintf("shop list unexpected: code=%d n=%d", sgl.Code, len(sgl.Goods)))
	require.NotEmpty(t, sgl.Goods)
	goods := sgl.Goods[0]
	before := bagItem(clientA, t, goods.ItemId)
	buy, err := clientA.ShopBuy(goods.GoodsId, 2)
	require.NoError(t, err, "shop buy")
	require.EqualValues(t, 0, buy.Code, fmt.Sprintf("shop buy code=%d bought=%d", buy.Code, buy.Bought))
	require.EqualValues(t, 2, buy.Bought)
	after := bagItem(clientA, t, goods.ItemId)
	expectGain := int64(goods.Count) * 2
	require.EqualValues(t, expectGain, after-before)
	// Write-Behind 落库：等待 flusher 周期（2s）后校验库内一致
	waitItemCount(t, database, roleA, goods.ItemId, after)
	// 商品不存在 → 6401
	if resp, err := clientA.ShopBuy(999999, 1); err != nil {
		t.Fatalf("shop buy missing: %v", err)
	} else if resp.Code != 6401 {
		t.Fatalf("expect 6401 for missing goods, got %d", resp.Code)
	}

	// ============ B4：活动 ============
	al, err := clientA.ActivityList()
	require.NoError(t, err, "activity list")
	require.EqualValues(t, 0, al.Code, fmt.Sprintf("activity list unexpected: code=%d n=%d", al.Code, len(al.Activities)))
	require.NotEmpty(t, al.Activities)
	// 活动 2（server_progress，target 14）未完成 → 6502
	if resp, err := clientA.ActivityClaim(2); err != nil {
		t.Fatalf("activity claim unfinished: %v", err)
	} else if resp.Code != 6502 {
		t.Fatalf("expect 6502 for unfinished activity, got %d", resp.Code)
	}
	// 活动不存在 → 6501
	if resp, err := clientA.ActivityClaim(999); err != nil {
		t.Fatalf("activity claim missing: %v", err)
	} else if resp.Code != 6501 {
		t.Fatalf("expect 6501 for missing activity, got %d", resp.Code)
	}

	// ⑪ 优雅关闭
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()
	_ = gwSrv.Shutdown(shutdownCtx)
	_ = gameSrv.Shutdown(shutdownCtx)
	_ = loginSrv.Shutdown(shutdownCtx)
}

// newRole 登录 → 握手 → 创角，返回 (playerID, client)。
func newRole(t *testing.T, loginURL, gwAddr, namePrefix string) (int64, *robot.Client) {
	t.Helper()
	c := robot.New(loginURL, gwAddr)
	err := c.GuestLogin(fmt.Sprintf("%s-%d", namePrefix, time.Now().UnixNano()))
	require.NoError(t, err, fmt.Sprintf("%s guest login: %v", namePrefix, err))
	err = c.Connect()
	require.NoError(t, err, fmt.Sprintf("%s connect: %v", namePrefix, err))
	err = c.Handshake()
	require.NoError(t, err, fmt.Sprintf("%s handshake: %v", namePrefix, err))
	_, err = c.SendHandshakeReq("1.0.0", "pc")
	require.NoError(t, err, fmt.Sprintf("%s handshake req: %v", namePrefix, err))
	roleName := fmt.Sprintf("%s%d", namePrefix, time.Now().UnixNano()%1000000)
	cr, err := c.CreateRole(roleName, 1)
	require.NoError(t, err)
	require.EqualValues(t, 0, cr.Code, fmt.Sprintf("%s create role code=%d role=%d", namePrefix, cr.Code, cr.RoleId))
	require.NotEqualValues(t, 0, cr.RoleId)
	return cr.RoleId, c
}

// itemCount 直接查库读道具数量（校验落库侧一致）。
func itemCount(t *testing.T, database *db.DB, playerID int64, itemID int32) int64 {
	t.Helper()
	var n int64
	err := database.QueryRow(
		`SELECT count FROM player_item WHERE player_id = ? AND item_id = ?`, playerID, itemID).Scan(&n)
	if err != nil {
		return 0
	}
	return n
}

// bagItem 通过背包协议读内存态道具数量（Write-Behind 下内存为真）。
func bagItem(c *robot.Client, t *testing.T, itemID int32) int64 {
	t.Helper()
	bag, err := c.BagList()
	require.NoError(t, err, "bag list")
	for _, it := range bag.Items {
		if it.ItemId == itemID {
			return int64(it.Count)
		}
	}
	return 0
}

// waitItemCount 轮询等待 Write-Behind flusher 把内存态落到库。
func waitItemCount(t *testing.T, database *db.DB, playerID int64, itemID int32, want int64) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if itemCount(t, database, playerID, itemID) == want {
			return
		}
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatalf("item %d not flushed to db: want %d got %d",
		itemID, want, itemCount(t, database, playerID, itemID))
}
