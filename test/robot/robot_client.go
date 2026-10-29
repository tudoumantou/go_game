// test/robot/robot_client.go —— 机器人客户端（P1-P4 验收）
package robot

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"game_study/internal/common/constant"
	"game_study/pkg/codec"
	"game_study/pkg/crypto"

	gamev1 "game_study/pkg/pb/game/v1"
	gatev1 "game_study/pkg/pb/gate/v1"
	"google.golang.org/protobuf/proto"
)

// Client 机器人客户端。
type Client struct {
	LoginURL   string
	GatewayURL string
	Token      string
	AccountID  int64
	PlayerID   int64 // 兼容旧字段：游客登录时等于 AccountID

	conn   net.Conn
	cipher *crypto.SessionCipher

	// pendingPush 收到的非目标响应消息（推送），供后续 Wait* 消费。
	// 不加这个缓存的话，call() 等响应时读到的推送会被直接丢弃 ——
	// 曾导致「登录后的离线补投消息永远收不到」的假阴性。
	pendingPush []codec.Packet
}

// New 创建机器人。
func New(loginURL, gatewayURL string) *Client {
	return &Client{LoginURL: loginURL, GatewayURL: gatewayURL}
}

// GuestLogin 调用登录服获取 token。
func (c *Client) GuestLogin(deviceID string) error {
	body, _ := json.Marshal(map[string]string{"device_id": deviceID})
	resp, err := http.Post(c.LoginURL+"/v1/auth/guest", "application/json", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("login request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	var out struct {
		Token     string `json:"token"`
		AccountID int64  `json:"account_id"`
		PlayerID  int64  `json:"player_id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return fmt.Errorf("decode login: %w", err)
	}
	if out.Token == "" {
		return fmt.Errorf("empty token")
	}
	c.Token = out.Token
	c.AccountID = out.AccountID
	if c.AccountID == 0 {
		c.AccountID = out.PlayerID
	}
	c.PlayerID = c.AccountID
	return nil
}

// Connect 建立 TCP 连接。
func (c *Client) Connect() error {
	conn, err := net.DialTimeout("tcp", c.GatewayURL, 5*time.Second)
	if err != nil {
		return err
	}
	c.conn = conn
	return nil
}

// Close 关闭连接。
func (c *Client) Close() {
	if c.conn != nil {
		_ = c.conn.Close()
	}
}

// readPacket 读取一个完整包。
func (c *Client) readPacket(timeout time.Duration) (codec.Packet, error) {
	_ = c.conn.SetReadDeadline(time.Now().Add(timeout))
	var lenBuf [2]byte
	if _, err := io.ReadFull(c.conn, lenBuf[:]); err != nil {
		return codec.Packet{}, err
	}
	length := int(binary.BigEndian.Uint16(lenBuf[:]))
	buf := make([]byte, length)
	if _, err := io.ReadFull(c.conn, buf); err != nil {
		return codec.Packet{}, err
	}
	return codec.DecodePacket(buf)
}

// writePacket 写一个完整包。
func (c *Client) writePacket(p codec.Packet) error {
	data, err := codec.EncodePacket(p)
	if err != nil {
		return err
	}
	_, err = c.conn.Write(data)
	return err
}

// Handshake 完成 RSA/AES 握手。
func (c *Client) Handshake() error {
	// 1. 收 ServerHello
	pkt, err := c.readPacket(5 * time.Second)
	if err != nil {
		return fmt.Errorf("read server hello: %w", err)
	}
	if pkt.MsgID != constant.MsgIDServerHello {
		return fmt.Errorf("expect ServerHello, got msgID=%d", pkt.MsgID)
	}

	// body = pubKeyPEM || salt
	pubPEM, salt := splitSaltAndPEM(pkt.Body)
	if len(salt) == 0 {
		return fmt.Errorf("invalid server hello body")
	}

	// 2. 客户端握手
	client, err := crypto.NewHandshakeClient()
	if err != nil {
		return err
	}
	if err := client.OnServerHello(pubPEM, salt); err != nil {
		return fmt.Errorf("client server hello: %w", err)
	}
	cek, err := client.ClientKeyExchange()
	if err != nil {
		return err
	}
	if err := c.writePacket(codec.Packet{MsgID: constant.MsgIDClientKeyExchange, Seq: 0, Body: cek}); err != nil {
		return err
	}

	// 3. 派生会话密钥 + 发送 Finished
	c.cipher, err = crypto.NewSessionCipher(client.SessionKey())
	if err != nil {
		return err
	}
	finishedBody, seq, err := c.cipher.Encrypt(constant.MsgIDFinished, []byte("finished"))
	if err != nil {
		return err
	}
	if err := c.writePacket(codec.Packet{MsgID: constant.MsgIDFinished, Seq: seq, Body: finishedBody}); err != nil {
		return err
	}

	// 4. 收服务端 Finished
	fin, err := c.readPacket(5 * time.Second)
	if err != nil {
		return fmt.Errorf("read server finished: %w", err)
	}
	if fin.MsgID != constant.MsgIDFinished {
		return fmt.Errorf("expect Finished, got msgID=%d", fin.MsgID)
	}
	return nil
}

// SendHandshakeReq 发送加密的 HandshakeReq。
func (c *Client) SendHandshakeReq(clientVersion, platform string) (*gatev1.HandshakeResp, error) {
	req := &gatev1.HandshakeReq{Token: c.Token, ClientVersion: clientVersion, Platform: platform}
	body, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}
	enc, seq, err := c.cipher.Encrypt(constant.MsgIDHandshakeReq, body)
	if err != nil {
		return nil, err
	}
	if err := c.writePacket(codec.Packet{MsgID: constant.MsgIDHandshakeReq, Seq: seq, Body: enc}); err != nil {
		return nil, err
	}

	pkt, err := c.readPacket(5 * time.Second)
	if err != nil {
		return nil, err
	}
	if pkt.MsgID != constant.MsgIDHandshakeResp {
		return nil, fmt.Errorf("expect HandshakeResp, got %d", pkt.MsgID)
	}
	msgID, _, plain, err := c.cipher.Decrypt(pkt.Body)
	if err != nil {
		return nil, err
	}
	if msgID != constant.MsgIDHandshakeResp {
		return nil, fmt.Errorf("inner msgID mismatch")
	}
	resp := &gatev1.HandshakeResp{}
	if err := proto.Unmarshal(plain, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// Heartbeat 发送心跳并等待响应。
func (c *Client) Heartbeat() (*gatev1.HeartbeatResp, error) {
	body, _ := proto.Marshal(&gatev1.HeartbeatReq{})
	enc, seq, err := c.cipher.Encrypt(constant.MsgIDHeartbeatReq, body)
	if err != nil {
		return nil, err
	}
	if err := c.writePacket(codec.Packet{MsgID: constant.MsgIDHeartbeatReq, Seq: seq, Body: enc}); err != nil {
		return nil, err
	}
	pkt, err := c.readPacket(5 * time.Second)
	if err != nil {
		return nil, err
	}
	_, _, plain, err := c.cipher.Decrypt(pkt.Body)
	if err != nil {
		return nil, err
	}
	resp := &gatev1.HeartbeatResp{}
	if err := proto.Unmarshal(plain, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// splitSaltAndPEM 按「salt 定长前缀 + PEM」切分 ServerHello。
//
// 历史坑：早期实现是 PEM 在前、salt 在后，靠「跳过 PEM 尾随换行」定位 salt ——
// salt 是随机二进制，有 2/256 的概率以 0x0a/0x0d 开头，会被当成换行吃掉一个字节，
// 导致会话密钥不一致（表现为偶发的 finished 解密失败，实测失败率 ~1.3%）。
// 现在 salt 前置且长度固定，切分无歧义。
func splitSaltAndPEM(body []byte) (pem, salt []byte) {
	const saltSize = crypto.SaltSize
	if len(body) <= saltSize {
		return nil, nil
	}
	return body[saltSize:], body[:saltSize]
}

// call 发送请求并等待指定响应（跳过中间推送）。
func (c *Client) call(msgID uint32, req proto.Message, respID uint32, resp proto.Message) error {
	body, err := proto.Marshal(req)
	if err != nil {
		return err
	}
	enc, seq, err := c.cipher.Encrypt(msgID, body)
	if err != nil {
		return err
	}
	if err := c.writePacket(codec.Packet{MsgID: msgID, Seq: seq, Body: enc}); err != nil {
		return err
	}

	for i := 0; i < 16; i++ {
		pkt, err := c.readPacket(5 * time.Second)
		if err != nil {
			return fmt.Errorf("wait resp %d: %w", respID, err)
		}
		if pkt.MsgID != respID {
			// 推送/其它消息：缓存起来给后续 Wait* 用，不能丢
			c.pendingPush = append(c.pendingPush, pkt)
			continue
		}
		_, _, plain, err := c.cipher.Decrypt(pkt.Body)
		if err != nil {
			return err
		}
		return proto.Unmarshal(plain, resp)
	}
	return fmt.Errorf("no response for msgID %d (expect %d)", msgID, respID)
}

// EnterGame 进游戏（EnterGameReq/EnterGameResp）。
func (c *Client) EnterGame() (*gamev1.EnterGameResp, error) {
	resp := &gamev1.EnterGameResp{}
	if err := c.call(codec.MsgEnterGameReq, &gamev1.EnterGameReq{}, codec.MsgEnterGameResp, resp); err != nil {
		return nil, err
	}
	if resp.RoleId > 0 {
		c.PlayerID = resp.RoleId
	}
	return resp, nil
}

// CreateRole 创角（CreateRoleReq/CreateRoleResp）。
func (c *Client) CreateRole(name string, career int32) (*gamev1.CreateRoleResp, error) {
	resp := &gamev1.CreateRoleResp{}
	req := &gamev1.CreateRoleReq{Name: name, Career: career}
	if err := c.call(codec.MsgCreateRoleReq, req, codec.MsgCreateRoleResp, resp); err != nil {
		return nil, err
	}
	if resp.RoleId > 0 {
		c.PlayerID = resp.RoleId
	}
	return resp, nil
}

// PlayerInfo 拉取玩家信息（PlayerInfoReq/PlayerInfoResp）。
func (c *Client) PlayerInfo() (*gamev1.PlayerInfoResp, error) {
	resp := &gamev1.PlayerInfoResp{}
	if err := c.call(codec.MsgPlayerInfoReq, &gamev1.PlayerInfoReq{}, codec.MsgPlayerInfoResp, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// BagList 背包列表（BagListReq/BagListResp）。
func (c *Client) BagList() (*gamev1.BagListResp, error) {
	resp := &gamev1.BagListResp{}
	if err := c.call(codec.MsgBagListReq, &gamev1.BagListReq{}, codec.MsgBagListResp, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// UseItem 使用道具（UseItemReq/UseItemResp）。
func (c *Client) UseItem(itemID, count int32) (*gamev1.UseItemResp, error) {
	resp := &gamev1.UseItemResp{}
	req := &gamev1.UseItemReq{ItemUid: int64(itemID), Count: count}
	if err := c.call(codec.MsgUseItemReq, req, codec.MsgUseItemResp, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// MailList 邮件列表（MailListReq/MailListResp）。
func (c *Client) MailList() (*gamev1.MailListResp, error) {
	resp := &gamev1.MailListResp{}
	if err := c.call(codec.MsgMailListReq, &gamev1.MailListReq{}, codec.MsgMailListResp, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// TakeMail 领取邮件附件（MailTakeAttachmentReq/MailTakeAttachmentResp）。
func (c *Client) TakeMail(mailID int64) (*gamev1.MailTakeAttachmentResp, error) {
	resp := &gamev1.MailTakeAttachmentResp{}
	if err := c.call(codec.MsgMailTakeAttachmentReq, &gamev1.MailTakeAttachmentReq{MailId: mailID}, codec.MsgMailTakeAttachmentResp, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// RankList 榜单查询（RankListReq/RankListResp）。
//
// 中心服不可用时服务端回 code 6702 而不是空榜——调用方必须区分这两种情况。
func (c *Client) RankList(rankType string) (*gamev1.RankListResp, error) {
	resp := &gamev1.RankListResp{}
	if err := c.call(codec.MsgRankListReq, &gamev1.RankListReq{RankType: rankType}, codec.MsgRankListResp, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// ---------------- B5 场景（R9） ----------------

// SceneEnter 进入场景（SceneEnterReq/SceneEnterResp）。X/Y 为 0 时由服务端钳制。
func (c *Client) SceneEnter(sceneID, x, y int32) (*gamev1.SceneEnterResp, error) {
	resp := &gamev1.SceneEnterResp{}
	req := &gamev1.SceneEnterReq{SceneId: sceneID, X: x, Y: y}
	if err := c.call(codec.MsgSceneEnterReq, req, codec.MsgSceneEnterResp, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// SceneMove 移动到目标坐标（SceneMoveReq/SceneMoveResp）。Seq 由调用方给，回执带回。
func (c *Client) SceneMove(x, y, seq int32) (*gamev1.SceneMoveResp, error) {
	resp := &gamev1.SceneMoveResp{}
	req := &gamev1.SceneMoveReq{X: x, Y: y, Seq: seq}
	if err := c.call(codec.MsgSceneMoveReq, req, codec.MsgSceneMoveResp, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// SceneLeave 离开当前场景（SceneLeaveReq/SceneLeaveResp）。
func (c *Client) SceneLeave() (*gamev1.SceneLeaveResp, error) {
	resp := &gamev1.SceneLeaveResp{}
	if err := c.call(codec.MsgSceneLeaveReq, &gamev1.SceneLeaveReq{}, codec.MsgSceneLeaveResp, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// SceneSync 拉取当前场景全量视野（SceneSyncReq/SceneSyncResp）。
func (c *Client) SceneSync(sceneID int32) (*gamev1.SceneSyncResp, error) {
	resp := &gamev1.SceneSyncResp{}
	req := &gamev1.SceneSyncReq{SceneId: sceneID}
	if err := c.call(codec.MsgSceneSyncReq, req, codec.MsgSceneSyncResp, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// WaitSceneNotify 等待一条场景实体通知（SceneEntityNotify）。
func (c *Client) WaitSceneNotify(timeout time.Duration) (*gamev1.SceneEntityNotify, error) {
	pkt, ok, err := c.nextPush(codec.MsgSceneEntityNotify, timeout)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("no scene notify within %s", timeout)
	}
	_, _, plain, err := c.cipher.Decrypt(pkt.Body)
	if err != nil {
		return nil, err
	}
	msg := &gamev1.SceneEntityNotify{}
	if err := proto.Unmarshal(plain, msg); err != nil {
		return nil, err
	}
	return msg, nil
}

// ---------------- B1 等级 / 任务 ----------------

// QuestList 任务列表（QuestListReq/QuestListResp）。
func (c *Client) QuestList() (*gamev1.QuestListResp, error) {
	resp := &gamev1.QuestListResp{}
	if err := c.call(codec.MsgQuestListReq, &gamev1.QuestListReq{}, codec.MsgQuestListResp, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// QuestAccept 接取任务（QuestAcceptReq/QuestAcceptResp）。
func (c *Client) QuestAccept(questID int32) (*gamev1.QuestAcceptResp, error) {
	resp := &gamev1.QuestAcceptResp{}
	if err := c.call(codec.MsgQuestAcceptReq, &gamev1.QuestAcceptReq{QuestId: questID}, codec.MsgQuestAcceptResp, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// QuestClaim 领取任务奖励（QuestClaimReq/QuestClaimResp）。
func (c *Client) QuestClaim(questID int32) (*gamev1.QuestClaimResp, error) {
	resp := &gamev1.QuestClaimResp{}
	if err := c.call(codec.MsgQuestClaimReq, &gamev1.QuestClaimReq{QuestId: questID}, codec.MsgQuestClaimResp, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// ---------------- B2 装备 / 技能 ----------------

// EquipList 装备列表（EquipListReq/EquipListResp）。
func (c *Client) EquipList() (*gamev1.EquipListResp, error) {
	resp := &gamev1.EquipListResp{}
	if err := c.call(codec.MsgEquipListReq, &gamev1.EquipListReq{}, codec.MsgEquipListResp, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// EquipWear 穿戴装备（EquipWearReq/EquipWearResp）。
func (c *Client) EquipWear(uid int64) (*gamev1.EquipWearResp, error) {
	resp := &gamev1.EquipWearResp{}
	if err := c.call(codec.MsgEquipWearReq, &gamev1.EquipWearReq{Uid: uid}, codec.MsgEquipWearResp, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// EquipTakeOff 脱下装备（EquipTakeOffReq/EquipTakeOffResp）。
func (c *Client) EquipTakeOff(uid int64) (*gamev1.EquipTakeOffResp, error) {
	resp := &gamev1.EquipTakeOffResp{}
	if err := c.call(codec.MsgEquipTakeOffReq, &gamev1.EquipTakeOffReq{Uid: uid}, codec.MsgEquipTakeOffResp, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// SkillList 技能列表（SkillListReq/SkillListResp）。
func (c *Client) SkillList() (*gamev1.SkillListResp, error) {
	resp := &gamev1.SkillListResp{}
	if err := c.call(codec.MsgSkillListReq, &gamev1.SkillListReq{}, codec.MsgSkillListResp, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// SkillLearn 学习技能（SkillLearnReq/SkillLearnResp）。
func (c *Client) SkillLearn(skillID int32) (*gamev1.SkillLearnResp, error) {
	resp := &gamev1.SkillLearnResp{}
	if err := c.call(codec.MsgSkillLearnReq, &gamev1.SkillLearnReq{SkillId: skillID}, codec.MsgSkillLearnResp, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// SkillUpgrade 升级技能（SkillUpgradeReq/SkillUpgradeResp）。
func (c *Client) SkillUpgrade(skillID int32) (*gamev1.SkillUpgradeResp, error) {
	resp := &gamev1.SkillUpgradeResp{}
	if err := c.call(codec.MsgSkillUpgradeReq, &gamev1.SkillUpgradeReq{SkillId: skillID}, codec.MsgSkillUpgradeResp, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// ---------------- B3 聊天 ----------------

// ChatSend 发送聊天（ChatSendReq/ChatSendResp）。
func (c *Client) ChatSend(channel int32, peerID int64, content string) (*gamev1.ChatSendResp, error) {
	resp := &gamev1.ChatSendResp{}
	req := &gamev1.ChatSendReq{Channel: channel, PeerId: peerID, Content: content}
	if err := c.call(codec.MsgChatSendReq, req, codec.MsgChatSendResp, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// ChatHistory 拉取聊天历史（ChatHistoryReq/ChatHistoryResp）。
func (c *Client) ChatHistory(peerID int64, limit int32) (*gamev1.ChatHistoryResp, error) {
	resp := &gamev1.ChatHistoryResp{}
	req := &gamev1.ChatHistoryReq{PeerId: peerID, Limit: limit}
	if err := c.call(codec.MsgChatHistoryReq, req, codec.MsgChatHistoryResp, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// nextPush 取一条指定协议号的推送：优先消费缓存，没有再读连接。
func (c *Client) nextPush(msgID uint32, timeout time.Duration) (codec.Packet, bool, error) {
	for i, p := range c.pendingPush {
		if uint32(p.MsgID) == msgID {
			c.pendingPush = append(c.pendingPush[:i], c.pendingPush[i+1:]...)
			return p, true, nil
		}
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		pkt, err := c.readPacket(time.Until(deadline))
		if err != nil {
			return codec.Packet{}, false, err
		}
		if pkt.MsgID == msgID {
			return pkt, true, nil
		}
		c.pendingPush = append(c.pendingPush, pkt)
	}
	return codec.Packet{}, false, nil
}

// WaitChatNotify 等待一条聊天推送（ChatMessageNotify），用于验证跨服投递。
func (c *Client) WaitChatNotify(timeout time.Duration) (*gamev1.ChatMessageNotify, error) {
	pkt, ok, err := c.nextPush(codec.MsgChatMessageNotify, timeout)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("no chat notify within %s", timeout)
	}
	_, _, plain, err := c.cipher.Decrypt(pkt.Body)
	if err != nil {
		return nil, err
	}
	msg := &gamev1.ChatMessageNotify{}
	if err := proto.Unmarshal(plain, msg); err != nil {
		return nil, err
	}
	return msg, nil
}

// ---------------- B4 公会 / 组队 / 副本 / 商店 / 活动 ----------------

// GuildInfo 公会信息（GuildInfoReq/GuildInfoResp）。
func (c *Client) GuildInfo() (*gamev1.GuildInfoResp, error) {
	resp := &gamev1.GuildInfoResp{}
	if err := c.call(codec.MsgGuildInfoReq, &gamev1.GuildInfoReq{}, codec.MsgGuildInfoResp, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// GuildCreate 创建公会（GuildCreateReq/GuildCreateResp）。
func (c *Client) GuildCreate(name string) (*gamev1.GuildCreateResp, error) {
	resp := &gamev1.GuildCreateResp{}
	if err := c.call(codec.MsgGuildCreateReq, &gamev1.GuildCreateReq{Name: name}, codec.MsgGuildCreateResp, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// GuildJoin 加入公会（GuildJoinReq/GuildJoinResp）。
func (c *Client) GuildJoin(guildID int64) (*gamev1.GuildJoinResp, error) {
	resp := &gamev1.GuildJoinResp{}
	if err := c.call(codec.MsgGuildJoinReq, &gamev1.GuildJoinReq{GuildId: guildID}, codec.MsgGuildJoinResp, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// GuildQuit 退出公会（GuildQuitReq/GuildQuitResp）。
func (c *Client) GuildQuit() (*gamev1.GuildQuitResp, error) {
	resp := &gamev1.GuildQuitResp{}
	if err := c.call(codec.MsgGuildQuitReq, &gamev1.GuildQuitReq{}, codec.MsgGuildQuitResp, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// TeamInfo 队伍信息（TeamInfoReq/TeamInfoResp）。
func (c *Client) TeamInfo() (*gamev1.TeamInfoResp, error) {
	resp := &gamev1.TeamInfoResp{}
	if err := c.call(codec.MsgTeamInfoReq, &gamev1.TeamInfoReq{}, codec.MsgTeamInfoResp, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// TeamCreate 创建队伍（TeamCreateReq/TeamCreateResp）。
func (c *Client) TeamCreate(copyID int32) (*gamev1.TeamCreateResp, error) {
	resp := &gamev1.TeamCreateResp{}
	if err := c.call(codec.MsgTeamCreateReq, &gamev1.TeamCreateReq{CopyId: copyID}, codec.MsgTeamCreateResp, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// TeamJoin 加入队伍（TeamJoinReq/TeamJoinResp）。
func (c *Client) TeamJoin(teamID int64) (*gamev1.TeamJoinResp, error) {
	resp := &gamev1.TeamJoinResp{}
	if err := c.call(codec.MsgTeamJoinReq, &gamev1.TeamJoinReq{TeamId: teamID}, codec.MsgTeamJoinResp, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// TeamLeave 退出队伍（TeamLeaveReq/TeamLeaveResp）。
func (c *Client) TeamLeave() (*gamev1.TeamLeaveResp, error) {
	resp := &gamev1.TeamLeaveResp{}
	if err := c.call(codec.MsgTeamLeaveReq, &gamev1.TeamLeaveReq{}, codec.MsgTeamLeaveResp, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// CopyList 副本列表（CopyListReq/CopyListResp）。
func (c *Client) CopyList() (*gamev1.CopyListResp, error) {
	resp := &gamev1.CopyListResp{}
	if err := c.call(codec.MsgCopyListReq, &gamev1.CopyListReq{}, codec.MsgCopyListResp, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// CopySettle 副本结算（CopySettleReq/CopySettleResp）。
func (c *Client) CopySettle(copyID, star int32) (*gamev1.CopySettleResp, error) {
	resp := &gamev1.CopySettleResp{}
	if err := c.call(codec.MsgCopySettleReq, &gamev1.CopySettleReq{CopyId: copyID, Star: star}, codec.MsgCopySettleResp, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// ShopList 商店列表（ShopListReq/ShopListResp）。
func (c *Client) ShopList() (*gamev1.ShopListResp, error) {
	resp := &gamev1.ShopListResp{}
	if err := c.call(codec.MsgShopListReq, &gamev1.ShopListReq{}, codec.MsgShopListResp, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// ShopBuy 购买商品（ShopBuyReq/ShopBuyResp）。
func (c *Client) ShopBuy(goodsID, count int32) (*gamev1.ShopBuyResp, error) {
	resp := &gamev1.ShopBuyResp{}
	if err := c.call(codec.MsgShopBuyReq, &gamev1.ShopBuyReq{GoodsId: goodsID, Count: count}, codec.MsgShopBuyResp, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// SigninInfo 查询当前角色七日签到状态（SigninInfoReq/SigninInfoResp）。
func (c *Client) SigninInfo() (*gamev1.SigninInfoResp, error) {
	resp := &gamev1.SigninInfoResp{}
	if err := c.call(codec.MsgSigninInfoReq, &gamev1.SigninInfoReq{}, codec.MsgSigninInfoResp, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// SigninClaim 领取今日签到奖励（SigninClaimReq/SigninClaimResp），重复请求不会再次发奖。
func (c *Client) SigninClaim() (*gamev1.SigninClaimResp, error) {
	resp := &gamev1.SigninClaimResp{}
	if err := c.call(codec.MsgSigninClaimReq, &gamev1.SigninClaimReq{}, codec.MsgSigninClaimResp, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// ActivityList 活动列表（ActivityListReq/ActivityListResp）。
func (c *Client) ActivityList() (*gamev1.ActivityListResp, error) {
	resp := &gamev1.ActivityListResp{}
	if err := c.call(codec.MsgActivityListReq, &gamev1.ActivityListReq{}, codec.MsgActivityListResp, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// ActivityClaim 活动领奖（ActivityClaimReq/ActivityClaimResp）。
func (c *Client) ActivityClaim(activityID int32) (*gamev1.ActivityClaimResp, error) {
	resp := &gamev1.ActivityClaimResp{}
	if err := c.call(codec.MsgActivityClaimReq, &gamev1.ActivityClaimReq{ActivityId: activityID}, codec.MsgActivityClaimResp, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// ---------------- P13 战斗 / 跨服 ----------------

// Match 发起匹配（MatchReq/MatchResp）。
func (c *Client) Match(mode string) (*gamev1.MatchResp, error) {
	resp := &gamev1.MatchResp{}
	if err := c.call(codec.MsgMatchReq, &gamev1.MatchReq{Mode: mode}, codec.MsgMatchResp, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// WaitBattleStart 等待开战通知（BattleStartNotify 推送）。
func (c *Client) WaitBattleStart(timeout time.Duration) (*gamev1.BattleStartNotify, error) {
	pkt, ok, err := c.nextPush(codec.MsgBattleStartNotify, timeout)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("no battle start within %s", timeout)
	}
	_, _, plain, err := c.cipher.Decrypt(pkt.Body)
	if err != nil {
		return nil, err
	}
	msg := &gamev1.BattleStartNotify{}
	if err := proto.Unmarshal(plain, msg); err != nil {
		return nil, err
	}
	return msg, nil
}

// BattleOp 上报战斗操作（BattleOpReport，无响应）。
func (c *Client) BattleOp(roomID uint64, msgID uint32, body []byte) error {
	req := &gamev1.BattleOpReport{RoomId: roomID, MsgId: msgID, Body: body}
	raw, err := proto.Marshal(req)
	if err != nil {
		return err
	}
	enc, seq, err := c.cipher.Encrypt(codec.MsgBattleOpReport, raw)
	if err != nil {
		return err
	}
	return c.writePacket(codec.Packet{MsgID: codec.MsgBattleOpReport, Seq: seq, Body: enc})
}

// WaitBattleSync 等待对手战斗事件（BattleSyncNotify 推送）。
func (c *Client) WaitBattleSync(timeout time.Duration) (*gamev1.BattleSyncNotify, error) {
	pkt, ok, err := c.nextPush(codec.MsgBattleSyncNotify, timeout)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("no battle sync within %s", timeout)
	}
	_, _, plain, err := c.cipher.Decrypt(pkt.Body)
	if err != nil {
		return nil, err
	}
	msg := &gamev1.BattleSyncNotify{}
	if err := proto.Unmarshal(plain, msg); err != nil {
		return nil, err
	}
	return msg, nil
}

// BattleResult 上报战斗结论（BattleResultReport/BattleSettleNotify）。
func (c *Client) BattleResult(roomID uint64, token string, result int32) (*gamev1.BattleSettleNotify, error) {
	resp := &gamev1.BattleSettleNotify{}
	req := &gamev1.BattleResultReport{
		RoomId:      roomID,
		BattleToken: token,
		Result:      result,
		FinishedAt:  time.Now().Unix(),
	}
	if err := c.call(codec.MsgBattleResultReport, req, codec.MsgBattleSettleNotify, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// WaitBattleSettle 等待结算推送（BattleSettleNotify 推送，兜底路径）。
func (c *Client) WaitBattleSettle(timeout time.Duration) (*gamev1.BattleSettleNotify, error) {
	pkt, ok, err := c.nextPush(codec.MsgBattleSettleNotify, timeout)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("no battle settle within %s", timeout)
	}
	_, _, plain, err := c.cipher.Decrypt(pkt.Body)
	if err != nil {
		return nil, err
	}
	msg := &gamev1.BattleSettleNotify{}
	if err := proto.Unmarshal(plain, msg); err != nil {
		return nil, err
	}
	return msg, nil
}
