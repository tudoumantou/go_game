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
	pubPEM, salt := splitPEMAndSalt(pkt.Body)
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

// splitPEMAndSalt 从 ServerHello body 中拆出 PEM 与 salt。
func splitPEMAndSalt(body []byte) (pem, salt []byte) {
	// PEM 以 "-----END RSA PUBLIC KEY-----" 结尾
	end := []byte("-----END RSA PUBLIC KEY-----")
	idx := bytes.Index(body, end)
	if idx < 0 {
		return nil, nil
	}
	pemEnd := idx + len(end)
	// 跳过尾随换行
	for pemEnd < len(body) && (body[pemEnd] == '\n' || body[pemEnd] == '\r') {
		pemEnd++
	}
	return body[:pemEnd], body[pemEnd:]
}

// call 发送请求并等待指定响应（跳过中间推送）。
func (c *Client) call(msgID uint32, req proto.Message, respID uint32, resp proto.Message) error {
	body, err := proto.Marshal(req)
	if err != nil {
		return err
	}
	enc, seq, err := c.cipher.Encrypt(uint16(msgID), body)
	if err != nil {
		return err
	}
	if err := c.writePacket(codec.Packet{MsgID: uint16(msgID), Seq: seq, Body: enc}); err != nil {
		return err
	}

	for i := 0; i < 8; i++ {
		pkt, err := c.readPacket(5 * time.Second)
		if err != nil {
			return fmt.Errorf("wait resp %d: %w", respID, err)
		}
		if uint32(pkt.MsgID) != respID {
			continue // 推送或其它消息，跳过
		}
		_, _, plain, err := c.cipher.Decrypt(pkt.Body)
		if err != nil {
			return err
		}
		return proto.Unmarshal(plain, resp)
	}
	return fmt.Errorf("no response for msgID %d (expect %d)", msgID, respID)
}

// EnterGame 进游戏（1010/1011）。
func (c *Client) EnterGame() (*gamev1.EnterGameResp, error) {
	resp := &gamev1.EnterGameResp{}
	if err := c.call(1010, &gamev1.EnterGameReq{}, 1011, resp); err != nil {
		return nil, err
	}
	if resp.RoleId > 0 {
		c.PlayerID = resp.RoleId
	}
	return resp, nil
}

// CreateRole 创角（1020/1021）。
func (c *Client) CreateRole(name string, career int32) (*gamev1.CreateRoleResp, error) {
	resp := &gamev1.CreateRoleResp{}
	req := &gamev1.CreateRoleReq{Name: name, Career: career}
	if err := c.call(1020, req, 1021, resp); err != nil {
		return nil, err
	}
	if resp.RoleId > 0 {
		c.PlayerID = resp.RoleId
	}
	return resp, nil
}

// PlayerInfo 拉取玩家信息（2001/2002）。
func (c *Client) PlayerInfo() (*gamev1.PlayerInfoResp, error) {
	resp := &gamev1.PlayerInfoResp{}
	if err := c.call(2001, &gamev1.PlayerInfoReq{}, 2002, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// BagList 背包列表（3003/3004）。
func (c *Client) BagList() (*gamev1.BagListResp, error) {
	resp := &gamev1.BagListResp{}
	if err := c.call(3003, &gamev1.BagListReq{}, 3004, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// UseItem 使用道具（3001/3002）。
func (c *Client) UseItem(itemID, count int32) (*gamev1.UseItemResp, error) {
	resp := &gamev1.UseItemResp{}
	req := &gamev1.UseItemReq{ItemUid: int64(itemID), Count: count}
	if err := c.call(3001, req, 3002, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// MailList 邮件列表（7001/7002）。
func (c *Client) MailList() (*gamev1.MailListResp, error) {
	resp := &gamev1.MailListResp{}
	if err := c.call(7001, &gamev1.MailListReq{}, 7002, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// TakeMail 领取邮件附件（7020/7021）。
func (c *Client) TakeMail(mailID int64) (*gamev1.MailTakeAttachmentResp, error) {
	resp := &gamev1.MailTakeAttachmentResp{}
	if err := c.call(7020, &gamev1.MailTakeAttachmentReq{MailId: mailID}, 7021, resp); err != nil {
		return nil, err
	}
	return resp, nil
}
