// internal/game/module/chat/chat_test.go —— 聊天模块单测
//
// 私聊是「发送 → 三路投递 → 历史」的编排：任何一路走错，
// 表现都是「消息发出去对方收不到」或「收到了但历史里没有」。
// 这里用 fake messenger 把三路互斥关系钉死，用 fake repo 盯住落库方向与次数。
package chat

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"game_study/internal/common/errcode"
	"game_study/internal/game/module"
	"game_study/internal/game/module/moduletest"
	"game_study/pkg/codec"
	"game_study/pkg/log"
	pkgmq "game_study/pkg/mq"
	gamev1 "game_study/pkg/pb/game/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// ============================================================================
// 测试替身
// ============================================================================

type insertCall struct {
	OwnerID, PeerID, MsgID int64
	Direction, Channel     int32
	Content                string
}

type fakeRepo struct {
	inserts []insertCall
	lines   []ChatLine
	insErr  error
	listErr error
}

func (f *fakeRepo) Insert(_ context.Context, ownerID, peerID, msgID int64, direction, channel int32, content string) error {
	f.inserts = append(f.inserts, insertCall{ownerID, peerID, msgID, direction, channel, content})
	return f.insErr
}

func (f *fakeRepo) ListRecent(_ context.Context, _, _ int64, _ int) ([]ChatLine, error) {
	return f.lines, f.listErr
}

type pushCall struct {
	PlayerID int64
	MsgID    uint32
	Body     []byte
}

type fakeMessenger struct {
	self   string
	node   string
	online bool

	pushed     []pushCall
	crossed    []*pkgmq.ChatCrossMsg
	pended     []*pkgmq.ChatCrossMsg
	pushOK     bool
	produceErr error
	pendErr    error
}

func (f *fakeMessenger) SelfNode() string { return f.self }

func (f *fakeMessenger) OnlineNode(context.Context, int64) (string, bool) {
	return f.node, f.online
}

func (f *fakeMessenger) Push(_ context.Context, playerID int64, msgID uint32, body []byte) bool {
	f.pushed = append(f.pushed, pushCall{playerID, msgID, append([]byte(nil), body...)})
	return f.pushOK
}

func (f *fakeMessenger) ProduceCross(_ context.Context, msg *pkgmq.ChatCrossMsg) error {
	f.crossed = append(f.crossed, msg)
	return f.produceErr
}

func (f *fakeMessenger) PendOffline(_ context.Context, _ int64, msg *pkgmq.ChatCrossMsg) error {
	f.pended = append(f.pended, msg)
	return f.pendErr
}

// ============================================================================
// 装配
// ============================================================================

func newTestModule(repo chatRepo, msg Messenger) (*Module, *moduletest.State, *moduletest.Recorder, *module.Context) {
	st := moduletest.NewState(100)
	st.SetProfile(module.Profile{Name: "阿甲"})
	rec := moduletest.NewRecorder()
	m := New(repo, msg, log.New("chat-test", log.WithLevel("error")))
	return m, st, rec, moduletest.NewContext(st, rec)
}

func send(t *testing.T, m *Module, ctx *module.Context, rec *moduletest.Recorder, req *gamev1.ChatSendReq) *gamev1.ChatSendResp {
	t.Helper()
	raw, err := moduletest.Call(m, ctx, rec, codec.MsgChatSendReq, codec.MsgChatSendResp, req)
	require.NoError(t, err)
	require.NotNil(t, raw, "发送没有回包")
	resp := &gamev1.ChatSendResp{}
	require.NoError(t, proto.Unmarshal(raw, resp))
	return resp
}

func history(t *testing.T, m *Module, ctx *module.Context, rec *moduletest.Recorder, req *gamev1.ChatHistoryReq) *gamev1.ChatHistoryResp {
	t.Helper()
	raw, err := moduletest.Call(m, ctx, rec, codec.MsgChatHistoryReq, codec.MsgChatHistoryResp, req)
	require.NoError(t, err)
	require.NotNil(t, raw, "历史没有回包")
	resp := &gamev1.ChatHistoryResp{}
	require.NoError(t, proto.Unmarshal(raw, resp))
	return resp
}

// ============================================================================
// 发送校验
// ============================================================================

// TestSendRejectsClosedChannel P6 只开放私聊；其他频道明确回 4001，
// 且不能落库、不能投递。
func TestSendRejectsClosedChannel(t *testing.T) {
	repo := &fakeRepo{}
	msg := &fakeMessenger{self: "game-01"}
	m, _, rec, ctx := newTestModule(repo, msg)

	resp := send(t, m, ctx, rec, &gamev1.ChatSendReq{Channel: 1, PeerId: 200, Content: "hi"})
	require.EqualValues(t, int32(errcode.ErrChannelClosed), resp.Code)
	require.Empty(t, repo.inserts, "频道关闭不能落历史")
	require.Empty(t, msg.pushed)
	require.Empty(t, msg.crossed)
	require.Empty(t, msg.pended)
}

// TestSendAllowsZeroChannelAsPrivate 老客户端不传 channel，0 按私聊兼容。
func TestSendAllowsZeroChannelAsPrivate(t *testing.T) {
	repo := &fakeRepo{}
	msg := &fakeMessenger{self: "game-01", online: false}
	m, _, rec, ctx := newTestModule(repo, msg)

	resp := send(t, m, ctx, rec, &gamev1.ChatSendReq{Channel: 0, PeerId: 200, Content: "hi"})
	require.EqualValues(t, int32(errcode.OK), resp.Code)
	require.Len(t, repo.inserts, 1, "channel=0 应走私聊并落 out 历史")
}

// TestSendRejectsEmptyContentAndBadPeer 空内容与非正对端都回 4005。
func TestSendRejectsEmptyContentAndBadPeer(t *testing.T) {
	for _, c := range []*gamev1.ChatSendReq{
		{Channel: channelPrivate, PeerId: 200, Content: ""},
		{Channel: channelPrivate, PeerId: 0, Content: "hi"},
		{Channel: channelPrivate, PeerId: -5, Content: "hi"},
	} {
		repo := &fakeRepo{}
		m, _, rec, ctx := newTestModule(repo, &fakeMessenger{self: "game-01"})
		resp := send(t, m, ctx, rec, c)
		require.EqualValues(t, int32(errcode.ErrContentEmpty), resp.Code, fmt.Sprintf("req=%+v", c))
		require.Empty(t, repo.inserts, "校验失败不能落库")
	}
}

// ============================================================================
// 三路投递（互斥）
// ============================================================================

// TestSendOfflinePends 对端离线：只暂存，不落 in、不推送、不走 Kafka。
func TestSendOfflinePends(t *testing.T) {
	repo := &fakeRepo{}
	msg := &fakeMessenger{self: "game-01", online: false}
	m, _, rec, ctx := newTestModule(repo, msg)

	resp := send(t, m, ctx, rec, &gamev1.ChatSendReq{Channel: channelPrivate, PeerId: 200, Content: "在吗"})
	require.EqualValues(t, int32(errcode.OK), resp.Code)
	require.Len(t, msg.pended, 1, "离线必须暂存")
	require.EqualValues(t, 200, msg.pended[0].ToID)
	require.EqualValues(t, "在吗", msg.pended[0].Content)
	require.Empty(t, msg.pushed)
	require.Empty(t, msg.crossed)

	// 只落发送方 out 那一行
	require.Len(t, repo.inserts, 1)
	require.EqualValues(t, DirectionOut, repo.inserts[0].Direction)
	require.EqualValues(t, 100, repo.inserts[0].OwnerID)
	require.EqualValues(t, 200, repo.inserts[0].PeerID)
}

// TestSendLocalOnlinePushes 对端在本服：落 in 历史 + 推送，不走 Kafka/暂存。
func TestSendLocalOnlinePushes(t *testing.T) {
	repo := &fakeRepo{}
	msg := &fakeMessenger{self: "game-01", node: "game-01", online: true, pushOK: true}
	m, _, rec, ctx := newTestModule(repo, msg)

	resp := send(t, m, ctx, rec, &gamev1.ChatSendReq{Channel: channelPrivate, PeerId: 200, Content: "在吗"})
	require.EqualValues(t, int32(errcode.OK), resp.Code)
	require.NotEmpty(t, resp.MsgId, "回包要带 msgID，客户端据此回显")
	require.NotZero(t, resp.SentAt)

	require.Len(t, msg.pushed, 1, "本服在线必须直推")
	require.EqualValues(t, 200, msg.pushed[0].PlayerID)
	require.EqualValues(t, codec.MsgChatMessageNotify, msg.pushed[0].MsgID)
	require.Empty(t, msg.crossed)
	require.Empty(t, msg.pended)

	// 两条历史：发送方 out + 接收方 in，共用同一个 msgID
	require.Len(t, repo.inserts, 2)
	require.EqualValues(t, DirectionOut, repo.inserts[0].Direction)
	require.EqualValues(t, DirectionIn, repo.inserts[1].Direction)
	require.EqualValues(t, 200, repo.inserts[1].OwnerID, "in 历史归属接收方")
	require.EqualValues(t, repo.inserts[0].MsgID, repo.inserts[1].MsgID, "两行必须同一个 msgID")

	var notify gamev1.ChatMessageNotify
	require.NoError(t, proto.Unmarshal(msg.pushed[0].Body, &notify))
	require.EqualValues(t, 100, notify.SenderId, "身份取会话，不信协议体")
	require.EqualValues(t, "阿甲", notify.SenderName)
	require.EqualValues(t, "在吗", notify.Content)
}

// TestSendCrossNodeProducesKafka 对端在别的节点：走 Kafka，且毫秒换算只此一处。
func TestSendCrossNodeProducesKafka(t *testing.T) {
	repo := &fakeRepo{}
	msg := &fakeMessenger{self: "game-01", node: "game-02", online: true}
	m, _, rec, ctx := newTestModule(repo, msg)

	resp := send(t, m, ctx, rec, &gamev1.ChatSendReq{Channel: channelPrivate, PeerId: 200, Content: "跨服"})
	require.EqualValues(t, int32(errcode.OK), resp.Code)

	require.Len(t, msg.crossed, 1, "跨节点必须走 Kafka")
	require.EqualValues(t, 200, msg.crossed[0].ToID)
	require.EqualValues(t, "game-02", msg.node)
	require.EqualValues(t, resp.SentAt*1000, msg.crossed[0].SendTime, "MQ 用毫秒，协议用秒")
	require.Empty(t, msg.pushed, "跨节点不能直接推本地会话")
	require.Empty(t, msg.pended)
	require.Len(t, repo.inserts, 1, "跨服时本地只落 out，in 由对端节点落")
}

// TestSendNoMessengerDegrades messenger 为 nil（单机降级）时只落库，不 panic。
func TestSendNoMessengerDegrades(t *testing.T) {
	repo := &fakeRepo{}
	m, _, rec, ctx := newTestModule(repo, nil)

	resp := send(t, m, ctx, rec, &gamev1.ChatSendReq{Channel: channelPrivate, PeerId: 200, Content: "hi"})
	require.EqualValues(t, int32(errcode.OK), resp.Code)
	require.Len(t, repo.inserts, 1)
}

// TestSendNoRepoDegrades repo 为 nil（无库环境）时仍要投递，只是不留历史。
func TestSendNoRepoDegrades(t *testing.T) {
	msg := &fakeMessenger{self: "game-01", node: "game-01", online: true, pushOK: true}
	m, _, rec, ctx := newTestModule(nil, msg)

	resp := send(t, m, ctx, rec, &gamev1.ChatSendReq{Channel: channelPrivate, PeerId: 200, Content: "hi"})
	require.EqualValues(t, int32(errcode.OK), resp.Code)
	require.Len(t, msg.pushed, 1, "没库也要推送")
}

// TestSendSurvivesDeliveryFailures 落库/暂存/生产失败都不影响回包。
//
// 私聊是尽力而为链路：回包只表示「服务端已受理」，失败的那一路由
// 后续补投（离线队列）或 broker 重试兜底。
func TestSendSurvivesDeliveryFailures(t *testing.T) {
	repo := &fakeRepo{insErr: context.DeadlineExceeded}
	msg := &fakeMessenger{
		self: "game-01", online: false,
		pendErr: context.DeadlineExceeded, produceErr: context.DeadlineExceeded,
	}
	m, _, rec, ctx := newTestModule(repo, msg)

	require.EqualValues(t, int32(errcode.OK), send(t, m, ctx, rec, &gamev1.ChatSendReq{
		Channel: channelPrivate, PeerId: 200, Content: "hi",
	}).Code, "落库/暂存失败不能把发送判失败")

	require.Equal(t, 1, rec.Count(codec.MsgChatSendResp), "一次请求只能回一个包")
}

// ============================================================================
// 历史
// ============================================================================

// TestHistoryReversesAndRestoresSides 倒序查出、正序回包，并按 direction 还原收发方。
func TestHistoryReversesAndRestoresSides(t *testing.T) {
	repo := &fakeRepo{lines: []ChatLine{
		{MsgID: 3, Direction: DirectionIn, Content: "第三条"},
		{MsgID: 2, Direction: DirectionOut, Content: "第二条"},
		{MsgID: 1, Direction: DirectionIn, Content: "第一条"},
	}}
	m, _, rec, ctx := newTestModule(repo, nil)

	resp := history(t, m, ctx, rec, &gamev1.ChatHistoryReq{PeerId: 200, Limit: 10})
	require.EqualValues(t, int32(errcode.OK), resp.Code)
	require.Len(t, resp.Messages, 3)
	require.EqualValues(t, "1", resp.Messages[0].MsgId, "回包必须按时间正序")
	require.EqualValues(t, "3", resp.Messages[2].MsgId)

	// in：发送方是对端，接收方是自己
	require.EqualValues(t, 200, resp.Messages[0].SenderId)
	require.EqualValues(t, 100, resp.Messages[0].PeerId)
	// out：发送方是自己，接收方是对端
	require.EqualValues(t, 100, resp.Messages[1].SenderId)
	require.EqualValues(t, 200, resp.Messages[1].PeerId)

	require.False(t, resp.HasMore, "3 条 < 10，不该提示还有更多")
}

// TestHistoryHasMoreWhenPageFull 取满一页即提示可能还有更多。
func TestHistoryHasMoreWhenPageFull(t *testing.T) {
	repo := &fakeRepo{lines: []ChatLine{
		{MsgID: 2, Direction: DirectionOut, Content: "b"},
		{MsgID: 1, Direction: DirectionOut, Content: "a"},
	}}
	m, _, rec, ctx := newTestModule(repo, nil)

	resp := history(t, m, ctx, rec, &gamev1.ChatHistoryReq{PeerId: 200, Limit: 2})
	require.True(t, resp.HasMore)
}

// TestHistoryNoRepoDegrades 无库环境回空列表而不是报错。
func TestHistoryNoRepoDegrades(t *testing.T) {
	m, _, rec, ctx := newTestModule(nil, nil)
	resp := history(t, m, ctx, rec, &gamev1.ChatHistoryReq{PeerId: 200, Limit: 10})
	require.EqualValues(t, int32(errcode.OK), resp.Code)
	require.Empty(t, resp.Messages)
}

// TestHistoryRepoErrorPropagates 查库真故障要上抛（不能让客户端以为「没有历史」）。
func TestHistoryRepoErrorPropagates(t *testing.T) {
	repo := &fakeRepo{listErr: context.DeadlineExceeded}
	m, _, rec, ctx := newTestModule(repo, nil)

	_, err := moduletest.Call(m, ctx, rec, codec.MsgChatHistoryReq, codec.MsgChatHistoryResp,
		&gamev1.ChatHistoryReq{PeerId: 200, Limit: 10})
	require.Error(t, err)
}

// ============================================================================
// 跨服来讯
// ============================================================================

// TestDeliverInboundWritesInDirection 跨服消费落地时只写 in 方向。
func TestDeliverInboundWritesInDirection(t *testing.T) {
	repo := &fakeRepo{}
	m, _, _, _ := newTestModule(repo, nil)

	err := m.DeliverInbound(context.Background(), &pkgmq.ChatCrossMsg{
		MsgID: 9, FromID: 200, ToID: 100, Channel: channelPrivate, Content: "来了",
	})
	require.NoError(t, err)
	require.Len(t, repo.inserts, 1)
	require.EqualValues(t, 100, repo.inserts[0].OwnerID, "落地归属接收方")
	require.EqualValues(t, 200, repo.inserts[0].PeerID)
	require.EqualValues(t, DirectionIn, repo.inserts[0].Direction)
	require.EqualValues(t, 9, repo.inserts[0].MsgID)
}

// TestDeliverInboundNoRepoIsNoop 无库环境静默跳过，不能阻断 Kafka 消费。
func TestDeliverInboundNoRepoIsNoop(t *testing.T) {
	m, _, _, _ := newTestModule(nil, nil)
	require.NoError(t, m.DeliverInbound(context.Background(), &pkgmq.ChatCrossMsg{MsgID: 1}))
}

// ============================================================================
// 其他
// ============================================================================

// TestNextMsgIDBitLayout 位域正确 + 时间戳单调。
//
// 这里**不**断言「N 条互不相同」：低 20 位是随机段，唯一性是概率性的
// （每毫秒 1000 条约 48% 会碰撞，见 nextMsgID 注释）。用 1000 条连发去断言
// 唯一，是在测一个实现本来就没承诺的性质——写的时候连续跑 50 次能撞 3 次。
//
// 真正该测的是位域边界：随机段不能侵占时间戳（侵占了 ID 会提前溢出成负数，
// MySQL 主键变负值，Kafka 分区键也乱）。
func TestNextMsgIDBitLayout(t *testing.T) {
	const mask = int64(0xFFFFF)

	before := time.Now().UnixMilli()
	var prev int64
	for i := 0; i < 200; i++ {
		id := nextMsgID()
		require.Greater(t, id, int64(0), "不能溢出成负数")
		require.GreaterOrEqual(t, id&mask, int64(0), "随机段必须非负")
		require.LessOrEqual(t, id&mask, mask, "随机段不能超过 20 位")

		ms := id >> 20
		require.GreaterOrEqual(t, ms, before, "高位必须是当前或之后的毫秒时间戳")
		require.GreaterOrEqual(t, ms, prev, "毫秒时间戳必须单调不减")
		prev = ms
	}
	after := time.Now().UnixMilli()
	require.LessOrEqual(t, prev, after, "高位不能跑到未来（说明时间戳被随机段污染）")
}

// TestOnLogoutMarksDirty 下线标脏（保持与其他模块一致的生命周期行为）。
func TestOnLogoutMarksDirty(t *testing.T) {
	m, st, _, ctx := newTestModule(nil, nil)
	require.NoError(t, m.OnLogout(ctx))
	require.Contains(t, st.DirtyModules(), "chat")
}

// TestSendRejectsTooLongContent 内容超长直接拒绝，不落库不投递。
//
// 两条上限分开测：中文按「字符」算（200 字中文 = 600 字节，合法的），
// 而极端 emoji 组合按「字节」兜底（视觉上很短但字节数超线）。
func TestSendRejectsTooLongContent(t *testing.T) {
	cases := []struct {
		name    string
		content string
	}{
		{"201 个中文字符", strings.Repeat("龙", maxContentRunes+1)},
		{"超长 ASCII", strings.Repeat("a", maxContentBytes+1)},
		{"emoji 组合撑爆字节", strings.Repeat("👨‍👩‍👧‍👦", 40)}, // 每个约 25 字节
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			repo := &fakeRepo{}
			msg := &fakeMessenger{self: "game-01", online: false}
			m, _, rec, ctx := newTestModule(repo, msg)

			resp := send(t, m, ctx, rec, &gamev1.ChatSendReq{
				Channel: channelPrivate, PeerId: 200, Content: c.content,
			})
			require.EqualValues(t, int32(errcode.ErrContentTooLong), resp.Code,
				fmt.Sprintf("len=%d runes=%d", len(c.content), utf8.RuneCountInString(c.content)))
			require.Empty(t, repo.inserts, "超长内容不能落库")
			require.Empty(t, msg.pended, "超长内容不能投递")
		})
	}
}

// TestSendAcceptsBoundaryContent 边界值必须放行：正好 200 字中文合法。
//
// 上限写错一个等号（> 写成 >=）会让玩家在最常见的长度上突然发不出去。
func TestSendAcceptsBoundaryContent(t *testing.T) {
	content := strings.Repeat("龙", maxContentRunes) // 600 字节，200 字符

	repo := &fakeRepo{}
	m, _, rec, ctx := newTestModule(repo, &fakeMessenger{self: "game-01"})
	resp := send(t, m, ctx, rec, &gamev1.ChatSendReq{
		Channel: channelPrivate, PeerId: 200, Content: content,
	})
	require.EqualValues(t, int32(errcode.OK), resp.Code, "正好 200 字必须允许")
	require.Len(t, repo.inserts, 1)
}

// clock 可推进的测试时钟（限流器要「过一秒」才能再发，不能真睡）。
type clock struct {
	mu sync.Mutex
	t  time.Time
}

func newClock() *clock { return &clock{t: time.Unix(1700000000, 0)} }

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// TestSendRateLimited 连发超过突发额度后回 4003，且不再落库/投递。
//
// 频控的价值在于「单条成本」：聊天每条都要写 MySQL、推对手、进 Kafka。
// 网关的包级限流只挡总量，挡不住「每秒一条、永不停止」。
func TestSendRateLimited(t *testing.T) {
	repo := &fakeRepo{}
	msg := &fakeMessenger{self: "game-01"}
	m, _, rec, ctx := newTestModule(repo, msg)

	clk := newClock()
	m.chatLimiter.SetClock(clk.Now)

	req := &gamev1.ChatSendReq{Channel: channelPrivate, PeerId: 200, Content: "hi"}
	for i := 0; i < 3; i++ { // 默认突发 3
		resp := send(t, m, ctx, rec, req)
		require.EqualValues(t, int32(errcode.OK), resp.Code, "第 %d 条应放行", i+1)
	}

	resp := send(t, m, ctx, rec, req)
	require.EqualValues(t, int32(errcode.ErrChatTooFast), resp.Code, "超过突发额度必须回 4003")
	require.Len(t, repo.inserts, 3, "被限流的消息不能落库")
	require.Len(t, msg.pended, 3, "被限流的消息不能投递")

	// 过一秒：补一个令牌
	clk.Advance(1100 * time.Millisecond)
	resp = send(t, m, ctx, rec, req)
	require.EqualValues(t, int32(errcode.OK), resp.Code, "间隔够了应恢复发送")
	require.Len(t, repo.inserts, 4)
}

// TestSendBadContentDoesNotConsumeQuota 内容校验失败的请求不占额度。
//
// 否则玩家打字打错一次就要等一秒，体验比限流本身更糟。
func TestSendBadContentDoesNotConsumeQuota(t *testing.T) {
	repo := &fakeRepo{}
	m, _, rec, ctx := newTestModule(repo, &fakeMessenger{self: "game-01"})
	clk := newClock()
	m.chatLimiter.SetClock(clk.Now)

	// 先打 10 次空内容
	for i := 0; i < 10; i++ {
		resp := send(t, m, ctx, rec, &gamev1.ChatSendReq{Channel: channelPrivate, PeerId: 200})
		require.EqualValues(t, int32(errcode.ErrContentEmpty), resp.Code)
	}

	// 额度应该还在：连续 3 条正常消息全部放行
	for i := 0; i < 3; i++ {
		resp := send(t, m, ctx, rec, &gamev1.ChatSendReq{Channel: channelPrivate, PeerId: 200, Content: "hi"})
		require.EqualValues(t, int32(errcode.OK), resp.Code, "第 %d 条正常消息应放行（脏请求不该占额度）", i+1)
	}
}
