package scenemod

import (
	"context"
	"encoding/json"
	"strconv"
	"sync"
	"testing"
	"time"

	"game_study/internal/game/module"
	"game_study/internal/game/player"
	scn "game_study/internal/game/scene"
	"game_study/pkg/actor"
	"game_study/pkg/codec"
	"game_study/pkg/gamedata"
	"game_study/pkg/log"
	"game_study/pkg/pb/game/v1"
	"google.golang.org/protobuf/proto"
)

// fakeState 只实现场景模块用到的部分。
type fakeState struct {
	id      int64
	profile module.Profile
}

func (f *fakeState) PlayerID() int64                 { return f.id }
func (f *fakeState) Profile() module.Profile         { return f.profile }
func (f *fakeState) ExpMax() int64                   { return 0 }
func (f *fakeState) GetAttr(string) int64            { return 0 }
func (f *fakeState) SetAttr(string, int64)           {}
func (f *fakeState) AddBonus(string, int64)          {}
func (f *fakeState) ClearBonus()                     {}
func (f *fakeState) GetItemCount(int32) int64        { return 0 }
func (f *fakeState) Items() map[int32]int64          { return nil }
func (f *fakeState) AddItem(int32, int64)            {}
func (f *fakeState) RemoveItem(int32, int64) bool    { return true }
func (f *fakeState) MarkDirty(string)                {}
func (f *fakeState) SetSceneID(id int32)             { f.profile.SceneID = id }
func (f *fakeState) Ext(string) json.RawMessage      { return nil }
func (f *fakeState) SetExt(string, json.RawMessage)  {}
func (f *fakeState) CanHold([]module.ItemGrant) bool { return true }
func (f *fakeState) ClaimBiz(string) bool            { return true }
func (f *fakeState) HasBiz(string) bool              { return false }

// logoutCmd 让 fakePlayer 在自己的 Actor 上下文里调用模块 OnLogout。
type logoutCmd struct{}

// fakePlayer 模拟玩家 Actor 的消息分发：
//
//	PacketMsg    → 模块 OnMessage（与真实玩家 Actor 一致）
//	SceneResult  → 更新场景归属（模拟 onSceneResult）+ 收集
//	ScenePush    → 收集
//
// 用 channel 收集回执而不是切片：跨 goroutine 断言需要明确的 happens-before。
// 依靠 sys.Drain 是不可靠的——它只检查瞬时状态，看不到「刚发出、尚未入队」的消息。
// outPacket 模块直接回给客户端的包。
type outPacket struct {
	id   uint32
	body []byte
}

type fakePlayer struct {
	state  *fakeState
	mod    *Module
	res    chan *player.SceneResult
	out    chan *outPacket
	mu     sync.Mutex
	pushes []*player.ScenePush
	errs   []error
}

func (p *fakePlayer) Receive(ctx *actor.Context) {
	mctx := &module.Context{
		Actor: ctx,
		State: p.state,
		Send: func(id uint32, body []byte) error {
			p.out <- &outPacket{id: id, body: body}
			return nil
		},
	}
	switch m := ctx.Message().(type) {
	case *player.PacketMsg:
		if err := p.mod.OnMessage(mctx, m.MsgID, m.Body); err != nil {
			p.mu.Lock()
			p.errs = append(p.errs, err)
			p.mu.Unlock()
		}
	case *logoutCmd:
		if err := p.mod.OnLogout(mctx); err != nil {
			p.mu.Lock()
			p.errs = append(p.errs, err)
			p.mu.Unlock()
		}
	case *player.SceneResult:
		// 与 player.Actor.onSceneResult 保持一致。
		if m.SetScene && p.state.profile.SceneID != m.SceneID {
			p.state.SetSceneID(m.SceneID)
		}
		p.res <- m
	case *player.ScenePush:
		p.mu.Lock()
		p.pushes = append(p.pushes, m)
		p.mu.Unlock()
	}
}

func (p *fakePlayer) pushCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.pushes)
}

func (p *fakePlayer) push(i int) *player.ScenePush {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.pushes[i]
}

func (p *fakePlayer) errors() []error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]error(nil), p.errs...)
}

type rig struct {
	t   *testing.T
	sys *actor.System
	mgr *scn.Manager
	mod *Module
}

func newRig(t *testing.T) *rig {
	t.Helper()
	logger := log.New("test", log.WithLevel("error"))

	sys := actor.NewSystem(actor.Config{ShardCount: 8, MailboxSize: 512})
	ctx, cancel := context.WithCancel(context.Background())
	sys.Start(ctx)
	t.Cleanup(func() {
		cancel()
		sys.Stop()
	})

	if len(gamedata.SceneCfgList()) == 0 {
		t.Fatal("scene config is empty")
	}
	mgr := scn.NewManager(sys, logger)
	if err := mgr.Boot(); err != nil {
		t.Fatalf("boot scenes: %v", err)
	}
	return &rig{t: t, sys: sys, mgr: mgr, mod: New(mgr, logger)}
}

func (r *rig) player(id int64) (*fakePlayer, *actor.PID) {
	p := &fakePlayer{
		state: &fakeState{id: id},
		mod:   r.mod,
		res:   make(chan *player.SceneResult, 32),
		out:   make(chan *outPacket, 32),
	}
	pid, err := r.sys.Spawn("player:"+strconv.FormatInt(id, 10), p)
	if err != nil {
		r.t.Fatalf("spawn player: %v", err)
	}
	return p, pid
}

func (r *rig) call(pid *actor.PID, msgID uint32, msg proto.Message) {
	r.t.Helper()
	body, err := proto.Marshal(msg)
	if err != nil {
		r.t.Fatalf("marshal: %v", err)
	}
	if err := r.sys.Send(pid, &player.PacketMsg{MsgID: msgID, Body: body}); err != nil {
		r.t.Fatalf("send packet: %v", err)
	}
}

// result 阻塞等待一条场景回执。
func (r *rig) result(p *fakePlayer) *player.SceneResult {
	r.t.Helper()
	select {
	case m := <-p.res:
		return m
	case <-time.After(3 * time.Second):
		r.t.Fatalf("timeout waiting for scene result (errs=%v)", p.errors())
		return nil
	}
}

// waitPushes 轮询等待推送数量达到 n。
func (r *rig) waitPushes(p *fakePlayer, n int) {
	r.t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if p.pushCount() >= n {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	r.t.Fatalf("timeout waiting for %d pushes, got %d", n, p.pushCount())
}

// count 查询场景当前在场人数（通过消息，避免跨 goroutine 读 map）。
func (r *rig) count(sceneID int32) int {
	r.t.Helper()
	pid := r.mgr.PID(sceneID)
	if pid == nil {
		r.t.Fatalf("scene %d not booted", sceneID)
	}
	reply := make(chan int, 1)
	if err := r.sys.Send(pid, &scn.CountMsg{Reply: reply}); err != nil {
		r.t.Fatalf("send count: %v", err)
	}
	select {
	case n := <-reply:
		return n
	case <-time.After(3 * time.Second):
		r.t.Fatal("timeout waiting for count")
		return -1
	}
}

// waitCount 轮询直到场景人数满足条件（离场/进场是异步的）。
func (r *rig) waitCount(sceneID int32, want int) {
	r.t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if r.count(sceneID) == want {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	r.t.Fatalf("scene %d count = %d, want %d", sceneID, r.count(sceneID), want)
}

func enterResp(t *testing.T, res *player.SceneResult) *gamev1.SceneEnterResp {
	t.Helper()
	var resp gamev1.SceneEnterResp
	if err := proto.Unmarshal(res.Body, &resp); err != nil {
		t.Fatalf("decode enter resp: %v", err)
	}
	return &resp
}

func sceneIDs(resp *gamev1.SceneEnterResp) map[int64]bool {
	out := make(map[int64]bool, len(resp.Entities))
	for _, e := range resp.Entities {
		out[e.PlayerId] = true
	}
	return out
}

// TestEnterUnknownScene 未知场景直接回 4001。
func TestEnterUnknownScene(t *testing.T) {
	r := newRig(t)
	p, pid := r.player(1)

	r.call(pid, codec.MsgSceneEnterReq, &gamev1.SceneEnterReq{SceneId: 999})

	// 未知场景由模块直接拒绝，不经过场景 Actor——回包走 Send 而不是 SceneResult。
	select {
	case pkt := <-p.out:
		if pkt.id != codec.MsgSceneEnterResp {
			t.Fatalf("reply id = %d", pkt.id)
		}
		var resp gamev1.SceneEnterResp
		if err := proto.Unmarshal(pkt.body, &resp); err != nil {
			t.Fatal(err)
		}
		if resp.Code != codeNoScene {
			t.Fatalf("code = %d, want %d", resp.Code, codeNoScene)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("timeout waiting for reply (errs=%v)", p.errors())
	}
}

// TestTwoPlayersSeeEachOther 两人进同一场景互见（R9.1 验收项）。
func TestTwoPlayersSeeEachOther(t *testing.T) {
	r := newRig(t)
	sceneID := int32(gamedata.SceneCfgList()[0].Id)

	a, aPID := r.player(1)
	b, bPID := r.player(2)

	r.call(aPID, codec.MsgSceneEnterReq, &gamev1.SceneEnterReq{SceneId: sceneID, X: 10, Y: 10})
	if resp := enterResp(t, r.result(a)); resp.Code != 0 {
		t.Fatalf("a enter code = %d", resp.Code)
	}

	r.call(bPID, codec.MsgSceneEnterReq, &gamev1.SceneEnterReq{SceneId: sceneID, X: 20, Y: 20})
	if resp := enterResp(t, r.result(b)); resp.Code != 0 {
		t.Fatalf("b enter code = %d", resp.Code)
	}

	// a 收到 b 的出现通知。
	r.waitPushes(a, 1)
	var notify gamev1.SceneEntityNotify
	if err := proto.Unmarshal(a.push(0).Body, &notify); err != nil {
		t.Fatal(err)
	}
	if notify.Type != 1 || notify.Entity.PlayerId != 2 {
		t.Fatalf("appear notify = %s", notify.String())
	}
}

// TestSceneFullReturns4002 满员拒绝，上限来自配置。
func TestSceneFullReturns4002(t *testing.T) {
	r := newRig(t)
	cfg := gamedata.SceneCfgList()[0]
	limit, sceneID := int(cfg.Col5), int32(cfg.Id)

	for i := 0; i < limit; i++ {
		p, pid := r.player(int64(1000 + i))
		r.call(pid, codec.MsgSceneEnterReq, &gamev1.SceneEnterReq{SceneId: sceneID, X: 1, Y: 1})
		if resp := enterResp(t, r.result(p)); resp.Code != 0 {
			t.Fatalf("player %d enter code = %d", i, resp.Code)
		}
	}

	p, pid := r.player(9999)
	r.call(pid, codec.MsgSceneEnterReq, &gamev1.SceneEnterReq{SceneId: sceneID, X: 1, Y: 1})
	if resp := enterResp(t, r.result(p)); resp.Code != 4002 {
		t.Fatalf("code = %d, want 4002（上限 %d）", resp.Code, limit)
	}
}

// TestSwitchSceneLeavesOld 切场景后旧场景不再有该实体。
func TestSwitchSceneLeavesOld(t *testing.T) {
	r := newRig(t)
	cases := gamedata.SceneCfgList()
	if len(cases) < 2 {
		t.Skip("需要至少两个场景配置")
	}
	oldID, newID := int32(cases[0].Id), int32(cases[1].Id)

	p, pid := r.player(1)
	r.call(pid, codec.MsgSceneEnterReq, &gamev1.SceneEnterReq{SceneId: oldID, X: 5, Y: 5})
	r.result(p)
	r.waitCount(oldID, 1)

	r.call(pid, codec.MsgSceneEnterReq, &gamev1.SceneEnterReq{SceneId: newID, X: 5, Y: 5})
	r.result(p)
	r.waitCount(oldID, 0)
	r.waitCount(newID, 1)

	if got := p.state.profile.SceneID; got != newID {
		t.Fatalf("profile scene = %d, want %d", got, newID)
	}
}

// TestLogoutNotifiesScene 下线后场景里不再有该实体。
func TestLogoutNotifiesScene(t *testing.T) {
	r := newRig(t)
	sceneID := int32(gamedata.SceneCfgList()[0].Id)

	_, pid := r.player(1)
	r.call(pid, codec.MsgSceneEnterReq, &gamev1.SceneEnterReq{SceneId: sceneID, X: 5, Y: 5})
	r.waitCount(sceneID, 1)

	if err := r.sys.Send(pid, &logoutCmd{}); err != nil {
		t.Fatalf("send logout: %v", err)
	}
	r.waitCount(sceneID, 0)
}

// TestMoveWithinScene 移动回执带权威坐标，同场景其他玩家收到通知。
func TestMoveWithinScene(t *testing.T) {
	r := newRig(t)
	sceneID := int32(gamedata.SceneCfgList()[0].Id)

	a, aPID := r.player(1)
	b, bPID := r.player(2)
	r.call(aPID, codec.MsgSceneEnterReq, &gamev1.SceneEnterReq{SceneId: sceneID, X: 10, Y: 10})
	r.result(a)
	r.call(bPID, codec.MsgSceneEnterReq, &gamev1.SceneEnterReq{SceneId: sceneID, X: 20, Y: 20})
	r.result(b)
	r.waitPushes(a, 1) // b 进场

	r.call(aPID, codec.MsgSceneMoveReq, &gamev1.SceneMoveReq{X: 13, Y: 10, Seq: 7})
	var mv gamev1.SceneMoveResp
	if err := proto.Unmarshal(r.result(a).Body, &mv); err != nil {
		t.Fatal(err)
	}
	if mv.Code != 0 || mv.X != 13 || mv.Seq != 7 {
		t.Fatalf("move resp = %s", mv.String())
	}

	// b 收到 a 的移动通知。
	r.waitPushes(b, 1)
	var notify gamev1.SceneEntityNotify
	if err := proto.Unmarshal(b.push(0).Body, &notify); err != nil {
		t.Fatal(err)
	}
	if notify.Type != 2 || notify.Entity.PlayerId != 1 || notify.Entity.X != 13 {
		t.Fatalf("move notify = %s", notify.String())
	}
}
