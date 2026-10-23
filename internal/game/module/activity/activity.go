// internal/game/module/activity/activity.go —— 活动模块（6501-6504）
//
// 活动进度与领取位图放在玩家快照 Ext["activity"]：领奖与发放在同一次 Redis CAS
// 提交，Redis 失败时两者一起回滚（契约见 docs/R2.1-缓存原子变更契约.md）。
//
// 架构职责：活动进度累加 + 领奖。进度有两个来源：登录类活动在 OnLogin
// +1，行为类活动走 OnEvent 累加；全服里程碑活动额外上报 Kafka。
// Ext 槽位：activity_record（Records JSON 列）。
// 事件订阅：消费方（EventAware），按配置 Col2 匹配事件类型。
// 配置表：activity.xml（Col1=名称，Col2=事件类型或 "login"，Col3=目标）、
// activity_period.xml（开放周期，col1=周期类型）。
// 懒初始化：未参与活动返回空 Data；窗口判定不落库——
// 「是否开放」每次请求现算（openNow），过期活动直接从列表隐藏。
package activity

import (
	"context"
	"fmt"
	"time"

	"game_study/internal/game/module"
	"game_study/pkg/activity"
	"game_study/pkg/codec"
	"game_study/pkg/gamedata"
	"game_study/pkg/log"
	pkgmq "game_study/pkg/mq"

	gamev1 "game_study/pkg/pb/game/v1"
	"google.golang.org/protobuf/proto"
)

// stateKey 同时是 Ext 槽位键、模块名与落库归属标签。
const stateKey = "activity"

// 协议号（4.9：6501/6502 列表，6503/6504 领奖）。
const (
	msgIDListReq   = codec.MsgActivityListReq
	msgIDListResp  = codec.MsgActivityListResp
	msgIDClaimReq  = codec.MsgActivityClaimReq
	msgIDClaimResp = codec.MsgActivityClaimResp
)

// 业务码（4.10）：活动段 6501-6505。
const (
	codeOK               int32 = 0
	codeActivityNotExist int32 = 6501
	codeNotFinished      int32 = 6502
	codeAlreadyClaimed   int32 = 6503
	codeBagFull          int32 = 6504
	codeNotOpen          int32 = 6505 // 活动不在开放期（下发的窗口已过期）
)

// 奖励道具（P6 固定：钻石 1001）。
const itemDiamond int32 = 1001

// Record 单个活动的进度与领取位图。
//
// 字段语义：
//   - Progress：当前进度，由 OnEvent 累加并在配置 Col3 目标处截断；
//   - RewardMask：领取位图，bit0=第一档奖励已领；多档奖励时按位扩展。
//
// Go 知识点：位图（bitmask）把「哪几档领了」压成一个整数——
// 单档活动只用 bit0（&1 / |=1），多档时第 n 档是 1<<(n-1)，
// 比 []bool 或 map 都省空间，且整型比较可并行做多重判定。
type Record struct {
	Progress   int32 `json:"progress"`
	RewardMask int64 `json:"reward_mask"`
}

// Data 是活动模块的私有状态：activity_id -> 进度与已领位图。
type Data struct {
	Records map[int32]Record `json:"records"`
}

// Module 活动模块。
type Module struct {
	// Stateful 内嵌 Thin：消息注册与生命周期仍由 Thin 提供，
	// 多出来的是「Data ↔ Ext ↔ 表行」的编解码与标脏。
	*module.Stateful[Data]

	logger   *log.Logger
	producer ProgressProducer // 全服里程碑贡献上报；nil 表示不上报（单机/测试）
}

// ProgressProducer 贡献上报能力（*pkgmq.Producer 实现）。
//
// 接口注入而非直接依赖 mq 包：单测可注入假 producer，
// 且活动模块不关心 Kafka 细节（topic/分区/重试）。
type ProgressProducer interface {
	// ProduceEnvelope 投递一条全服活动贡献消息；失败由调用方降级为日志。
	ProduceEnvelope(ctx context.Context, env *pkgmq.Envelope) error
}

// New 创建活动模块。
//
// producer 允许 nil：单机/测试环境不上报全服进度，
// 本地活动进度照常推进——「全服榜」是增值功能不是正确性前提。
func New(logger *log.Logger, producer ProgressProducer) *Module {
	m := &Module{logger: logger}
	if producer != nil {
		m.producer = producer
	}
	m.Stateful = module.NewStateful(stateKey, newData,
		module.ThinLogger(logger),
		module.ThinLogin(m.onLogin),
		module.ThinOnEvent(m.onEvent),
	)
	m.HandleR(msgIDListReq, msgIDListResp, &gamev1.ActivityListResp{}, m.onList)
	m.HandleR(msgIDClaimReq, msgIDClaimResp, &gamev1.ActivityClaimResp{}, module.Typed(msgIDClaimResp, m.onClaim))
	return m
}

// onLogin 登录类活动进度 +1，纯内存改动，随本次快照提交（经 ThinLogin 挂接）。
//
// 与 quest.OnEvent 的分工：登录是「事件」的一种，但发生在协议之外，
// 所以单独在 OnLogin 钩子里推进，而不是伪造一条 Event。
// 状态损坏时吞错继续登录：活动进度丢一次可以原谅，
// 把玩家拦在登录外不能。
func (m *Module) onLogin(ctx *module.Context) error {
	// 一次 Mutate 走完「读状态 → 逐活动推进」；没有任何活动推进时不产生提交。
	if err := m.Mutate(ctx, func(d *Data) bool {
		changed := false
		for _, cfg := range gamedata.ActivityCfgList() {
			if cfg.Col2 != "login" {
				continue
			}
			id := int32(cfg.Id)
			rec := d.Records[id]
			if rec.Progress >= int32(cfg.Col3) {
				continue // 已达目标，登录不再推进
			}
			rec.Progress++
			d.Records[id] = rec
			changed = true
		}
		return changed
	}); err != nil {
		// 状态损坏不阻断登录：后续消息会再次报错，把玩家拦在登录外不能。
		m.warn("activity login progress failed", ctx, err)
	}
	return nil
}

// onEvent 把玩家行为事件累加为活动进度（经 ThinOnEvent 挂接）。
//
// 与 quest.OnEvent 同构，差异有两点：
//  1. 进度到目标后仍然接受事件（只是截断），不改变任何领取态；
//  2. server_progress 类活动要额外收集上报，攒批后统一发 Kafka。
func (m *Module) onEvent(ctx *module.Context, evt module.Event) {
	var reports [][2]int32 // 全服贡献上报：(activity_id, progress)
	if err := m.Mutate(ctx, func(d *Data) bool {
		changed := false
		for _, cfg := range gamedata.ActivityCfgList() {
			if cfg.Col2 != evt.Type {
				continue
			}
			id := int32(cfg.Id)
			rec := d.Records[id]
			next := rec.Progress + int32(evt.Delta)
			if next > int32(cfg.Col3) {
				next = int32(cfg.Col3)
			}
			if next == rec.Progress {
				continue // 已封顶，无变化
			}
			rec.Progress = next
			d.Records[id] = rec
			changed = true
			if cfg.Col2 == "server_progress" {
				// 全服活动：进度变化后上报新值（绝对值，中心服按差值累加）。
				reports = append(reports, [2]int32{id, next})
			}
		}
		return changed
	}); err != nil {
		m.warn("activity progress update failed", ctx, err)
	}
	// 上报在提交之后：本地进度先落，再尝试广播——
	// 广播失败不影响本地正确性（见 reportProgress）。
	for _, r := range reports {
		m.reportProgress(ctx, r[0], r[1])
	}
}

// reportProgress 上报全服活动贡献（R7.3）。
//
// 上报绝对值而不是增量：重投相同值在中心服得到差值 0，天然幂等。
// 上报失败只记日志——玩家本地进度已保存，下一次事件会带上更大的进度继续推进。
func (m *Module) reportProgress(ctx *module.Context, activityID, progress int32) {
	if m.producer == nil || progress <= 0 {
		return
	}
	now := time.Now().Unix()
	instanceID := instanceIDFor(activityID, now)
	msg := pkgmq.ActivityProgressMsg{
		InstanceID: instanceID,
		ActivityID: activityID,
		PlayerID:   ctx.State.PlayerID(),
		Progress:   progress,
	}
	// envelope 的 key 是「实例:玩家」：同一玩家同一实例的消息进同一分区，
	// Kafka 内保序，中心服按序累加不会乱。
	env, err := pkgmq.NewEnvelope(fmt.Sprintf("%s:%d", instanceID, ctx.State.PlayerID()),
		pkgmq.TopicActivityProgress, instanceID, msg)
	if err != nil {
		return
	}
	if err := m.producer.ProduceEnvelope(context.Background(), env); err != nil {
		m.logger.Debug("report activity progress failed", "activity_id", activityID, "err", err)
	}
}

// instanceIDFor 当前活动实例：优先用中心服下发的，没有就按周期自算。
//
// 实例 ID = 活动 + 周期序号：同一活动的每一轮（每天/每周）是不同实例，
// 进度与奖励按实例隔离，上一轮的 Record 不会串到下一轮。
func instanceIDFor(activityID int32, now int64) string {
	if w, ok := DefaultRegistry().Get(activityID); ok {
		return w.InstanceID
	}
	if cfg, ok := gamedata.GetActivityPeriodCfg(int64(activityID)); ok {
		_, _, period := activity.Window(int32(cfg.Col1), now)
		return fmt.Sprintf("%d-%d", activityID, period)
	}
	// 无周期配置：按天兜底，保证实例 ID 单调不减
	return fmt.Sprintf("%d-%d", activityID, now/86400)
}

// onList 活动列表（配置 + 进度 + 是否已领）。
//
// 过期活动直接不下发：客户端定时刷新列表即可看到活动消失，
// 不需要服务端推「活动结束」通知——少一种推送，少一类时序问题。
func (m *Module) onList(ctx *module.Context, _ uint32, _ []byte) error {
	resp := &gamev1.ActivityListResp{Code: codeOK}
	now := time.Now().Unix()
	if err := m.Read(ctx, func(d *Data) {
		for _, cfg := range gamedata.ActivityCfgList() {
			// 下发的窗口已过期就不展示：客户端仍会定时刷新。
			if !openNow(int32(cfg.Id), now) {
				continue
			}
			entry := &gamev1.ActivityEntry{
				ActivityId: int32(cfg.Id),
				Name:       cfg.Col1,
				Type:       cfg.Col2,
				Target:     int32(cfg.Col3),
			}
			if rec, ok := d.Records[int32(cfg.Id)]; ok {
				entry.Progress = rec.Progress
				entry.Claimed = rec.RewardMask&1 != 0
			}
			resp.Activities = append(resp.Activities, entry)
		}
	}); err != nil {
		return err
	}
	return reply(ctx, msgIDListResp, resp)
}

// onClaim 领奖：进度达标 + 位图防重复，领奖事实与奖励同一次快照提交。
func (m *Module) onClaim(ctx *module.Context, req *gamev1.ActivityClaimReq, resp *gamev1.ActivityClaimResp) error {
	*resp = gamev1.ActivityClaimResp{}
	cfg, ok := gamedata.GetActivityCfg(int64(req.ActivityId))
	if !ok {
		resp.Code = codeActivityNotExist
		return nil
	}
	// 领取同样校验开放期：不能依赖客户端「恰好没收到结束通知」。
	// 服务端权威原则——客户端的任何展示状态都不能当准入条件。
	if !openNow(req.ActivityId, time.Now().Unix()) {
		resp.Code = codeNotOpen
		return nil
	}

	var (
		applied bool
		changes []module.BagChange
	)
	err := m.Mutate(ctx, func(d *Data) bool {
		rec := d.Records[req.ActivityId]
		if rec.Progress < int32(cfg.Col3) {
			resp.Code = codeNotFinished
			return false
		}
		// 位图判重：bit0=已领。活动天生多档位，枚举会爆炸。
		if rec.RewardMask&1 != 0 {
			resp.Code = codeAlreadyClaimed
			return false
		}
		// 发奖：target * 20 钻石；满包/超限时不发也不置领取位。
		gain := int64(cfg.Col3) * 20
		c, grantErr := module.Grant(ctx.State, []module.ItemGrant{{ItemID: itemDiamond, Count: gain}})
		if grantErr != nil {
			resp.Code = codeBagFull
			return false
		}
		changes = c
		rec.RewardMask |= 1 // 置位 bit0：与道具同一次快照 CAS
		d.Records[req.ActivityId] = rec
		applied = true
		return true
	})
	if err != nil {
		return err
	}
	if !applied {
		// 闭包里已设好拒绝码，不能再被下面的成功码覆盖。
		return nil
	}

	resp.Code = codeOK
	if raw, err := proto.Marshal(&gamev1.BagChangeNotify{Changes: module.ToItemChanges(changes)}); err == nil {
		resp.Rewards = raw
	}
	return nil
}

// reply 序列化 proto 并经 ctx.Reply 下发。
func reply(ctx *module.Context, msgID uint32, msg proto.Message) error {
	body, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	return ctx.Reply(msgID, body)
}

// warn 统一告警入口（logger 可为 nil）。
func (m *Module) warn(msg string, ctx *module.Context, err error) {
	if m.logger != nil {
		m.logger.Warn(msg, "player_id", ctx.State.PlayerID(), "err", err)
	}
}
