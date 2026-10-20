package rpc

// mapper.go —— proto ↔ domain 的转换全部集中在这里。
//
// 为什么把转换集中而不是散落在每个 handler 里：
//
//  1. 转换是最容易出「静默错误」的地方（漏字段、枚举默认值、单位混用）。
//     集中之后每个字段只在一处被解释，review 与单测都有唯一入口；
//  2. RPC 层要「薄」：handler 只负责鉴权、路由、错误分层，字段解释
//     交给本文件；这样将来协议加字段（例如 proto3 optional）时，
//     改动不会渗进业务分支；
//  3. 反向映射（domain → proto）集中后，输出顺序/排序规则也能统一，
//     避免「这个接口排了序，那个接口没排」造成的客户端缓存抖动。
//
// 本文件不返回 gRPC status：错误一律是普通 error（带上下文），
// 由 map_server.go 决定它是 InvalidArgument 还是业务码。分层原则：
//
//	解析/映射失败 = 协议错误（status）
//	业务规则拒绝 = 业务码（CommandReply.code）
//
// 两者混用会让客户端分不清「我参数写错了」和「服务器按规则拒绝了我」。

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sort"

	"game_study/internal/worldmap/application"
	"game_study/internal/worldmap/domain"
	"game_study/pkg/pb/internalv2"
)

// toEnvelope 把线上命令信封映射成领域信封，并做格式校验。
//
// 字段对应关系（以生成代码的字段名为准）：
//
//	WorldId / PlayerId / RequestId          → 幂等键三元组
//	PayloadHash                             → “同键不同参”检测
//	ExpectedObjectVersion / WorldEpoch      → 乐观并发与代次栅栏
//	DeadlineMS / TraceId                    → 超时与链路追踪
//
// PlayerID 在这里不做鉴权——鉴权在 handler 里先做（先拒绝未认证的请求，
// 再解析 body，避免为攻击者做无谓的解析工作）；这里只做「值是否合法」。
func toEnvelope(req *internalv2.CommandRequest) (domain.CommandEnvelope, error) {
	if req == nil {
		return domain.CommandEnvelope{}, fmt.Errorf("请求为空")
	}
	// payload_hash 约定是 32 字节摘要。允许留空（M1 的调用方还没接签名），
	// 但拒绝「半截摘要」：长度非 0 又非 32 说明上游序列化出了问题，
	// 直接 copy 进 [32]byte 会静默截断/补零，导致两个不同请求被当成
	// 同一个幂等键，属于最难排查的一类数据错误。
	rawHash := req.GetPayloadHash()
	if len(rawHash) != 0 && len(rawHash) != sha256.Size {
		return domain.CommandEnvelope{}, fmt.Errorf(
			"payload_hash 长度必须是 %d 字节或留空, got %d", sha256.Size, len(rawHash))
	}

	env := domain.CommandEnvelope{
		WorldID:               domain.WorldID(req.GetWorldId()),
		PlayerID:              req.GetPlayerId(),
		RequestID:             req.GetRequestId(),
		ExpectedObjectVersion: req.GetExpectedObjectVersion(),
		WorldEpoch:            req.GetWorldEpoch(),
		DeadlineMS:            req.GetDeadlineMs(),
		TraceID:               req.GetTraceId(),
	}
	copy(env.PayloadHash[:], rawHash)

	if err := env.Validate(); err != nil {
		return domain.CommandEnvelope{}, err
	}
	return env, nil
}

// toCommand 用 oneof 的类型 switch 把命令体翻译成 application.Command。
//
// Go 知识点：proto3 的 oneof 在生成代码里是「未导出接口 + 若干包装结构体」
// （这里是 isCommandRequest_Body / CommandRequest_March / ...）。
// 判断当前设置了哪个字段有两种写法：
//
//	(1) if m := req.GetMarch(); m != nil { ... } else if r := req.GetRecall(); ...
//	(2) switch body := req.GetBody().(type) { case *CommandRequest_March: ... }
//
// 这里用 (2)：类型 switch 是穷举的，新增 oneof 成员时 default 分支会
// 立刻暴露“这个版本不认识新命令”，而不是像 (1) 那样全部判空后
// 悄悄落进“未知命令”的兜底。body 为 nil 表示调用方一个字段都没设，
// 必须显式拒绝——绝不能默认当成某种命令。
func toCommand(req *internalv2.CommandRequest, sceneID domain.SceneID) (application.Command, error) {
	// TargetScene 由路由结果填写（handler 按 req.SceneId 选场景）。
	// 规划器会用它与快照场景比对，防止命令投错场景后被“凑合执行”。
	cmd := application.Command{TargetScene: sceneID}

	switch body := req.GetBody().(type) {
	case *internalv2.CommandRequest_March:
		m := body.March
		if m == nil {
			return cmd, fmt.Errorf("march body 为空")
		}
		intent, err := toDomainIntent(m.GetIntent())
		if err != nil {
			return cmd, err
		}
		cmd.Kind = application.CmdMarchStart
		cmd.March = application.MarchParams{
			SlotID:     m.GetSlotId(),
			HomeCityID: domain.ObjectID(m.GetHomeCityId()),
			Target: domain.TargetRef{
				TargetID: domain.ObjectID(m.GetTargetId()),
				Intent:   intent,
			},
		}

	case *internalv2.CommandRequest_Recall:
		r := body.Recall
		if r == nil {
			return cmd, fmt.Errorf("recall body 为空")
		}
		cmd.Kind = application.CmdRecall
		cmd.Recall = application.RecallParams{
			TroopID:         domain.ObjectID(r.GetTroopId()),
			ExpectedVersion: r.GetExpectedVersion(),
			// Force 永远为 false：玩家协议没有“强制召回”，
			// 该字段只给服务端管理命令用（见 application.RecallParams 注释）。
			Force: false,
		}

	case nil:
		return cmd, fmt.Errorf("命令 body 缺失（oneof march/recall 必须设置一个）")

	default:
		// 只有上游比本进程新（滚动发布期间）才会走到这里。
		return cmd, fmt.Errorf("不支持的命令 body 类型 %T", body)
	}
	return cmd, nil
}

// toDomainIntent 把 proto 的 MarchIntent 映射成领域枚举。
//
// 关键约束：未知值/UNSPECIFIED 一律返回错误，绝不 default 成 Collect。
// 原因不是洁癖——「漏传 intent 被当成采集」是一个有业务副作用的静默默认
// （部队会真的跑去资源点开始采集），客户端字段没填的 bug 会因此变成
// 线上事故。proto3 的枚举零值无法区分「没传」与「显式传 0」，所以
// 唯一安全的做法是对 0 也报错。
func toDomainIntent(v internalv2.MarchIntent) (domain.MarchIntent, error) {
	switch v {
	case internalv2.MarchIntent_MARCH_INTENT_COLLECT:
		return domain.MarchIntentCollect, nil
	case internalv2.MarchIntent_MARCH_INTENT_ATTACK:
		return domain.MarchIntentAttack, nil
	case internalv2.MarchIntent_MARCH_INTENT_UNSPECIFIED:
		return domain.MarchIntentUnspecified, fmt.Errorf(
			"intent 必须显式指定（COLLECT/ATTACK），收到 UNSPECIFIED")
	default:
		return domain.MarchIntentUnspecified, fmt.Errorf("未知 intent 枚举值 %d", int32(v))
	}
}

// toProtoIntent 是 toDomainIntent 的反向映射。
//
// 当前 TroopView 不携带 intent（客户端只需要知道“它在动”，不需要知道
// 为什么动），但双向映射仍然保留：将来加“行军意图”展示或做协议回放
// 校验时，反向映射必须与正向严格对称。同样对未知值返回错误——
// 静默映射成 COLLECT 会让“未知意图”在回程变成“采集”，与正向的严格
// 校验自相矛盾。
func toProtoIntent(v domain.MarchIntent) (internalv2.MarchIntent, error) {
	switch v {
	case domain.MarchIntentCollect:
		return internalv2.MarchIntent_MARCH_INTENT_COLLECT, nil
	case domain.MarchIntentAttack:
		return internalv2.MarchIntent_MARCH_INTENT_ATTACK, nil
	case domain.MarchIntentUnspecified:
		return internalv2.MarchIntent_MARCH_INTENT_UNSPECIFIED, fmt.Errorf("意图未指定，无法映射")
	default:
		return internalv2.MarchIntent_MARCH_INTENT_UNSPECIFIED, fmt.Errorf("未知领域意图 %d", int32(v))
	}
}

// toTroopView 把部队领域对象转成线上视图。
//
// 坐标语义（文档 9.2）：权威位置不是“上次写库的值 + 每帧累加”，而是
// 按 MoveSegment 用 (from,to,now) 现算。这样重启、丢帧、时钟小幅跳变
// 都不会让客户端与服务端位置产生累计偏差。
//
// nowMS 由调用方从 SceneSnapshot.NowMS() 取，而不是这里调 time.Now()：
// 场景循环用的是注入的 Clock（测试里是 FakeClock），RPC 层如果自己取
// 墙上时钟，假时钟推进后查询出来的坐标就会与业务状态脱节，
// 测试也无法断言。时间来源必须与“产生状态的那个时钟”一致。
func toTroopView(t *domain.Troop, nowMS int64) (*internalv2.TroopView, error) {
	if t == nil {
		return nil, fmt.Errorf("部队为空")
	}
	view := &internalv2.TroopView{
		TroopId:  uint64(t.Header.ID),
		PlayerId: t.PlayerID,
		SlotId:   t.SlotID,
		Phase:    int32(t.Phase),
		TargetId: uint64(t.Target.TargetID),
		Version:  t.Header.Version,
		Cargo:    cargoEntries(t.Cargo),
	}
	if t.Move != nil {
		pos, err := t.Move.PositionAt(nowMS)
		if err != nil {
			// 位置推导失败属于数据/规则错误（溢出等），不能返回 0,0
			// 假装部队在原点：那会让客户端把它画到世界角落。
			return nil, fmt.Errorf("部队 %d 位置推导失败: %w", t.Header.ID, err)
		}
		view.X, view.Y = pos.X, pos.Y
		view.ArriveAtMs = t.Move.EndAtMS
	}
	// t.Move == nil 的部队（尚未出征/已交接）没有坐标，保持 0,0。
	// 客户端应按 phase 判断是否忽略坐标，而不是把 0,0 当成真实位置。
	return view, nil
}

// toResourceView 把资源点领域对象转成线上视图。
//
// 坐标用定点 units（与 TroopView 一致），不用格：资源点在格内也有
// 精确位置，统一单位可以省掉客户端“有的字段是格、有的字段是 units”
// 的换算错误。格坐标只在视野矩形（StreamView/QueryResources）里出现，
// 因为那里的语义天然是“格子范围”。
func toResourceView(r *domain.ResourceNode) *internalv2.ResourceView {
	if r == nil {
		return nil
	}
	return &internalv2.ResourceView{
		ResourceId:      uint64(r.Header.ID),
		X:               r.Position.X,
		Y:               r.Position.Y,
		ResourceType:    r.ResourceType,
		Remaining:       r.Remaining,
		Total:           r.Total,
		ActiveCollectId: r.ActiveCollectID,
		RichBlockId:     r.RichBlockID,
		Version:         r.Header.Version,
	}
}

// cargoEntries 把 map[int32]int64 展开成 repeated CargoEntry，并按 item_id 升序。
//
// 为什么必须排序（这是本文件里最重要的一行注释）：
//
//	Go 的 map 迭代顺序是**随机化**的（runtime 每次 range 都从随机桶开始，
//	并且故意打乱小 map 的顺序）。如果直接把 range 结果 append 进 repeated
//	字段，同一支部队连续两次查询得到的 cargo 顺序可能不同——protobuf 编码
//	字节随之不同，客户端如果按“响应字节”做缓存键/ETag，会看到缓存永远失效；
//	对账、快照 diff 也会产生虚假变更。
//
// 排序键选 item_id 而不是数量：item_id 是稳定标识，数量会随业务变化，
// 用数量排序会在数值相等时退化成不稳定顺序（sort.Slice 不是稳定排序）。
func cargoEntries(cargo map[int32]int64) []*internalv2.CargoEntry {
	if len(cargo) == 0 {
		return nil
	}
	items := make([]int32, 0, len(cargo))
	for itemID := range cargo {
		items = append(items, itemID)
	}
	sort.Slice(items, func(i, j int) bool { return items[i] < items[j] })

	out := make([]*internalv2.CargoEntry, 0, len(items))
	for _, itemID := range items {
		out = append(out, &internalv2.CargoEntry{ItemId: itemID, Count: cargo[itemID]})
	}
	return out
}

// receiptPayload 是首次回执 ResultJSON 的结构。
//
// 规划器写回执时用的是 map[string]any（见 application.setReceipt），
// 正常成功的回执只有业务字段（troop_id/move_id/...），拒绝回执则带
// code/message。这里只关心这两个字段，用匿名结构体反序列化即可，
// 不需要把整个 JSON 解成 map（避免为每个请求分配一堆 any）。
type receiptPayload struct {
	Code    int32  `json:"code"`
	Message string `json:"message"`
}

// fillReplyFromReceipt 用已提交的回执补齐 CommandReply。
//
// 为什么以回执为准而不是以本次 error 为准：
//   - 幂等重放时本次 SubmitSync 返回 nil（规划器直接返回首次回执），
//     但首次可能是业务失败（例如 44005）。若以“nil=成功”构造回复，
//     客户端重试一次失败命令就会突然收到成功，属于严重语义错误。
//   - 回执里的 CommandID 是首次分配的，重放必须原样返回，否则客户端
//     用它查 operation 会查不到。
func fillReplyFromReceipt(reply *internalv2.CommandReply, receipt domain.CommandReceipt) {
	if reply == nil {
		return
	}
	reply.Code = receipt.ResultCode
	reply.CommandId = receipt.CommandID
	// 深拷贝 ResultJSON：它来自场景快照，虽然快照本身是副本，
	// 但把切片直接交给 proto 对象仍然共享底层数组；后续若有人
	// 改写 reply.ResultJson，会污染快照里的回执（Go 切片是引用语义）。
	reply.ResultJson = append([]byte(nil), receipt.ResultJSON...)

	var payload receiptPayload
	if len(receipt.ResultJSON) > 0 && json.Unmarshal(receipt.ResultJSON, &payload) == nil {
		if payload.Message != "" {
			reply.Message = payload.Message
		}
	}
}

// inCellRect 判断格 (x,y) 是否落在 left-closed/right-open 的矩形内。
//
// 参数是格坐标（不是 units）。视野/查询协议统一使用左闭右开，
// 与 view.Rect、拓扑的 SceneBounds 保持同一套语义：
// 相邻两个矩形共享边界时，边界格只属于右边那个，不会重复也不会遗漏。
func inCellRect(x, y, minX, minY, maxX, maxY int64) bool {
	return x >= minX && x < maxX && y >= minY && y < maxY
}
