package rpc

// map_server.go —— MapService 的内部 gRPC 实现（设计文档第 15 章）。
//
// 分层（自上而下，谁负责什么必须清楚）：
//
//	┌──────────────────────────────────────────────────────────────┐
//	│ 传输层：metadata 取 token、gRPC status 表达“请求没被接受”      │
//	├──────────────────────────────────────────────────────────────┤
//	│ 本文件：鉴权 → 路由（world/scene）→ proto↔domain（mapper.go）  │
//	├──────────────────────────────────────────────────────────────┤
//	│ runtime：WorldInstance.SubmitSync / SceneLoop 单写者串行执行  │
//	├──────────────────────────────────────────────────────────────┤
//	│ application：纯规划（不读时钟/不读库），domain：规则与业务码   │
//	└──────────────────────────────────────────────────────────────┘
//
// 两种错误通道，绝不混用（这是本文件最重要的设计约束）：
//
//	1. gRPC status（err 返回值）：请求**没有到达业务语义**。
//	   - Unauthenticated / PermissionDenied：鉴权失败；
//	   - InvalidArgument：报文格式/枚举非法（未接受，不写回执）；
//	   - NotFound：世界未加载 / 场景不存在（路由不到）；
//	   - ResourceExhausted：过载（未接受，可用同一 request_id 重试）；
//	   - Unavailable：世界 NotReady / 场景关闭 / 结果待解析；
//	   - DeadlineExceeded / Canceled：调用方超时或取消。
//
//	2. 业务码（CommandReply.code = 44001..44017）：请求**到了业务层，
//	   被规则拒绝**。例如槽位占用（44005）、路径受阻（44010）。
//	   这类失败会被写成幂等回执，客户端应当按码修正参数或查询状态，
//	   而不是“换个连接重试”。
//
// 为什么不能用 gRPC error 表达业务失败：客户端无法区分“请求丢了”
// 与“请求到了但被业务拒绝”。前者可以安全重试，后者重试一万次结果
// 都一样；混在一起会让客户端要么重复下单，要么把可修正的问题当网络
// 故障处理。
//
// 唯一需要留意的交叉点：runtime.ErrOverloaded 是一个**基础设施**错误
// （邮箱满/事务池满，命令根本没被接受），它必须映射成
// codes.ResourceExhausted，让上游的 gRPC 重试策略生效；绝不能落进
// 业务码 44013 然后被当成“已受理的失败”写进回执。

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"game_study/internal/worldmap/domain"
	"game_study/internal/worldmap/runtime"
	"game_study/internal/worldmap/view"
	"game_study/pkg/pb/internalv2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// tokenMetadataKey 是内部 token 的 metadata 键。
//
// gRPC 的 metadata key 会被规范化为小写；HTTP/2 头的名字本身也是
// 大小写不敏感的。所以客户端写 "X-Internal-Token" 或
// "x-internal-token" 到达服务端后都是小写形式，md.Get 用小写键即可。
//
// 为什么不放在 proto 字段里：凭证属于传输层信息，不应进入业务消息——
// 否则它会被写进日志、被序列化进 trace、被业务代码误当作可信输入。
// 用 metadata 也让「换一套鉴权（mTLS 客户端证书）」不影响协议。
const tokenMetadataKey = "x-internal-token"

// WorldRouter 是本层对世界管理器的**窄依赖**。
//
// 为什么不直接依赖 *runtime.WorldManager：
//   - 本层只用到 World(id) 一个方法，依赖具体类型会把管理器的全部
//     方法（LoadWorld/Shutdown/TickRefresh…）都变成“可用 API”，
//     下一个人很容易在这里顺手调 LoadWorld，把懒加载语义破坏掉；
//   - 窄接口让测试可以传一个“永远找不到世界”的假实现，验证路由
//     错误分支，而不必启动真实场景循环。
//
// *runtime.WorldManager 的方法集天然满足这个接口（Go 的接口是隐式
// 实现的，不需要管理器知道本包存在），所以生产装配直接传真实管理器。
type WorldRouter interface {
	World(id domain.WorldID) (*runtime.WorldInstance, bool)
}

// 编译期断言：必须实现生成的 MapServiceServer。
//
// 为什么需要：生成代码的接口包含未导出的 mustEmbedUnimplemented...
// 方法，只有嵌入 UnimplementedMapServiceServer 才能满足。这条断言让
// “忘了嵌入”在编译期就失败，而不是等到 Register 时 panic。
var _ internalv2.MapServiceServer = (*Server)(nil)

// Server 实现 internalv2.MapServiceServer。
//
// 并发：gRPC 每个 RPC 一个 goroutine，Server 的所有字段必须是
// 构造后只读（manager/coordinator/logger/verifier 都不在请求路径上被写）。
// 真正的并发控制发生在下层：SceneLoop 用单 goroutine 串行化写，
// Snapshot/QueryTroops 用读锁拷贝。本层不持有任何可变状态——这样
// 水平扩容时不需要考虑“RPC 层的粘性会话”。
type Server struct {
	// 嵌入未实现版本：生成代码要求如此（向前兼容），
	// 我们覆写了三个方法，这里实际不会被调用。
	internalv2.UnimplementedMapServiceServer

	manager     WorldRouter
	coordinator *view.Coordinator // 可空：M2 早期视野流可能尚未装配
	logger      runtime.Logger
	verifier    TokenVerifier
}

// NewServer 构造地图 RPC 服务。
//
// 允许 nil 的依赖与默认值：
//   - manager 为 nil：所有请求返回 FailedPrecondition/NotFound，
//     而不是 panic——装配错误应该表现为“请求失败”，不是进程崩溃；
//   - coordinator 为 nil：StreamView 返回 Unimplemented，其余 RPC 正常；
//   - logger 为 nil：用 NopLogger（日志缺失不应让服务起不来）；
//   - verifier 为 nil：用 NopVerifier（仅开发方便；生产装配必须显式传
//     一个真正的校验器，见 verifier.go 的警告）。
func NewServer(manager WorldRouter, coordinator *view.Coordinator,
	logger runtime.Logger, verifier TokenVerifier) *Server {
	if logger == nil {
		logger = runtime.NopLogger{}
	}
	if verifier == nil {
		verifier = NopVerifier{}
	}
	return &Server{
		manager:     manager,
		coordinator: coordinator,
		logger:      logger,
		verifier:    verifier,
	}
}

// Register 把本服务挂到 gRPC Server（通常是 pkg/rpc.Server.GRPCServer()）。
//
// 必须在本进程开始 Serve 之前调用：gRPC 不允许运行中修改服务表，
// Serve 之后注册会 panic（见 pkg/rpc 的 GRPCServer 注释）。
func (s *Server) Register(gs *grpc.Server) {
	internalv2.RegisterMapServiceServer(gs, s)
}

// Submit 提交地图命令（出征/召回）。
//
// 执行顺序（顺序本身是设计的一部分）：
//
//	鉴权 → 路由 → 报文解析 → 幂等探测 → SubmitSync → 错误分层 → 回执补齐
//
// 鉴权放在最前面：未认证的请求不应该消耗任何解析/路由资源，也不应该
// 因为“世界未加载”之类的信息泄漏服务端拓扑。
func (s *Server) Submit(ctx context.Context, req *internalv2.CommandRequest) (*internalv2.CommandReply, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "请求不能为空")
	}
	playerID, err := s.authenticate(ctx, req.GetPlayerId())
	if err != nil {
		return nil, err
	}

	worldID := domain.WorldID(req.GetWorldId())
	sceneID := domain.SceneID(req.GetSceneId())
	world, _, err := s.route(worldID, sceneID)
	if err != nil {
		return nil, err
	}

	env, err := toEnvelope(req)
	if err != nil {
		// 信封格式错误 = 未接受，不写回执（文档 15.3）。
		// 客户端应修正参数后换一个新的 request_id，而不是原样重试。
		return nil, status.Errorf(codes.InvalidArgument, "命令信封非法: %v", err)
	}
	cmd, err := toCommand(req, sceneID)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "命令参数非法: %v", err)
	}

	// 幂等探测：SubmitSync 只返回 error，不返回“是否重放”。
	// 领域里判断重放的依据是「提交前已存在同键同 hash 的回执」，
	// 这里在提交前读一次快照来复现这个判断。
	//
	// 已知的窄窗口：若首次请求的事务尚未提交，第二个并发同键请求
	// 会看到“没有回执”，最终 first.Replayed=false，但它依然不会产生
	// 第二次副作用（场景串行执行，事务层 UNIQUE 幂等键兜底）。
	// 用一次深拷贝快照换取重放标记是可接受的 M2 取舍；M3 接 store
	// 后可以改成按幂等键点查 map_command，不必整场景快照。
	_, replayed := s.existingReceipt(world, sceneID, env)

	// SubmitSync 的语义：返回 nil = 被接受并成功提交；返回 error =
	// 未接受（基础设施）或规划阶段拒绝（业务）。两者在这里分流。
	submitErr := world.SubmitSync(ctx, runtime.RoutedCommand{
		SceneID: sceneID,
		Env:     env,
		Cmd:     cmd,
	})

	if submitErr != nil {
		if st := infrastructureStatus(submitErr); st != nil {
			s.logger.Warn("地图命令未被接受",
				"actor", actorKey(worldID, playerID, sceneID),
				"request_id", env.RequestID,
				"trace_id", env.TraceID,
				"err", submitErr)
			return nil, st
		}
	}

	// 读取“提交后”的回执。业务拒绝（规划阶段拒绝）也会写回执，
	// 所以它能作为「这次是业务失败而非基础设施故障」的判据。
	receipt, hasReceipt := s.existingReceipt(world, sceneID, env)

	if submitErr != nil && !hasReceipt {
		// 特例：同 request_id 换了参数（payload_hash 不一致）。
		//
		// 规划器会直接返回 ErrIdempotencyConflict，而 rejectionPlan 想写的
		// 回执与既有回执主键相同、hash 不同——事务层会拒绝这次写入
		// （这正是“同键不同参”必须被挡住的地方）。结果是：错误稳定、
		// 但内存里没有本次回执。它仍然是**业务失败**（44009），
		// 不能降级成 Internal：客户端需要知道“这个 request_id 已经
		// 用于别的参数，换 request_id 重来”，而不是无脑重试。
		if errors.Is(submitErr, domain.ErrIdempotencyConflict) {
			return &internalv2.CommandReply{
				Code:    domain.CodeIdempotencyConflict,
				Message: submitErr.Error(),
			}, nil
		}
		// 既不是已知的基础设施错误，也没有业务回执：只剩“提交阶段
		// 失败且没有稳定结果”这一种可能（例如 SQL 回滚）。
		// 按 Internal 返回，让上游知道“结果不确定”，不要翻译成业务码。
		return nil, status.Errorf(codes.Internal, "地图命令 %s 执行失败: %v", env.RequestID, submitErr)
	}

	reply := &internalv2.CommandReply{Code: domain.CodeOK, Message: "ok", Replayed: replayed}
	if submitErr != nil {
		// 先按错误拿码，再用回执校正（回执是权威）。
		reply.Code = domain.CodeOf(submitErr)
		reply.Message = submitErr.Error()
	}
	if hasReceipt {
		fillReplyFromReceipt(reply, receipt)
	}

	if reply.Code != domain.CodeOK {
		s.logger.Info("地图命令被业务拒绝",
			"actor", actorKey(worldID, playerID, sceneID),
			"request_id", env.RequestID,
			"code", reply.Code,
			"replayed", replayed)
	}
	return reply, nil
}

// Query 查询已提交状态（部队列表 / 矩形内资源点）。
//
// 只读路径不排队：直接拿场景快照（持读锁深拷贝），不与在途事务竞争。
// 这是设计上的取舍——查询不阻塞在途事务，返回的是“取快照那一刻”的
// 已提交状态，而不是“等到最新事务完成”。
//
// revision 与数据的一致性：revision 取自快照、部队列表取自
// scene.QueryTroops，两者之间可能插入一次已提交的命令（窗口极窄，
// 因为同一场景的写是串行的）。此时 revision 是数据版本的**下界**：
// 客户端会把多出来的那条变化在收到对应 revision 的事件时幂等重放，
// 不会产生错误状态。资源查询走同一个快照，revision 与数据严格一致。
func (s *Server) Query(ctx context.Context, req *internalv2.MapQueryRequest) (*internalv2.MapQueryReply, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "请求不能为空")
	}
	// 查询同样要鉴权：部队列表含玩家私有信息（槽位、货物），
	// 只靠 player_id 字段不足以阻止伪造。
	if _, err := s.authenticate(ctx, req.GetPlayerId()); err != nil {
		return nil, err
	}

	_, scene, err := s.route(domain.WorldID(req.GetWorldId()), domain.SceneID(req.GetSceneId()))
	if err != nil {
		return nil, err
	}

	// 快照提供 revision 与 nowMS：revision 让客户端知道“这份结果对
	// 应哪个版本”，nowMS 让位置推导与场景状态使用同一个时钟。
	snap := scene.Snapshot()
	nowMS := snap.NowMS()

	switch body := req.GetBody().(type) {
	case *internalv2.MapQueryRequest_QueryTroops:
		if body.QueryTroops == nil {
			return nil, status.Error(codes.InvalidArgument, "query_troops body 为空")
		}
		troops := scene.QueryTroops(req.GetPlayerId())
		views := make([]*internalv2.TroopView, 0, len(troops))
		for _, t := range troops {
			// slot_id = 0 表示“全部槽位”；非 0 只返回该槽位。
			// 过滤放在这一层而不是 QueryTroops：场景接口保持“按玩家”
			// 这一个维度，槽位是协议偏好，不属于运行时契约。
			if slot := body.QueryTroops.GetSlotId(); slot != 0 && t.SlotID != slot {
				continue
			}
			tv, err := toTroopView(t, nowMS)
			if err != nil {
				return nil, status.Errorf(codes.Internal, "部队视图转换失败: %v", err)
			}
			views = append(views, tv)
		}
		// 排序：QueryTroops 从 map 取值，顺序随机。不排序会让同一份
		// 状态每次返回不同顺序，客户端按数组下标做 diff 会看到满屏变更。
		sort.Slice(views, func(i, j int) bool { return views[i].TroopId < views[j].TroopId })
		return &internalv2.MapQueryReply{Revision: snap.Revision(), Troops: views}, nil

	case *internalv2.MapQueryRequest_QueryResources:
		if body.QueryResources == nil {
			return nil, status.Error(codes.InvalidArgument, "query_resources body 为空")
		}
		q := body.QueryResources
		topo := snap.Topo()
		views := make([]*internalv2.ResourceView, 0)
		for _, r := range snap.AllResources() {
			if r.Header.Deleted {
				continue
			}
			// 资源点存的是定点 units，查询矩形是格：先换算再比较。
			// 用截断（UnitsToCells）而不是四舍五入，与 AOI 分桶的
			// “格 = 左闭右开区间”语义保持一致。
			cellX, cellY := topo.UnitsToCells(r.Position)
			if !inCellRect(cellX, cellY, q.GetMinX(), q.GetMinY(), q.GetMaxX(), q.GetMaxY()) {
				continue
			}
			views = append(views, toResourceView(r))
		}
		sort.Slice(views, func(i, j int) bool { return views[i].ResourceId < views[j].ResourceId })
		return &internalv2.MapQueryReply{Revision: snap.Revision(), Resources: views}, nil

	case nil:
		return nil, status.Error(codes.InvalidArgument, "查询 body 缺失（oneof query_troops/query_resources 必须设置一个）")

	default:
		return nil, status.Errorf(codes.InvalidArgument, "不支持的查询 body 类型 %T", body)
	}
}

// StreamView 视野流：先全量，之后按 revision 推增量。
//
// 流的生命周期（gRPC 知识点）：
//
//	handler 返回 = 流结束。返回 nil 表示正常结束（对端收到 EOF），
//	返回 status 表示异常终止。handler 必须在自己的 goroutine 里
//	把事件发完；Send 不是并发安全的，所以这里只有本 goroutine 写。
//
// 三种退出路径都必须走到 defer sub.Close()：
//   - 客户端主动取消（关页面/断网）：ctx.Done() 触发；
//   - 服务端关闭/场景视图关闭：订阅 channel 被 close；
//   - 慢客户端：Send 返回错误（HTTP/2 流控窗口耗尽后写超时）。
//
// 慢客户端不做重试：视野流是幂等且可重同步的，客户端重连后带
// since_revision 重新订阅即可；在一个已经堵塞的流上重试只会把
// 服务端的内存（订阅队列）也拖垮（文档 12.6 的背压原则）。
func (s *Server) StreamView(req *internalv2.StreamViewRequest,
	stream grpc.ServerStreamingServer[internalv2.ViewEvent]) error {
	if s.coordinator == nil {
		// 装配层面没有视野子系统：返回 Unimplemented 而不是空流。
		// 空流会让客户端以为“这片区域没有对象”，从而清空缓存。
		return status.Error(codes.Unimplemented, "视野流未启用（coordinator 未装配）")
	}
	if req == nil {
		return status.Error(codes.InvalidArgument, "请求不能为空")
	}
	ctx := stream.Context()
	if _, err := s.authenticate(ctx, req.GetPlayerId()); err != nil {
		return err
	}

	// 格坐标矩形，左闭右开。裁剪（世界边界 + 单次视野上限）由
	// view.Coordinator 完成：它持有拓扑与配置，本层不重复实现规则。
	rect := view.Rect{
		MinX: req.GetMinX(),
		MinY: req.GetMinY(),
		MaxX: req.GetMaxX(),
		MaxY: req.GetMaxY(),
	}
	sub, err := s.coordinator.SubscribeContext(
		ctx, domain.SceneID(req.GetSceneId()), rect, req.GetSinceRevision())
	if err != nil {
		return streamSubscribeStatus(err)
	}
	// defer 而不是在每个 return 前手写：订阅是“注销必须与注册配对”的
	// 资源，Go 里最可靠的配对方式就是 defer（panic 时也会执行）。
	defer sub.Close()

	for {
		select {
		case <-ctx.Done():
			// context 取消会沿调用链向上传播：这里返回 status.FromContextError
			// 让 gRPC 用正确的码（Canceled/DeadlineExceeded）结束流，
			// 而不是返回 nil 伪装成“正常结束”。
			return status.FromContextError(ctx.Err()).Err()

		case ev, ok := <-sub.Events():
			if !ok {
				// 订阅 channel 被关闭（场景视图关闭或协调器 Close）。
				// 返回 nil：对客户端来说这是一次正常的流结束，重连即可。
				return nil
			}
			if err := stream.Send(ev); err != nil {
				// 慢客户端/连接断开：立即退出并 Close（defer），不重试。
				s.logger.Warn("视野流发送失败，断开订阅",
					"actor", actorKey(domain.WorldID(req.GetWorldId()), req.GetPlayerId(), domain.SceneID(req.GetSceneId())),
					"revision", ev.GetRevision(),
					"err", err)
				return err
			}
		}
	}
}

// authenticate 从 metadata 取 token 并校验 player_id 绑定。
//
// 返回值是“已验证的玩家 ID”。当前实现直接返回请求里的 player_id
// （因为绑定关系已校验），保留返回值是为了将来支持「服务身份代提交」时
// 由校验器决定最终 actor，而不是再次相信请求。
func (s *Server) authenticate(ctx context.Context, requestPlayerID int64) (int64, error) {
	token, err := internalTokenFrom(ctx)
	if err != nil {
		return 0, err
	}
	if requestPlayerID <= 0 {
		// 放在 token 校验之前：这属于报文错误，不是凭证错误。
		return 0, status.Errorf(codes.InvalidArgument, "player_id 必须为正, got %d", requestPlayerID)
	}

	bound, err := s.verifier.Verify(ctx, token)
	if err != nil {
		// 校验器的内部故障与“token 不对”对上游都是“需要换凭证/重新认证”，
		// 统一用 Unauthenticated，不泄漏校验器实现细节。
		return 0, status.Errorf(codes.Unauthenticated, "内部 token 校验失败: %v", err)
	}
	// 防伪造：token 若绑定了具体玩家，请求里的 player_id 必须一致。
	// 只校验“token 合法”而不校验绑定，等于给任何合法服务身份开了
	// 冒充任意玩家的后门（文档 15.5 的最后一条）。
	if bound != AnyPlayerID && bound != requestPlayerID {
		return 0, status.Errorf(codes.PermissionDenied,
			"token 绑定玩家 %d，与请求玩家 %d 不一致", bound, requestPlayerID)
	}
	return requestPlayerID, nil
}

// internalTokenFrom 从 incoming metadata 取内部 token。
//
// 用 metadata.FromIncomingContext（服务端）而不是 FromOutgoingContext：
// 后者是客户端发请求时用的，方向搞反会永远读不到值。
// 没有 metadata（例如直接单测调用 handler）也按 Unauthenticated 处理——
// 鉴权不能有“没带 metadata 就放行”的隐式后门。
func internalTokenFrom(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", status.Error(codes.Unauthenticated, "缺少 gRPC metadata（未携带内部凭证）")
	}
	values := md.Get(tokenMetadataKey)
	if len(values) == 0 || values[0] == "" {
		return "", status.Errorf(codes.Unauthenticated, "缺少 %s", tokenMetadataKey)
	}
	return values[0], nil
}

// route 按 (world, scene) 找到运行时实例。
//
// 世界未加载 / 场景不存在都属于“基础设施未就绪”，用 status 表达：
// 业务码 44003（ObjectNotFound）留给“业务对象不存在”，两者含义不同——
// 前者是部署/启动顺序问题，后者是玩家操作问题。
func (s *Server) route(worldID domain.WorldID, sceneID domain.SceneID) (*runtime.WorldInstance, *runtime.SceneLoop, error) {
	if s.manager == nil {
		return nil, nil, status.Error(codes.FailedPrecondition, "世界管理器未装配")
	}
	world, ok := s.manager.World(worldID)
	if !ok {
		return nil, nil, status.Errorf(codes.NotFound, "世界 %d 未加载", worldID)
	}
	scene, ok := world.Scene(sceneID)
	if !ok {
		return nil, nil, status.Errorf(codes.NotFound, "世界 %d 不存在场景 %d", worldID, sceneID)
	}
	return world, scene, nil
}

// existingReceipt 读取场景快照里“同幂等键且同 payload hash”的回执。
//
// 只认 hash 相同的回执：同键不同 hash 是 ErrIdempotencyConflict 的场景，
// 不能把它当成“这次请求的首次结果”返回给客户端，否则客户端会拿到
// 另一个请求的 result_json。
func (s *Server) existingReceipt(world *runtime.WorldInstance, sceneID domain.SceneID,
	env domain.CommandEnvelope) (domain.CommandReceipt, bool) {
	snap, ok := world.Snapshot(sceneID)
	if !ok {
		return domain.CommandReceipt{}, false
	}
	receipt, ok := snap.Receipt(env.Envelope())
	if !ok || receipt.PayloadHash != env.PayloadHash {
		return domain.CommandReceipt{}, false
	}
	return receipt, true
}

// infrastructureStatus 把“命令未被接受/未完成”的错误翻译成 gRPC status。
//
// 返回 nil 表示这不是基础设施错误（即业务拒绝），调用方继续用
// domain.CodeOf 取业务码。判定顺序有讲究：
//
//  1. context 错误最先判：它是调用方视角的“根本没有结果”，
//     比任何业务语义都强；
//  2. 然后才是 runtime 的三个哨兵。ErrOverloaded 必须用
//     ResourceExhausted——这是 gRPC 唯一“明确可重试、且没有副作用”
//     的语义，上游的退避重试策略认这个码；
//  3. 其余一律返回 nil，交给业务码分支。
//
// 注意：errors.Is 能穿透 %w 包装。SceneLoop 返回的错误几乎都是
// fmt.Errorf("%w: ...") 包过的，直接用 == 比较会漏判。
func infrastructureStatus(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, "调用方已取消")
	case errors.Is(err, context.DeadlineExceeded):
		// 注意区分：这是“调用方 ctx 到期”，不是“业务超时”。
		// 设计文档 15.4：RPC 超时只说明没在时限内完成，不代表操作失败。
		return status.Error(codes.DeadlineExceeded, "地图命令处理超时（结果未知，可用同一 request_id 查询）")
	case errors.Is(err, runtime.ErrOverloaded):
		return status.Error(codes.ResourceExhausted,
			"地图场景过载，命令未被接受（可用同一 request_id 重试）")
	case errors.Is(err, runtime.ErrSceneClosed):
		return status.Error(codes.Unavailable, "地图场景已关闭或世界不可写")
	case errors.Is(err, runtime.ErrPendingUnknown):
		return status.Error(codes.Unavailable, "上一个事务结果待解析，场景暂时冻结")
	default:
		return nil
	}
}

// streamSubscribeStatus 把订阅失败翻译成 gRPC status。
func streamSubscribeStatus(err error) error {
	switch {
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, "调用方已取消")
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, "订阅视野超时")
	case errors.Is(err, view.ErrNoLoader):
		// 装配错误：视图层没有数据源。属于启动配置问题，
		// 用 FailedPrecondition 表达“服务状态不允许此操作”。
		return status.Error(codes.FailedPrecondition, "视野子系统未注入数据源")
	case errors.Is(err, runtime.ErrSceneClosed):
		return status.Error(codes.Unavailable, "场景视图已关闭")
	case errors.Is(err, domain.ErrInvalidArgument):
		// 视野为空/完全在世界外/场景号越界：都是请求参数问题。
		return status.Errorf(codes.InvalidArgument, "视野订阅参数非法: %v", err)
	default:
		return status.Errorf(codes.Internal, "订阅视野失败: %v", err)
	}
}

// actorKey 生成日志用的操作者标识。
//
// 把 world/player/scene 放在一行里而不是三个独立字段：排障时最常见的
// 动作是“拿玩家投诉的时间点去 grep 日志”，一行完整的三元组可以直接
// 复制；拆成三个字段反而要在心里拼。player 放在中间是因为它是最常用
// 的过滤维度。
func actorKey(worldID domain.WorldID, playerID int64, sceneID domain.SceneID) string {
	return fmt.Sprintf("world=%d/player=%d/scene=%d", worldID, playerID, sceneID)
}
