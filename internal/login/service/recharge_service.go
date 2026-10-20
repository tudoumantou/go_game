// internal/login/service/recharge_service.go —— 充值下单与支付回调（Tier-1）
//
// 架构职责：充值域的业务核心——三个入口：CreateOrder（客户端下单）、
// PayCallback（渠道回调推进状态 + 触发发货）、MarkDelivered（游戏服
// 发货确认闭环），外加 ReplayPaidUndelivered 对账补偿。订单状态机
// PENDING→PAID→DELIVERED 的全部规则都在本文件。
// 协作关系：handler 调 CreateOrder/PayCallback；mq.DeliveredConsumer
// 调 MarkDelivered；server.reconcileLoop 每小时调 ReplayPaidUndelivered；
// 发货动作本身在游戏服——本服务只推进状态并发 Kafka 消息，两侧
// 通过 MQ 最终一致。
// 设计取舍（Tier-1 严格一致，涉及钱，宁可重试不可错）：
// ① 金额只信服务端配置——CreateOrder 的 amountFen 由商品表 col3
// 推导，客户端传价一律无视（防 98 元报成 1 分）；
// ② 回调幂等三层防护——状态预判（已 PAID/DELIVERED 直接成功）、
// CAS 推进（MarkPaid 带 WHERE status=0）、RowsAffected=0 按
// 「被别人抢先」处理，重复回调/并发回调都收敛到成功；
// ③ 消息发送失败不阻塞回调——订单已置 PAID，Kafka 丢消息由
// 每小时对账重投兜底；反过来（先发消息后置状态）会产生
// 「消息到了状态没到」的窗口，游戏服会拒收，故必须状态先行；
// ④ NopOrderProducer 降级——无 Kafka 时订单照常落库，纯对账驱动。
package service

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"game_study/internal/login/repo"
	"game_study/pkg/gamedata"
	"game_study/pkg/log"
	"game_study/pkg/model"
)

// OrderProducer 订单消息生产者（由 internal/login/mq 实现，接口化便于单测）。
// 接口而非具体类型：单测注入内存实现验证「回调成功必然发消息」，
// 不需要起 Kafka；生产环境注入 mq.RechargeProducer。
type OrderProducer interface {
	ProduceOrder(ctx context.Context, order *model.RechargeOrder) error
}

// NopOrderProducer 空实现（未配置 Kafka 时降级：订单仍落库，等待对账补偿）。
// 知识点：空实现（Null Object 模式）把「无依赖」变为一种合法策略——
// 调用方无需到处判 nil，NewRechargeService 里统一替换，调用点
// 永远拿到可用实例（最坏是什么都不做）。
type NopOrderProducer struct{}

// ProduceOrder 实现 OrderProducer。
// ctx/order 显式 _ = 丢弃：签名由接口固定，空实现无可执行逻辑；
// 显式标注意图，避免读者误以为漏写。
func (NopOrderProducer) ProduceOrder(ctx context.Context, order *model.RechargeOrder) error {
	_ = ctx
	_ = order
	return nil
}

// RechargeService 充值服务。
// repo 持久化、producer 消息、logger 现场；三者注入，单测可全替。
type RechargeService struct {
	repo     *repo.OrderRepo
	producer OrderProducer
	logger   *log.Logger
}

// NewRechargeService 创建充值服务。
// producer nil → NopOrderProducer：契约「producer 永远非 nil」，
// 方法体内无需判空——把可选性收敛在构造一处。
func NewRechargeService(orderRepo *repo.OrderRepo, producer OrderProducer, logger *log.Logger) *RechargeService {
	if producer == nil {
		producer = NopOrderProducer{}
	}
	return &RechargeService{repo: orderRepo, producer: producer, logger: logger}
}

// CreateOrder 创建待支付订单，返回业务订单号与金额（分）。
//
// 金额只由服务端配置决定（recharge_product.xml 的 col3 = 元），不接受客户端传值：
// 否则客户端可以把 98 元的档位报成 1 分。PlayerID 也由调用方从登录态推导。
// 校验顺序：playerID 合法 → 商品存在 → 价格合法——每一步失败
// 都是 400 级客户端问题，不碰数据库。
func (s *RechargeService) CreateOrder(ctx context.Context, playerID int64, productID, channel int32) (string, int32, error) {
	if playerID <= 0 {
		return "", 0, fmt.Errorf("invalid player id")
	}
	// 商品配置来自 gamedata 包（启动时加载的 XML 表）：价格是
	// 服务端事实，客户端只配选哪个商品
	cfg, ok := gamedata.GetRechargeProductCfg(int64(productID))
	if !ok {
		return "", 0, fmt.Errorf("unknown product %d", productID)
	}
	if cfg.Col3 <= 0 {
		return "", 0, fmt.Errorf("product %d has invalid price", productID)
	}
	// 元 → 分：渠道以分为单位，配置以元为单位，换算只在此处一次
	amountFen := int32(cfg.Col3 * 100)
	orderID, err := generateOrderID()
	if err != nil {
		return "", 0, err
	}
	order := &model.RechargeOrder{
		OrderId:   orderID,
		PlayerId:  playerID,
		ProductId: productID,
		AmountFen: amountFen,
		Channel:   channel,
		Status:    repo.OrderStatusPending,
	}
	if err := s.repo.Create(ctx, order); err != nil {
		return "", 0, err
	}
	// 注意：下单不发 Kafka——发货由「支付成功」触发，未付订单
	// 发消息会让游戏服提前发货（白给）
	return orderID, amountFen, nil
}

// PayCallbackResult 支付回调结果。
// Code 即回给渠道的业务码：0 = 已受理（幂等成功也返回 0），
// 非 0 由 handler 翻译为 5004/5005/5006。
type PayCallbackResult struct {
	Code int32
}

// PayCallback 处理渠道支付回调：
//  1. 查订单（不存在返回错误）
//  2. 金额匹配（渠道回调金额必须等于下单金额，防止改价与部分支付）
//  3. CAS 推进 待支付→已支付（重复回调/已支付幂等成功）
//  4. 发 Kafka gs.recharge.order 触发发货
//
// 签名由渠道适配器在 handler 层校验；这里只处理「已验签的事实」。
// 职责分层：验签（消息可信）在 handler/pay_channel，本方法只面对
// 可信输入做状态推进——两层各自可独立单测。
func (s *RechargeService) PayCallback(ctx context.Context, orderID string, amountFen int32) (*PayCallbackResult, error) {
	order, err := s.repo.FindByOrderID(ctx, orderID)
	if err != nil {
		return nil, err
	}
	// 金额匹配是防改价的最后一道：回调金额由渠道按下单金额生成，
	// 不一致 = 下单金额被篡改过或渠道数据错乱，一律拒
	if amountFen != order.AmountFen {
		return nil, fmt.Errorf("amount mismatch for order %s: callback=%d order=%d", orderID, amountFen, order.AmountFen)
	}

	// 已支付/已发货：幂等返回成功，不重复发消息
	// 预判省一次 CAS：渠道重试的绝大多数落在这里，直接成功
	if order.Status == repo.OrderStatusPaid || order.Status == repo.OrderStatusDelivered {
		return &PayCallbackResult{Code: 0}, nil
	}
	if order.Status == repo.OrderStatusClosed {
		return nil, fmt.Errorf("order %s is closed", orderID)
	}

	advanced, err := s.repo.MarkPaid(ctx, orderID, time.Now())
	if err != nil {
		return nil, err
	}
	if !advanced {
		// 并发下被其它回调抢先推进：按幂等成功处理
		// 两个并发回调都通过预判（都是 PENDING），但 CAS 只有一个
		// RowsAffected=1——输家也返回成功，因为订单确实已支付
		s.logger.Info("pay callback already advanced", "order_id", orderID)
		return &PayCallbackResult{Code: 0}, nil
	}

	order.Status = repo.OrderStatusPaid
	if err := s.producer.ProduceOrder(ctx, order); err != nil {
		// 消息发送失败不阻塞回调（订单已置为已支付，由对账重投兜底）
		// 状态先行的代价窗口：此刻若进程崩溃，订单=PAID 但消息从未
		// 发出——对账循环（每小时扫 PAID 未 DELIVERED）正是为这个
		// 窗口存在的；反向顺序（先消息后状态）则会丢「消息已发但
		// 状态 PENDING」的单，被对账重投变成重复发货，更糟
		s.logger.Error("produce recharge order failed", "order_id", orderID, "err", err)
	}
	return &PayCallbackResult{Code: 0}, nil
}

// MarkDelivered 发货确认（消费 gs.recharge.delivered 调用）。
// CAS 推进 已支付→已发货；幂等——重复确认/迟到确认都是 no-op。
// 流水（recharge_record）只在「本次真的推进了」时写：表上有
// uk_order_id 唯一键兜底，但少一次无用 INSERT 更好。
func (s *RechargeService) MarkDelivered(ctx context.Context, orderID string) error {
	advanced, err := s.repo.MarkDelivered(ctx, orderID, time.Now())
	if err != nil {
		return err
	}
	if !advanced {
		// 已发货或订单不存在：幂等忽略
		return nil
	}
	order, err := s.repo.FindByOrderID(ctx, orderID)
	if err == nil {
		if recErr := s.repo.InsertRecord(ctx, order); recErr != nil {
			// 流水写失败只 warn：订单已是 DELIVERED（钱货两讫的事实），
			// 流水是账本副本，失败由人工/对账补，不回滚主状态
			s.logger.Warn("insert recharge_record failed", "order_id", orderID, "err", recErr)
		}
	}
	return nil
}

// ReplayPaidUndelivered 对账补偿：把已支付未发货订单重新投递（每小时任务调用）。
// 重投天然幂等：游戏服侧按订单状态机消费（同 MarkDelivered 的
// CAS 思路），重复投递最多重复发货一次尝试、被状态拒掉。
// 逐单 continue 而非整体失败：一批里个别单投递失败（如临时网络
// 分区）不影响其他单，下一轮对账会再带上它们。
func (s *RechargeService) ReplayPaidUndelivered(ctx context.Context, limit int) (int, error) {
	orders, err := s.repo.ListPaidUndelivered(ctx, limit)
	if err != nil {
		return 0, err
	}
	count := 0
	for _, o := range orders {
		if err := s.producer.ProduceOrder(ctx, o); err != nil {
			s.logger.Warn("replay order failed", "order_id", o.OrderId, "err", err)
			continue
		}
		count++
	}
	return count, nil
}

// IsNotFound 判断是否为订单不存在。
// handler 用它把 ErrOrderNotFound 映射为渠道错误码 5004；
// 封装 errors.Is 让调用方不直接依赖 repo 包的哨兵变量。
func IsNotFound(err error) bool {
	return errors.Is(err, repo.ErrOrderNotFound)
}

// generateOrderID 生成业务订单号：O + 年月日时分秒 + 4 字节随机 hex。
// 时间前缀保证字典序≈时间序（运维按 ID 区间扫单即按时间）；
// 随机后缀防同秒碰撞（大促秒级并发下单可达数百）。4 字节 = 32bit，
// 同秒 65536 单的碰撞概率已低于数据库唯一索引冲突的处理成本，
// 真撞了按 ErrOrderExists 上抛，客户端重试即得新号。
func generateOrderID() (string, error) {
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return fmt.Sprintf("O%s%s", time.Now().Format("20060102150405"), hex.EncodeToString(b)), nil
}
