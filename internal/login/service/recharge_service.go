// internal/login/service/recharge_service.go —— 充值下单与支付回调（Tier-1）
package service

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"game_study/internal/login/repo"
	"game_study/pkg/log"
	"game_study/pkg/model"
)

// OrderProducer 订单消息生产者（由 internal/login/mq 实现，接口化便于单测）。
type OrderProducer interface {
	ProduceOrder(ctx context.Context, order *model.RechargeOrder) error
}

// NopOrderProducer 空实现（未配置 Kafka 时降级：订单仍落库，等待对账补偿）。
type NopOrderProducer struct{}

// ProduceOrder 实现 OrderProducer。
func (NopOrderProducer) ProduceOrder(ctx context.Context, order *model.RechargeOrder) error {
	_ = ctx
	_ = order
	return nil
}

// RechargeService 充值服务。
type RechargeService struct {
	repo     *repo.OrderRepo
	producer OrderProducer
	logger   *log.Logger
}

// NewRechargeService 创建充值服务。
func NewRechargeService(orderRepo *repo.OrderRepo, producer OrderProducer, logger *log.Logger) *RechargeService {
	if producer == nil {
		producer = NopOrderProducer{}
	}
	return &RechargeService{repo: orderRepo, producer: producer, logger: logger}
}

// CreateOrder 创建待支付订单，返回业务订单号。
func (s *RechargeService) CreateOrder(ctx context.Context, playerID int64, productID, amountFen, channel int32) (string, error) {
	orderID, err := generateOrderID()
	if err != nil {
		return "", err
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
		return "", err
	}
	return orderID, nil
}

// PayCallbackResult 支付回调结果。
type PayCallbackResult struct {
	Code int32
}

// PayCallback 处理渠道支付回调：
//  1. 查订单（不存在返回错误）
//  2. CAS 推进 待支付→已支付（重复回调/已支付幂等成功）
//  3. 发 Kafka gs.recharge.order 触发发货
func (s *RechargeService) PayCallback(ctx context.Context, orderID string) (*PayCallbackResult, error) {
	order, err := s.repo.FindByOrderID(ctx, orderID)
	if err != nil {
		return nil, err
	}

	// 已支付/已发货：幂等返回成功，不重复发消息
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
		s.logger.Info("pay callback already advanced", "order_id", orderID)
		return &PayCallbackResult{Code: 0}, nil
	}

	order.Status = repo.OrderStatusPaid
	if err := s.producer.ProduceOrder(ctx, order); err != nil {
		// 消息发送失败不阻塞回调（订单已置为已支付，由对账重投兜底）
		s.logger.Error("produce recharge order failed", "order_id", orderID, "err", err)
	}
	return &PayCallbackResult{Code: 0}, nil
}

// MarkDelivered 发货确认（消费 gs.recharge.delivered 调用）。
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
			s.logger.Warn("insert recharge_record failed", "order_id", orderID, "err", recErr)
		}
	}
	return nil
}

// ReplayPaidUndelivered 对账补偿：把已支付未发货订单重新投递（每小时任务调用）。
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
func IsNotFound(err error) bool {
	return errors.Is(err, repo.ErrOrderNotFound)
}

func generateOrderID() (string, error) {
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return fmt.Sprintf("O%s%s", time.Now().Format("20060102150405"), hex.EncodeToString(b)), nil
}
