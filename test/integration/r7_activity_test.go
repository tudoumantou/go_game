//go:build integration

// test/integration/r7_activity_test.go —— 活动贡献 → 里程碑 → 发奖任务端到端（R7.3/R7.4）
//
// 用真实 Kafka + MySQL + Redis，验证三件事：
//  1. 贡献消息被消费后按「绝对值差值」累加，重投不重复累加；
//  2. 达到阈值生成发奖任务（每个贡献玩家一条），未达阈值不生成；
//  3. 任务发奖后落为已完成，重复 Tick 不会重复发。
//
// 前置：KAFKA_BROKERS 指向可用 broker（本地 make dev 的 127.0.0.1:9093）。
package integration

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"game_study/internal/center/milestone"
	"game_study/pkg/activity"
	"game_study/pkg/log"
	"game_study/pkg/model"
	pkgmq "game_study/pkg/mq"
	"github.com/stretchr/testify/require"
)

func TestR7ActivityMilestone(t *testing.T) {
	raw := os.Getenv("KAFKA_BROKERS")
	if raw == "" {
		t.Skip("KAFKA_BROKERS not set, skip activity milestone integration test")
	}
	brokers := strings.Split(raw, ",")

	database, rdb, _ := startStorage(t)
	err := ensureTopics(brokers, pkgmq.TopicActivityProgress, pkgmq.TopicMail)
	require.NoError(t, err, fmt.Sprintf("ensure topics: %v", err))

	logger := log.New("r7-test", log.WithLevel("error"))
	producer := pkgmq.NewProducer(pkgmq.Config{Brokers: brokers})
	defer producer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// 每个测试用独立消费组，避免与其它用例抢分区。
	consumer := pkgmq.NewConsumer(pkgmq.Config{Brokers: brokers},
		pkgmq.TopicActivityProgress, fmt.Sprintf("r7-milestone-%d", time.Now().UnixNano()), producer)
	svc := milestone.New(database, rdb, producer, consumer, logger)
	go func() {
		if err := svc.RunConsumer(ctx); err != nil && ctx.Err() == nil {
			t.Logf("consumer stopped: %v", err)
		}
	}()
	defer svc.Close()

	// 活动 2 的周期是 14 天（activity_period.xml），按同一算法算实例 ID。
	now := time.Now().Unix()
	_, _, period := activity.Window(14, now)
	instanceID := fmt.Sprintf("2-%d", period)
	totalKey := "activity:total:" + instanceID

	// 两个玩家各贡献 30 / 25：合计 55，够 50 的里程碑、不够 100 的。
	send := func(playerID int64, progress int32) {
		t.Helper()
		msg := pkgmq.ActivityProgressMsg{
			InstanceID: instanceID,
			ActivityID: 2,
			PlayerID:   playerID,
			Progress:   progress,
		}
		env, err := pkgmq.NewEnvelope(
			fmt.Sprintf("progress:%s:%d:%d", instanceID, playerID, progress),
			pkgmq.TopicActivityProgress, instanceID, msg)
		require.NoError(t, err, "build envelope")
		err := producer.ProduceEnvelope(ctx, env)
		require.NoError(t, err, fmt.Sprintf("produce: %v", err))
	}
	send(1, 30)
	send(2, 25)

	// 等消费者把两条都累加上去。
	deadline := time.Now().Add(20 * time.Second)
	for {
		total, err := rdb.Get(ctx, totalKey).Int64()
		if err == nil && total >= 55 {
			break
		}
		require.False(t, time.Now().After(deadline), fmt.Sprintf("total not accumulated, got %d err=%v", total, err))
		time.Sleep(200 * time.Millisecond)
	}

	// 重投同一条：绝对值上报的差值应为 0，总量不变。
	send(1, 30)
	time.Sleep(800 * time.Millisecond)
	total, _ := rdb.Get(ctx, totalKey).Int64()
	require.EqualValues(t, 55, total, fmt.Sprintf("duplicate progress must not accumulate: total=%d", total))

	// 第一次 Tick：达阈值 → 生成任务；未达的里程碑不生成。
	err := svc.Tick(ctx)
	require.NoError(t, err, fmt.Sprintf("tick: %v", err))
	var tasks int
	if err := database.QueryRow(
		`SELECT COUNT(*) FROM activity_grant_task WHERE instance_id = ?`, instanceID).Scan(&tasks); err != nil {
		t.Fatalf("count tasks: %v", err)
	}
	require.EqualValues(t, 2, tasks)

	// 第二次 Tick：发放并标记完成。
	err := svc.Tick(ctx)
	require.NoError(t, err, fmt.Sprintf("tick 2: %v", err))
	var pending int
	if err := database.QueryRow(
		`SELECT COUNT(*) FROM activity_grant_task WHERE instance_id = ? AND status = ?`,
		instanceID, 0).Scan(&pending); err != nil {
		t.Fatalf("count pending: %v", err)
	}
	require.EqualValues(t, 0, pending)

	// 第三次 Tick：不得重复发（任务已全部完成，不会新建）。
	var before int
	_ = database.QueryRow(`SELECT COUNT(*) FROM activity_grant_task`).Scan(&before)
	err := svc.Tick(ctx)
	require.NoError(t, err, fmt.Sprintf("tick 3: %v", err))
	var after int
	_ = database.QueryRow(`SELECT COUNT(*) FROM activity_grant_task`).Scan(&after)
	require.EqualValues(t, before, after)

	// 里程碑 2（阈值 100）未达成，不应有对应任务。
	var high int
	if err := database.QueryRow(
		`SELECT COUNT(*) FROM activity_grant_task WHERE instance_id = ? AND milestone_id = 2`,
		instanceID).Scan(&high); err != nil {
		t.Fatalf("count milestone2: %v", err)
	}
	require.EqualValues(t, 0, high)

	_ = model.ActivityGrantTask{}
}
