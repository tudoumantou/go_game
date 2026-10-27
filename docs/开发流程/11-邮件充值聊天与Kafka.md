# 11 邮件充值聊天与Kafka

[上一章](10-增量存档与数据一致性.md) · [课程总览](00-总览与阅读顺序.md)

## 这一章做出什么

串起邮件、充值和跨服私聊，理解异步消息如何驱动玩家，而不是把 Kafka 当作自动事务。

## 前置条件与本章文件

已有模块与保存机制；需要真实 Kafka 时使用开发环境或 testcontainers。

- [pkg/mq/message.go](../../pkg/mq/message.go)：业务信封。
- [pkg/mq/topics.go](../../pkg/mq/topics.go)：topic 清单。
- [pkg/mq/kafka.go](../../pkg/mq/kafka.go)：生产、消费、重试与 offset。
- [pkg/cache/idem.go](../../pkg/cache/idem.go)：Redis SETNX 去重。
- [internal/game/mq/recharge_consumer.go](../../internal/game/mq/recharge_consumer.go)：奖励投递。
- [internal/game/module/mail/mail.go](../../internal/game/module/mail/mail.go)：邮件领取。
- [internal/login/service/recharge_service.go](../../internal/login/service/recharge_service.go)：订单与回调。
- [internal/game/module/chat/chat.go](../../internal/game/module/chat/chat.go)：私聊与历史。

## 按顺序实现

先用“打印一条业务信封”理解 biz_id、topic、key 和 payload。biz_id 应来自稳定业务事实，比如订单号，而不是每次重试重新生成随机值。partition key 决定相关事件怎样进入同一分区。

第二步实现 Producer 与 Consumer，Fetch → 解码 → handler → 有界重试 → 持久死信或成功 → commit。不要写成先 commit 后做业务，也不要把失败消息没有保存却当作已经进入死信。

第三步接充值：下单和支付回调属于 Login，投递奖励在 Game，完成后有发货确认回传。HTTP 回调、Kafka 投递、Actor 修改和长期存档处在不同系统，必须明确每一步的幂等事实。

第四步接邮件：列表只查询当前角色邮件；领取锁定邮件行、校验 owner、更新 claimed/status，再给内存背包加物品。当前 5002 能拦已领取的重复请求，但不能自动恢复“状态已领、奖励未落库”的崩溃窗口。

第五步接聊天：本服在线推送、跨服 Kafka、离线暂存和历史落库分别读对应路径；不额外增加一个参考项目式 chatserver。聊天的实时推送失败可以通过历史补拉恢复，经济奖励则不能简单照用这种丢失容忍度。

```text
订单回调 → 订单状态 → Kafka RechargeOrder
 → 消费去重 → 在线 Actor / 离线暂存 → 发奖
 → delivered 事件 → Login 确认订单
```

这是一条需要补偿与确认的流程图，不是把不同提交点连成箭头后就获得原子事务。

## 核心源码与注释阅读

源码节选：[pkg/cache/idem.go](../../pkg/cache/idem.go)。这是现有包中的片段，依赖同文件类型，不是可单独运行的完整程序。

```go
func (d *RedisDeduper) FirstTime(ctx context.Context, topic, bizID string, ttl time.Duration) (bool, error) {
	if bizID == "" {
		return false, fmt.Errorf("biz_id is empty")
	}
	if ttl <= 0 {
		ttl = mq.DefaultIdemTTL
	}
	ok, err := d.client.SetNX(ctx, mq.IdemKey(topic, bizID), 1, ttl).Result()
	if err != nil {
		return false, fmt.Errorf("idem setnx %s: %w", bizID, err)
	}
	return ok, nil
}
```

源码节选：[internal/game/module/mail/mail.go](../../internal/game/module/mail/mail.go)。这是现有包中的片段，依赖同文件类型，不是可单独运行的完整程序。

```go
func (r *Repo) TakeAttachments(ctx context.Context, playerID, mailID int64) ([]Attachment, error) {
	var result []Attachment
	err := r.db.Tx(ctx, func(tx *sql.Tx) error {
		var ownerID int64
		var status int32
		err := tx.QueryRowContext(ctx,
			`SELECT player_id, status FROM mail WHERE id = ? FOR UPDATE`, mailID).Scan(&ownerID, &status)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrMailNotExist
		}
		if err != nil {
			return err
		}
		if ownerID != playerID {
			return ErrMailNotExist
		}
		if status == 2 {
			return ErrAlreadyTaken
		}

		rows, err := tx.QueryContext(ctx,
			`SELECT item_id, count FROM mail_attachment WHERE mail_id = ? AND claimed = 0 FOR UPDATE`, mailID)
		if err != nil {
			return err
		}
		for rows.Next() {
			var a Attachment
			if err := rows.Scan(&a.ItemID, &a.Count); err != nil {
				_ = rows.Close()
				return err
			}
			result = append(result, a)
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return err
		}
		_ = rows.Close()

		if len(result) == 0 {
			return ErrNoAttachment
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE mail_attachment SET claimed = 1 WHERE mail_id = ? AND claimed = 0`, mailID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE mail SET status = 2 WHERE id = ?`, mailID); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}
```

## 逐段解释与知识点

SETNX 抢到标记只是“尝试取得处理权”。如果进程在副作用前死亡，标记会残留；如果副作用后删除标记/重投，则可能重复。业务唯一事实与可恢复状态比一条短 TTL 标记更稳妥。

TakeAttachments 用同一事务和 FOR UPDATE 保护邮件领取竞争，但事务结束后 AddItem 在内存发生，数据库事务已不覆盖它。新增经济功能要先画提交点，不把 Actor 串行与数据库原子性混同。

当前 Consumer 达到重试次数后尝试 DeadLetter，但忽略其错误并继续 commit，这是需要补确认的可靠性边界。Kafka 自身的日志持久性不能替代业务代码的确认策略。

## 怎么验证与定位错误

纯组件：`go test ./pkg/mq ./pkg/cache`。集成参考：`go test -tags=integration ./test/integration/ -run TestKafkaRechargeFlow -v -timeout 10m`；本机连接 Compose Kafka 用 127.0.0.1:9093，容器用 kafka:9092。

验证重复 biz_id、handler 失败后重试、死信写失败、提交前后崩溃窗口、不同玩家邮件越权和离线私聊补拉。不要把有 Kafka 测试等于已经验证所有支付边界。

## 独立练习与通过标准

用同一个 biz_id 连续投递两次，记录数据库、Redis、Actor 和 offset 各自变化。通过标准：能指出现有方案能防哪些重复，以及无法自动恢复的窗口。

下一章：[第 12 章 任务装备技能与社交玩法](12-任务装备技能与社交玩法.md)。
