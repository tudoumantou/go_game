// store.go —— chat_record 表的访问层。
//
// uk_owner_msg 唯一键保证同一 (owner_id, msg_id) 只有一行：写入路径可能重投
// （Kafka 重试 / 双端重复发送），幂等靠唯一键而不是靠调用方小心。
package chat

import (
	"context"
	"fmt"

	"game_study/internal/game/module"
	"game_study/pkg/db"
	"game_study/pkg/model"
)

// Repo 聊天记录仓储。
//
// MySQL 直写模块（与好友/公会同类的「不过 Persister」设计）：
// 历史是流水数据、量随聊天天数增长，放玩家快照 JSON 列会把快照撑爆，
// 因此独立成表直写。db 可为 nil（无 DB 环境），调用方需判空降级。
type Repo struct {
	*module.Shared
}

// NewRepo 创建仓储。
func NewRepo(database *db.DB) *Repo {
	return &Repo{Shared: module.NewShared(database, "chat")}
}

// Insert 落库一条聊天记录（uk_owner_msg 幂等，重复插入忽略）。
//
// 幂等实现从 `ON DUPLICATE KEY UPDATE id = id` 换成「普通 INSERT + 1062 视为
// 已存在」：两者等价（chat_record 只有 uk_owner_msg 会冲突），后者不依赖
// MySQL 方言，冲突判定走 db.IsDuplicate 的类型断言而不是错误文本。
// 写入路径可能重投（Kafka 消费重试 / 双端重复发送），依赖它保证同一
// (owner_id, msg_id) 永远只有一行。
func (r *Repo) Insert(ctx context.Context, ownerID, peerID, msgID int64, direction int32, channel int32, content string) error {
	_, err := model.InsertChatRecord(ctx, r.SQL(), &model.ChatRecord{
		OwnerId:   ownerID,
		PeerId:    peerID,
		MsgId:     msgID,
		Direction: direction,
		Channel:   channel,
		Content:   content,
	})
	if err != nil && !db.IsDuplicate(err) {
		return fmt.Errorf("insert chat_record: %w", err)
	}
	return nil
}

// ChatLine 聊天记录行。

// ListRecent 读取与某好友的最近 N 条记录。
//
// limit 钳制到 [1,100]，越界/非法值回落 50：防客户端传 0 或负数导致
// 语义异常，也防超大页把 DB 和回包一起拖垮。
// ORDER BY id DESC LIMIT ?：利用主键索引倒序取最近 N 条（不回表排序）。
func (r *Repo) ListRecent(ctx context.Context, ownerID, peerID int64, limit int) ([]ChatLine, error) {
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	const q = `SELECT msg_id, direction, content
	           FROM chat_record
	           WHERE owner_id = ? AND peer_id = ?
	           ORDER BY id DESC LIMIT ?`
	rows, err := r.SQL().QueryContext(ctx, q, ownerID, peerID, limit)
	if err != nil {
		return nil, err
	}
	// database/sql 契约：rows 必须 Close，否则连接一直占着直到 GC 兜底；
	// 闭包吞掉 Close 错误是惯用法——查询语义已完成，关闭失败无补救意义
	defer func() { _ = rows.Close() }()

	var out []ChatLine
	for rows.Next() {
		var c ChatLine
		if err := rows.Scan(&c.MsgID, &c.Direction, &c.Content); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	// rows.Err() 不能省：Next() 中途遇错会直接结束循环，
	// 不查 Err 会把「查了一半」当成「查完了」，静默丢数据
	return out, rows.Err()
}

// chatRepo 是模块对历史仓储的全部依赖（接口隔离）。
//
// 抽成接口只为一件事：让「频道校验」「三路投递」「方向还原」这些纯编排逻辑
// 能在内存里测，不必为一条断言拉起 MySQL。*Repo 隐式实现它，装配层不变。
