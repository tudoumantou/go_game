-- sql/schema/chat_record.sql
-- 聊天记录表（游戏服，分表键：player_id % 512；双端各存一份，见 17.3）
CREATE TABLE `chat_record` (
  `id`         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增ID',
  `owner_id`   BIGINT UNSIGNED NOT NULL COMMENT '归属玩家(历史查询方)',
  `peer_id`    BIGINT UNSIGNED NOT NULL COMMENT '对方玩家',
  `msg_id`     BIGINT UNSIGNED NOT NULL COMMENT '全局消息ID(幂等键)',
  `direction`  TINYINT      NOT NULL COMMENT '0 发出 1 收到',
  `channel`    TINYINT      NOT NULL DEFAULT 1 COMMENT '1 私聊',
  `content`    VARBINARY(1024) NOT NULL COMMENT '消息内容(业务序列化)',
  `created_at` TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_owner_msg` (`owner_id`, `msg_id`),   -- 幂等：双端落库不重复
  KEY `idx_owner_peer` (`owner_id`, `peer_id`, `id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='聊天记录表(每服从本服玩家视角各存一份)';
