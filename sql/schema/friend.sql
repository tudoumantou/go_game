-- sql/schema/friend.sql
-- 好友表 + 申请表（游戏服，分表键：player_id % 512；Redis idx:friend:{pid}）
CREATE TABLE `friend` (
  `id`         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '关系ID',
  `player_id`  BIGINT UNSIGNED NOT NULL COMMENT '归属玩家',
  `friend_id`  BIGINT UNSIGNED NOT NULL COMMENT '好友玩家ID',
  `intimacy`   INT NOT NULL DEFAULT 0 COMMENT '亲密度',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_pair` (`player_id`, `friend_id`),
  KEY `idx_friend` (`player_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='好友表';

CREATE TABLE `friend_apply` (
  `id`         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '申请ID',
  `from_id`    BIGINT UNSIGNED NOT NULL COMMENT '申请方玩家ID',
  `to_id`      BIGINT UNSIGNED NOT NULL COMMENT '接收方玩家ID',
  `status`     TINYINT NOT NULL DEFAULT 0 COMMENT '0待处理 1已同意 2已拒绝',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_to_status` (`to_id`, `status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='好友申请表';
