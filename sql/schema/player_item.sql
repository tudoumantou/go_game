-- sql/schema/player_item.sql
-- 玩家道具表（游戏服，分表键：player_id % 512；属主索引 idx:player_item:{pid}）
CREATE TABLE `player_item` (
  `id`         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '道具实例ID',
  `player_id`  BIGINT UNSIGNED NOT NULL COMMENT '归属玩家',
  `item_id`    INT            NOT NULL COMMENT '道具配置ID(见 item.xml)',
  `count`      INT            NOT NULL DEFAULT 1 COMMENT '堆叠数量',
  `expire_at`  DATETIME      NULL DEFAULT NULL COMMENT '过期时间( null=永久 )',
  `created_at` TIMESTAMP      NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  -- 堆叠语义：同一玩家同一道具只有一行。没有这个唯一键，多行 UPSERT 无法生效
  -- （ON DUPLICATE KEY 只对主键/唯一键生效，会退化成插入重复行）
  UNIQUE KEY `uk_player_item` (`player_id`, `item_id`),
  KEY `idx_player` (`player_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='玩家道具表';
