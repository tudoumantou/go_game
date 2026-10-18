-- sql/schema/player_quest.sql
-- 玩家任务表（游戏服，分表键：player_id % 512；Redis e:player_quest:{id}）
CREATE TABLE `player_quest` (
  `id`         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '任务实例ID',
  `player_id`  BIGINT UNSIGNED NOT NULL COMMENT '归属玩家',
  `quest_id`   INT     NOT NULL COMMENT '任务配置ID(见 quest.xml)',
  `status`     TINYINT NOT NULL DEFAULT 0 COMMENT '0进行中 1已完成 2已领奖',
  `progress`   INT     NOT NULL DEFAULT 0 COMMENT '当前进度',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_player_quest` (`player_id`, `quest_id`),
  KEY `idx_player` (`player_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='玩家任务表';
