-- sql/schema/player_skill.sql
-- 玩家技能表（游戏服，分表键：player_id % 512；Redis e:player_skill:{id}）
CREATE TABLE `player_skill` (
  `id`         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '技能实例ID',
  `player_id`  BIGINT UNSIGNED NOT NULL COMMENT '归属玩家',
  `skill_id`   INT     NOT NULL COMMENT '技能配置ID(见 skill.xml)',
  `level`      TINYINT NOT NULL DEFAULT 1 COMMENT '技能等级',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_player_skill` (`player_id`, `skill_id`),
  KEY `idx_player` (`player_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='玩家技能表';
