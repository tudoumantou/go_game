-- sql/schema/player_equip.sql
-- 玩家装备表（游戏服，分表键：player_id % 512；Redis e:player_equip:{id}）
CREATE TABLE `player_equip` (
  `id`             BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '装备实例ID',
  `player_id`      BIGINT UNSIGNED NOT NULL COMMENT '归属玩家',
  `equip_id`       INT     NOT NULL COMMENT '装备配置ID(见 equip.xml)',
  `enhance_level`  TINYINT NOT NULL DEFAULT 0 COMMENT '强化等级',
  `quality`        TINYINT NOT NULL DEFAULT 0 COMMENT '品质',
  `created_at`     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at`     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_player` (`player_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='玩家装备表';
