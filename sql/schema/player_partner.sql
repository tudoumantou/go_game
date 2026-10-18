-- sql/schema/player_partner.sql
-- 玩家伙伴表（游戏服，分表键：player_id % 512；Redis e:player_partner:{id}）
CREATE TABLE `player_partner` (
  `id`          BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '伙伴实例ID',
  `player_id`   BIGINT UNSIGNED NOT NULL COMMENT '归属玩家',
  `partner_id`  INT     NOT NULL COMMENT '伙伴配置ID(见 partner.xml)',
  `star`        TINYINT NOT NULL DEFAULT 1 COMMENT '星级',
  `level`       TINYINT NOT NULL DEFAULT 1 COMMENT '等级',
  `exp`         INT     NOT NULL DEFAULT 0 COMMENT '经验',
  `created_at`  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at`  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_player` (`player_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='玩家伙伴表';
