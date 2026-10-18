-- sql/schema/copy_record.sql
-- 副本记录表（游戏服，分表键：player_id % 512；Redis e:copy:instance:{id} 为进行中实例）
CREATE TABLE `copy_record` (
  `id`         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '记录ID',
  `player_id`  BIGINT UNSIGNED NOT NULL COMMENT '玩家ID',
  `copy_id`    INT     NOT NULL COMMENT '副本配置ID(见 copy.xml)',
  `star`       TINYINT NOT NULL DEFAULT 0 COMMENT '历史最佳星级',
  `pass_count` INT     NOT NULL DEFAULT 0 COMMENT '累计通关次数',
  `first_pass` TINYINT NOT NULL DEFAULT 0 COMMENT '是否已首通(领过首通奖)',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_player_copy` (`player_id`, `copy_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='副本记录表';
