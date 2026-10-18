-- sql/schema/player.sql
-- 玩家表（游戏服，分表键：id % 512，依据 12.3 表清单）
CREATE TABLE `player` (
  `id`         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '玩家ID',
  `account_id` BIGINT UNSIGNED NOT NULL COMMENT '账号ID',
  `name`       VARCHAR(32)  NOT NULL DEFAULT '' COMMENT '昵称',
  `level`      INT          NOT NULL DEFAULT 1  COMMENT '等级',
  `exp`        BIGINT       NOT NULL DEFAULT 0  COMMENT '经验',
  `coin`       BIGINT       NOT NULL DEFAULT 0  COMMENT '金币',
  `scene_id`   INT          NOT NULL DEFAULT 0  COMMENT '所在场景',
  `created_at` TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_account_id` (`account_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='玩家表';
