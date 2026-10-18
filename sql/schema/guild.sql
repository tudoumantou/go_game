-- sql/schema/guild.sql
-- 公会表 + 成员表（游戏服，分表键：guild_id % 64；低频聚合数据，直连 Model CRUD）
CREATE TABLE `guild` (
  `id`           BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '公会ID',
  `name`         VARCHAR(64)  NOT NULL COMMENT '公会名(全局唯一)',
  `leader_id`    BIGINT UNSIGNED NOT NULL COMMENT '会长玩家ID',
  `level`        TINYINT NOT NULL DEFAULT 1 COMMENT '公会等级(见 guild_level.xml)',
  `exp`          INT     NOT NULL DEFAULT 0 COMMENT '公会经验',
  `notice`       VARCHAR(255) NOT NULL DEFAULT '' COMMENT '公会公告',
  `member_count` INT     NOT NULL DEFAULT 1 COMMENT '成员数(冗余)',
  `created_at`   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at`   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_name` (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='公会表';

CREATE TABLE `guild_member` (
  `id`          BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '成员记录ID',
  `guild_id`    BIGINT UNSIGNED NOT NULL COMMENT '公会ID',
  `player_id`   BIGINT UNSIGNED NOT NULL COMMENT '玩家ID',
  `title`       TINYINT NOT NULL DEFAULT 0 COMMENT '0成员 1会长 2官员',
  `contributed` INT     NOT NULL DEFAULT 0 COMMENT '累计贡献',
  `joined_at`   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '加入时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_guild_player` (`guild_id`, `player_id`),
  KEY `idx_player` (`player_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='公会成员表';
