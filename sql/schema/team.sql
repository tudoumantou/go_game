-- sql/schema/team.sql
-- 组队表 + 成员表（游戏服，分表键：无 —— 短期数据，文档注明可只存 Redis，
-- 落库仅作崩溃恢复兜底；P0 建表备用）
CREATE TABLE `team` (
  `id`         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '队伍ID',
  `leader_id`  BIGINT UNSIGNED NOT NULL COMMENT '队长玩家ID',
  `copy_id`    INT  NOT NULL DEFAULT 0 COMMENT '目标副本(见 copy.xml)',
  `status`     TINYINT NOT NULL DEFAULT 0 COMMENT '0招募中 1副本中 2已解散',
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='组队表';

CREATE TABLE `team_member` (
  `id`         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '成员记录ID',
  `team_id`    BIGINT UNSIGNED NOT NULL COMMENT '队伍ID',
  `player_id`  BIGINT UNSIGNED NOT NULL COMMENT '玩家ID',
  `ready`      TINYINT NOT NULL DEFAULT 0 COMMENT '0未准备 1已准备',
  `joined_at`  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '加入时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_team_player` (`team_id`, `player_id`),
  KEY `idx_player` (`player_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='队伍成员表';
