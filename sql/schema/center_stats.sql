-- sql/schema/center_stats.sql
-- 中心服统计表（分表键：group_id % 10）：排行榜历史快照 + 分组功能统计
CREATE TABLE `rank_history` (
  `id`          BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增ID',
  `group_id`    INT   NOT NULL COMMENT '分组ID',
  `rank_type`   TINYINT NOT NULL COMMENT '排行榜类型(见 rank.xml)',
  `season`      INT   NOT NULL COMMENT '赛季',
  `player_id`   BIGINT UNSIGNED NOT NULL COMMENT '玩家ID',
  `score`       BIGINT NOT NULL DEFAULT 0 COMMENT '分数',
  `rank_no`     INT   NOT NULL DEFAULT 0 COMMENT '名次',
  `snapshot_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '快照时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_group_type_season_player` (`group_id`, `rank_type`, `season`, `player_id`),
  KEY `idx_rank` (`group_id`, `rank_type`, `season`, `rank_no`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='排行榜历史快照表';

CREATE TABLE `feature_stats` (
  `id`           BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增ID',
  `group_id`     INT   NOT NULL COMMENT '分组ID',
  `feature`      VARCHAR(64) NOT NULL COMMENT '功能标识(如 online/dau/retain)',
  `window_start` TIMESTAMP NOT NULL COMMENT '统计窗口起',
  `window_end`   TIMESTAMP NOT NULL COMMENT '统计窗口止',
  `value`        BIGINT NOT NULL DEFAULT 0 COMMENT '统计值',
  `updated_at`   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_feature_window` (`group_id`, `feature`, `window_start`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='分组功能统计表';
